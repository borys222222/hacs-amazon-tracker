"""IMAP client for receiving Amazon notification emails."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, timedelta
import logging
from typing import Any

import aioimaplib

from .const import DEFAULT_IMAP_FOLDER
from .email_parser import AmazonEmailParser, build_imap_search_query

_LOGGER = logging.getLogger(__name__)

# IMAP IDLE timeout - RFC recommends <30 minutes
IDLE_TIMEOUT = 29 * 60  # 29 minutes in seconds

# How long to wait for the server to acknowledge DONE after idle_done()
IDLE_STOP_TIMEOUT = 10  # seconds

# Reconnect backoff
INITIAL_BACKOFF = 30  # seconds
MAX_BACKOFF = 600  # 10 minutes


class ImapClient:
    """IMAP client with IDLE support for push notifications.

    aioimaplib 2.x semantics (the version Home Assistant ships): ``idle_start()`` is a
    coroutine returning a future that completes when the server acknowledges DONE,
    ``wait_server_push()`` is a coroutine, and ``idle_done()`` is a plain synchronous call.
    Any other command sent while an IDLE is pending is not answered by the server until
    DONE, so every SEARCH/FETCH first leaves IDLE and takes ``_command_lock``; the IDLE
    loop holds the same lock for the duration of one IDLE cycle.
    """

    def __init__(
        self,
        server: str,
        port: int,
        email_addr: str,
        password: str,
        ssl: bool = True,
        folder: str = DEFAULT_IMAP_FOLDER,
        domains: list[str] | None = None,
        on_new_packages: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        """Initialize the IMAP client."""
        self._server = server
        self._port = port
        self._email = email_addr
        self._password = password
        self._ssl = ssl
        self._folder = folder
        self._parser = AmazonEmailParser(domains or [])
        self._domains = domains or []
        self._on_new_packages = on_new_packages

        self._client: aioimaplib.IMAP4_SSL | aioimaplib.IMAP4 | None = None
        self._idle_task: asyncio.Task | None = None
        self._idle_future: asyncio.Future | None = None
        self._command_lock = asyncio.Lock()
        self._running = False
        self._backoff = INITIAL_BACKOFF

    @property
    def is_connected(self) -> bool:
        """Return True if the IMAP client has an active connection."""
        return self._client is not None

    async def connect(self) -> None:
        """Connect to the IMAP server."""
        try:
            if self._ssl:
                self._client = aioimaplib.IMAP4_SSL(
                    host=self._server,
                    port=self._port,
                )
            else:
                self._client = aioimaplib.IMAP4(
                    host=self._server,
                    port=self._port,
                )

            await self._client.wait_hello_from_server()
            response = await self._client.login(self._email, self._password)

            if response.result != "OK":
                raise ConnectionError(f"Login failed: {response.result}")

            response = await self._client.select(self._folder)
            if response.result != "OK":
                raise ConnectionError(f"Failed to select folder {self._folder}")

            self._backoff = INITIAL_BACKOFF
            _LOGGER.info("Connected to IMAP server %s", self._server)

        except Exception as err:
            _LOGGER.error("Failed to connect to IMAP server: %s", err)
            self._client = None
            raise

    async def disconnect(self) -> None:
        """Disconnect from the IMAP server."""
        self._running = False

        if self._idle_task and not self._idle_task.done():
            self._idle_task.cancel()
            try:
                await self._idle_task
            except asyncio.CancelledError:
                pass
            self._idle_task = None

        if self._client:
            try:
                await self._stop_idle()
                await self._client.logout()
            except Exception:
                pass
            self._client = None

        _LOGGER.debug("Disconnected from IMAP server")

    async def start_idle(self) -> None:
        """Start the IMAP IDLE loop as a background task."""
        self._running = True
        self._idle_task = asyncio.create_task(self._idle_loop())

    async def _stop_idle(self) -> None:
        """Leave IDLE (if pending) and wait for the server to acknowledge DONE.

        Safe to call from any task and when no IDLE is pending.
        """
        client = self._client
        if client is not None and client.has_pending_idle():
            client.idle_done()
        future = self._idle_future
        if future is not None:
            self._idle_future = None
            try:
                await asyncio.wait_for(future, IDLE_STOP_TIMEOUT)
            except Exception as err:  # noqa: BLE001 - never let a stale IDLE block a command
                _LOGGER.debug("IDLE did not stop cleanly: %s", err)

    @staticmethod
    def _has_new_mail(msg: Any) -> bool:
        """Return True if an IDLE push announced new messages."""
        if not isinstance(msg, (list, tuple)):
            return False
        for line in msg:
            if isinstance(line, bytes):
                line = line.decode("utf-8", errors="replace")
            if "EXISTS" in str(line) or "RECENT" in str(line):
                return True
        return False

    async def _idle_loop(self) -> None:
        """IMAP IDLE loop - waits for new emails."""
        while self._running:
            try:
                if not self._client:
                    await self._reconnect()
                    if not self._client:
                        continue

                async with self._command_lock:
                    if not self._client:
                        continue
                    self._idle_future = await self._client.idle_start(timeout=IDLE_TIMEOUT)
                    # Returns on a server push, on the IDLE timeout, or when another
                    # task called idle_done() to run a command.
                    msg = await self._client.wait_server_push()
                    await self._stop_idle()

                if self._has_new_mail(msg):
                    _LOGGER.debug("New email detected via IDLE")
                    await self._fetch_new_emails()

            except asyncio.CancelledError:
                _LOGGER.debug("IDLE loop cancelled")
                return
            except Exception as err:
                _LOGGER.warning("IDLE loop error: %s", err)
                self._client = None
                self._idle_future = None
                if self._running:
                    await self._reconnect()

    async def _search_and_parse(self, since_date: date, limit: int | None = None) -> list[dict[str, Any]]:
        """Run one SEARCH + FETCH pass outside IDLE; caller handles exceptions."""
        query = build_imap_search_query(self._domains, since_date)

        await self._stop_idle()
        async with self._command_lock:
            if not self._client:
                return []
            response = await self._client.search(query)
            if response.result != "OK":
                _LOGGER.warning("IMAP search failed: %s", response.result)
                return []

            message_ids = response.lines[0].split() if response.lines else []
            if limit is not None:
                message_ids = message_ids[-limit:]
            _LOGGER.debug("Found %d emails to scan", len(message_ids))

            packages = []
            for msg_id in message_ids:
                msg_id_str = msg_id if isinstance(msg_id, str) else msg_id.decode()
                fetch_response = await self._client.fetch(msg_id_str, "(RFC822)")
                if fetch_response.result == "OK":
                    for line in fetch_response.lines:
                        if isinstance(line, bytes) and len(line) > 100:
                            pkg = self._parser.parse_email(line)
                            if pkg:
                                packages.append(pkg)
            return packages

    async def _fetch_new_emails(self) -> None:
        """Fetch and parse new emails."""
        if not self._client:
            return

        try:
            # Only the last few messages (most recent)
            packages = await self._search_and_parse(date.today() - timedelta(days=1), limit=10)
            if packages and self._on_new_packages:
                self._on_new_packages(packages)

        except Exception as err:
            _LOGGER.error("Error fetching new emails: %r", err)

    async def fetch_existing_emails(self, since_days: int = 14) -> list[dict[str, Any]]:
        """Scan existing emails from the last N days."""
        if not self._client:
            return []

        try:
            packages = await self._search_and_parse(date.today() - timedelta(days=since_days))
            _LOGGER.info("Parsed %d packages from existing emails", len(packages))
            return packages

        except Exception as err:
            _LOGGER.error("Error fetching existing emails: %r", err)
            return []

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        if not self._running:
            return

        _LOGGER.info("Reconnecting to IMAP in %d seconds...", self._backoff)
        await asyncio.sleep(self._backoff)

        try:
            if self._client:
                try:
                    await self._client.logout()
                except Exception:
                    pass
                self._client = None
            self._idle_future = None

            await self.connect()
        except Exception as err:
            _LOGGER.warning("Reconnection failed: %s", err)
            self._backoff = min(self._backoff * 2, MAX_BACKOFF)

    @staticmethod
    async def test_connection(
        server: str,
        port: int,
        email_addr: str,
        password: str,
        ssl: bool = True,
        folder: str = DEFAULT_IMAP_FOLDER,
    ) -> bool:
        """Test IMAP connection for config flow validation."""
        client = None
        try:
            if ssl:
                client = aioimaplib.IMAP4_SSL(host=server, port=port)
            else:
                client = aioimaplib.IMAP4(host=server, port=port)

            await client.wait_hello_from_server()
            response = await client.login(email_addr, password)
            if response.result != "OK":
                return False

            response = await client.select(folder)
            if response.result != "OK":
                return False

            await client.logout()
            return True

        except Exception as err:
            _LOGGER.debug("Connection test failed: %s", err)
            return False
        finally:
            if client:
                try:
                    await client.logout()
                except Exception:
                    pass
