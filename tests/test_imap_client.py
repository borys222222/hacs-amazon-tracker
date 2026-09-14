"""Tests for IMAP client."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncio

import pytest

from custom_components.amazon_tracker.imap_client import ImapClient


class TestImapClient:
    """Test ImapClient class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.client = ImapClient(
            server="imap.example.com",
            port=993,
            email_addr="user@example.com",
            password="password123",
            ssl=True,
            folder="INBOX",
            domains=["amazon.de"],
        )

    def test_initialization(self):
        """Test client initialization."""
        assert self.client._server == "imap.example.com"
        assert self.client._port == 993
        assert self.client._email == "user@example.com"
        assert self.client._ssl is True
        assert self.client._folder == "INBOX"
        assert self.client._client is None
        assert self.client._running is False

    def test_initialization_non_ssl(self):
        """Test client initialization without SSL."""
        client = ImapClient(
            server="imap.example.com",
            port=143,
            email_addr="user@example.com",
            password="password123",
            ssl=False,
        )
        assert client._ssl is False
        assert client._port == 143

    def test_callback_is_stored(self):
        """Test that the callback is stored."""
        callback = MagicMock()
        client = ImapClient(
            server="imap.example.com",
            port=993,
            email_addr="user@example.com",
            password="password123",
            on_new_packages=callback,
        )
        assert client._on_new_packages is callback

    def test_default_domains(self):
        """Test default domains is empty list."""
        client = ImapClient(
            server="imap.example.com",
            port=993,
            email_addr="user@example.com",
            password="password123",
        )
        assert client._domains == []

    @pytest.mark.asyncio
    async def test_connect_ssl(self):
        """Test SSL connection."""
        mock_imap = AsyncMock()
        mock_imap.wait_hello_from_server = AsyncMock()
        mock_imap.login = AsyncMock(return_value=MagicMock(result="OK"))
        mock_imap.select = AsyncMock(return_value=MagicMock(result="OK"))

        with patch("custom_components.amazon_tracker.imap_client.aioimaplib") as mock_lib:
            mock_lib.IMAP4_SSL = MagicMock(return_value=mock_imap)
            await self.client.connect()

        assert self.client._client is not None
        mock_imap.login.assert_called_once_with("user@example.com", "password123")
        mock_imap.select.assert_called_once_with("INBOX")

    @pytest.mark.asyncio
    async def test_connect_login_failure(self):
        """Test connection with login failure."""
        mock_imap = AsyncMock()
        mock_imap.wait_hello_from_server = AsyncMock()
        mock_imap.login = AsyncMock(return_value=MagicMock(result="NO"))

        with patch("custom_components.amazon_tracker.imap_client.aioimaplib") as mock_lib:
            mock_lib.IMAP4_SSL = MagicMock(return_value=mock_imap)
            with pytest.raises(ConnectionError):
                await self.client.connect()

    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test disconnection."""
        mock_imap = AsyncMock()
        mock_imap.logout = AsyncMock()
        mock_imap.has_pending_idle = MagicMock(return_value=False)
        self.client._client = mock_imap

        await self.client.disconnect()

        assert self.client._client is None
        assert self.client._running is False
        mock_imap.logout.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        """Test disconnect when already disconnected."""
        await self.client.disconnect()
        # Should not raise

    @pytest.mark.asyncio
    async def test_fetch_existing_emails_not_connected(self):
        """Test fetch_existing_emails when not connected."""
        result = await self.client.fetch_existing_emails()
        assert result == []

    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test static test_connection method."""
        mock_imap = AsyncMock()
        mock_imap.wait_hello_from_server = AsyncMock()
        mock_imap.login = AsyncMock(return_value=MagicMock(result="OK"))
        mock_imap.select = AsyncMock(return_value=MagicMock(result="OK"))
        mock_imap.logout = AsyncMock()

        with patch("custom_components.amazon_tracker.imap_client.aioimaplib") as mock_lib:
            mock_lib.IMAP4_SSL = MagicMock(return_value=mock_imap)
            result = await ImapClient.test_connection(
                server="imap.example.com",
                port=993,
                email_addr="user@example.com",
                password="password123",
            )

        assert result is True

    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test static test_connection with auth failure."""
        mock_imap = AsyncMock()
        mock_imap.wait_hello_from_server = AsyncMock()
        mock_imap.login = AsyncMock(return_value=MagicMock(result="NO"))
        mock_imap.logout = AsyncMock()

        with patch("custom_components.amazon_tracker.imap_client.aioimaplib") as mock_lib:
            mock_lib.IMAP4_SSL = MagicMock(return_value=mock_imap)
            result = await ImapClient.test_connection(
                server="imap.example.com",
                port=993,
                email_addr="user@example.com",
                password="wrong",
            )

        assert result is False

    @pytest.mark.asyncio
    async def test_test_connection_exception(self):
        """Test static test_connection with connection exception."""
        with patch("custom_components.amazon_tracker.imap_client.aioimaplib") as mock_lib:
            mock_lib.IMAP4_SSL = MagicMock(side_effect=Exception("Connection refused"))
            result = await ImapClient.test_connection(
                server="imap.example.com",
                port=993,
                email_addr="user@example.com",
                password="password123",
            )

        assert result is False


def _response(result="OK", lines=None):
    return MagicMock(result=result, lines=lines if lines is not None else [])


class TestIdleAioimaplib2:
    """aioimaplib 2.x semantics: idle_done() is synchronous, commands must leave IDLE first."""

    def setup_method(self):
        self.client = ImapClient(
            server="imap.example.com",
            port=993,
            email_addr="user@example.com",
            password="secret",
            domains=["amazon.de"],
        )

    @pytest.mark.asyncio
    async def test_idle_cycle_does_not_await_idle_done(self):
        loop = asyncio.get_running_loop()
        idle_future = loop.create_future()
        idle_future.set_result(None)
        mock_imap = AsyncMock()
        mock_imap.idle_start = AsyncMock(return_value=idle_future)
        async def push():
            await asyncio.sleep(0.01)  # a real IDLE suspends; without this the loop never yields
            return [b"stop_wait_server_push"]

        mock_imap.wait_server_push = push
        mock_imap.idle_done = MagicMock(return_value=None)
        pending = {"v": True}
        mock_imap.has_pending_idle = MagicMock(side_effect=lambda: pending["v"])
        self.client._client = mock_imap
        self.client._running = True

        async def stop_after_first_cycle():
            await asyncio.sleep(0.05)
            self.client._running = False
            pending["v"] = False

        stopper = asyncio.create_task(stop_after_first_cycle())
        await asyncio.wait_for(self.client._idle_loop(), 2)
        await stopper

        mock_imap.idle_start.assert_awaited()
        mock_imap.idle_done.assert_called()
        # a crash would have reset the client to None and gone into reconnect
        assert self.client._client is mock_imap

    @pytest.mark.asyncio
    async def test_search_leaves_idle_first(self):
        loop = asyncio.get_running_loop()
        idle_future = loop.create_future()
        idle_future.set_result(None)
        order = []
        mock_imap = AsyncMock()
        mock_imap.has_pending_idle = MagicMock(return_value=True)
        mock_imap.idle_done = MagicMock(side_effect=lambda: order.append("idle_done"))

        async def search(*_a, **_k):
            order.append("search")
            return _response("OK", [b""])

        mock_imap.search = search
        self.client._client = mock_imap
        self.client._idle_future = idle_future

        result = await self.client.fetch_existing_emails(since_days=1)

        assert result == []
        assert order == ["idle_done", "search"]
        assert self.client._idle_future is None
