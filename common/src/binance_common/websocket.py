import aiohttp
import asyncio
import json
import logging

from pydantic import BaseModel
from typing import Callable, Optional, Dict, Generic, Union, TypeVar, Type
from urllib.parse import urlsplit, urlunsplit

from binance_common.configuration import (
    ConfigurationWebSocketAPI,
    ConfigurationWebSocketStreams,
)
from binance_common.constants import WebsocketMode
from binance_common.models import (
    WebsocketApiResponse,
    WebsocketApiOptions,
    WebsocketApiUserDataEndpoints,
)
from binance_common.signature import Signers
from binance_common.utils import (
    get_uuid,
    get_random_int,
    parse_proxies,
    parse_user_event,
    parse_ws_rate_limit_headers,
    redact_sensitive_info,
    ws_api_payload,
)

T = TypeVar("T", bound=BaseModel)


class StreamConnectionsMap:
    def __init__(self):
        self.stream_connections_map: dict[Union[str, int], WebSocketConnection] = {}


global_stream_connections = StreamConnectionsMap()
global_user_stream_connections = StreamConnectionsMap()


class WebSocketConnection:
    """Represents a WebSocket connection.

    Attributes:
        id (Union[str, int]): Unique identifier for the WebSocket connection.
        pending_request (dict): Dictionary to hold pending requests.
        stream_callback_map (dict): Map of stream names to their callback functions.
        response_types (dict): Map of stream names to their response types.
        ws_type (str): Type of WebSocket connection (API or Stream).
        websocket (aiohttp.ClientWebSocketResponse): The WebSocket response object.
        reconnect (bool): Flag indicating if the connection should reconnect.
        is_session_log_on (bool): Flag indicating if the session is logged on.
        session_logon_request (Optional[dict]): The session logon request data.
        url_path (Optional[str]): The URL path for the WebSocket connection.
    """

    def __init__(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        id: Union[str, int],
        ws_type: str,
        url_path: Optional[str] = None,
    ):
        self.id = id
        self.pending_request = {}
        self.stream_callback_map = {}
        self.response_types = {}
        self.ws_type = ws_type
        self.websocket = websocket
        self.reconnect = False
        self.close_initiated = False
        self.is_session_log_on = False
        self.session_logon_request = None
        self.url_path = url_path
        self.scheduled_reconnect_task = None


class WebSocketCommon:
    def __init__(
        self,
        configuration: Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams],
        user_data_endpoints: Optional[WebsocketApiUserDataEndpoints] = None,
    ):
        """Initialize the WebSocketCommon class.

        Args:
            configuration (Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams]): Configuration object.
        """

        self.connections = []
        self.reconnect_tasks = []
        self.round_robin_index = 0
        self.configuration = configuration
        self.session = None
        self.user_data_endpoints = user_data_endpoints
        self.close_initiated = False

    async def connect(
        self,
        url: str,
        configuration: Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams],
        ws_id: Optional[Union[str, int]] = None,
        url_paths: Optional[list[str]] = None,
    ):
        """Connect to the Binance WebSocket server.

        Args:
            url (str): WebSocket URL.
            configuration (Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams]): Configuration object.
            ws_id (Optional[Union[str, int]]): Optional WebSocket ID for the connection.
            url_paths (Optional[list[str]]): Optional list of URL paths for the connection.
        """

        try:
            if self.session is None:
                self.session = aiohttp.ClientSession()

            pool_size = (
                configuration.pool_size
                if configuration.mode == WebsocketMode.POOL
                else 1
            )
            urls = url_paths if url_paths else [None]

            for url_path in urls:
                for _ in range(pool_size):
                    await self.init_connection(
                        url, configuration, ws_id=ws_id, url_path=url_path
                    )
            return self
        except Exception as e:
            logging.error(f"WebSocket failed to connect: {e}")

    async def init_connection(
        self,
        url,
        configuration: Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams],
        url_path: Optional[str] = None,
        ws_id: Optional[Union[str, int]] = None,
    ):
        """Initialize a WebSocket connection.

        Args:
            url (str): WebSocket URL.
            configuration (Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams]): Configuration object.
            url_path (Optional[str]): Optional URL path for the connection.
            ws_id (Optional[Union[str, int]]): Optional WebSocket ID for the connection.
        """

        user_agent = configuration.user_agent

        proxy = (
            parse_proxies(self.configuration.proxy)[configuration.proxy["protocol"]]
            if configuration.proxy is not None
            else None
        )

        if configuration.time_unit:
            url = f"{url}?timeUnit={configuration.time_unit.value}"
        logging.info(f"Connecting to {url} with proxy {proxy}")

        if url_path:
            url = self._stream_url_for_route(url, url_path)

        if type(configuration).__name__ == "ConfigurationWebSocketAPI":
            websocket = await self.session.ws_connect(
                url,
                compress=configuration.compression,
                headers={"User-Agent": user_agent},
                max_msg_size=20 * 1024 * 1024,
                proxy=proxy,
                ssl=configuration.https_agent,
                timeout=configuration.timeout / 1000,
            )
            if ws_id:
                id = ws_id
            else:
                id = (
                    websocket._response.headers.get("x-mbx-uuid")
                    if websocket._response.headers.get("x-mbx-uuid")
                    else get_uuid()
                )
        else:
            websocket = await self.session.ws_connect(
                url,
                compress=configuration.compression,
                headers={"User-Agent": user_agent},
                max_msg_size=20 * 1024 * 1024,
                proxy=proxy,
                ssl=configuration.https_agent,
            )
            id = ws_id if ws_id else get_uuid()

        logging.info(f"Establishing Websocket connection with id {id} to: {url}")
        connection = WebSocketConnection(
            websocket, id, type(configuration).__name__, url_path
        )

        self.connections.append(connection)

        connection.scheduled_reconnect_task = asyncio.create_task(
            self.schedule_reconnect(connection, configuration, 23 * 3600)
        )
        asyncio.create_task(self.receive_loop(connection))

    @staticmethod
    def _stream_url_for_route(url: str, url_path: str) -> str:
        parsed = urlsplit(url)
        routes = {"public", "market", "private"}
        path_parts = [part for part in parsed.path.split("/") if part]
        if path_parts and path_parts[-1] in {"stream", "ws"}:
            path_parts = path_parts[:-1]
        if path_parts and path_parts[-1] in routes | {"ws"}:
            path_parts = path_parts[:-1]

        # TODO(binance-sdk-migration): normalize legacy futures /ws or /stream
        # inputs onto Binance's routed /public|/market|/private/stream entries.
        path_parts.extend([url_path, "stream"])
        path = "/" + "/".join(path_parts)
        return urlunsplit(
            (parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment)
        )

    def _emit_websocket_closed(self, connection: WebSocketConnection, reason: str):
        if (
            getattr(connection, "close_initiated", False)
            or connection.reconnect
            or getattr(self, "close_initiated", False)
        ):
            return

        callbacks = connection.stream_callback_map.get("WebSocketclosed") or []
        if not callbacks:
            return

        # TODO(binance-sdk-migration): expose routed close events so callers can
        # reconnect the exact /public, /market, or /private stream instead of
        # failing silently on Binance's post-2026-04-23 websocket split.
        payload = {
            "stream": "WebSocketclosed",
            "data": {
                "StopAsyncIteration": reason,
                "urlPath": connection.url_path,
                "connectionId": connection.id,
            },
        }
        for callback in callbacks:
            try:
                callback(payload)
            except Exception as exc:
                logging.error(
                    f"Error in WebSocketclosed callback for {connection.id}: {exc}",
                    exc_info=True,
                )

    async def receive_loop(self, connection: WebSocketConnection):
        """Continuously receive messages from the WebSocket server.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
        """
        close_reason = "websocket receive loop ended"
        try:
            async for msg in connection.websocket:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)

                    request_id = data.get("id")
                    if request_id and request_id in connection.pending_request:
                        future = connection.pending_request.pop(request_id)
                        if data.get("error") or (
                            data.get("code") is not None and data.get("msg")
                        ):
                            future.set_exception(
                                ValueError(
                                    "Error received from server: "
                                    f"{data.get('error') or data}"
                                )
                            )
                        else:
                            future.set_result(data)
                    elif (
                        data.get("event", {}).get("e") == "serverShutdown"
                        and not connection.reconnect
                        and connection.id not in self.reconnect_tasks
                        and not connection.close_initiated
                        and not getattr(self, "close_initiated", False)
                    ):
                        logging.warning(
                            "Server shutdown event received, scheduling reconnect"
                        )
                        await self.schedule_reconnect(
                            connection,
                            self.configuration,
                            5,
                            close_old_connection=False,
                        )
                        await self.close_connection(connection, False)
                        if connection.id in self.reconnect_tasks:
                            self.reconnect_tasks.remove(connection.id)
                    else:
                        if data.get("error") or (
                            data.get("code") is not None and data.get("msg")
                        ):
                            raise ValueError(
                                "Error received from server: "
                                f"{data.get('error') or data}"
                            )

                        stream = data.get("stream")
                        subscription_id = data.get("subscriptionId")

                        key = stream or subscription_id
                        callbacks = (
                            connection.stream_callback_map.get(key)
                            if key is not None
                            else None
                        )

                        if callbacks:
                            try:
                                if stream:
                                    response_model = connection.response_types.get(
                                        stream
                                    )
                                    payload = data["data"] if response_model else data

                                    for callback in callbacks:
                                        if response_model:
                                            if response_model.__pydantic_fields__.get(
                                                "one_of_schemas"
                                            ):
                                                parsed = payload
                                            elif isinstance(payload, list):
                                                parsed = [
                                                    response_model.model_validate_json(
                                                        json.dumps(item)
                                                    )
                                                    for item in payload
                                                ]
                                            else:
                                                parsed = (
                                                    response_model.model_validate_json(
                                                        json.dumps(payload)
                                                    )
                                                )
                                            callback(parsed)
                                        else:
                                            callback(payload)
                                else:
                                    response_model = connection.response_types.get(
                                        subscription_id
                                    )
                                    payload = data["event"]

                                    for callback in callbacks:
                                        if response_model:
                                            if isinstance(payload, list):
                                                parsed = [
                                                    parse_user_event(
                                                        item, response_model
                                                    )
                                                    for item in payload
                                                ]
                                            else:
                                                parsed = parse_user_event(
                                                    payload, response_model
                                                )
                                            callback(parsed)
                                        else:
                                            callback(payload)
                            except Exception as e:
                                raise ValueError(
                                    f"Error in callback for key {key}: {e}"
                                )
                        else:
                            logging.info(f"Received message: {data}")
                elif msg.type == aiohttp.WSMsgType.PING:
                    logging.info("Received PING from server")
                    # TODO(binance-sdk-migration): Binance requires pong frames to
                    # copy the server ping payload on futures market streams and
                    # websocket API connections.
                    await connection.websocket.pong(getattr(msg, "data", None))
                elif msg.type == aiohttp.WSMsgType.PONG:
                    logging.info("Received PONG from server")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    websocket_error = connection.websocket.exception()
                    close_reason = f"websocket error: {websocket_error}"
                    logging.error("Received error from server")
                    logging.error(websocket_error)
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    close_reason = "websocket close frame received"
                    logging.info("WebSocket closed")
                    break
        except asyncio.CancelledError:
            close_reason = None
            raise
        except Exception as exc:
            close_reason = f"websocket receive loop exception: {exc}"
            logging.error(close_reason, exc_info=True)
        finally:
            if close_reason:
                self._emit_websocket_closed(connection, close_reason)

    async def send_message(
        self,
        payload: Dict,
        connection: WebSocketConnection,
    ):
        """Send a message to the WebSocket server.

        Args:
            payload (Dict): Payload to send.
            connection (WebSocketConnection): WebSocket connection object.
        """

        websocket = connection.websocket
        if payload.get("id") not in connection.pending_request:
            future = asyncio.get_event_loop().create_future()
            connection.pending_request[payload.get("id")] = future
        else:
            future = connection.pending_request[payload.get("id")]

        logging.info(
            f"Sending message to WebSocket {connection.id}: {redact_sensitive_info(payload)}"
        )
        await websocket.send_str(json.dumps(payload))
        return future

    async def ping(self, connection: WebSocketConnection):
        """Send a ping message to the WebSocket server.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
        """

        websocket = connection.websocket
        try:
            await websocket.ping()
            logging.info(f"Ping sent to WebSocket {connection.id}")
        except Exception as e:
            logging.error(f"Error sending ping to WebSocket {connection.id}: {e}")
            raise

    async def schedule_reconnect(
        self,
        connection: WebSocketConnection,
        configuration: Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams],
        delay: int,
        close_old_connection: bool = True,
    ):
        """Schedule a reconnect attempt after a delay.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
            configuration (Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams]): Configuration object.
            delay (int): Delay in seconds.
            close_old_connection (bool): Whether to close the old connection before reconnecting.
        """

        await asyncio.sleep(delay)

        if connection.close_initiated or getattr(self, "close_initiated", False):
            return

        if close_old_connection:
            connection.reconnect = True

        if connection.is_session_log_on:
            await WebSocketCommon.send_message(
                self,
                {
                    "method": self.user_data_endpoints.user_data_stream_logout,
                    "params": {},
                    "id": get_uuid(),
                },
                connection,
            )
            await asyncio.sleep(1)
            connection.is_session_log_on = False
        if connection.id not in self.reconnect_tasks:
            self.reconnect_tasks.append(connection.id)
        try:
            await self.reconnect(connection, configuration, close_old_connection)
        finally:
            if (
                getattr(connection, "scheduled_reconnect_task", None)
                is asyncio.current_task()
            ):
                connection.scheduled_reconnect_task = None

    def _cancel_scheduled_reconnect(self, connection: WebSocketConnection):
        task = getattr(connection, "scheduled_reconnect_task", None)
        if not task:
            return

        if task is not asyncio.current_task() and not task.done():
            task.cancel()

        if task is not asyncio.current_task():
            connection.scheduled_reconnect_task = None

    async def reconnect(
        self,
        connection: WebSocketConnection,
        configuration: Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams],
        close_old_connection: bool = True,
    ):
        """Reconnect to the WebSocket server.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
            configuration (Union[ConfigurationWebSocketAPI, ConfigurationWebSocketStreams]): Configuration object.
            close_old_connection (bool): Whether to close the old connection before reconnecting.
        """

        if len(connection.pending_request) > 0:
            connection.pending_request.clear()

        try:
            if close_old_connection:
                await self.close_connection(connection, False)
            if configuration.reconnect_delay:
                await asyncio.sleep(configuration.reconnect_delay / 1000)

            if self.session is None:
                self.session = aiohttp.ClientSession()

            # TODO(binance-sdk-migration): reconnect a routed stream onto the same
            # Binance /public, /market, or /private entry it originally used.
            await self.init_connection(
                configuration.stream_url,
                configuration,
                url_path=connection.url_path,
                ws_id=connection.id,
            )

            new_connection = next(
                (
                    c
                    for c in reversed(self.connections)
                    if c.id == connection.id and c is not connection
                ),
                None,
            )
            if not new_connection:
                logging.error("Reconnect failed: new connection not found")
                return

            if connection.session_logon_request and self.configuration.session_re_logon:
                await self.session_re_log_on(
                    connection.session_logon_request, new_connection
                )
                await asyncio.sleep(1)
                await self._resubscribe_user_streams(connection, new_connection)

            await self._resubscribe_global_streams(connection, new_connection)
            self._copy_internal_stream_callbacks(connection, new_connection)
            logging.info(f"Reconnected WebSocket {close_old_connection}")
        finally:
            if close_old_connection:
                if connection.id in self.reconnect_tasks:
                    self.reconnect_tasks.remove(connection.id)
                connection.reconnect = False

    async def _resubscribe_user_streams(
        self, old_connection: WebSocketConnection, new_connection: WebSocketConnection
    ):
        """Resubscribe all user streams from old_connection to new_connection.

        Args:
            old_connection (WebSocketConnection): The old WebSocket connection.
            new_connection (WebSocketConnection): The new WebSocket connection.
        """
        for stream, old_target in old_connection.stream_callback_map.items():
            if stream not in global_user_stream_connections.stream_connections_map:
                continue

            json_msg = {
                "method": self.user_data_endpoints.user_data_stream_subscribe,
                "params": {},
                "id": old_connection.id,
            }
            await WebSocketCommon.send_message(self, json_msg, new_connection)

            global_user_stream_connections.stream_connections_map[stream] = (
                new_connection
            )
            new_connection.stream_callback_map[stream] = old_target
            new_connection.response_types[stream] = old_connection.response_types.get(
                stream
            )

    def _copy_internal_stream_callbacks(
        self, old_connection: WebSocketConnection, new_connection: WebSocketConnection
    ):
        callbacks = old_connection.stream_callback_map.get("WebSocketclosed")
        if callbacks:
            new_connection.stream_callback_map["WebSocketclosed"] = callbacks

    async def _resubscribe_global_streams(
        self, old_connection: WebSocketConnection, new_connection: WebSocketConnection
    ):
        """Resubscribe all global streams from old_connection to new_connection.

        Args:
            old_connection (WebSocketConnection): The old WebSocket connection.
            new_connection (WebSocketConnection): The new WebSocket connection.
        """
        for stream, conn in list(
            global_stream_connections.stream_connections_map.items()
        ):
            if conn != old_connection or not isinstance(stream, str):
                continue

            json_msg = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": old_connection.id,
            }
            await self.send_message(json_msg, new_connection)
            global_stream_connections.stream_connections_map[stream] = new_connection

            new_connection.stream_callback_map[stream] = (
                old_connection.stream_callback_map.get(stream)
            )
            new_connection.response_types[stream] = old_connection.response_types.get(
                stream
            )

    async def session_re_log_on(self, request, connection: WebSocketConnection):
        """Re-logon the session.
        Args:
            connection (WebSocketConnection): WebSocket connection object.
        """

        if request and not connection.is_session_log_on:
            data = {
                "method": request["method"],
                "params": request["params"],
                "id": request["id"],
            }
            signer = Signers.get_signer(
                self.configuration.private_key,
                self.configuration.private_key_passphrase,
            )
            websocket_options = WebsocketApiOptions(
                signer=signer, api_key=False, is_signed=True, skip_auth=True
            )
            payload = ws_api_payload(self.configuration, data, websocket_options)

            try:
                await WebSocketCommon.send_message(self, payload, connection)
                connection.is_session_log_on = True
            except Exception as e:
                logging.error(
                    f"Session re-logon failed for connection {connection.id}: {e}"
                )

    async def close_connection(
        self,
        connection: Optional[WebSocketConnection] = None,
        close_session: bool = True,
    ):
        """Close the WebSocket connection.

        Args:
            connection (Optional[WebSocketConnection]): WebSocket connection object to close.
            close_session (bool): Whether to close the aiohttp session.
        """

        if len(self.connections) == 0:
            logging.warning("No WebSocket connections to close.")
        elif connection:
            try:
                self._cancel_scheduled_reconnect(connection)
                connection.close_initiated = True
                await connection.websocket.close()
                logging.info(f"WebSocket {connection.id} closed.")
                self.connections.remove(connection)
            except Exception as e:
                logging.error(f"Error closing WebSocket {connection.id}: {e}")
        else:
            for connection in self.connections[:]:
                try:
                    self._cancel_scheduled_reconnect(connection)
                    connection.close_initiated = True
                    await connection.websocket.close()
                    logging.info(f"WebSocket {connection.id} closed.")
                    self.connections.remove(connection)
                except Exception as e:
                    logging.error(f"Error closing WebSocket {connection.id}: {e}")

        if close_session and self.session is not None:
            await self.session.close()
            self.session = None


class WebSocketStreamBase(WebSocketCommon):
    def __init__(
        self,
        configuration: ConfigurationWebSocketStreams,
        id_strict_int: Optional[bool] = False,
        url_paths: Optional[str] = None,
    ):
        """Initialize the WebSocketStreamBase class.

        Args:
            configuration (ConfigurationWebSocketStreams): Configuration object.
            id_strict_int (Optional[bool]): Whether to use strict integer IDs.
            url_paths (Optional[str]): URL paths for the WebSocket connection.
        """

        if configuration.stream_url and not configuration.stream_url.endswith("stream"):
            configuration.stream_url = configuration.stream_url + "/stream"
        super().__init__(configuration)
        self.configuration = configuration
        self.id_strict_int = id_strict_int
        self.url_paths = url_paths

    async def create_connection(self):
        """Create a WebSocket connection.

        Returns:
            WebSocketConnection: The created WebSocket connection.
        """

        return await self.connect(
            self.configuration.stream_url, self.configuration, url_paths=self.url_paths
        )

    @staticmethod
    def _expected_usds_futures_route(stream: str) -> Optional[str]:
        stream_name = stream.lower()
        parts = stream_name.split("@")
        event_name = parts[1] if len(parts) > 1 else stream_name

        if stream_name == "!bookticker" or event_name.startswith(
            ("bookticker", "depth", "rpidepth")
        ):
            return "public"

        if stream_name.startswith(
            (
                "!markprice",
                "!ticker",
                "!miniticker",
                "!forceorder",
                "!contractinfo",
                "!compositeindex",
                "!assetindex",
            )
        ) or event_name.startswith(
            (
                "aggtrade",
                "markprice",
                "kline",
                "ticker",
                "miniticker",
                "forceorder",
                "contractinfo",
                "compositeindex",
                "indexprice",
                "continuouskline",
                "assetindex",
            )
        ):
            return "market"

        return None

    def _validate_stream_route(self, streams: list[str], stream_url: Optional[str]):
        if not stream_url:
            return

        for stream in streams:
            expected_route = self._expected_usds_futures_route(stream)
            if expected_route and expected_route != stream_url:
                # TODO(binance-sdk-migration): fail fast when a futures stream is
                # sent to the wrong /public, /market, or /private route.
                raise ValueError(
                    f"Stream {stream} belongs to /{expected_route}, "
                    f"not /{stream_url}"
                )

    async def subscribe(
        self,
        streams: list[str],
        response_model: Optional[Type[T]] = None,
        stream_url: Optional[str] = None,
        callback: Optional[Callable[[T], None]] = None,
    ):
        """Subscribe to a list of streams.

        Args:
            streams (list[str]): List of streams to subscribe to.
        """

        if not streams:
            logging.warning("No streams to subscribe to.")
            return

        if isinstance(streams, str):
            streams = [streams]

        self._validate_stream_route(streams, stream_url)

        if len(self.connections) == 0 and len(self.reconnect_tasks) == 0:
            await self.close_connection(close_session=True)
            raise ValueError("No WebSocket connections available.")

        if not any(not connection.reconnect for connection in self.connections):
            logging.warning("No available WebSocket connections for subscription.")
            return

        filtered_streams = []
        for stream in streams:
            existing_connection = global_stream_connections.stream_connections_map.get(
                stream
            )
            if existing_connection is None:
                filtered_streams.append(stream)
                continue

            existing_websocket = getattr(existing_connection, "websocket", None)
            if getattr(existing_websocket, "closed", False):
                # TODO(binance-sdk-migration): stale routed registrations must not
                # make a new /public, /market, or /private subscribe silently no-op.
                global_stream_connections.stream_connections_map.pop(stream, None)
                existing_connection.stream_callback_map.pop(stream, None)
                existing_connection.response_types.pop(stream, None)
                filtered_streams.append(stream)
                continue

            existing_route = getattr(existing_connection, "url_path", None)
            if stream_url and existing_route != stream_url:
                # TODO(binance-sdk-migration): fail fast if a stream is already
                # bound to a different Binance routed websocket entry.
                raise ValueError(
                    f"Stream {stream} is already registered on /{existing_route}, "
                    f"not /{stream_url}"
                )
            if callback:
                callbacks = existing_connection.stream_callback_map.setdefault(
                    stream, []
                )
                if callback not in callbacks:
                    callbacks.append(callback)
                existing_connection.response_types[stream] = response_model

        streams = filtered_streams

        for stream in streams:
            if stream_url:
                candidates = [c for c in self.connections if c.url_path == stream_url]
            else:
                candidates = self.connections

            if self.configuration.mode == WebsocketMode.SINGLE:
                connection = candidates[0] if candidates else None
            else:
                connection = (
                    candidates[self.round_robin_index % len(candidates)]
                    if candidates
                    else None
                )
                self.round_robin_index = (
                    (self.round_robin_index + 1) % len(candidates) if candidates else 0
                )

            if connection is None:
                message = f"No matching connection found for stream: {stream}"
                if stream_url:
                    # TODO(binance-sdk-migration): an explicit Binance route with no
                    # matching connection is a hard setup failure, not a warning.
                    raise ValueError(f"{message} on /{stream_url}")
                logging.warning(message)
                continue

            logging.info(f"Subscribing to streams: {streams}")
            json_msg = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": get_random_int() if self.id_strict_int else get_uuid(),
            }
            global_stream_connections.stream_connections_map[stream] = connection
            # TODO(binance-sdk-migration): seed callbacks before SUBSCRIBE is sent.
            # Binance can push the first trade immediately after accepting a live
            # subscription, before subscribe() returns to the caller for on().
            connection.stream_callback_map[stream] = [callback] if callback else []
            connection.response_types[stream] = response_model
            try:
                await asyncio.sleep(0.5)
                await self.send_message(json_msg, connection)
            except Exception:
                if (
                    global_stream_connections.stream_connections_map.get(stream)
                    is connection
                ):
                    global_stream_connections.stream_connections_map.pop(stream, None)
                connection.stream_callback_map.pop(stream, None)
                connection.response_types.pop(stream, None)
                raise

    def on(self, event: str, callback: Callable[[T], None], stream: str) -> None:
        """Set the callback function for incoming messages on a specific stream.

        Args:
            event (str): Event type.
            callback (Callable): Callback function.
            stream (str): Stream name.
        """

        if event != "message":
            raise ValueError(f"Unsupported event: {event}")
        connection = (
            global_stream_connections.stream_connections_map[stream]
            if stream in global_stream_connections.stream_connections_map
            else None
        )

        if connection:
            connection.stream_callback_map[stream].append(callback)
        else:
            logging.warning(f"Stream {stream} not connected.")

    async def unsubscribe(self, streams: list[str]):
        """Unsubscribe from a list of streams.

        Args:
            streams (list[str]): List of streams to unsubscribe from.
        """

        if not streams:
            logging.warning("No streams to unsubscribe to.")
            return

        if self.connections is None or len(self.connections) == 0:
            logging.warning("No WebSocket connections available for unsubscription.")
            return

        if isinstance(streams, str):
            streams = [streams]

        missing_stream = [
            stream
            for stream in streams
            if stream not in global_stream_connections.stream_connections_map
        ]

        if missing_stream:
            logging.warning(f"Stream {missing_stream} is not subscribed.")
            return

        for stream in streams:
            connection = (
                global_stream_connections.stream_connections_map[stream]
                if stream in global_stream_connections.stream_connections_map
                else None
            )
            if connection:
                json_msg = json.dumps(
                    {"method": "UNSUBSCRIBE", "params": streams, "id": get_uuid()}
                )
                await connection.websocket.send_str(json_msg)

                logging.info(f"Unsubscribed from stream: {stream}")
                global_stream_connections.stream_connections_map.pop(stream, None)
                connection.stream_callback_map.pop(stream, None)
                connection.response_types.pop(stream, None)
            else:
                raise ValueError(f"Stream {stream} not connected.")

    async def list_subscribe(self) -> dict:
        """List all subscriptions.

        Returns:
            dict: Current subscriptions.
        """

        for connection in self.connections:
            json_msg = {"method": "LIST_SUBSCRIPTIONS", "id": get_uuid()}
            future = await self.send_message(json_msg, connection)
            try:
                response = await asyncio.wait_for(future, timeout=20)
                logging.info(f"Current subscriptions: {response}")
                return response
            except asyncio.TimeoutError:
                logging.warning(
                    f"Timeout waiting for response to LIST_SUBSCRIPTIONS for connection {connection.id}"
                )

    async def ping_ws_stream(self, connection: WebSocketConnection):
        """Send a ping message to the WebSocket server.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
        """

        await super().ping(connection)


class WebSocketAPIBase(WebSocketCommon):
    def __init__(
        self,
        configuration: ConfigurationWebSocketAPI,
        user_data_endpoints: Optional[WebsocketApiUserDataEndpoints] = None,
    ):
        super().__init__(configuration, user_data_endpoints)
        self.configuration = configuration

    async def create_connection(self):
        return await self.connect(self.configuration.stream_url, self.configuration)

    async def send_signed_message(
        self,
        payload: Dict,
        signer: Optional[Signers] = None,
        promised: bool = True,
        response_model: Optional[Type[T]] = None,
        api_key: Optional[bool] = False,
        session_logon: Optional[bool] = False,
        session_logout: Optional[bool] = False,
    ) -> WebsocketApiResponse[T]:
        """Send a message to the WebSocket server.

        Args:
            payload (Dict): Payload to send.
            promised (bool): Whether the response is promised.
            response_model (Optional[Type[T]]): Response model.
            api_key (Optional[bool]): Whether to include the API key in the request.
            session_logon (Optional[bool]): Whether the message is for session logon.
            session_logout (Optional[bool]): Whether the message is for session logout.
        Returns:
            WebsocketApiResponse[T]: Response from the server.
        """

        if len(self.connections) == 0 and len(self.reconnect_tasks) == 0:
            await self.close_connection(close_session=True)
            raise ValueError("No WebSocket connections available.")

        if not any(not connection.reconnect for connection in self.connections):
            logging.warning("WebSocket Connection Reconnecting")
            return WebsocketApiResponse(
                data_function=lambda: "Websocket Reconnect", rate_limits=[]
            )

        if self.configuration.mode == WebsocketMode.SINGLE:
            connection = self.connections[0]
        else:
            connection = self.connections[
                self.round_robin_index % len(self.connections)
            ]
            self.round_robin_index = (self.round_robin_index + 1) % len(
                self.connections
            )

        skip_auth = False if session_logon else connection.is_session_log_on is True
        websocket_options = WebsocketApiOptions(
            signer=signer, api_key=api_key, is_signed=True, skip_auth=skip_auth
        )

        if not self.configuration.return_rate_limits:
            if "params" in payload:
                payload["params"].update({"returnRateLimits": False})
            else:
                payload["params"] = {"returnRateLimits": False}

        _payload = ws_api_payload(self.configuration, payload, websocket_options)

        future = await super().send_message(_payload, connection)
        if promised:
            try:
                ws_response = await asyncio.wait_for(future, timeout=20)
                if session_logon:
                    payload["id"] = _payload["id"]
                    connection.is_session_log_on = True
                    connection.session_logon_request = payload

                return WebsocketApiResponse[T](
                    data_function=lambda: (
                        response_model.model_validate(ws_response)
                        if response_model
                        else ws_response
                    ),
                    rate_limits=(
                        parse_ws_rate_limit_headers(ws_response["rateLimits"])
                        if self.configuration.return_rate_limits
                        else []
                    ),
                )
            except asyncio.TimeoutError:
                logging.warning(
                    f"Timeout waiting for response to message ID {payload.get('id')}"
                )
                return WebsocketApiResponse[T](
                    data_function=lambda: {"error": "timeout"},
                    rate_limits=[],
                )
            except Exception as e:
                logging.warning(f"Connection with user closed: {e}")
                error_message = str(e)

                return WebsocketApiResponse[T](
                    data_function=lambda: {"error": error_message},
                    rate_limits=[],
                )

    async def send_message(
        self,
        payload: Dict,
        promised: bool = True,
        response_model: Optional[Type[T]] = None,
        api_key: Optional[bool] = False,
        session_logon: Optional[bool] = None,
        session_logout: Optional[bool] = None,
    ) -> WebsocketApiResponse[T]:
        """Send a message to the WebSocket server.

        Args:
            payload (Dict): Payload to send.
            promised (bool): Whether the response is promised.
            response_model (Type[T]): Response model.
            api_key (Optional[bool]): Whether to include the API key in the request.
            session_logon (Optional[bool]): Whether the message is for session logon.
            session_logout (Optional[bool]): Whether the message is for session logout.
        Returns:
            WebsocketApiResponse[T]: Response from the server.
        """

        if len(self.connections) == 0 and len(self.reconnect_tasks) == 0:
            await self.close_connection(close_session=True)
            raise ValueError("No WebSocket connections available.")

        if not any(not connection.reconnect for connection in self.connections):
            logging.warning("WebSocket Connection Reconnecting")
            return WebsocketApiResponse(
                data_function=lambda: "Websocket Reconnect", rate_limits=[]
            )

        if self.configuration.mode == WebsocketMode.SINGLE:
            connection = self.connections[0]
        else:
            connection = self.connections[
                self.round_robin_index % len(self.connections)
            ]
            self.round_robin_index = (self.round_robin_index + 1) % len(
                self.connections
            )

        skip_auth = False if session_logon else connection.is_session_log_on is True

        websocket_options = WebsocketApiOptions(
            api_key=api_key, is_signed=False, skip_auth=skip_auth
        )

        if not self.configuration.return_rate_limits:
            if "params" in payload:
                payload["params"].update({"returnRateLimits": False})
            else:
                payload["params"] = {"returnRateLimits": False}

        _payload = ws_api_payload(self.configuration, payload, websocket_options)

        future = await super().send_message(_payload, connection)
        if promised:
            try:
                ws_response = await asyncio.wait_for(future, timeout=20)

                if session_logon:
                    payload["id"] = _payload["id"]
                    connection.is_session_log_on = True
                    connection.session_logon_request = payload

                is_oneof = self.is_one_of_model(response_model)
                if is_oneof or hasattr(response_model, "from_dict"):

                    def data_function():
                        return response_model.from_dict(ws_response)

                elif response_model:

                    def data_function():
                        return response_model.model_validate(ws_response)

                else:

                    def data_function():
                        return ws_response

                return WebsocketApiResponse[T](
                    data_function=data_function,
                    rate_limits=(
                        parse_ws_rate_limit_headers(ws_response["rateLimits"])
                        if self.configuration.return_rate_limits
                        else []
                    ),
                )
            except asyncio.TimeoutError:
                logging.warning(
                    f"Timeout waiting for response to message ID {payload.get('id')}"
                )
                return WebsocketApiResponse[T](
                    data_function=lambda: {"error": "timeout"},
                    rate_limits=[],
                )
            except Exception as e:
                logging.warning(f"Connection with user closed: {e}")
                error_message = str(e)

                return WebsocketApiResponse[T](
                    data_function=lambda: {"error": error_message},
                    rate_limits=[],
                )

    def is_one_of_model(self, model_cls: Type[T]) -> bool:
        """Check if the model is a oneof model.

        Args:
            model_cls (Type[T]): Model class to check.
        Returns:
            bool: True if the model is a oneof model, False otherwise.
        """

        return hasattr(model_cls, "is_oneof_model") and model_cls.is_oneof_model()

    async def ping_ws_api(self, connection: WebSocketConnection):
        """Send a ping message to the WebSocket server.

        Args:
            connection (WebSocketConnection): WebSocket connection object.
        """

        await super().ping(connection)

    async def subscribe_user_data(
        self, id: str, response_model: Optional[Type[T]] = None
    ):
        """Subscribe to user data updates for a specific user.

        Args:
            id (str): User Data ID.
            response_model (Optional[Type[T]]): Pydantic model to validate the response data.
        """
        if self.configuration.mode == WebsocketMode.SINGLE:
            connection = self.connections[0]
        else:
            connection = self.connections[
                self.round_robin_index % len(self.connections)
            ]
            self.round_robin_index = (self.round_robin_index + 1) % len(
                self.connections
            )
        global_user_stream_connections.stream_connections_map[id] = connection
        connection.stream_callback_map.update({id: []})
        connection.response_types.update({id: response_model})

    def on(self, event: str, callback: Callable[[T], None], id: str) -> None:
        """Set the callback function for incoming messages on a specific ID.

        Args:
            event (str): Event type.
            callback (Callable): Callback function.
            id (str): User Data ID.
        """

        if event != "message":
            raise ValueError(f"Unsupported event: {event}")

        connection = (
            global_user_stream_connections.stream_connections_map[id]
            if id in global_user_stream_connections.stream_connections_map
            else None
        )

        if connection:
            connection.stream_callback_map[id].append(callback)
        else:
            logging.warning(f"Stream {id} not connected.")

    async def unsubscribe(self, id: str):
        """Unsubscribe from a user data ID.

        Args:
            id (str): user data ID to unsubscribe from.
        """

        if self.connections is None or len(self.connections) == 0:
            logging.warning("No user data connections available for unsubscription.")
            return

        if id not in global_user_stream_connections.stream_connections_map:
            logging.warning(f"Stream {id} is not subscribed.")
            return

        connection = (
            global_user_stream_connections.stream_connections_map[id]
            if id in global_user_stream_connections.stream_connections_map
            else None
        )
        if connection:
            global_user_stream_connections.stream_connections_map.pop(id, None)
            logging.info(f"Unsubscribed from stream: {id}")
        else:
            raise ValueError(f"Subscription id {id} not connected.")


class RequestStreamHandle(Generic[T]):
    """A wrapper for Request Stream Method.

    :param websocket_base: WebSocket base.
    :param stream: Stream name.
    :param response_model: The Pydantic model to validate the response data.
    """

    def __init__(
        self,
        websocket_base: WebSocketStreamBase or WebSocketAPIBase,
        stream: str,
        response_model: Optional[Type[T]] = None,
    ):
        self._websocket_base = websocket_base
        self._stream = stream
        self._response_model = response_model

    async def unsubscribe(self) -> None:
        if isinstance(self._websocket_base, WebSocketStreamBase):
            await self._websocket_base.unsubscribe(streams=self._stream)
        else:
            await self._websocket_base.unsubscribe(id=self._stream)

    def on(self, event: str, callback: Callable[[T], None]) -> None:
        self._websocket_base.on(event, callback, self._stream)


async def RequestStream(
    websocket_base: WebSocketStreamBase or WebSocketAPIBase,
    stream: str,
    response_model: Optional[Type[T]] = None,
    stream_url: Optional[str] = None,
) -> RequestStreamHandle[T]:
    """Decorator to create a request stream for a specific stream.

    Args:
        websocket_base (WebSocketStreamBase or WebSocketAPIBase): WebSocket base.
        stream (str): Stream name.
        response_model (Type[T], optional): Response model for the stream.
    """

    if isinstance(websocket_base, WebSocketStreamBase):
        await websocket_base.subscribe(
            streams=[stream], response_model=response_model, stream_url=stream_url
        )
    else:
        await websocket_base.subscribe_user_data(
            id=stream, response_model=response_model
        )

    return RequestStreamHandle(websocket_base, stream, response_model)
