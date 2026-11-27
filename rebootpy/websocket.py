# -*- coding: utf-8 -*-

"""
MIT License

Copyright (c) 2024 Oli

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import asyncio
import aiohttp
import json
import logging
import base64
import datetime
import random

from collections import deque

from .message import FriendMessage, PartyMessage
from .presence import Presence
from .errors import STOMPError
from .utils import websocket_messages

log = logging.getLogger(__name__)


def decode_message_body(body: str) -> str:
    try:
        decoded = base64.b64decode(body).decode('utf-8')
        decoded = decoded.rstrip('\x00')
        parsed = json.loads(decoded)
        return parsed.get('msg', body)
    except (ValueError, AttributeError):
        return body


class WebsocketClient:
    _handshake_timeout = 15

    def __init__(self, client) -> None:
        self.client = client

        self.wss_session = None
        self.websocket = None
        self.ws_task = None

        self.heartbeat_started = False
        self._ready_event = asyncio.Event()

        self.connection_id = None
        self.public_connection_id = None
        self.private_connection_id = None
        self._eas_public_sub_id = None
        self._eas_private_sub_id = None

        self.message_history = deque(maxlen=200)
        self._user_close = False
        self._reconnect_attempt = 0
        self._restore_party = False
        self._session_was_ready = False
        self._heartbeat_task = None
        self._restore_task = None

    async def set_session(self) -> None:
        self.wss_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(
            sock_connect=self._handshake_timeout, sock_read=self._handshake_timeout
        ))

    async def send_presence(self, connection_id: str) -> None:
        await self.client.http.chat_send_presence(
            connection_id=connection_id,
            auth="EAS_ACCESS_TOKEN"
        )

    async def send_heartbeat(self, websocket, delay: float) -> None:
        try:
            while not websocket.closed:
                await asyncio.sleep(delay)
                await websocket.send_str('\n')
        except (aiohttp.ClientError, OSError):
            await websocket.close()

    async def parse_message(self, raw: str) -> None:
        raw_headers, separator, raw_json = raw.partition('\n\n')
        if not separator:
            return
        header_lines = raw_headers.splitlines()
        message_type = header_lines[0]

        headers = {}
        for line in header_lines[1:]:
            key, value = line.split(':', 1)
            headers[key.strip()] = value.strip()

        data = json.loads(raw_json.rstrip('\x00')) if raw_json else {}
        if not isinstance(data, dict):
            return
        payload = data.get('payload')
        payload = payload if isinstance(payload, dict) else {}
        conversation = payload.get('conversation') or {}
        event = data.get('type')

        if message_type == 'MESSAGE' and event != 'core.connect.v1.connected':
            message_id = data.get('id')
            if not message_id and event in ('social.chat.v1.NEW_MESSAGE', 'social.chat.v1.NEW_WHISPER'):
                message = payload.get('message') or {}
                if message.get('senderId') and message.get('time') is not None:
                    message_id = (message['senderId'], message['time'])
            if message_id:
                if message_id in self.message_history:
                    return
                self.message_history.append(message_id)

        log.debug(
            f'{datetime.datetime.now(datetime.timezone.utc)} - Received websocket message with type'
            f' {message_type} with the headers {headers} and body \n{data}.')

        if message_type == 'CONNECTED' and not self.heartbeat_started:
            self.heartbeat_started = True
            session_id = headers.get('session', '')

            server_interval = int(headers.get('heart-beat', '0,0').split(',')[1])
            delay = max(30000, server_interval) / 1000
            self._heartbeat_task = self.client.loop.create_task(
                self.send_heartbeat(self.websocket, delay)
            )

            eas_n = str(random.randint(1, 0xffffffff))
            self._eas_public_sub_id = f'sub-eas-{eas_n}'
            self._eas_private_sub_id = f'sub-eas-private-{eas_n}'

            token = self.client.auth.eas_access_token
            destination = (
                f'deploymentId/{self.client.deployment_id}/'
                f'epicAccountId/{self.client.user.id}'
            )
            eas_headers = {
                'authorization': f'Bearer {token}',
                'ec-coord-accept-language': 'en',
            }

            sub_public = (
                'SUBSCRIBE\n'
                f'id:{self._eas_public_sub_id}\n'
                f'destination:{destination}\n'
            )
            if session_id:
                sub_public += f'receipt:sub-0-{session_id}\n'
            for key, value in eas_headers.items():
                sub_public += f'{key}:{value}\n'
            await self.websocket.send_str(sub_public + '\n\x00')

            sub_private = (
                'SUBSCRIBE\n'
                f'id:{self._eas_private_sub_id}\n'
                f'destination:{destination}\n'
                'ec-coord-temporary-subscription:parties-internal\n'
            )
            if session_id:
                sub_private += f'receipt:sub-1-{session_id}\n'
            for key, value in eas_headers.items():
                sub_private += f'{key}:{value}\n'
            await self.websocket.send_str(sub_private + '\n\x00')
        elif (message_type == 'MESSAGE' and event == 'core.connect.v1.connected'
              and not self._ready_event.is_set()):
            if not all((data.get('connectionId'), self._eas_public_sub_id,
                        self._eas_private_sub_id)):
                await self.websocket.close()
                return
            self.connection_id = data['connectionId']

            if self._eas_public_sub_id:
                self.public_connection_id = (
                    f'{self.connection_id}#{self._eas_public_sub_id}'
                )
            if self._eas_private_sub_id:
                self.private_connection_id = (
                    f'{self.connection_id}#{self._eas_private_sub_id}'
                )

            self._session_was_ready = True
            self._ready_event.set()
            self._restore_task = self.client.loop.create_task(
                self._restore_party_state(self.websocket)
            )
        elif (
            message_type == 'MESSAGE' and
            isinstance(data.get('type'), str) and
            data['type'].startswith('party.v2.')
        ):
            asyncio.ensure_future(
                self.client.handle_epic_party_notification(
                    data['type'], data.get('payload') or {}
                )
            )
        elif (
            message_type == 'MESSAGE' and
            data.get('type') == 'social.chat.v1.MEMBERS_JOIN'
        ):
            asyncio.ensure_future(self._apply_chat_reportable(
                payload, self.websocket
            ))
        elif (
            message_type == 'MESSAGE' and
            (event == 'social.chat.v1.NEW_WHISPER' or
             (event == 'social.chat.v1.NEW_MESSAGE' and conversation.get('type') == 'dm'))
        ):
            message = payload['message']
            user_id = message['senderId']
            if user_id == self.client.user.id:
                return
            author = self.client.get_friend(user_id)
            if author is None:
                try:
                    author = await self.client.wait_for(
                        'friend_add',
                        check=lambda f: f.id == user_id,
                        timeout=2
                    )
                except asyncio.TimeoutError:
                    return

            self.client.dispatch_event('friend_message', FriendMessage(
                client=self.client, author=author,
                content=decode_message_body(message['body'])
            ))
        elif (
            message_type == 'MESSAGE' and
            data.get('type') == 'social.chat.v1.NEW_MESSAGE' and
            conversation.get('type') in ('party', 'epic_party')
        ):
            conversation_type = data['payload']['conversation']['type']
            conversation_id = data['payload']['conversation']['conversationId']  # noqa
            user_id = data['payload']['message']['senderId']
            party = self.client.party

            client_party_id = party.id if party is not None else None
            if conversation_type == 'epic_party':
                bare_id = conversation_id[3:] if conversation_id.startswith(
                    'ep-'
                ) else conversation_id
                matches_party = party is not None and bare_id == party.epic_party_id
            else:
                bare_id = conversation_id[2:] if conversation_id.startswith(
                    'p-'
                ) else conversation_id
                matches_party = client_party_id == bare_id

            if (
                party is None
                or not matches_party
                or user_id == self.client.user.id
                or user_id not in party._members
            ):
                return

            decoded_content = decode_message_body(
                data['payload']['message']['body']
            )
            self.client.dispatch_event('party_message', PartyMessage(
                client=self.client,
                party=party,
                author=party._members[data['payload']['message']['senderId']],
                content=decoded_content
            ))
        elif (
            message_type == 'MESSAGE' and
            data.get('type') == 'presence.v1.UPDATE'
        ):
            user_id = payload['accountId']
            if payload.get('status') == 'offline':
                before = self.client._presences.pop(user_id, None)
                if before is not None:
                    friend = self.client.get_friend(user_id)
                    if friend is not None:
                        friend._update_last_logout(datetime.datetime.now(datetime.timezone.utc))
                        self.client.dispatch_event('friend_offline', friend)
                return

            friend = self.client.get_friend(user_id)
            if friend is None:
                try:
                    friend = await self.client.wait_for(
                        'friend_add',
                        check=lambda f: f.id == user_id,
                        timeout=1
                    )
                except asyncio.TimeoutError:
                    return

            _pres = Presence(
                self.client,
                payload
            )

            before_pres = friend.last_presence

            self.client._presences[user_id] = _pres

            self.client.dispatch_event('friend_presence', before_pres, _pres)
        elif (
            (message_type == 'ERROR' and
             (data.get('statusCode') == 4019 or not self._ready_event.is_set()))
            or (message_type == 'MESSAGE' and event == 'core.connect.v1.connect-failed')
        ):
            log.debug('STOMP connection rejected: %s', data.get('statusCode'))
            await self.websocket.close()
        elif (
            message_type == 'MESSAGE' and
            isinstance(data.get('type'), str) and
            ('party' in data['type'].lower() or 'invite' in data['type'].lower())  # noqa
        ):
            asyncio.ensure_future(self.client._epic_party_poll())

    async def _apply_chat_reportable(self, payload: dict, websocket) -> None:
        if (
            getattr(self.client, 'disable_epic_party_service', False)
            or payload.get('type') != 'epic_party'
            or payload.get('namespace') != '_'
            or not isinstance(payload.get('isReportable'), bool)
        ):
            return

        lock = getattr(self.client, '_join_party_lock', None)
        if lock is not None:
            await lock.wait()

        party = self.client.party
        conversation_id = payload.get('conversationId')
        if (
            self.websocket is not websocket
            or party is None
            or not party.epic_party_id
            or conversation_id != f'ep-{party.epic_party_id}'
        ):
            return

        party.set_reportable(payload['isReportable'], from_chat=True)

    async def _restore_party_state(self, websocket) -> None:
        try:
            if self._restore_party:
                await self.client.recover_party(True)
            if not websocket.closed:
                self._restore_party = False
                self._reconnect_attempt = 0
        except Exception as exc:
            log.debug('STOMP party restore failed: %s', exc)
            if not websocket.closed:
                await websocket.close()
            return

        if websocket.closed or self.websocket is not websocket:
            return
        try:
            await self.client.send_eos_presence()
        except Exception as exc:
            log.debug('STOMP presence failed: %s', exc)

    async def _sleep_before_reconnect(self, had_party: bool) -> bool:
        limit = getattr(self.client, 'stomp_max_connection_retries', 4)
        if self._reconnect_attempt >= limit:
            log.debug('STOMP disconnected, retry limit reached')
            self.client._request_close()
            return False

        backoff = getattr(
            self.client,
            'stomp_connection_retry_backoff',
            (5.0, 15.0, 60.0, 600.0),
        )
        delay = backoff[min(self._reconnect_attempt, len(backoff) - 1)]
        self._reconnect_attempt += 1
        self._restore_party = self._restore_party or had_party
        log.debug('STOMP disconnected, reconnecting in %s seconds', delay)
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return False
        return not self._user_close

    def _clear_stomp_session(self) -> None:
        self.heartbeat_started = False
        self.connection_id = None
        self.public_connection_id = None
        self.private_connection_id = None
        self._eas_public_sub_id = None
        self._eas_private_sub_id = None
        self._ready_event.clear()

    async def _connect_once(self) -> None:
        headers = {
            'Authorization': f'Bearer {self.client.auth.eas_access_token}',
            'Epic-Connect-Protocol': 'stomp',
            'Epic-Connect-Device-Id': " ",
        }
        async with self.wss_session.ws_connect(
            "wss://connect.epicgames.dev/v2",
            headers=headers
        ) as websocket:
            self.websocket = websocket
            connect_frame = (
                "CONNECT\n"
                "accept-version:1.0,1.1,1.2\n"
                "heart-beat:30000,0\n"
                f"authorization:Bearer {self.client.auth.eas_access_token}\n"
                "\n\x00"
            )
            await websocket.send_str(connect_frame)

            async for msg in websocket_messages(
                websocket, self._ready_event.is_set, self._handshake_timeout
            ):
                if msg.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    continue
                try:
                    raw = msg.data.decode() if isinstance(msg.data, bytes) else msg.data
                    for frame in raw.replace('\r\n', '\n').split('\x00'):
                        await self.parse_message(frame.lstrip('\n'))
                except (ValueError, KeyError, AttributeError, TypeError) as exc:
                    log.debug('Invalid STOMP frame: %s', exc)

    async def connect_to_websocket(self) -> None:
        while not self._user_close:
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug('STOMP connection error: %s', exc)
            finally:
                had_party = self._ready_event.is_set() and self.client.party is not None
                self._clear_stomp_session()
                tasks = [task for task in (self._heartbeat_task, self._restore_task)
                         if task is not None]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._heartbeat_task = self._restore_task = None
                self.websocket = None

            if self._user_close:
                return
            if not self._session_was_ready:
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    return
                continue
            if not await self._sleep_before_reconnect(had_party):
                return

    async def run(self) -> None:
        log.debug('Starting STOMP websocket client')
        if self.ws_task is None or self.ws_task.done():
            self._user_close = False
            if self.wss_session is None or self.wss_session.closed:
                await self.set_session()
            self.ws_task = self.client.loop.create_task(
                self.connect_to_websocket()
            )

        try:
            await asyncio.wait_for(self._ready_event.wait(), self._handshake_timeout)
        except asyncio.TimeoutError:
            await self.close()
            raise STOMPError('Timed out connecting to STOMP') from None

    async def close(self) -> None:
        log.debug('Closing STOMP websocket client')
        self._user_close = True
        self._restore_party = False
        self._session_was_ready = False
        self._reconnect_attempt = 0
        self.message_history.clear()

        task = self.ws_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.ws_task = None
        if self.wss_session is not None and not self.wss_session.closed:
            await self.wss_session.close()
        self._clear_stomp_session()

    async def restart(self) -> None:
        log.debug('Restarting STOMP websocket client')
        had_party = self.client.party is not None
        await self.close()
        self._restore_party = had_party
        await self.run()
