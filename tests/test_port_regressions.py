import asyncio
import base64
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp

from rebootpy import AwayStatus, Client, Platform
from rebootpy.enums import PartyPrivacy
from rebootpy.auth import Auth
from rebootpy.errors import HTTPException, STOMPError, _RefreshRecoveryFailed
from rebootpy.party import (
    ClientParty, ClientPartyMember, PartyBase, PartyMember, PartyMemberMeta,
    ReceivedPartyInvitation, _variant_channels,
)
from rebootpy.enums import Platform
from rebootpy.presence import Presence, PresenceParty


def api_error(code=None, *, variables=None, error=None):
    return HTTPException(
        SimpleNamespace(status=400), '/test',
        {'errorCode': code, 'messageVars': variables or [], 'error': error}, {},
    )


def frame(event, **data):
    return 'MESSAGE\n\n' + json.dumps({'type': event, **data}) + '\x00'


class Socket:
    def __init__(self, *frames):
        self.closed = False
        self.send_str = AsyncMock()
        self.messages = asyncio.Queue()
        for frame in frames:
            self.feed(frame)

    def feed(self, data):
        kind = aiohttp.WSMsgType.BINARY if isinstance(data, bytes) else aiohttp.WSMsgType.TEXT
        self.messages.put_nowait(aiohttp.WSMessage(kind, data, ''))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def close(self):
        if not self.closed:
            self.closed = True
            self.messages.put_nowait(None)


class Session:
    def __init__(self, *connections):
        self.connections = iter(connections)
        self.closed = False

    def ws_connect(self, *args, **kwargs):
        connection = next(self.connections)
        if isinstance(connection, Exception):
            raise connection
        return connection

    async def close(self):
        self.closed = True


async def wait_for(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), 1)


class PortRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = Client(Auth(), xmpp_connection_retry_backoff=0,
                             stomp_connection_retry_backoff=0)
        await self.client.init()
        self.client.user = SimpleNamespace(id='self', display_name='Bot')
        self.client.auth.eas_access_token = 'test-eas'
        self.client.auth.ios_access_token = 'test-ios'
        self.client.auth.eos_product_user_id = 'test-product-user'
        self.addAsyncCleanup(self.client.xmpp.close)
        self.addAsyncCleanup(self.client.websocket.close)

    async def test_stomp_reads_text_and_binary_recovers_after_auth_error(self):
        stomp = self.client.websocket
        order = []

        async def recover(*args, **kwargs):
            order.append('party')

        async def presence(*args, **kwargs):
            order.append('presence')

        self.client.recover_party = AsyncMock(side_effect=recover)
        self.client.send_eos_presence = AsyncMock(side_effect=presence)
        self.client.party = object()
        connected = 'CONNECTED\nheart-beat:0,0\n\n\x00'
        first = Socket('\n' + connected + frame('core.connect.v1.connected', connectionId='one'))
        second = Socket(connected.encode(), frame('core.connect.v1.connected', connectionId='two').encode())
        stomp.wss_session = Session(first, OSError('connect failed'), second)
        await stomp.run()
        self.assertTrue(stomp.private_connection_id.startswith('one#sub-eas-private-'))
        first.feed('ERROR\n\n{"statusCode":4019}\x00')
        await wait_for(lambda: stomp.connection_id == 'two' and stomp._reconnect_attempt == 0)
        await wait_for(lambda: 'party' in order and 'presence' in order[order.index('party') + 1:])
        self.client.recover_party.assert_awaited_once_with(True)
        self.assertTrue(first.closed)
        self.assertTrue(stomp.public_connection_id.startswith('two#sub-eas-'))

    async def test_stomp_timeout_closes_pending_connection(self):
        stomp = self.client.websocket
        socket = Socket()
        session = Session(socket)
        stomp.wss_session = session
        stomp._handshake_timeout = 0.02
        with self.assertRaises(STOMPError):
            await stomp.run()
        self.assertTrue(socket.closed)
        self.assertTrue(session.closed)
        self.assertIsNone(stomp.ws_task)

    async def test_xmpp_restores_caches_and_party_after_failed_attempt(self):
        xmpp = self.client.xmpp
        self.client.party = object()
        self.client.refresh_caches = AsyncMock()
        self.client.recover_party = AsyncMock()
        handshake = ('<stream:features>', '<success/>',
                     '<stream:features>xmpp-bind', '<iq type="result" id="bind_1"/>')
        first, second = Socket(*handshake), Socket(*handshake)
        xmpp.http_session = Session(first, OSError('connect failed'), second)
        await xmpp.run()
        await first.close()
        await wait_for(lambda: xmpp.websocket is second and xmpp._ready
                       and xmpp._reconnect_attempt == 0)
        self.client.refresh_caches.assert_awaited_once()
        self.client.recover_party.assert_awaited_once_with(True)

    async def test_stomp_close_cancels_backoff(self):
        stomp = self.client.websocket
        first = Socket('CONNECTED\n\n\x00', frame('core.connect.v1.connected', connectionId='one'))
        stomp.wss_session = Session(first)
        self.client.send_eos_presence = AsyncMock()
        self.client.stomp_connection_retry_backoff = (60,)
        await stomp.run()
        await first.close()
        await wait_for(lambda: stomp._reconnect_attempt == 1)
        await stomp.close()
        self.assertIsNone(stomp.ws_task)
        self.assertEqual(stomp._reconnect_attempt, 0)

    async def test_stomp_requests_shutdown_at_retry_limit(self):
        stomp = self.client.websocket
        first = Socket('CONNECTED\n\n\x00', frame('core.connect.v1.connected', connectionId='one'))
        self.client.stomp_max_connection_retries = 1
        self.client._request_close = Mock()
        stomp.wss_session = Session(first, OSError('connect failed'))
        await stomp.run()
        await first.close()
        await asyncio.wait_for(stomp.ws_task, 1)
        self.client._request_close.assert_called_once()

    async def test_whisper_and_direct_message_are_deduplicated(self):
        stomp = self.client.websocket
        friend = SimpleNamespace(id='friend')
        self.client.get_friend = Mock(return_value=friend)
        self.client.dispatch_event = Mock()
        payload = {'message': {'senderId': 'friend', 'body': 'hello', 'time': 123}}
        await stomp.parse_message(frame('social.chat.v1.NEW_WHISPER', payload=payload))
        payload['conversation'] = {'type': 'dm'}
        await stomp.parse_message(frame('social.chat.v1.NEW_MESSAGE', payload=payload))
        self.client.dispatch_event.assert_called_once()
        event, message = self.client.dispatch_event.call_args.args
        self.assertEqual(event, 'friend_message')
        self.assertEqual(message.content, 'hello')

    async def test_away_status_is_sent_to_public_and_private_presence(self):
        self.client.away = AwayStatus.AWAY
        self.client.party = SimpleNamespace(
            id='lobby', epic_party_id='party', member_count=2, max_size=16,
            playlist_info=('island',), me=SimpleNamespace(leader=True),
            config={'privacy': {'partyType': 'Public', 'presencePermission': 'Leader'}},
        )
        self.client.websocket.connection_id = 'connection'
        self.client.websocket.public_connection_id = 'public'
        self.client.websocket.private_connection_id = 'private'
        send = self.client.http.eos_presence_send = AsyncMock()
        await self.client.send_eos_presence()
        self.assertEqual([call.args[0] for call in send.await_args_list], ['public', 'private'])
        calls = {call.args[0]: call.args[1] for call in send.await_args_list}
        self.assertEqual(calls['public']['status'], 'away')
        self.assertEqual(calls['public']['props']['FortPartySize'], 'i2')
        self.assertEqual(calls['public']['props']['IslandCode'], 'sisland')
        self.assertIn('"p": "lobby"', calls['public']['props']['party.joininfodata.286331153'])
        self.assertEqual(calls['private']['status'], 'away')
        self.assertEqual(calls['private']['party']['id'], 'party')
        self.assertEqual(calls['private']['party']['type'], 'OPEN')
        self.assertTrue(calls['private']['party']['clientJoinable'])

        self.client.party.config['privacy']['partyType'] = 'Private'
        self.client.party.config['privacy']['presencePermission'] = 'Noone'
        send.reset_mock()
        await self.client.send_eos_presence()
        private_calls = {call.args[0]: call.args[1] for call in send.await_args_list}
        self.assertEqual(
            private_calls['public']['props']['party.joininfodata.286331153'],
            'm{"bIsPrivate":true}',
        )
        self.assertEqual(private_calls['private']['party']['type'], 'INVITE_ONLY')
        self.assertFalse(private_calls['private']['party']['clientJoinable'])

    async def test_presence_without_activity_and_legacy_presence(self):
        modern = Presence(self.client, {'perNs': [{
            'ns': self.client.deployment_id, 'status': 'away',
            'props': {'FortLFG': 'sinvalid', 'FortPartySize': 'i4', 'EOS_Platform': 'WIN'},
        }]})
        self.assertIsNone(modern.status)
        self.assertEqual(modern.away, AwayStatus.AWAY)
        self.assertEqual(modern.party_size, 4)
        self.assertEqual(modern.platform, Platform.WINDOWS)
        self.assertIsNone(modern.lfg)
        legacy = Presence(self.client, {'perNs': [{'status': 'In a lobby'}]})
        self.assertEqual(legacy.status, 'In a lobby')
        self.client.disable_epic_party_service = True
        self.client.join_legacy_party = AsyncMock()
        await PresenceParty(self.client, {'p': 'a' * 32}).join()
        self.client.join_legacy_party.assert_awaited_once_with('a' * 32)

    def test_signed_message_matches_fnbr(self):
        self.client.generate_keypair()
        before = int(time.time() * 1000)
        body, signature = self.client.create_signed_message(
            'ep-abc', 'hello', type='Persistent'
        )
        info = json.loads(base64.b64decode(body))
        self.assertRegex(
            info['mid'],
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        )
        self.assertGreaterEqual(info['tst'], before)
        self.assertLessEqual(info['tst'], int(time.time() * 1000))
        self.assertGreater(info['tst'], 10**12)
        self.assertEqual(info['cty'], 'Persistent')
        self.assertEqual(info['rid'], 'ep-abc')
        self.assertEqual(info['seq'], 1)
        self.assertTrue(signature)
        self.assertNotIn(' ', base64.b64decode(body).decode())

    async def test_reportability_retry_stays_in_the_original_party(self):
        client = self.client
        party = PartyBase(client, {'id': 'lobby', 'epic_party_id': 'party'})
        party._members = {name: SimpleNamespace(id=name) for name in ('self', 'friend')}
        client.party = party
        client.key_data = {'jwt': 'test'}
        client.create_signed_message = Mock(return_value=('body', 'signature'))
        values = []
        mismatch = api_error('errors.com.epicgames.social.chat.is_reportable_mismatch')

        async def post(route, *, json, auth, headers):
            values.append(json['isReportable'])
            self.assertNotIn('NPM', json['metadata'])
            self.assertTrue(headers['X-Epic-Correlation-ID'].startswith('EOS-'))
            if len(values) == 1:
                raise mismatch

        client.http.post = AsyncMock(side_effect=post)
        await client.http.party_send_message('hello', party=party)
        self.assertEqual(values, [False, True])
        self.assertTrue(party.is_reportable)
        party.set_reportable(False)
        self.assertTrue(party.is_reportable)

        async def switch_party(*args, **kwargs):
            self.assertTrue(
                kwargs['headers']['X-Epic-Correlation-ID'].startswith('EOS-')
            )
            client.party = object()
            raise mismatch

        client.http.post = AsyncMock(side_effect=switch_party)
        with self.assertRaises(HTTPException):
            await client.http.party_send_message('hello', party=party)
        client.http.post.assert_awaited_once()

    async def test_member_patch_retries_only_the_stale_chunk(self):
        party = PartyBase(self.client, {'id': 'lobby', 'epic_party_id': 'party'})
        member = ClientPartyMember(self.client, party, {
            'id': 'self', 'revision': 7, 'joined_at': '2026-11-21T00:00:00.000Z',
        })
        stale = api_error('errors.com.epicgames.social.party.stale_revision', variables=['', '12'])
        send = self.client.http.party_update_member_meta = AsyncMock(side_effect=[None, stale, None])
        await member.patch({f'Default:key{i}': 'value' for i in range(34)}, ['Default:old'])
        self.assertEqual([call.kwargs['revision'] for call in send.await_args_list], [7, 8, 12])
        self.assertEqual([len(call.kwargs['updated_meta']) for call in send.await_args_list], [32, 2, 2])
        self.assertEqual([call.kwargs['deleted_meta'] for call in send.await_args_list], [['Default:old'], [], []])
        await member.patch({'notSent': 'value'})
        self.assertEqual(member.revision, 13)
        self.assertEqual(send.await_count, 3)

    async def test_failed_invalid_refresh_requests_shutdown_without_success_event(self):
        client = self.client
        client.restart_on_invalid_refresh = True
        client._set_ready()
        client._request_close = Mock()
        client.dispatch_event = Mock()
        client.auth.ios_refresh_token = 'expired'
        client.auth.grant_refresh_token = AsyncMock(side_effect=api_error(error='invalid_grant'))
        client.auth.reauthenticate = AsyncMock(side_effect=NotImplementedError)
        with self.assertRaises(_RefreshRecoveryFailed):
            await client.auth.do_refresh()
        client.auth.reauthenticate.assert_awaited_once()
        client._request_close.assert_called_once()
        client.dispatch_event.assert_not_called()
        self.assertFalse(client.auth.refreshing())

    async def test_forced_refresh_does_not_wait_for_locked_http_requests(self):
        client = self.client
        client.auth.ios_refresh_token = 'expired'
        client.auth.grant_refresh_token = AsyncMock(side_effect=api_error(error='invalid_grant'))
        client.auth.reauthenticate = AsyncMock()
        client.dispatch_and_wait_event = AsyncMock()
        client.dispatch_event = Mock()
        async with client._reauth_lock:
            await client.auth.do_refresh()
        client.dispatch_and_wait_event.assert_not_awaited()
        self.assertEqual([call.args[0] for call in client.dispatch_event.call_args_list],
                         ['internal_auth_refresh', 'auth_refresh'])

    def test_party_config_accepts_both_invite_ttl_field_names(self):
        for field in ('invite_ttl', 'invite_ttl_seconds'):
            with self.subTest(field=field):
                party = PartyBase(self.client, {'id': 'lobby', 'config': {field: 123}})
                self.assertEqual(party.invite_ttl_seconds, 123)

    async def test_eos_party_actions_follow_fnbr(self):
        client = self.client
        party = PartyBase(client, {'id': 'lobby', 'epic_party_id': 'eos-party'})
        party.me = SimpleNamespace(leader=True)
        member = PartyMember(client, party, {
            'id': 'friend', 'revision': 1, 'joined_at': '2026-11-21T00:00:00.000Z',
        })
        remove = client.http.epic_party_remove_member = AsyncMock()
        legacy_kick = client.http.party_kick_member = AsyncMock()
        await member.kick()
        remove.assert_awaited_once_with('eos-party', 'friend')
        legacy_kick.assert_not_awaited()

        promote = client.http.party_promote_member = AsyncMock()
        eos_promote = client.http.epic_party_promote_member = AsyncMock()
        await member.promote()
        promote.assert_awaited_once_with('lobby', 'friend')
        eos_promote.assert_awaited_once_with('eos-party', 'friend')

        client_party = ClientParty(client, {
            'id': 'lobby', 'epic_party_id': 'eos-party', 'revision': 1,
        })
        client.party = client_party
        client.store_user = Mock(return_value=SimpleNamespace(id='friend'))
        send_invite = client.http.epic_party_send_invite = AsyncMock()
        legacy_invite = client.http.party_send_invite = AsyncMock()
        ping = client.http.party_send_ping = AsyncMock()
        await client_party._invite(SimpleNamespace(id='friend', get_raw=lambda: {}))
        send_invite.assert_awaited_once_with('friend')
        legacy_invite.assert_not_awaited()
        ping.assert_not_awaited()

        patch = client_party.patch = AsyncMock()
        joinability = client.set_epic_party_joinability = AsyncMock()
        await client_party.set_privacy(PartyPrivacy.PRIVATE)
        patch.assert_awaited_once()
        joinability.assert_awaited_once_with('eos-party', 'INVITE_ONLY', reraise=True)

        decline = client.http.epic_party_decline_invite = AsyncMock()
        invitation = ReceivedPartyInvitation.__new__(ReceivedPartyInvitation)
        invitation.client = client
        invitation.epic_party_id = 'eos-party'
        invitation.sender = SimpleNamespace(id='host')
        await invitation.decline()
        decline.assert_awaited_once_with('host')

    def test_crown_stats_follow_fnbr_names(self):
        meta = PartyMemberMeta(SimpleNamespace(client=self.client, id='member'))

        def stats():
            raw = json.loads(meta.schema['Default:LoadoutMeta_j'])
            return {item['statName']: item['statValue'] for item in raw['LoadoutMeta']['stats']}

        meta.set_cosmetic_loadout(victory_crowns=4)
        self.assertEqual(stats()['TotalVictoryCrowns'], 4)
        self.assertEqual(stats()['TotalRoyalRoyales'], 4)
        self.assertEqual(stats()['HasCrown'], 1)
        self.assertEqual(meta.victory_crowns, 4)
        self.assertEqual(meta.has_crown, 1)

        meta.set_cosmetic_loadout(victory_crowns=0)
        self.assertEqual(stats()['TotalVictoryCrowns'], 0)
        self.assertEqual(stats()['HasCrown'], 0)

        meta.set_cosmetic_loadout(has_crown=True)
        self.assertEqual(meta.has_crown, 1)
        self.assertEqual(stats()['TotalVictoryCrowns'], 0)

        meta.set_cosmetic_loadout(rank=9)
        self.assertEqual(meta.rank, 9)
        self.assertEqual(stats()['HabaneroProgression'], 9)

    async def test_party_island_size_and_leave_follow_fnbr(self):
        party = ClientParty(self.client, {
            'id': 'lobby', 'epic_party_id': 'eos-party', 'revision': 1,
        })
        self.assertEqual(party.meta.get_prop('Default:CurrentRegionId_s'), 'EU')
        updated = party.meta.set_selected_island('playlist_defaultduo', 3)
        island = json.loads(updated['Default:SelectedIsland_j'])
        playlist = json.loads(updated['Default:PlaylistData_j'])
        self.assertEqual(
            island['SelectedIsland']['linkId'],
            {'mnemonic': 'playlist_defaultduo', 'version': 3},
        )
        self.assertEqual(playlist['PlaylistData']['playlistName'], 'playlist_defaultduo')

        party.patch = AsyncMock()
        self.client._epic_party_revision = 4
        config = self.client.http.epic_party_set_config = AsyncMock()
        await party.set_max_size(8)
        config.assert_awaited_once_with(
            'eos-party', 'OPEN', revision=4, max_size=8,
        )
        self.assertEqual(self.client._epic_party_revision, 5)

        order = []

        async def leave_fortnite(*args, **kwargs):
            order.append('fortnite')

        async def leave_eos(*args, **kwargs):
            order.append('eos')

        self.client.http.party_leave = AsyncMock(side_effect=leave_fortnite)
        self.client.http.epic_party_leave = AsyncMock(side_effect=leave_eos)
        await party._leave()
        self.assertEqual(order, ['fortnite', 'eos'])

    def test_client_member_hides_the_default_hero(self):
        self.client.platform = Platform.PLAYSTATION_5
        party = PartyBase(self.client, {'id': 'lobby'})
        member = ClientPartyMember(self.client, party, {
            'id': 'self', 'account_id': 'self',
            'joined_at': '2026-11-24T00:00:00.000Z', 'meta': {},
        })
        hero = json.loads(member.meta.schema['Default:CampaignHero_j'])
        platform = json.loads(member.meta.schema['Default:PlatformData_j'])
        outfit = json.loads(member.meta.schema['Default:MpLoadout1_j'])
        matchmaking = json.loads(member.meta.schema['Default:MatchmakingInfo_j'])
        self.assertEqual(hero['CampaignHero']['heroType'], 'None')
        self.assertEqual(matchmaking['MatchmakingInfo']['result'], 'None')
        self.assertEqual(
            platform['PlatformData']['platform']['platformDescription']['name'],
            'PS5',
        )
        self.assertTrue(outfit['MpLoadout1']['s']['ac']['i'])

        kept = ClientPartyMember(self.client, party, {
            'id': 'self', 'account_id': 'self',
            'joined_at': '2026-11-24T00:00:00.000Z',
            'meta': {
                'Default:CampaignHero_j': json.dumps({
                    'CampaignHero': {'heroItemInstanceId': '', 'heroType': 'HID_Keep'},
                }),
            },
        })
        stored = json.loads(kept.meta.schema['Default:CampaignHero_j'])
        self.assertEqual(stored['CampaignHero']['heroType'], 'HID_Keep')

    def test_invalid_index_is_skipped_without_changing_classic_variants(self):
        classic = [{'c': 'Material', 'v': 'Mat1', 'dE': 0}]
        self.assertEqual(_variant_channels(classic), classic)
        indexed = [{'channelIndex': 'invalid'}, {}, {'channelIndex': 2, 'variantIndex': 4}]
        self.assertEqual(_variant_channels(indexed), [0, 0, 4])
