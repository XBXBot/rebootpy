# -*- coding: utf-8 -*-

"""
MIT License

Copyright (c) 2019-2021 Terbau

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

import re
import datetime
import json

from typing import TYPE_CHECKING

from .errors import Forbidden, PartyError
from .enums import Platform, AwayStatus


def _parse_presence_value(value):
    """Decode an EOS presence property prefix (``b``, ``i``, ``m`` or ``s``)."""
    if not isinstance(value, str) or not value:
        return value

    kind, raw = value[0], value[1:]
    if kind == 'b':
        return raw == 'true'
    if kind == 'i':
        try:
            return int(raw)
        except ValueError:
            return None
    if kind == 'm':
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return None
    if kind == 's':
        return raw
    return value


def _presence_int(value):
    try:
        return int(value) if isinstance(value, (str, int)) else None
    except ValueError:
        return None


def _presence_platform(value):
    try:
        return Platform(value) if value else None
    except (ValueError, TypeError):
        return None

if TYPE_CHECKING:
    from .client import Client
    from .friend import Friend
    from .party import ClientParty


class PresenceGameplayStats:
    """Represents gameplaystats received from presence.

    Attributes
    ----------
    friend: :class:`Friend`
        The friend these stats belong to.
    state: :class:`str`
        The state.

        .. note::

            It's not really known what value this property might
            hold. This is pretty much always an empty string.
    playlist: :class:`str`
        The playlist.

        .. note::

            The playlist from the gameplay stats property usually
            isn't updated. Consider using :attr:`Presence.playlist` instead
            as that seems to always be the correct playlist.
    players_alive: :class:`int`
        The amount of players alive in the current game.
    kills: :class:`int`
        The amount of kills the friend currently has. Aliased to ``num_kills``
        as well for legacy reasons.
    fell_to_death: :class:`bool`
        ``True`` if friend fell to death in its current game, else ``False``
    """

    __slots__ = ('friend', 'state', 'playlist', 'players_alive', 'kills',
                 'num_kills', 'fell_to_death')

    def __init__(
        self,
        friend: 'Friend',
        data: str,
        players_alive: int,
        playlist: str
    ) -> None:
        self.friend = friend
        self.state = data.get('state')
        self.playlist = playlist
        self.players_alive = players_alive

        self.kills = data.get('numKills')
        if self.kills is not None:
            self.kills = int(self.kills)

        self.num_kills = self.kills

        self.fell_to_death = True if data.get('bFellToDeath') else False

    def __repr__(self) -> str:
        return ('<PresenceGameplayStats friend={0.friend!r} '
                'players_alive={0.players_alive} num_kills={0.num_kills} '
                'playlist={0.playlist!r}>'.format(self))


class PresenceParty:
    """Represents a party received from presence.

    Before accessing any of this class' attributes or functions
    you should always check if the party is private: ::

        @client.event
        async def event_friend_presence(before, after):
            # after is the newly received presence
            presence = after

            # check if presence is from the account 'Terbau'
            # NOTE: you should always use id over display_name
            # but for this example i've use display_name just
            # to demonstrate.
            if presence.friend.display_name != 'Terbau':
                return

            # check if party is private
            if presence.party.private:
                return

            # if all the checks above succeeds we join the party
            await presence.party.join()


    .. note::

        If the party is private, all attributes below private will
        be ``None``.

    Attributes
    ----------
    client: :class:`str`
        The client.
    private: :class:`bool`
        ``True`` if the party is private else ``False``.
    platform: :class:`Platform`
        The platform of the friend.
    id: :class:`str`
        The party's id.
    party_type_id: :class:`str`
        The party's type id.
    app_id: :class:`str`
        The party's app id.
    build_id: :class:`str`
        The party's build id. Similar format to :attr:`Client.party_build_id`.
    net_cl: :class:`str`
        The party's net_cl. Similar format to :attr:`Client.net_cl`.
    party_flags: :class:`str`
        The party's flags.
    not_accepting_reason: :class:`str`
        The party's not accepting reason.
    playercount: :class:`int`
        The party's playercount.
    """

    __slots__ = ('client', 'private', 'platform', 'id', 'party_type_id',
                 'app_id', 'build_id', 'net_cl', 'party_flags',
                 'not_accepting_reason', 'playercount', 'raw')

    def __init__(
        self,
        client: 'Client',
        data: dict
    ) -> None:
        self.client = client
        self.raw = data

        # In order to even get here, the key party.joininfodata.286331153
        # needs to exist, so we know the party type id is always 286331153
        self.party_type_id = 286331153

        self.platform = _presence_platform(data.get('sP'))
        self.private = data.get('bIsPrivate', False)
        self.id = data.get('partyId') or data.get('p')
        self.app_id = data.get('appId') or data.get('d')
        self.build_id = data.get('buildId') or data.get('b')

        if isinstance(self.build_id, str) and self.build_id.startswith('1:3:'):
            self.net_cl = self.build_id[4:]
        else:
            self.net_cl = None

        self.party_flags = data.get('partyFlags')
        if self.party_flags is None:
            self.party_flags = data.get('f')
        self.not_accepting_reason = data.get('notAcceptingReason')
        if self.not_accepting_reason is None:
            self.not_accepting_reason = data.get('nAR')
        self.playercount = _presence_int(data.get('pc'))

    def __repr__(self) -> str:
        return ('<PresenceParty private={0.private} id={0.id!r} '
                'playercount={0.playercount}>'.format(self))

    async def join(self) -> 'ClientParty':
        """|coro|

        Joins the friends' party.

        Raises
        ------
        PartyError
            You are already a member of this party.
        Forbidden
            The party is private.
        HTTPException
            Something else went wrong when trying to join this party.

        Returns
        -------
        :class:`ClientParty`
            The party that was just joined.
        """
        if self.client.party is not None and (
            self.client.party.id == self.id
            or self.client.party.epic_party_id == self.id
        ):
            raise PartyError('You are already a member of this party.')

        if self.private or not isinstance(self.id, str) or not self.id:
            raise Forbidden('You cannot join a private party.')

        if not self.client.disable_epic_party_service and re.fullmatch(
            r'[0-9a-fA-F]{32}(?:-\d+-default)?', self.id
        ):
            return await self.client.join_party(self.id)

        return await self.client.join_legacy_party(self.id)


class Presence:
    """Represents a presence received from a friend

    Attributes
    ----------
    client: :class:`Client`
        The client.
    away: :class:`AwayStatus`
        The users away status.
    friend: :class:`Friend`
        The friend you received this presence from.
    platform: :class:`Platform`
        The platform this presence was sent from.
    received_at: :class:`datetime.datetime`
        The UTC time of when the client received this presence.
    status: :class:`str`
        The friend's status.
    joinable: :class:`bool`
        Says if friend is joinable.
    session_id: :class:`str`
        The friend's current session id. Often referred to as
        server key or game key. Returns ``None`` if the friend is not currently
        in a game.
    has_properties: :class:`bool`
        ``True`` if the presence has properties else ``False``.

        .. warning::

            All attributes below this point will be ``None`` if
            :attr:`has_properties` is ``False``.
    party: :class:`PresenceParty`
        The friend's party.
    gameplay_stats: Optional[:class:`PresenceGameplayStats`]
        The friend's gameplay stats. Will be ``None`` if no gameplay stats
        are currently available.
    homebase_rating: :class:`str`
        The friend's homebase rating
    lfg: :class:`bool`
        ``True`` if the friend is currently looking for a game.
    sub_game: :class:`int`
        The friend's current subgame.
    in_unjoinable_match: :class:`bool`
        ``True`` if friend is in unjoinable match else ``False``.
    playlist: :class:`str`
        The friend's current playlist.
    party_size: :class:`int`
        The size of the friend's party.
    max_party_size: :class:`int`
        The max size of the friend's party.
    server_player_count: :class:`str`
        The playercount of the friend's server.
    island_code: :class:`str`
        The friend's current experience, playlist or island code.
    """

    __slots__ = ('client', 'away', 'friend', 'platform',
                 'received_at', 'status', 'joinable',
                 'session_id', 'has_properties', 'homebase_rating', 'lfg',
                 'sub_game', 'in_unjoinable_match', 'playlist', 'party_size',
                 'max_party_size', 'server_player_count',
                 'gameplay_stats', 'party', 'island_code')

    def __init__(
        self,
        client: 'Client',
        raw: dict
    ) -> None:
        self.client = client
        raw = raw if isinstance(raw, dict) else {}
        from_id = raw.get('accountId')
        per_ns = raw.get('perNs')
        if not isinstance(per_ns, list) or not per_ns:
            data = {}
        else:
            data = next(
                (
                    entry for entry in per_ns
                    if isinstance(entry, dict) and (
                        entry.get('productId') == 'Fortnite'
                        or entry.get('ns') == client.deployment_id
                    )
                ),
                per_ns[0] if isinstance(per_ns[0], dict) else {},
            )

        self.friend = self.client.get_friend(from_id) if from_id else None
        self.received_at = datetime.datetime.utcnow()
        online_type = raw.get('status') or data.get('status')
        self.away = {
            'away': AwayStatus.AWAY, 'xa': AwayStatus.EXTENDED_AWAY
        }.get(online_type, AwayStatus.ONLINE)

        activity = data.get('activity')
        self.status = activity.get('value') if isinstance(activity, dict) else None
        if 'activity' not in data and data.get('status') not in ('online', 'away', 'xa', 'offline', 'dnd'):
            self.status = data.get('status')

        props = data.get('props')
        if not isinstance(props, dict):
            props = {}

        def prop(name):
            if name not in props:
                return None
            value = props[name]
            if isinstance(name, str) and name.startswith('EOS_'):
                return value
            return _parse_presence_value(value)

        self.has_properties = props != {}

        self.platform = _presence_platform(prop('EOS_Platform'))

        self.session_id = prop('SessionIdAttributeKey') or None

        basic_info = prop('FortBasicInfo')
        if not isinstance(basic_info, dict):
            basic_info = {}
        self.homebase_rating = basic_info.get('homeBaseRating')

        lfg = _presence_int(prop('FortLFG'))
        self.lfg = None if lfg is None else lfg == 1

        self.sub_game = prop('FortSubGame')

        unjoinable = prop('InUnjoinableMatch')
        if unjoinable is None:
            self.in_unjoinable_match = None
        elif isinstance(unjoinable, bool):
            self.in_unjoinable_match = unjoinable
        else:
            self.in_unjoinable_match = str(unjoinable) == 'true'

        self.playlist = prop('GamePlaylistName')
        self.island_code = prop('IslandCode')

        players_alive = _presence_int(prop('Event_PlayersAlive'))
        self.party_size = _presence_int(prop('FortPartySize'))
        if self.party_size is None:
            self.party_size = _presence_int(prop('Event_PartySize'))
        self.max_party_size = _presence_int(prop('Event_PartyMaxSize'))
        self.server_player_count = _presence_int(prop('ServerPlayerCount'))

        gameplay = prop('FortGameplayStats')
        if isinstance(gameplay, dict):
            self.gameplay_stats = PresenceGameplayStats(
                self.friend,
                gameplay,
                players_alive,
                self.playlist
            )
        else:
            self.gameplay_stats = None

        key = "party.joininfodata.286331153"
        party_data = prop(key)
        if not isinstance(party_data, dict):
            self.party = None
            self.joinable = None
        else:
            self.party = PresenceParty(
                self.client,
                party_data
            )
            self.joinable = bool(self.party.id) and not self.party.private

    def __repr__(self) -> str:
        return (
            f'<Presence friend={self.friend!r} away={self.away} '
            f'received_at={self.received_at!r}>'
        )
