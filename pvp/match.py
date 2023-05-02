"""
Run a live match.
"""
import asyncio
import math
import random

import discord
import discord.ui as dui

import cog.util
from cog.util import ReprMixin


PVP_DEFAULT_HEX = 0x0dd42e
EMPTY = 'N/A'
BUT_CANCEL = 'Cancel'
BUT_JOIN = 'Join'
BUT_LEAVE = 'Leave'
BUT_ROLL = 'Roll teams'
MATCHES = {}
MATCH_NUM = 0
INTER_DELAY = 2
MATCH_TIMEOUT = 2 * 60 * 60


class Match(ReprMixin):
    """
    Table to store matches.
    """
    _repr_keys = ['id', 'channel_id', 'limit', 'players', 'teams']

    def __init__(self, mat_id, channel_id, *, limit=20):
        self.id = mat_id
        self.channel_id = channel_id
        self.limit = limit
        self.players = []
        self.teams = {}
        self.msg = None

    @property
    def num_players(self):
        """ The number of players in match. """
        return len(self.players)

    def add_players(self, players):
        """
        Add all named players to the match until full.
        If the teams have been rolled, balance out existing teams.

        Args:
            players: A list of player names to put into match.
        """
        while players:
            if self.num_players >= self.limit:
                break

            player = players.pop()
            if player not in self.players:
                self.players += [player]
                if self.teams:
                    self.balance_teams(player)

        self.sort_players()

    def balance_teams(self, player):
        """
        Add the named player to rolled teams in a balanced fashion.

        Args:
            player: The name of a single player to add.
        """
        teams = [1, 2]
        if len(self.teams[1]) > len(self.teams[2]):
            teams = [2]
        elif len(self.teams[1]) < len(self.teams[2]):
            teams = [1]

        team_num = random.choice(teams)
        self.teams[team_num] += [player]

    def sort_players(self):
        """ Sort players registered and in teams. """
        self.players = list(sorted(self.players, key=lambda x: x.lower()))
        for key in self.teams:
            self.teams[key] = list(sorted(self.teams[key], key=lambda x: x.lower()))

    def remove_players(self, players):
        """
        Remove all named players from the match.
        If the teams have been rolled, remove players from the match.

        Args:
            players: A list of player names to put into match.
        """
        self.players = [x for x in self.players if x not in players]
        for key in self.teams:
            self.teams[key] = [x for x in self.teams[key] if x not in players]
        self.sort_players()

    def roll_teams(self):
        """
        Randomly select players to form two random teams.
        There will be at most 1 player difference if num players is odd.
        """
        half_players = self.num_players / 2
        # When half an odd number, randomly choose which team short 1
        team1_size = random.randint(math.floor(half_players), math.ceil(half_players))
        self.teams[1] = random.sample(self.players, team1_size)
        self.teams[2] = set(self.players) - set(self.teams[1])
        self.sort_players()

        return self.teams[1], self.teams[2]

    def embed_dict(self, *, color=None):
        """
        Generate an embed that describes the current state of a match.
        It can be used with discord.Embed.from_dict to create an embed.

        Returns: A dictionary to create an Embed with.
        """
        color = color if color else PVP_DEFAULT_HEX
        embed_values = [{'name': 'Registered', 'value': '\n'.join(self.players), 'inline': True}]
        for team_num, players in self.teams.items():
            embed_values += [{'name': f'Team {team_num}', 'value': '\n'.join(players), 'inline': True}]
        embed_values = sorted(embed_values, key=lambda x: x['name'])

        return {
            'color': color,
            'author': {
                'name': 'PvP Match',
                'icon_url': cog.util.BOT.user.display_avatar.url if cog.util.BOT else None,
            },
            'provider': {
                'name': cog.util.BOT.user.name if cog.util.BOT else EMPTY,
            },
            'title': f'PVP Match ID: {self.id}, {self.num_players}/{self.limit}',
            "fields": embed_values,
        }

    async def update_msg(self, client):  # pragma: no cover
        """
        Update (or send) the message to the channel requesting a team.

        Args:
            client: The Bot itself.

        Returns: The message sent to the channel.
        """
        notice = f"This match will be automatically deleted after {MATCH_TIMEOUT // 3600} hour(s) of inactivity."
        dembed = discord.Embed.from_dict(self.embed_dict())
        if self.msg:
            await self.msg.edit(embed=dembed)
        else:
            channel = client.get_channel(self.channel_id)
            buttons = dui.View().\
                add_item(dui.Button(label=BUT_JOIN, custom_id=BUT_JOIN, style=discord.ButtonStyle.green)).\
                add_item(dui.Button(label=BUT_LEAVE, custom_id=BUT_LEAVE, style=discord.ButtonStyle.blurple)).\
                add_item(dui.Button(label=BUT_ROLL, custom_id=BUT_ROLL, style=discord.ButtonStyle.grey)).\
                add_item(dui.Button(label=BUT_CANCEL, custom_id=BUT_CANCEL, style=discord.ButtonStyle.red))
            self.msg = await channel.send(notice, embed=dembed, view=buttons)

        return self.msg

    async def delete_msg(self):  # pragma: no cover
        """
        Cleanup the match message and remove the match from storage.
        """
        try:
            del MATCHES[self.id]
        except KeyError:
            pass
        try:
            await self.msg.delete()
        except discord.DiscordException:
            pass

    async def post_creation_loop(self, client):  # pragma: no cover
        """
        Loop to handle updating this message from the provided buttons on match.

        Args:
            client: The Bot itself.
        """
        try:
            while True:
                await self.update_msg(client)

                inter = await client.wait_for(
                    'interaction',
                    check=lambda inter: inter.message == self.msg,
                    timeout=MATCH_TIMEOUT,
                )

                if inter.data['custom_id'] == BUT_JOIN:
                    await inter.response.send_message(f"Adding: {inter.user.display_name}", delete_after=INTER_DELAY)
                    self.add_players([inter.user.display_name])
                elif inter.data['custom_id'] == BUT_LEAVE:
                    await inter.response.send_message(f"Removing: {inter.user.display_name}", delete_after=INTER_DELAY)
                    self.remove_players([inter.user.display_name])
                elif inter.data['custom_id'] == BUT_ROLL:
                    await inter.response.send_message("Teams rolled", delete_after=INTER_DELAY)
                    self.roll_teams()
                elif inter.data['custom_id'] == BUT_CANCEL:
                    await inter.response.send_message("Deleting match", delete_after=INTER_DELAY)
                    await self.delete_msg()
                    break

        except asyncio.TimeoutError:
            await self.delete_msg()


def get_match(mat_id):
    """
    Get the match with the mat_id supplied.

    Returns: The Match found, None if not found.
    """
    return MATCHES.get(mat_id)


def create_match(*, channel_id=None, limit=20):
    """
    Create a match and store it.

    Args:
        channel_id: The channel ID.
        limit: The limit of players for the match.

    Returns: The Match created.
    """
    global MATCH_NUM
    MATCH_NUM += 1
    match_id = MATCH_NUM
    mat = Match(match_id, channel_id, limit=limit)
    MATCHES[MATCH_NUM] = mat

    return mat
