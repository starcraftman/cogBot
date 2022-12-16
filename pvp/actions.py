"""
Specific actions for pvp bot
"""
import discord

import cog.tbl
# Make available reused actions
from cog.actions import (Action, Donate, Feedback, Near, Repair, Route, Status, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import


class Help(Action):
    """
    Provide an overview of help.
    """
    async def execute(self):
        prefix = self.bot.prefix
        overview = '\n'.join([
            'Here is an overview of my commands.',
            '',
            f'For more information do: `{prefix}Command -h`',
            f'       Example: `{prefix}drop -h`'
            '',
        ])
        lines = [
            ['Command', 'Effect'],
            ['{prefix}dist', 'Determine the distance from the first system to all others'],
            ['{prefix}donate', 'Information on supporting the dev.'],
            ['{prefix}feedback', 'Give feedback or report a bug'],
            ['{prefix}help', 'This help command.'],
            ['{prefix}near', 'Find things near you.'],
            ['{prefix}repair', 'Show the nearest orbitals with shipyards'],
            ['{prefix}route', 'Plot the shortest route between these systems'],
            ['{prefix}status', 'Info about this bot'],
            ['{prefix}time', 'Show game time and time to ticks'],
            ['{prefix}trigger', 'Calculate fort and um triggers for systems'],
            ['{prefix}whois', 'Search for commander on inara.cz'],
        ]
        lines = [[line[0].format(prefix=prefix), line[1]] for line in lines]

        response = overview + cog.tbl.format_table(lines, header=True)[0]
        await self.bot.send_ttl_message(self.msg.channel, response)
        try:
            await self.msg.delete()
        except discord.HTTPException:
            pass
