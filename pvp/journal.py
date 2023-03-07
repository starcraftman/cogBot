"""
Simple parser for Frontier's Player Journal written out while playing Elite: Dangerous.

Provide ability to read, parse and insert events into the database.

Reference:
    https://edcodex.info/?m=doc
"""
import asyncio
import datetime
import functools
import json
import logging
import pathlib
import re
import shutil
import zipfile

import aiofiles
import discord
import sqlalchemy as sqla

import cogdb
import cogdb.eddb
from cogdb.eddb import LEN as EDDB_LEN
from cogdb.spy_squirrel import ship_type_to_id_map
import cog.inara
from cog.util import TIME_STRP, DISCORD_RATE_LIMIT
import pvp.schema

COMBAT_RANK_TO_VALUE = {x: ind for ind, x in enumerate(cog.inara.COMBAT_RANKS)}
VALUE_TO_COMBAT_RANK = {y: x for x, y in COMBAT_RANK_TO_VALUE.items()}
PARSED_EVENTS = [
    'Fileheader', 'FileHeader', 'Location', 'FSDJump', 'SupercruiseEntry', 'SupercruiseExit',
    'PVPKill', 'Died', 'Interdiction', 'Interdicted', 'EscapeInterdiction',
]


class ParserError(Exception):
    """
    Simple exception to denote unsupported parsing.
    """


def parse_died(eddb_session, data):
    """
    Parse Died messages in log file.
    Handles both single death and wing deaths.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    is_wing_kill = "Killers" in data
    data = clean_died_killers(data)
    if not data.get("Killers"):
        return None

    try:
        death = eddb_session.query(pvp.schema.PVPDeath).\
            filter(pvp.schema.PVPDeath.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPDeath.system_id == data['system_id'],
                   pvp.schema.PVPDeath.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        death = pvp.schema.PVPDeath(
            cmdr_id=data['cmdr_id'],
            system_id=data['system_id'],
            is_wing_kill=is_wing_kill,
            event_at=data['event_at'],
        )
        eddb_session.add(death)
        eddb_session.flush()

        ship_map = ship_name_map()
        for killer in data["Killers"]:
            try:
                ship_id = ship_map[killer['Ship'].lower()]
            except KeyError:
                ship_id = ship_map['sidewinder']
                logging.getLogger(__name__).error("Could not map ship named: %s", killer['Ship'])

            eddb_session.add(
                pvp.schema.PVPDeathKiller(
                    cmdr_id=data['cmdr_id'],
                    ship_id=ship_id,
                    pvp_death_id=death.id,
                    name=clean_cmdr_name(killer['Name']),
                    rank=COMBAT_RANK_TO_VALUE[killer['Rank']],
                    event_at=data['event_at'],
                )
            )
            eddb_session.flush()

    return death


def parse_kill(eddb_session, data):
    """
    Parse PVPKill messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    try:
        kill = eddb_session.query(pvp.schema.PVPKill).\
            filter(pvp.schema.PVPKill.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPKill.system_id == data['system_id'],
                   pvp.schema.PVPKill.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        kill = pvp.schema.PVPKill(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
            victim_name=clean_cmdr_name(data['Victim']),
            victim_rank=data.get('CombatRank'),
            event_at=data['event_at'],
        )
        eddb_session.add(kill)
        eddb_session.flush()

    return kill


def parse_interdiction(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    if not data['IsPlayer'] or not data.get('Interdicted'):
        return None

    try:
        interdiction = eddb_session.query(pvp.schema.PVPInterdiction).\
            filter(pvp.schema.PVPInterdiction.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPInterdiction.system_id == data['system_id'],
                   pvp.schema.PVPInterdiction.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        interdiction = pvp.schema.PVPInterdiction(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
            victim_name=clean_cmdr_name(data['Interdicted']),
            is_player=data['IsPlayer'],
            is_success=data['Success'],
            victim_rank=data.get('CombatRank'),
            event_at=data['event_at'],
        )
        eddb_session.add(interdiction)
        eddb_session.flush()

    return interdiction


def parse_interdicted(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    if not data['IsPlayer'] or not data.get('Interdictor'):
        return None

    try:
        interdicted = eddb_session.query(pvp.schema.PVPInterdicted).\
            filter(pvp.schema.PVPInterdicted.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPInterdicted.system_id == data['system_id'],
                   pvp.schema.PVPInterdicted.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        interdicted = pvp.schema.PVPInterdicted(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
            did_submit=data['Submitted'],
            is_player=data['IsPlayer'],
            interdictor_name=clean_cmdr_name(data['Interdictor']),
            interdictor_rank=data.get('CombatRank'),
            event_at=data['event_at'],
        )
        eddb_session.add(interdicted)
        eddb_session.flush()

    return interdicted


def parse_escaped_interdiction(eddb_session, data):
    """
    Parse the EscapedInterdiction messages in the log file.
    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    if not data['IsPlayer'] or not data.get('Interdictor'):
        return None

    try:
        escape = eddb_session.query(pvp.schema.PVPEscapedInterdicted).\
            filter(pvp.schema.PVPEscapedInterdicted.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPEscapedInterdicted.system_id == data['system_id'],
                   pvp.schema.PVPEscapedInterdicted.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        escape = pvp.schema.PVPEscapedInterdicted(
            cmdr_id=data['cmdr_id'],
            system_id=data['system_id'],
            event_at=data['event_at'],
            interdictor_name=data['Interdictor'],
            is_player=data['IsPlayer'],
        )
        eddb_session.add(escape)
        eddb_session.flush()

    return escape


def parse_location(eddb_session, data):
    """
    Parse the Location messages in the log file.
    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    try:
        location = eddb_session.query(pvp.schema.PVPLocation).\
            filter(pvp.schema.PVPLocation.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPLocation.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        try:
            found, _ = cogdb.eddb.get_all_systems_named(eddb_session, [data['StarSystem']])
            location = pvp.schema.PVPLocation(
                cmdr_id=data['cmdr_id'],
                system_id=found[0].id,
                event_at=data['event_at'],
            )
            eddb_session.add(location)
            eddb_session.flush()
        except IndexError:
            location = None

    return location


def parse_cmdr_name(data):
    """
    Scan a line of log looking for possible commander name.

    Args:
        data: The JSON data.

    Returns: The CMDR name if found, otherwise None.
    """
    cmdr = None

    if data['event'] == 'LoadGame':
        cmdr = data['Commander']
    elif data['event'] == 'Commander':
        cmdr = data['Name']

    return cmdr


def link_interdiction_to_kill(eddb_session, interdiction, kill):
    """
    Link a PVPInterdiction event to a PVPKill event.

    Args:
        eddb_session: A session onto the EDDB db.
        interdiction: An PVPInterdiction event.
        kill: A PVPKill event.

    Returns: The new or existing PVPInterdictionKill object.
    """
    try:
        linked_event = eddb_session.query(pvp.schema.PVPInterdictionKill).\
            filter(pvp.schema.PVPInterdictionKill.pvp_interdiction_id == interdiction.id,
                   pvp.schema.PVPInterdictionKill.pvp_kill_id == kill.id).\
            one()
    except sqla.exc.NoResultFound:
        linked_event = pvp.schema.PVPInterdictionKill(
            cmdr_id=kill.cmdr_id,
            pvp_interdiction_id=interdiction.id,
            pvp_kill_id=kill.id,
            event_at=kill.event_at,
        )
        eddb_session.add(linked_event)
        interdiction.survived = False
        eddb_session.flush()

    return linked_event


def link_interdiction_to_death(eddb_session, interdiction, death):
    """
    Link a PVPInterdiction event to a PVPDeath event.

    Args:
        eddb_session: A session onto the EDDB db.
        interdiction: An PVPInterdiction event.
        death: A PVPDeath event.

    Returns: The new or existing PVPInterdictionKill object.
    """
    try:
        linked_event = eddb_session.query(pvp.schema.PVPInterdictionDeath).\
            filter(pvp.schema.PVPInterdictionDeath.pvp_interdiction_id == interdiction.id,
                   pvp.schema.PVPInterdictionDeath.pvp_death_id == death.id).\
            one()
    except sqla.exc.NoResultFound:
        linked_event = pvp.schema.PVPInterdictionDeath(
            cmdr_id=death.cmdr_id,
            pvp_interdiction_id=interdiction.id,
            pvp_death_id=death.id,
            event_at=death.event_at,
        )
        eddb_session.add(linked_event)
        eddb_session.flush()

    return linked_event


def link_interdicted_to_kill(eddb_session, interdicted, kill):
    """
    Link a PVPInterdicted event to a PVPKill event.

    Args:
        eddb_session: A session onto the EDDB db.
        interdicted: An PVPInterdicted event.
        kill: A PVPKill event.

    Returns: The new or existing PVPInterdictedKill object.
    """
    try:
        linked_event = eddb_session.query(pvp.schema.PVPInterdictedKill).\
            filter(pvp.schema.PVPInterdictedKill.pvp_interdicted_id == interdicted.id,
                   pvp.schema.PVPInterdictedKill.pvp_kill_id == kill.id).\
            one()
    except sqla.exc.NoResultFound:
        linked_event = pvp.schema.PVPInterdictedKill(
            cmdr_id=kill.cmdr_id,
            pvp_interdicted_id=interdicted.id,
            pvp_kill_id=kill.id,
            event_at=kill.event_at,
        )
        eddb_session.add(linked_event)
        eddb_session.flush()

    return linked_event


def link_interdicted_to_death(eddb_session, interdicted, death):
    """
    Link a PVPInterdicted event to a PVPDeath event.

    Args:
        eddb_session: A session onto the EDDB db.
        interdiction: An PVPInterdicted event.
        death: A PVPDeath event.

    Returns: The new or existing PVPInterdictionKill object.
    """
    try:
        linked_event = eddb_session.query(pvp.schema.PVPInterdictedDeath).\
            filter(pvp.schema.PVPInterdictedDeath.pvp_interdicted_id == interdicted.id,
                   pvp.schema.PVPInterdictedDeath.pvp_death_id == death.id).\
            one()
    except sqla.exc.NoResultFound:
        linked_event = pvp.schema.PVPInterdictedDeath(
            cmdr_id=death.cmdr_id,
            pvp_interdicted_id=interdicted.id,
            pvp_death_id=death.id,
            event_at=death.event_at,
        )
        interdicted.survived = False
        eddb_session.add(linked_event)
        eddb_session.flush()

    return linked_event


def datetime_to_tstamp(date_string):
    """
    Convert a tiemstamp string in a log line to a simpler integer timestamp
    for storage in database.

    Args:
        date_string: The string containing a date of format specified in cogdb.eddn.TIME_STRP

    Returns:
        A float representation of the datetime parsed.

    Raises:
        ValueError: The datetime did not conform to expected format.
    """
    try:
        return datetime.datetime.strptime(date_string, TIME_STRP).replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        logging.getLogger(__name__).error("Malformed timestamp on log line: %s", date_string)
        raise


def get_event_parser(data):
    """
    Lookup the required parser for a given event in the data.

    Args:
        eddb_session: A session onto the db.
        data: The JSON data object.
    """
    event = data['event']
    if event in EVENT_TO_PARSER:
        return event, EVENT_TO_PARSER[event]

    raise ParserError(f"No parser configured for: {event}")


class Parser():
    """
    Parse a given journal fragment.
    """
    def __init__(self, *, fname, cmdr_id, eddb_session):
        self.fname = fname
        self.cmdr_id = cmdr_id
        self.eddb_session = eddb_session
        self.lines = None
        self.data = {}  # Scratch space for objects

    def load(self):
        """
        Load and cleanup the lines from the journal fragment.
        """
        with open(self.fname, 'r', encoding='utf-8') as fin:
            self.lines = fin.readlines()

    def parse_line(self, line):
        """
        Parse a single line event and make required changes to both the database
        and the tracked data that assists in parsing forward.

        Args:
            line: The line from the journal uploaded.

        Returns: None if no parsing was possible. If success, returns the parsed event object in database.
        """
        result = None
        try:
            for loaded in json.loads(f'[ {line} ] '):
                loaded.update({
                    'event_at': datetime_to_tstamp(loaded['timestamp']),
                    'cmdr_id': self.cmdr_id,
                    'system_id': self.data['Location'].system_id if self.data.get('Location') else None,
                })
                event, parser = get_event_parser(loaded)

                # If a CMDR supercruises away reset events tracking, still same location
                if event == 'SupercruiseEntry':
                    self.data = {'Location': self.data.get('Location')}
                if event in ['SupercruiseEntry', 'SupercruiseExit']:  # Still same location
                    return result

                result = parser(self.eddb_session, loaded)
                self.post_parsing(event, result)
        except json.decoder.JSONDecodeError:
            logging.getLogger(__name__).error("Failed to JSON decode line: %s", line)
        except (ParserError, ValueError) as exc:
            logging.getLogger(__name__).debug(str(exc))
        except sqla.exc.IntegrityError:
            self.eddb_session.rollback()
            logging.getLogger(__name__).warning("Duplicate Event: %s", line)

        return result

    def post_parsing(self, event, result):
        """
        By tracking previous events in db between jump events, link events together.
        For instance, if a CMDR interdicts another ship then kills it, that is a PVPInterdictedKill, that will
        link to the individual PVPInterdiction and PVPKill records.

        Args:
            event: The event that triggered the log.
            result: The parsed database object.
        """
        # Link events that were connected for later statistics
        if result and event == 'PVPKill' and self.data.get('Interdiction') and\
                result.victim_name == self.data['Interdiction'].victim_name and\
                result.event_at >= self.data['Interdiction'].event_at:
            link_interdiction_to_kill(self.eddb_session, self.data['Interdiction'], result)

        elif result and event == 'Died' and self.data.get('Interdiction') and\
                result.killed_by(self.data['Interdiction'].victim_name) and\
                result.event_at >= self.data['Interdiction'].event_at:
            link_interdiction_to_death(self.eddb_session, self.data['Interdiction'], result)

        elif result and event == 'PVPKill' and self.data.get('Interdicted') and\
                result.victim_name == self.data['Interdicted'].interdictor_name and\
                result.event_at >= self.data['Interdicted'].event_at:
            link_interdicted_to_kill(self.eddb_session, self.data['Interdicted'], result)

        elif result and event == 'Died' and self.data.get('Interdicted') and\
                result.killed_by(self.data['Interdicted'].interdictor_name) and\
                result.event_at >= self.data['Interdicted'].event_at:
            link_interdicted_to_death(self.eddb_session, self.data['Interdicted'], result)

        # Always store event in data for later use
        if event in ['Interdiction', 'Interdicted', 'PVPKill']:
            self.data[event] = result

        # When changing location reset data and set Locaation
        elif event in ['Location', 'FSDJump']:
            self.data.clear()
            self.data['Location'] = result

        # When dying all connections reset
        elif event == 'Died':
            self.data.clear()

    def parse(self):
        """
        Parse all possible events from the journal fragment.

        Returns: All parsed main events.
        """
        to_return = []
        for line in self.lines:
            returned = self.parse_line(line)
            if returned:
                to_return += [returned]

        pvp.schema.update_pvp_stats(self.eddb_session, cmdr_id=self.cmdr_id)

        return to_return


async def find_cmdr_name(fname):
    """
    Given a journal file, scan until you can identify the CMDR name.

    Args:
        fname: The filename of the journal.

    Returns: The CMDR name if found printed in log, otherwise None if not found.
    """
    async with aiofiles.open(fname, 'r', encoding='utf-8') as fin:
        async for line in fin:
            try:
                for event in json.loads(f'[ {line} ] '):
                    cmdr_name = parse_cmdr_name(event)
                    if cmdr_name:
                        return cmdr_name
            except json.decoder.JSONDecodeError:
                logging.getLogger(__name__).error("Impossible to parse: %s", line)

    return None


def filter_log(fname, filtered_log, *, events=None):
    """
    filter a log file to retain only those events required.
    Preserve the order of the events of original while writing matching lines to output.

    Args:
        fname: The filename to filter.
        filtered_log: The output filename to write to.
        events: A list of strings, the names of events to look for.

    Returns: The filename of the filtered log.
    """
    if not events:
        events = PARSED_EVENTS

    events_str = '|'.join(events)
    rex = re.compile(f'event":"({events_str})"')

    with open(fname, 'r', encoding='utf-8') as fin, open(filtered_log, 'w', encoding='utf-8') as fout:
        for line in fin:
            if rex.search(line):
                fout.write(line)

    return filtered_log


def filter_archive(fname, *, output_d, filtered_archive, events=None):
    """
    filter a zipfile to retain only those events required.
    Preserve the order of the events of original while writing matching lines to output.
    The archive will be created as output, containing all the original files filtered.

    Args:
        fname: The zipfile to filter.
        output_d: The output filename archive name to write to.
        filtered_archive: The filename of the output archive with filtered logs.
        events: A list of strings, the names of events to look for.

    Returns: The filename of the new filtered archive.
    """
    new_archive = None
    try:
        dest = output_d / str(filtered_archive).replace('.zip', '')
        dest.mkdir()
        with cog.util.extracted_archive(fname) as logs:
            for log in logs:
                if cog.util.is_log_file(log):  # Ignore non valid logs
                    filtered_fname = dest / log.name.replace('.log', '.filter.log')
                    filter_log(log, filtered_fname, events=events)
        shutil.make_archive(dest, 'zip', dest.parent, dest.name)
        shutil.rmtree(dest)
        new_archive = f'{dest}.zip'

    except (zipfile.BadZipfile, OSError) as exc:
        logging.getLogger(__name__).error("Critial Error: %s", exc)
        raise

    return new_archive


async def filter_tempfile(*, pool, dest_dir, fname, output_fname, attach_fname):
    """
    filter the tempfile, depending on what is in the attachment.
    Handles both zipfiles and normal text logfiles.

    Args:
        pool: A ProcessPoolExecutor.
        dest_dir: The destination directory holding all filtered logs.
        fname: The tempfile storing the log file or archive to filter.
        output_fname: The output filename to output work to.

    Raises:
        pvp.journal.ParserError - The saved attachment is not supported.

    Returns: A coro if one was started to process an archive. Otherwise None.
    """
    func = None
    if await cog.util.is_log_file_async(fname):
        func = functools.partial(
            filter_log,
            fname, dest_dir / output_fname
        )

    elif await cog.util.is_zipfile_async(fname):
        func = functools.partial(
            filter_archive,
            fname, output_d=dest_dir, filtered_archive=output_fname
        )

    else:
        raise pvp.journal.ParserError(f"Unsupported file uploaded. Please try zip or log file. You uploaded: {attach_fname}")

    return asyncio.get_event_loop().run_in_executor(pool, func)


def group_filtered_logs(*, filtered_logs, size_limit=cog.util.DISCORD_FILE_LIMIT):
    """
    Map filtered logs onto groupings less than size_limit.
    Groups will be comprised of at least 1 file and will be under size_limit unless
    the single file exceeds it.

    Args:
        filtered_logs: The list of objects to group by local size of file, of form: [{'fname': <path>}]
        size_limit: The limit of total size of all files in a grouping.

    Returns: The groupings of filtered_logs records.
    """
    group, groups = [], []
    total_size = 0

    for log in filtered_logs:
        fname = pathlib.Path(log['fname'])
        size = fname.stat().st_size

        if group and total_size + size > size_limit:
            groups += [group]
            group = []
            total_size = 0

        total_size += size
        group += [log]

    if group:
        groups += [group]

    return groups


def archive_filtered_logs(*, target_dir, base_name, grouped_logs):
    """
    Archive a collection of filtered logs associated to one CMDR.
    Break the archives into the LEAST amount that fit under cog.util.DISCORD_FILE_LIMIT

    Args:
        target_dir: The target directory to put the archive in.
        base_name: The base filename without extension to name the archive.
        grouped_logs: The groupings of logs to map onto archives.
    """
    mapped_archives = {}

    for num, group in enumerate(grouped_logs):
        ddir = pathlib.Path(target_dir) / f'{base_name}_{num:02}'
        ddir.mkdir()
        for rec in group:
            shutil.copy2(rec['fname'], ddir)

        shutil.make_archive(ddir, 'zip', ddir.parent, ddir.name)

        archive = ddir.parent / (ddir.name + '.zip')
        mapped_archives[str(archive)] = group

    return mapped_archives


async def upload_filtered_archives(*, filter_chan, cmdr, archives):  # pragma: no cover, not worth testing
    """
    Upload the generated filter file to the log channel.
    If existing filtered files are already set in the database, delete and nullify those.

    Args:
        log_chan: The log channel to push the filtered logs to.
        filtered_archives: A list of archive objects as created by archive_filtered_logs.
    """
    for archive, records in archives.items():
        archive_msg = await filter_chan.send(upload_text(cmdr), file=discord.File(fp=archive))

        # Update all pvplogs for files in this archive to point to the uploaded msg
        for record in records:
            record['pvplog'].filtered_msg_id = archive_msg.id

        await asyncio.sleep(DISCORD_RATE_LIMIT)


async def purge_uploaded_logs(*, log_chan, cmdr_id):  # pragma: no cover, destructive to test
    """
    Purge any uploaded logs from the channel matching a given cmdr_id.
    Warning: This will be slow going due to rate limits.

    Args:
        log_chan: A valid discord.TextChannel to search.
        cmdr_id: The cmdr id to match on.
    """
    async for msg in log_chan.history(limit=1000000, oldest_first=True):
        if not msg.content.startswith('Discord ID:'):
            continue

        msg_did = int(msg.content.split('\n')[0].replace('Discord ID: ', ''))
        if msg_did == cmdr_id:
            await msg.delete()
            await asyncio.sleep(cog.util.DISCORD_RATE_LIMIT)

        await asyncio.sleep(cog.util.DISCORD_RATE_LIMIT / 10)


def upload_text(cmdr):
    """
    Generate the message content for an upload of a log.

    Args:
        cmdr: A PVPCmdr.

    Returns: A string.
    """
    return f"Discord ID: {cmdr.id}\nCMDR: {cmdr.name}"


def clean_cmdr_name(name):
    """
    Clean cmdr name for storage in db.
        - Strip off any leading 'cmdr'
        - Remove any extra whitespace
        - Limit to width of db field

    Args:
        name: The name of the cmdr to clean

    Returns: Cleaned cmdr name.
    """
    if name.lower().startswith('cmdr'):
        name = name[4:].strip()

    return name[:EDDB_LEN['pvp_name']]


def clean_died_killers(data):
    """
    Prune all entries that are NPCs.

    Args:
        data: The JSON object of data.

    Returns: The cleaned list of dillers.
    """
    if 'KillerName' in data and not data.get('Killers'):  # Reformat object so single case same as wing
        data['Killers'] = [{
            "Name": data["KillerName"],
            "Ship": data["KillerShip"],
            "Rank": data.get("KillerRank", 'Harmless'),
        }]

    cleaned = []
    for killer in data.get('Killers', []):
        try:
            if not killer['Name'].startswith('$'):
                cleaned += [killer]
        except KeyError:
            pass
    data['Killers'] = cleaned

    return data


def ship_name_map():
    """
    Generate a map of possibly seen names to IDs in the db.

    Returns: A dictionary of names onto ids.
    """
    if not CACHED.get('ship_map'):
        ship_map = ship_type_to_id_map(traffic_text=True)
        ship_map.update(ship_type_to_id_map(traffic_text=False))
        ship_map.update({key.lower(): value for key, value in ship_map.items() if key[0].isupper()})
        CACHED['ship_map'] = ship_map

    return CACHED['ship_map']


CACHED = {}
EVENT_TO_PARSER = {
    "Died": parse_died,
    "EscapeInterdiction": parse_escaped_interdiction,
    "FSDJump": parse_location,
    "Interdicted": parse_interdicted,
    "Interdiction": parse_interdiction,
    "SupercruiseEntry": None,
    "SupercruiseExit": None,
    "Location": parse_location,
    "PVPKill": parse_kill,
}
