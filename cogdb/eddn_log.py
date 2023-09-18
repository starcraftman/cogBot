"""
cogdb.eddn_log
A loger for tracking the last n EDDN messages into a folder.
"""
import logging
import os
import pathlib
import pprint
import shutil

import cog.util


def log_fname(msg):
    """
    Given an EDDN Message, create a uniquely identifying name suitable to write to disk.

    Args:
        An EDDN Message, a nested dictionary of information that was parsed.

    Returns: A string filename to write to.
    """
    try:
        timestamp = msg['message']['timestamp']
    except KeyError:
        timestamp = msg['header']['gatewayTimestamp']

    schema = '_'.join(msg["$schemaRef"].split('/')[-2:])
    fname = f"{schema}_{timestamp}_{msg['header']['softwareName']}".strip()

    return cog.util.clean_fname(fname, replacement='_', replace_spaces=True)


class EDDNLogger():
    """
    Create an EDDN Logger to manage writing out received EDDN messages, allows a dev to inspect
    the messages and see new changes or debug errors with processing a message.
    """
    def __init__(self, *, folder, keep_n=50, reset=False, disabled=False):
        self.count = 0
        self.folder = pathlib.Path(folder)
        self.keep_n = keep_n
        self.kept_messages = []
        self.initialize(reset)
        self.disabled = disabled

    def initialize(self, reset=False):
        """
        Handle resetting the dire

        Args:
            reset: When True, recreate the messages folder to get a clean slate.

        Raises:
            OSError: One of the following situations occurred.
                - Failed to reset the directory due to permissions.
                - Folder set is a file or no permission to write into it.
                - Failed to create the folder when it didn't exist.
        """
        if reset:
            try:
                shutil.rmtree(self.folder)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).error("EDDNLog: Failed to reset folder: %s", self.folder)
                raise

        if not self.folder.exists():
            self.folder.mkdir()

        if self.folder.is_file() or not os.access(str(self.folder), os.W_OK):
            raise OSError(f"EDDNLog: Set folder is a file or unable to write to directory: {self.folder}")

    def check_kept_messages(self):
        """
        Clean up messages while total # of messages > keep_n.
        """
        while len(self.kept_messages) >= self.keep_n:
            to_remove = self.kept_messages[0]
            try:
                os.remove(self.kept_messages[0])
            except OSError:
                logging.getLogger(__name__).error("EDDNLog: Failed to remove: %s", to_remove)
            self.kept_messages = self.kept_messages[1:]

    def write_msg(self, msg):
        """
        Given an EDDN Message, write it out to the disk prettily for logging purposes.
        Ensure before writing that we only keep the last keep_n messages.

        Args:
            msg: An EDDN Message.

        Returns: The path of the written log file or None if disabled.
        """
        if self.disabled:
            return None

        self.check_kept_messages()
        fpath = self.folder / f"{self.count:03}_{log_fname(msg)}"
        self.kept_messages.append(fpath)

        with open(fpath, 'w', encoding='utf-8') as fout:
            pprint.pprint(msg, stream=fout)

        self.count = (self.count + 1) % self.keep_n
        return fpath
