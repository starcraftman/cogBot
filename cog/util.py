"""
Utility functions
-----------------
    BOT - Global reference to cog.bot.CogBot set post startup.
    get_config - Pull out configuration information.
    rel_to_abs - Convert relative paths to rooted at project ones.
    init_logging - Project wide logging initialization.
    msg_splitter - Long message splitter, not ideal.
    pastebin_new_paste - Upload something to pastebin.
"""
import asyncio
import logging
import logging.handlers
import logging.config
import os
import re

import aiohttp
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import cog.exc

BOT = None
MSG_LIMIT = 1950  # Number chars before message truncation
PASTE_LOGIN = "https://pastebin.com/api/api_login.php"
PASTE_UPLOAD = "https://pastebin.com/api/api_post.php"
LOG_MSG = """See main.log for general traces.
Rolling over existing file logs as listed below.
    module_name -> output_file
    =========================="""


class RWLockWrite():
    """
    Implement a reader-writer lock. In this case, they are to be used to control sheet updates.

    The "readers" are in this case all requests from users that can update the sheet without issue.
    The "writers" are the full rescans of sheet that happen by dumping db.
    Writers will be prioritized as data is drifting out of sync.
    """
    def __init__(self):
        """
        This is a standard reader-writer lock.
        All required locks are internal.
        Lock is not to be pickled.
        """
        self.readers = 0
        self.writers = 0

        self.read_mut = asyncio.Lock()
        self.write_mut = asyncio.Lock()
        self.resource_mut = asyncio.Lock()

        self.read_allowed = asyncio.Event()
        self.read_allowed.set()

    def __repr__(self):
        keys = ['readers', 'writers', 'read_mut', 'write_mut', 'resource_mut',
                'read_allowed']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "RWLockWrite({})".format(', '.join(kwargs))

    def __str__(self):
        return repr(self)

    async def is_read_allowed(self):
        """ Simple check if reading is allowed. """
        async with self.write_mut:
            return self.read_allowed.is_set()

    async def r_aquire(self, wait_cb=None):
        """
        I wish to START an update TO the sheet.

        Args:
            wait_cb: A callback coroutine that will notify user of need to wait.
        """
        if wait_cb and not await self.is_read_allowed():
            await wait_cb.send_notice()
        await self.read_allowed.wait()

        async with self.read_mut:
            self.readers += 1
            if self.readers == 1:
                if wait_cb and self.resource_mut.locked():
                    await wait_cb.send_notice()
                await self.resource_mut.acquire()

    async def r_release(self):
        """
        I wish to FINISH an update TO the sheet.
        """
        async with self.read_mut:
            self.readers -= 1
            if self.readers == 0:
                self.resource_mut.release()

    async def w_aquire(self):
        """
        I wish to START an update FROM the sheet.
        """
        async with self.write_mut:
            self.writers += 1
            if self.writers == 1:
                self.read_allowed.clear()
            await self.resource_mut.acquire()

    async def w_release(self):
        """
        I wish to FINISH an update FROM the sheet.
        """
        async with self.write_mut:
            self.resource_mut.release()
            self.writers -= 1
            if self.writers == 0:
                self.read_allowed.set()


class WaitCB():
    """
    Tiny object created to ensure only one of each callback sent to user.
    """
    def __init__(self, *, notice_cb, resume_cb):
        self.notice_sent = False
        self.notice_cb = notice_cb
        self.resume_cb = resume_cb

    async def send_notice(self):
        """
        Send the notice to the user. Will only ever send once regardless of calls.
        """
        if not self.notice_sent:
            self.notice_sent = True
            await self.notice_cb()

    async def send_resume(self):
        """
        Send resumption message if and only if notice was sent first.
        """
        if self.notice_sent:
            await self.resume_cb()


def substr_match(seq, line, *, skip_spaces=True, ignore_case=True):
    """
    True iff the substr is present in string. Ignore spaces and optionally case.
    """
    return substr_ind(seq, line, skip_spaces=skip_spaces,
                      ignore_case=ignore_case) != []


def substr_ind(seq, line, *, skip_spaces=True, ignore_case=True):
    """
    Return the start and end + 1 index of a substring match of seq to line.

    Returns:
        [start, end + 1] if needle found in line
        [] if needle not found in line
    """
    if ignore_case:
        seq = seq.lower()
        line = line.lower()

    if skip_spaces:
        seq = seq.replace(' ', '')

    start = None
    count = 0
    for ind, char in enumerate(line):
        if skip_spaces and char == ' ':
            continue

        if char == seq[count]:
            if count == 0:
                start = ind
            count += 1
        else:
            count = 0
            start = None

        if count == len(seq):
            return [start, ind + 1]

    return []


def rel_to_abs(*path_parts):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, *path_parts)


def get_config(*keys, default=None):
    """
    Return keys straight from yaml config.

    Args:
        keys: The keys going down the config.
        default: A default value to return. If not set, will raise KeyError.

    Raises:
        KeyError: If no default set and keys were not in config.
    """
    try:
        with open(YAML_FILE) as fin:
            conf = yaml.load(fin, Loader=Loader)
    except FileNotFoundError:
        raise cog.exc.MissingConfigFile("Missing config.yml. Expected at: " + YAML_FILE)

    try:
        for key in keys:
            conf = conf[key]

        return conf
    except KeyError:
        if default:
            return default

        raise


def update_config(new_val, *keys):
    """
    Get current config and replace the value of keys with new_val.
    Then flush new config to the file.

    N. B. The key must exist, else KeyError
    """
    try:
        with open(YAML_FILE) as fin:
            conf = yaml.load(fin, Loader=Loader)
    except FileNotFoundError:
        raise cog.exc.MissingConfigFile("Missing config.yml. Expected at: " + YAML_FILE)

    whole_conf = conf
    for key in keys[:-1]:
        conf = conf[key]
    conf[keys[-1]] = new_val

    with open(YAML_FILE, 'w') as fout:
        yaml.dump(whole_conf, fout, Dumper=Dumper, default_flow_style=False)


def number_increment(line):
    """
    Take a string of form: text 10
    Parse the number part, increment it by 1 and return the original string with new number.

    Returns: The new string with number portion incremented.
    """
    match = re.search(r'\d+', line)
    if not match:
        raise ValueError("number_increment: line did NOT contain a parsable integer.")

    old_num = int(match.group(0))
    return line.replace(str(old_num), str(old_num + 1))


def init_logging():  # pragma: no cover
    """
    Initialize project wide logging. See config file for details and reference on module.

     - On every start the file logs are rolled over.
     - This must be the first invocation on startup to set up logging.
    """
    log_file = rel_to_abs(get_config('paths', 'log_conf'))
    try:
        with open(log_file) as fin:
            lconf = yaml.load(fin, Loader=Loader)
    except FileNotFoundError:
        raise cog.exc.MissingConfigFile("Missing log.yml. Expected at: " + log_file)

    for handler in lconf['handlers']:
        try:
            os.makedirs(os.path.dirname(lconf['handlers'][handler]['filename']))
        except (OSError, KeyError):
            pass

    with open(log_file) as fin:
        logging.config.dictConfig(yaml.load(fin, Loader=Loader))

    print(LOG_MSG)
    for name in lconf['handlers']:
        node = lconf['handlers'][name]
        if 'RotatingFileHandler' not in node['class']:
            continue

        for handler in logging.getLogger(name).handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                print('    %s -> %s' % (name, handler.baseFilename))
                handler.doRollover()


def dict_to_columns(data):
    """
    Transform the dict into columnar form with keys as column headers.
    """
    lines = []
    header = []

    for col, key in enumerate(sorted(data)):
        header.append('{} ({})'.format(key, len(data[key])))

        for row, item in enumerate(data[key]):
            try:
                lines[row]
            except IndexError:
                lines.append([])
            while len(lines[row]) != col:
                lines[row].append('')
            lines[row].append(item)

    return [header] + lines


def complete_blocks(parts):
    """
    Take a list of message parts, complete code blocks as needed to
    preserve intended formatting.

    Returns:
        List of messages that have been modified.
    """
    new_parts = []
    incomplete = False
    block = "```"

    for part in parts:
        num_blocks = part.count(block) % 2
        if incomplete and not num_blocks:
            part = block + part + block

        elif incomplete and num_blocks:
            part = block + part
            incomplete = not incomplete

        elif num_blocks:
            part = part + block
            incomplete = not incomplete

        new_parts += [part]

    return new_parts


def msg_splitter(msg):
    """
    Take a msg of arbitrary length and split it into parts that respect discord 2k char limit.

    Returns:
        List of messages to send in order.
    """
    parts = []
    part_line = ''

    for line in msg.split("\n"):
        line = line + "\n"

        if len(part_line) + len(line) > MSG_LIMIT:
            parts += [part_line.rstrip("\n")]
            part_line = line
        else:
            part_line += line

    if part_line:
        parts += [part_line.rstrip("\n")]

    return parts


# N.B. Intentionally untested, don't want to spam pastebin
#      I don't see a dummy flag in api.
async def pastebin_login(dev_key, user, pword):
    """
    Perform simple pastebin login.
    """
    data = {
        "api_dev_key": dev_key,
        "api_user_name": user,
        "api_user_password": pword,
    }
    async with aiohttp.ClientSession() as http:
        async with http.post(PASTE_LOGIN, data=data) as resp:
            if resp.status != 200:
                raise cog.exc.RemoteError("Pastebin upload failed!")

            return await resp.text()


async def pastebin_upload(dev_key, title, content, session=None):
    """
    Perform a simple paste to pastebin.

    Session is optional, if provided paste will be associated to account.
    """
    data = {
        "api_dev_key": dev_key,
        "api_option": "paste",
        "api_paste_code": content,
        "api_paste_expire_date": "1D",  # 1 day then expire
        "api_paste_name": title,
        "api_paste_private": 1,  # 0/public, 1/unlisted, 2/private
    }
    if session:
        data["api_user_key"] = session

    async with aiohttp.ClientSession() as http:
        async with http.post(PASTE_UPLOAD, data=data) as resp:
            if resp.status != 200:
                raise cog.exc.RemoteError("Pastebin upload failed!")

            return await resp.text()


async def pastebin_new_paste(title, content):
    """
    Simple wrapper to create a paste and return the url.
    """
    pbin = cog.util.get_config("pastebin")
    user_session = await pastebin_login(pbin["dev_key"], pbin["user"], pbin["pass"])
    return await pastebin_upload(pbin["dev_key"], title, content, session=user_session)


def transpose_table(table):
    """
    Transpose any table of values stored as list of lists.
    Table must be rectangular.

    Returns: Transposed list of lists.
    """
    n_table = []

    while len(n_table) != len(table[0]):
        n_table += [[]]

    for col_ind in range(0, len(table[0])):
        for row_ind in range(0, len(table)):
            n_table[col_ind] += [table[row_ind][col_ind]]

    return n_table


def clean_text(text, *, replace='_'):
    """
    Ensure input contains ONLY ASCII characters valid in filenames.
    Any other character will be replaced with 'replace'.
    Characters added in extras will be whitelisted in addiction to normal ASCII.

    Args:
        text: The text to clean.
        replace: The replacement character to use.
        extras: Additional characters to whitelist.

    Returns:
        The cleaned text.
    """
    text = re.sub(r'[^a-zA-Z0-9]', replace, text)
    text = re.sub(r'{r}{r}+'.format(r=replace), replace, text)

    return text


def shorten_text(text, len):
    """
    Shorten text to a particular len.
    Indicate text was cut out with a period if we end on middle of word.

    Args:
        text: The text to shorten.
        len: The length desired.

    Returns: Text guaranteed to be at most len.
    """
    if text[:len] != text:
        text = text[:len]
        if not text[-1].isspace():
            text = text[:-1] + '.'

    return text


def camel_to_c(word):
    """
    Convert camel case to c case.

    Args:
        word: A string.

    Returns:
        A c case string.
    """
    n_word = word[0]

    for chara in word[1:]:
        if chara.isupper():
            n_word += '_'

        n_word += chara

    return n_word.lower()


#  # Scenario multiple readers, always allowed
#  async def a_run1(lock):
    #  print("Run1 - aquire read")
    #  await lock.r_aquire()
    #  await asyncio.sleep(3)
    #  await lock.r_release()

    #  print("Run1 exit")


#  async def a_run2(lock):
    #  print("Run2 - aquire read")
    #  await lock.r_aquire()
    #  await lock.r_release()

    #  print("Run2 exit")


#  # Reader starts and writer comes along, readers no longer allowed
#  async def b_run1(lock):
    #  print("Run1 - aquire read")
    #  await lock.r_aquire()
    #  await asyncio.sleep(3)
    #  await lock.r_release()

    #  print("Run1 exit")


#  async def b_run2(lock):
    #  await asyncio.sleep(1)
    #  print("Run2 - aquire write")
    #  await lock.w_aquire()
    #  await asyncio.sleep(4)

    #  print("Run2 exit")
    #  await lock.w_release()


#  async def b_run3(lock):
    #  await asyncio.sleep(2)
    #  print("Run3 - aquire read")
    #  await lock.r_aquire()

    #  print("Run3 exit")


#  # Writer starts, reader comes aglong, another writer, reader goes last.
#  async def c_run1(lock):
    #  print("Run1 - aquire write")
    #  await lock.w_aquire()
    #  await asyncio.sleep(3)
    #  await lock.w_release()
    #  print("Run1 - release write")

    #  print("Run1 exit")


#  async def c_run2(lock):
    #  asyncio.sleep(1)
    #  print("Run2 - aquire read")
    #  await lock.r_aquire()  # Blocks until all writes done.

    #  print("Run2 exit")


#  async def c_run3(lock):
    #  await asyncio.sleep(2)
    #  print("Run3 - aquire write")
    #  await lock.w_aquire()
    #  await asyncio.sleep(6)
    #  print("Run3 - release write")
    #  await lock.w_release()

    #  print("Run3 exit")


#  # Two writers try to do exclusive resource changes.
#  async def d_run1(lock):
    #  print("Run1 - aquire write")
    #  await lock.w_aquire()
    #  async with lock:
        #  print("Run1 Taken exclusive.")
        #  await asyncio.sleep(3)
        #  print("Run1 Done exclusive.")
    #  await lock.w_release()
    #  print("Run1 - release write")

    #  print("Run1 exit")


#  async def d_run2(lock):
    #  await asyncio.sleep(1)
    #  print("Run2 - aquire write")
    #  await lock.w_aquire()
    #  async with lock:
        #  print("Run2 Taken exclusive.")
        #  await asyncio.sleep(3)
        #  print("Run2 Done exclusive.")
    #  print("Run2 - release write")
    #  await lock.w_release()

    #  print("Run3 exit")


#  async def e_take_context(lock):
    #  async with lock:
        #  print('Hello inside take.')


#  # TODO: Write some real tests, I'm fairly confident it is correct though.
#  def main():
    #  loop = asyncio.get_event_loop()

    #  #  lock = RWLockWrite()
    #  #  loop.run_until_complete(asyncio.gather(a_run1(lock), a_run2(lock)))

    #  #  lock = RWLockWrite()
    #  #  loop.run_until_complete(asyncio.gather(b_run1(lock), b_run2(lock), b_run3(lock)))

    #  #  lock = RWLockWrite()
    #  #  loop.run_until_complete(asyncio.gather(c_run1(lock), c_run2(lock), c_run3(lock)))

    #  #  lock = RWLockWrite()
    #  #  loop.run_until_complete(asyncio.gather(d_run1(lock), d_run2(lock)))

    #  lock = RWLockWrite()
    #  print(str(lock))
    #  loop.run_until_complete(e_take_context(lock))


#  if __name__ == "__main__":
    #  main()


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = rel_to_abs('data', 'config.yml')
