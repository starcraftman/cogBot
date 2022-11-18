"""
Run a simple server to accept POSTs. Pass POSTs received into the bot over zmq pub/sub.

Async replacement for Flask called Sanic
    https://sanic.readthedocs.io/en/latest/

POST with curl:
    curl -H "Content-Type: application/json" -d '{"scanner": "hudson_cattle", "time": 1}' http://localhost:8000/post

Run Gunicorn:
    PYTHONPATH=/project/root gunicorn -b 127.0.0.1:8000 web.app:main

Tutorial:
    https://www.digitalocean.com/community/tutorials/how-to-deploy-python-wsgi-apps-using-gunicorn-http-server-behind-nginx
"""
import asyncio
import atexit
import datetime
import functools
import logging
import logging.handlers
import json
import os
import tempfile
import time

import aiozmq
import aiozmq.rpc
from jinja2 import Template
import sanic
import sanic.response
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    print("Falling back to default python loop.")

import cog.util
import cogdb
import cogdb.query


app = sanic.Sanic('cogweb')
ADDR = 'tcp://127.0.0.1:{}'.format(cog.util.CONF.ports.zmq)
LOG_FILE = os.path.join(tempfile.gettempdir(), 'posts')
PUB = None
RECV = []
CHART_FMT = "%y/%m/%d %H:%M:%S"
TEMPLATES = {}


def pub_close(pub):
    """ Simple atexit hook. """
    pub.close()
    time.sleep(0.5)


def init_log():
    """ Setup simple file and stream logging. """
    logger = logging.getLogger('posts')
    logger.setLevel(logging.DEBUG)

    max_size = 1024 * 1024
    handler = logging.handlers.RotatingFileHandler(LOG_FILE, mode='a', maxBytes=max_size,
                                                   backupCount=1)
    handler.setLevel(logging.DEBUG)
    shandler = logging.StreamHandler()
    shandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    for hand in [handler, shandler]:
        hand.setFormatter(formatter)
        logger.addHandler(hand)


def get_vote_data(session, start=None, end=None):
    """
    Retrieve and format the vote data.
    """
    data = []
    for con in cogdb.query.get_consolidation_in_range(session, start=start, end=end):
        data += [{
            "x": datetime.datetime.strftime(con.updated_at, CHART_FMT),
            "y": con.amount,
            "cons_total": con.cons_total,
            "prep_total": con.prep_total,
        }]

    return data


@app.route('/vote', methods=['GET'])
async def vote(request):
    """
    The route to provide vote information via charts.
    """
    cur_cycle = cog.util.current_cycle()
    start = cog.util.cycle_to_start(cur_cycle)
    end = start + datetime.timedelta(weeks=1)
    with cogdb.session_scope(cogdb.Session) as session:
        data = get_vote_data(session, start, end)

    return sanic.response.html(TEMPLATES['vote'].render(request=request, data=data, cycle=cur_cycle))


@app.route('/data/voteRange/<start:int>/<end:int>', methods=['GET'])
async def vote_range(request, start, end):
    """Provide voting information after a given datetime.

    Args:
        request: The request object for flask.
        start: The timestamp (UTC) to get data after.
        end: The timestamp (UTC) to get data before.
    """
    start = datetime.datetime.utcfromtimestamp(start)
    end = datetime.datetime.utcfromtimestamp(end)
    with cogdb.session_scope(cogdb.Session) as session:
        return sanic.response.json(get_vote_data(session, start, end))


@app.route('/data/voteCycle/<cycle:int>', methods=['GET'])
async def vote_cycle(request, cycle):
    """Provide vote consolidation data for a given cycle.

    Args:
        request: The request object for flask.
        cycle: The cycle to get voting data for.
    """
    start = cog.util.cycle_to_start(cycle)
    end = start + datetime.timedelta(weeks=1)
    with cogdb.session_scope(cogdb.Session) as session:
        return sanic.response.json(get_vote_data(session, start, end))


@app.route('/post', methods=['GET', 'POST'])
async def post(request):
    """ Handle post requests. """
    global PUB
    if not PUB:
        PUB = await aiozmq.rpc.connect_pubsub(bind=ADDR)
        atexit.register(functools.partial(pub_close, PUB))

    if request.method == 'POST':
        data = request.json
        log = logging.getLogger('posts')
        log.info('%s %s', str(request), data)

        global RECV
        RECV.insert(0, data)
        RECV = RECV[:20]

        try:
            log.info('Publishing for scanner %s', data['scanner'])
            await PUB.publish('POSTs').remote_func(data['scanner'], data['time'])
        except KeyError:
            log.error('JSON request malformed ...' + str(data))

        return sanic.response.text('200')
    else:
        msg = '<h2>JSON Data Received</h2><pre>'
        for data in RECV:
            msg += '\n' + json.dumps(data, indent=4, sort_keys=True)
        return sanic.response.html(msg + '</pre>')


def main():
    """ Start sanic server and pass POSTs to bot. """
    init_log()
    port = cog.util.CONF.ports.sanic
    print("Sanic server listening on:", port)
    print("ZMQ pub/sub binding on:", ADDR)

    # Populate a templates cache myself for use later to render html.
    with open(cog.util.rel_to_abs('web', 'templates', 'vote.html'), 'r', encoding='utf-8') as fin:
        TEMPLATES['vote'] = Template(fin.read())

    debug = os.environ.get('DEBUG', False)
    app.run(host='0.0.0.0', port=port, debug=debug)


if __name__ == "__main__":
    main()
