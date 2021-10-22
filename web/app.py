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


app = sanic.Sanic('cog web')
ADDR = 'tcp://127.0.0.1:{}'.format(cog.util.CONF.ports.zmq)
LOG_FILE = os.path.join(tempfile.gettempdir(), 'posts')
PUB = None
RECV = []
CHART_FMT = "%d/%m %H:%M:%S"
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


@app.route('/vote', methods=['GET'])
async def vote(request):
    """
    The route to provide vote information via charts.
    """
    with cogdb.session_scope(cogdb.Session) as session:
        data = {
            "xvals": [],
            "yvals": [],
            "cons_totals": [],
            "prep_totals": [],
        }
        for con in cogdb.query.get_consolidation_after(session):
            data["xvals"].append(datetime.datetime.strftime(con.updated_at, CHART_FMT))
            data["yvals"].append(con.amount)
            data["cons_totals"].append(con.cons_total)
            data["prep_totals"].append(con.prep_total)

    # Populate a templates cache myself for use later to render html.
    with open(cog.util.rel_to_abs('web', 'templates', 'vote.html'), 'r', encoding='utf-8') as fin:
        TEMPLATES['vote'] = Template(fin.read())

    return sanic.response.html(TEMPLATES['vote'].render(request=request, data=data))


@app.route('/data/vote/<epoch:int>', methods=['GET'])
async def vote_data(request, epoch):
    """
    Provide the data via direct request.
    Response is JSON.
    """
    with cogdb.session_scope(cogdb.Session) as session:
        try:
            epoch = datetime.datetime.utcfromtimestamp(epoch)
        except ValueError:
            epoch = None

        data = {
            "xvals": [],
            "yvals": [],
            "cons_totals": [],
            "prep_totals": [],
        }
        for con in cogdb.query.get_consolidation_after(session, epoch):
            data["xvals"].append(datetime.datetime.strftime(con.updated_at, CHART_FMT))
            data["yvals"].append(con.amount)
            data["cons_totals"].append(con.cons_total)
            data["prep_totals"].append(con.prep_total)

    return sanic.response.json(data)


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
