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
from __future__ import absolute_import, print_function
import asyncio
import atexit
import functools
import logging
import logging.handlers
import json
import os
import tempfile
import time

import aiozmq
import aiozmq.rpc
import sanic
import sanic.response
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    print("Falling back to default python loop.")

import cog.util

app = sanic.Sanic()
ADDR = 'tcp://127.0.0.1:{}'.format(cog.util.get_config('ports', 'zmq'))
LOG_FILE = os.path.join(tempfile.gettempdir(), 'posts')
PUB = None
RECV = []


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

        RECV.append(data)
        if len(RECV) > 20:
            RECV.remove(RECV[0])

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
    port = cog.util.get_config('ports', 'sanic')
    print("Sanic server listening on:", port)
    print("ZMQ pub/sub binding on:", ADDR)
    app.run(host='0.0.0.0', port=port)


if __name__ == "__main__":
    main()
