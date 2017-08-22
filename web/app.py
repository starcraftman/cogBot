"""
Run a simple server to accept POSTs.

POST with curl:
    curl -H "Content-Type: application/json" -d '{"hello":"world"}' http://localhost:5000

Run Gunicorn:
    PYTHONPATH=/project/root gunicorn -b 127.0.0.1:8000 web.app:main

https://www.digitalocean.com/community/tutorials/how-to-deploy-python-wsgi-apps-using-gunicorn-http-server-behind-nginx
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import json
import os
import tempfile
import zmq
from flask import Flask, request


app = Flask(__name__)
RECV = []
LOG = os.path.join(tempfile.gettempdir(), 'posts')
SOCK = zmq.Context().socket(zmq.PUB)
SOCK.connect("tcp://127.0.0.1:9000")


def init_log():
    """ Setup simple file and stream logging. """
    logger = logging.getLogger('posts')
    logger.setLevel(logging.DEBUG)

    max_size = 1024 * 1024 * 1024
    handler = logging.handlers.RotatingFileHandler(LOG, mode='a', maxBytes=max_size,
                                                   backupCount=1)
    handler.setLevel(logging.DEBUG)
    shandler = logging.StreamHandler()
    shandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    for hand in [handler, shandler]:
        hand.setFormatter(formatter)
        logger.addHandler(hand)


init_log()


@app.route('/post', methods=['GET', 'POST'])
def post():
    """ Handle post requests. """
    if request.method == 'POST':
        global RECV
        data = request.get_json()
        log = logging.getLogger('posts')
        log.info('%s %s', str(request), data)

        RECV.append(data)
        if len(RECV) > 20:
            RECV = RECV[1:]

        # Filter this bot's own edits
        if data.get('user') == 'gearbot3003':
            log.info('Ignoring bot edit.')
            return '200'

        log.info('Publishing message for %s', data['spreadsheet'])
        SOCK.send_json(data)

        return '200'
    else:
        msg = '<h2>JSON Data Received</h2><pre>'
        for data in RECV:
            msg += '\n' + json.dumps(data, indent=4, sort_keys=True)
        return msg + '</pre>'


def main():
    """ Debug entry point, use gunicorn in prod. """
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()
