"""
Run a simple server to accept POSTs.

POST with curl:
    curl -H "Content-Type: application/json" -d '{"hello":"world"}' http://localhost:8000

Run Gunicorn:
    PYTHONPATH=/project/root gunicorn -b 127.0.0.1:8000 web.app:main

Tutorial:
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
LOG_FILE = os.path.join(tempfile.gettempdir(), 'posts')
PUB = zmq.Context().socket(zmq.PUB)
PUB.bind("tcp://127.0.0.1:9000")


def init_log():
    """ Setup simple file and stream logging. """
    logger = logging.getLogger('posts')
    logger.setLevel(logging.DEBUG)

    max_size = 1024 * 1024 * 1024
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
def post():
    """ Handle post requests. """
    if request.method == 'POST':
        data = request.get_json()
        log = logging.getLogger('posts')
        log.info('%s %s', str(request), data)

        RECV.append(data)
        if len(RECV) > 20:
            RECV.remove(RECV[0])

        try:
            log.info('Publishing for scanner %s', data['scanner'])
            PUB.send_json(data)
        except KeyError:
            pass

        return '200'
    else:
        msg = '<h2>JSON Data Received</h2><pre>'
        for data in RECV:
            msg += '\n' + json.dumps(data, indent=4, sort_keys=True)
        return msg + '</pre>'


def main():
    """ Debug entry point, use gunicorn in prod. """
    app.run(host='0.0.0.0')


init_log()
if __name__ == "__main__":
    main()
