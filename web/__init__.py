"""
This whole package exists to notify bot of changes to any sheets it is monitoring.

The path is as follows:
    1) Install an onChange hook to post my server.
    2) Server runs nginx as a proxy to pass it locally to gunicorn that is running flask.
    3) Flask application uses zmq.PUB socket to publish the POST to any number of running bots.
    4) Bot schedules and updates db. During update, disable the relevant commands impacted.
        Queue the commands during this window for post update invocation.
"""
