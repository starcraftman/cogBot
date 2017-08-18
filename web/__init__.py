"""
This whole package exists to notify bot of changes to any sheets it is monitoring.
The path is as follows:
    1) Install an onEdit hook to post my server.
    2) Server runs nginx as a proxy to pass it locally to gunicorn that is running flask.
    3) Flask application uses zmq.PAIR socket to do IPC into a background task running on
    the async loop.
    4) Bot schedules and updates db. During update, disable the relevant commands impacted.
        Alternatively, queue the commands for post update invocation.
"""
