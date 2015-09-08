
import json, logging

from django.http import HttpResponse
from google.appengine.api import taskqueue
from google.appengine.ext import deferred

from .strategies import cursor, map_reduce
from . import util

# lease errors only retried up to a minute, because cron task runs every minute anyway
LEASE_TASKS_MAX_ATTEMPTS = 3
LEASE_TASKS_BACKOFF_SECONDS = 15 # this value will be doubled on every subsequent retry

def setup_denorm_task(attempts):
    #print('[setup_denorm_task]')

    q = taskqueue.Queue('pull-denorm')

    while True:

        try:
            tasks = q.lease_tasks(60, 1)
        except taskqueue.TransientError as e:

            if attempts >= LEASE_TASKS_MAX_ATTEMPTS:
                logging.warning('[denorm.tasks.setup_denorm_task] lease_tasks raised TransientError on attempt # %d. aborting' % attempts)
                raise e

            backoff = attempts * LEASE_TASKS_BACKOFF_SECONDS

            logging.warning('[denorm.tasks.setup_denorm_task] lease_tasks raised TransientError on attempt # %d. will queue next attempt in %d seconds' % (attempts, backoff))

            # TODO: how do we configure queue name if we can't access task? specify in settings.conf?
            deferred.defer(setup_denorm_task, attempts + 1, _countdown=backoff, _queue='denorm')

            return

        if not tasks:
            logging.info('[denorm.tasks.setup_denorm_task] no tasks')
            break

        task = tasks[0]
        tag = task.tag

        payload = json.loads(task.payload)

        #print('[setup_denorm_task] payload = %s' % payload)

        strategy = payload['strategy']

        # TODO: use pipeline api
        logging.info('[denorm.tasks.setup_denorm_task] lease task with tag %s' % tag)

        if strategy == 'mapreduce':
            map_reduce.denorm_instance(payload)
        else:
            deferred.defer(cursor.denorm_instance, task.payload, _queue=payload['queue_name'])

        #
        # delete this task and all with same tag. even though task is not actually complete yet, we rely on push task integrity.
        #
        q.delete_tasks(task)
        util.delete_tasks_by_tag(tag)

@util.require_task_access
def denorm_task_handler(request):

    setup_denorm_task(1)

    return HttpResponse()