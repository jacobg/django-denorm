
import json, logging

from dateutil.parser import parse as parse_date
from django.conf import settings
from django.http import HttpResponse
from django.utils import timezone
from google.appengine.api import taskqueue
from google.appengine.ext import deferred

from .strategies import cursor, map_reduce
from . import util

# lease errors only retried up to a minute, because cron task runs every minute anyway
LEASE_TASKS_MAX_ATTEMPTS = 3
LEASE_TASKS_BACKOFF_SECONDS = 15 # this value will be doubled on every subsequent retry
TASK_MIN_AGE_SECONDS = 60 if not settings.DEBUG else 1

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

        payload_string = task.payload
        payload = json.loads(payload_string)

        #print('[setup_denorm_task] payload = %s' % payload)

        # lease all other tasks with this tag, merge the payload['fields'] together with newer tasks having priority,
        # and then delete the older tasks.
        dupe_tasks = q.lease_tasks_by_tag(60, 100, tag) or []

        # in the end, we'll need to delete all these tasks
        tasks_to_delete = [task] + dupe_tasks

        if dupe_tasks:

            # sort tasks from oldest to most recent
            dupe_tasks = map(lambda t: {'task': t, 'payload': json.loads(t.payload)}, dupe_tasks + [task])
            dupe_tasks = sorted(dupe_tasks, key=lambda t: t['payload']['created'])

            # iterate tasks, and merge fields into most recent task
            fields = {}
            for t in dupe_tasks:
                fields.update(t['payload']['fields'])

            # last (most recent) task is the prototype we will use for new task
            task_data = dupe_tasks[-1]
            task = task_data['task']
            payload = task_data['payload']
            payload['fields'] = fields
            payload_string = util.dump_json(payload)

            logging.info('[denorm.tasks.setup_denorm_task] merged %d tasks of tag %s into new payload %s' % (len(tasks_to_delete), tag, payload_string))

        #
        # if task is not old enough to execute:
        #    (a) if there were dupe tasks, then task payload fields is likely dirty. so we'll create a new task and delete this one.
        #    (b) else, ignore it and move one. the lease will expire on its own, and task will re-execute later.
        #
        age = (timezone.now() - parse_date(payload['created']))

        if age.total_seconds() < TASK_MIN_AGE_SECONDS:
            if dupe_tasks:
                q.add(taskqueue.Task(payload=payload_string, tag=tag, method='PULL'))
                q.delete_tasks(tasks_to_delete)

                # note that this newly merged task will be iterated once again in the current cron job, which is unavoidable due to not using countdown.
                # it's okay. next time, there'll be nothing to merge and then it will be ignored until lease expires.

            logging.info('[denorm.tasks.setup_denorm_task] task of tag %s with age %s is too young' % (tag, age))

            continue

        #
        # if we reached here, then we're ready to execute task
        #

        strategy = payload['strategy']

        # TODO: use pipeline api
        logging.info('[denorm.tasks.setup_denorm_task] queuing push task for tag %s' % tag)

        if strategy == 'mapreduce':
            map_reduce.denorm_instance(payload)
        else:
            deferred.defer(cursor.denorm_instance, payload_string, _queue=payload['queue_name'])

        # delete this task, and any dupes. even though this task is not actually complete yet, we rely on push task integrity.
        q.delete_tasks(tasks_to_delete)

@util.require_task_access
def denorm_task_handler(request):

    setup_denorm_task(1)

    return HttpResponse()