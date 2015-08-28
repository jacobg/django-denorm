
import logging

from django.db.models.loading import get_model
from django.conf import settings
from django.http import HttpResponseNotFound
from google.appengine.api import taskqueue

def get_model_by_name(name):

    return get_model(*name.split('.',1))

def get_model_name(model):

    # get unique name resolvable via django.db.models.loading.get_model
    return '%s.%s' % (model._meta.app_label, model._meta.object_name)

# convert_func_to_string is taken from djangoappengine.mapreduce.pipeline._convert_func_to_string
# use mapreduce.util.handler_for_name to convert string back to function handler
def convert_func_to_string(func):
    return '%s.%s' % (func.__module__, func.__name__)

def delete_tasks_by_tag(tag):

    q = taskqueue.Queue('pull-denorm')

    tasks = q.lease_tasks_by_tag(60, 100, tag)
    if tasks:
        logging.info('[delete_tasks_by_tag] delete %d tasks with tag %s' % (len(tasks), tag))
        q.delete_tasks(tasks)

# parse_rate is used for throttling, and its implementation is copied from Django REST Framework throttling.
def parse_rate(rate):
    """
    Given the request rate string, return a two tuple of:
    <allowed number of requests>, <period of time in seconds>
    """
    if rate is None:
        return (None, None)
    num, period = rate.split('/')
    num_requests = int(num)
    duration = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[period[0]]
    return (num_requests, duration)

# decorator for task request handler
def require_task_access(func):
    def wrapper(request, *args, **kwargs):

        user = request.user

        if user.is_authenticated():
            if not user.is_superuser:
                logging.warning('non-superuser unauthorized task access: %s' % user.email)
                return HttpResponseNotFound()

        elif not settings.DEBUG and request.META.get('HTTP_X_APPENGINE_CRON', 'false') != 'true':
            logging.warning('unauthorized task access')
            return HttpResponseNotFound()

        return func(request, *args, **kwargs)
    return wrapper