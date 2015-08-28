
import json, logging

from djangoappengine.db.utils import get_cursor, set_cursor
from google.appengine.ext import deferred

from denorm import util

ITEMS_PER_TASK = 100

# TODO: implement shared_dict storage implementation for cursor strategy
def denorm_instance(payload, cursor=None):
    logging.info('[cursor.denorm_instance] payload %s, cursor %s' % (payload, cursor))

    data = json.loads(payload)
    source_model = util.get_model_by_name(data['source_model'])
    target_model = util.get_model_by_name(data['target_model'])
    related_field_name = data['related_field']
    fields = data['fields']

    queryset = target_model.objects.filter(**{related_field_name+'_id': data['instance_id']})
    if cursor:
        queryset = set_cursor(queryset, cursor)
    results = queryset[0:ITEMS_PER_TASK]
    cursor = get_cursor(results)

    # TODO: batch save
    for item in results:
        #print('[denorm_instance] denorm target instance %s' % item)

        item._denorm_values = fields # provide denorm values directly so that pre_save signal receiver does not lookup related field
        item.save()

    if len(results) == ITEMS_PER_TASK:
        # there are likely more items
        logging.info('[denorm_instance] queue task with cursor %s' % cursor)
        deferred.defer(denorm_instance, payload, cursor, _queue=data['queue_name'])