
from datetime import timedelta
import json, logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone
from google.appengine.api import taskqueue
from inflector.inflector import Inflector
from mapreduce.util import handler_for_name

from denorm import core, exceptions, middleware, models, signals, util

def target_model_post_init(sender, instance, **kwargs):

    # for clarity
    target_model = sender
    target_instance = instance
    created = not target_instance.id

    #
    # keep state of original source primary keys to determine whether we will need to denormalize
    #

    # newly created instance always need to be denormalized
    if created:
        return

    target_instance._denorm_orig_sources = orig_sources = getattr(target_instance, '_denorm_orig_sources', {})

    target_graph = core.TARGET_GRAPH[target_model]

    for source_field, target_data in target_graph.iteritems():

        storage = target_data['storage']

        if storage == 'scalar':
            orig_sources[source_field] = getattr(target_instance, source_field + '_id')
        else:
            # Do we need to track this since we iterate dictionary?
            assert(storage == 'shared_dict')
            source_list_field = Inflector().pluralize(source_field)
            orig_sources[source_list_field] = getattr(target_instance, source_list_field)

def target_model_pre_save(sender, instance, raw, using, update_fields, **kwargs):

    # for clarity
    target_model = sender
    target_instance = instance
    created = not target_instance.id

    # check for existence of pre-computed denorm values
    denorm_values = getattr(target_instance, '_denorm_values', None)

    if denorm_values:

        for field_name, field_value in denorm_values.iteritems():
            if field_name == 'denorm_data':
                denorm_data = target_instance.denorm_data = getattr(target_instance, 'denorm_data', None) or {}

                for list_field_name, list_field_denorm_values in field_value.iteritems():
                    # merge dictionaries, where list_field_denorm_values overrides orig_denorm_data[list_field_name]
                    denorm_data[list_field_name] = dict(denorm_data.get(list_field_name, {}), **list_field_denorm_values)

            else:
                setattr(target_instance, field_name, field_value)

    else:
        #
        # we should only compute denormalized values if related field was changed or just set, unless _force_denorm attribute is set True
        #

        force_denorm = getattr(target_instance, '_force_denorm', False)
        orig_sources = getattr(target_instance, '_denorm_orig_sources', {})

        target_graph = core.TARGET_GRAPH[target_model]

        for source_field, target_data in target_graph.iteritems():

            storage = target_data['storage']
            fields = target_data['fields']

            if storage == 'scalar':

                source_pk = getattr(target_instance,  source_field + '_id')

                if force_denorm or created or source_pk != orig_sources[source_field]:

                    source_instance = getattr(target_instance,  source_field)

                    for field in fields:
                        source_field_value = getattr(source_instance, field) if source_instance else None
                        setattr(target_instance, '%s_%s' % (source_field, field), source_field_value)

            else:
                assert(storage == 'shared_dict')

                source_model = target_data['source_model']

                source_list_field_name = Inflector().pluralize(source_field)
                source_list_field = getattr(target_instance, source_list_field_name) or []

                denorm_data = target_instance.denorm_data = target_instance.denorm_data or {}
                source_denorm_data = denorm_data.get(source_list_field_name) or {}
                source_denorm_keys = source_denorm_data.keys()

                # we'll build up new_source_denorm_data as we iterate the list field
                denorm_data[source_list_field_name] = new_source_denorm_data = {}

                for source_pk in source_list_field:
                    if force_denorm or created or source_pk not in source_denorm_keys:
                        # TODO: do we want to allow target model to provide any possibly already-loaded source instances to avoid extra lookups?
                        source_instance = source_model.objects.get(id=source_pk)

                        source_denorm_values = {}
                        for field in fields:
                            # FIXME: it looks like we won't prepend source field name to beginning of key here.
                            # FIXME: but are we doing that consistently for shared_dict?
                            source_denorm_values[field] = getattr(source_instance, field)

                    else:
                        source_denorm_values = source_denorm_data[source_pk]

                    new_source_denorm_data[source_pk] = source_denorm_values

    # fire a post_denorm signal that receivers can listen to in order to any custom follow-up processing before instance is saved
    signals.post_denorm.send(sender=target_model, instance=target_instance)

def target_model_post_save(sender, instance, created, **kwargs):

    # for clarity
    target_model = sender
    target_instance = instance

    # re-run post_init to reset _denorm_orig_sources in case this instance gets saved again
    target_model_post_init(target_model, target_instance)

def source_model_post_init(sender, instance, **kwargs):

    # for clarity
    source_model = sender
    source_instance = instance

    # keep state of original value for all field values that have denorm dependencies

    source_graph_fields = core.SOURCE_GRAPH[source_model]['fields']

    source_instance._denorm_orig_values = orig_values = getattr(source_instance, '_denorm_orig_values', {})

    for source_field in source_graph_fields:
        orig_values[source_field] = getattr(source_instance, source_field)

def source_model_pre_save(sender, instance, raw, using, update_fields, **kwargs):

    # for clarity
    source_model = sender
    source_instance = instance
    created = not source_instance.id

    source_instance._denorm_affected_targets = affected_targets = {}

    # newly created instances will not need denormalization
    if created:
        return

    # denorm turned off
    if not getattr(source_instance, '_denorm', True):
        return

    source_graph = core.SOURCE_GRAPH[source_model]

    #
    # iterate through all fields to build up set of distinct affected targets that post_save signal receiver will process.
    #

    source_graph_fields = source_graph['fields']
    orig_values = source_instance._denorm_orig_values

    for source_field, targets in source_graph_fields.iteritems():

        old_value = orig_values[source_field]
        new_value = getattr(source_instance, source_field)

        if old_value != new_value:
            #logging.info('[%s] %s value changed from "%s" to "%s"' % (source_model, source_field, old_value, new_value))

            for target in targets:
                target_model = target['target_model']
                related_field_name = target['source']
                storage = target['storage']

                affected_targets[target_model] = affected_target = affected_targets.get(target_model, {
                    'related': related_field_name,
                    'strategy': target['strategy'],
                    'storage': storage,
                    'shards': target['shards'],
                    'fields': {}
                })

                # when task will update target, if storage is scalar, then field name is simply target model field name.
                # and if storage is shared_dict, then the field name is the dictionary key of the target model's denorm_data field.
                affected_fields_for_target = affected_target['fields']
                affected_fields_for_target['%s_%s' % (related_field_name, source_field)] = new_value

    if not affected_targets:
        return

    #
    # check that denorm throttling threshold is not exceeded
    #

    # get user from thread-local variable set by middleware
    user = middleware.get_current_user()
    if not user or not isinstance(user, get_user_model()) or not user.is_authenticated() or user.is_superuser:
        user = None

    source_instance._denorm_user = user

    # get denorm label used for throttling

    if 'label' in source_graph:
        # custom label set by application
        label = source_graph['label'](source_instance, user)
    else:
        # default label
        if user:
            label = '%s_%s' % (util.get_model_name(source_model), str(user.id))
        else:
            # no label
            label = None

    source_instance._denorm_label = label

    throttles = source_graph.get('throttles')

    if not label or not throttles:
        # no throttling
        return

    # now validate each throttle
    # FIXME: we need to figure out if there is already a denorm task scheduled, and if so, then don't penalize throttle.
    # FIXME: perhaps we can use a Task.status field in combination with filter for source instance id.

    now = timezone.now()

    for throttle in throttles:
        num_requests, duration = util.parse_rate(throttle)

        # FIXME: this is a naive, inefficient implementation. we should cache task counts.
        if models.Task.objects.filter(label=label, created__gt=now - timedelta(seconds=duration)).count() >= num_requests:
            raise exceptions.DenormThrottled


DENORM_DELAY_SECONDS = 60 if not settings.DEBUG else 1
DEFAULT_MAP_REDUCE_SHARDS = 3

def source_model_post_save(sender, instance, created, **kwargs):

    # for clarity
    source_model = sender
    source_instance = instance

    affected_targets = source_instance._denorm_affected_targets

    if not affected_targets:
        # nothing to denorm
        return

    #
    # create a task for each affected target to update its instances
    #

    for target_model, affected_target in affected_targets.iteritems():

        # if storage is shared_dict, then task will pluralize related_field_name to get target model's list field
        related_field_name = affected_target['related']
        strategy = affected_target['strategy']
        storage = affected_target['storage']
        shards = affected_target['shards']
        affected_fields = affected_target['fields']

        #logging.info('affected target %s.%s for source %s: %s' % (target_model, related_field_name, source_model, affected_fields))

        # for each affected target, delete existing tasks and create new one

        instance_id = source_instance.id
        tag = 'DENORM_SOURCE_%s_%s_TARGET_%s' % (util.get_model_name(source_model), instance_id, util.get_model_name(target_model))
        payload = {
            'strategy': strategy,
            'storage': storage,
            'instance_id': instance_id,
            'source_model': util.get_model_name(source_model),
            'target_model': util.get_model_name(target_model),
            'related_field': related_field_name,
            'fields': affected_fields,
            # TODO: queue name should be configurable
            'queue_name': 'denorm'
        }

        if strategy == 'mapreduce':
            payload['shards'] = handler_for_name(shards)(source_instance) if shards else DEFAULT_MAP_REDUCE_SHARDS

        util.delete_tasks_by_tag(tag)

        taskqueue.Queue('pull-denorm').add(
            taskqueue.Task(payload=json.dumps(payload), tag=tag, method='PULL', countdown=DENORM_DELAY_SECONDS)
        )

        # create a Task model instance used to track denorm tasks, particularly for throttling
        models.Task(
            source_model=util.get_model_name(source_model),
            source_instance_id=source_instance.id,
            user=source_instance._denorm_user,
            label=source_instance._denorm_label
        ).save()

    # re-run post_init to reset _denorm_orig_values in case this instance gets saved again
    source_model_post_init(source_model, source_instance)