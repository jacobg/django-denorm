
import copy, logging

from autoload import autodiscover as auto_discover
from django.db.models import Field, ForeignKey, signals as db_signals
from django.db.models.fields import FieldDoesNotExist
from djangotoolbox import fields as tb_fields
from inflector.inflector import Inflector
from json_field import JSONField, fields as json_fields

from denorm import core, receivers, util

def autodiscover():
    auto_discover('denorm_fields')

def _copy_field(field, field_name, owner_model_name):

    clone = copy.deepcopy(field)

    clone.creation_counter = Field.creation_counter
    Field.creation_counter += 1

    if hasattr(field, "model"):
        del clone.model

        # clear .name, .db_column and .verbose_name, so that field name passed in contribute_to_class call gets used
        clone.name = field_name
        clone.db_column = None
        clone.verbose_name = None

        # TODO: remove the following setting of attributes
        #clone.attname = field_name
        #clone.column = field_name

        if hasattr(clone, '_related_fields'):
            # this is important for building queries
            del clone._related_fields

        # denormalized fields are optional
        # FIXME: should make optional if either foreign key field is optional, or if source field is optional
        clone.null = True
        clone.blank = True

        # if field is a foreign key, then we need to add a unique related_name attribute of format <target_field_name>_<target_model_name_plural>
        if isinstance(clone, ForeignKey):
            related_name = '%s_%s' % (field_name, Inflector().pluralize(owner_model_name.lower().replace('.', '_')))
            clone.related_name = related_name
            clone.rel.related_name = related_name

    return clone

def register(target_model, options):
    logging.info('[denorm.register] %s' % target_model)

    if not hasattr(target_model, '_meta'):
        raise AttributeError('The model being registered must derive from Model.')

    target = util.get_model_name(target_model)
    target_options = target_model._meta

    # register signals for target. use dispatch_uid to prevent duplicates.
    # about signals: https://docs.djangoproject.com/en/1.8/topics/signals/
    # built-in signals: https://docs.djangoproject.com/en/1.8/ref/signals/
    db_signals.post_init.connect(receivers.target_model_post_init, sender=target_model, dispatch_uid='denorm_target_%s_post_init'%target)
    db_signals.pre_save.connect(receivers.target_model_pre_save, sender=target_model, dispatch_uid='denorm_target_%s_pre_save'%target)
    db_signals.post_save.connect(receivers.target_model_post_save, sender=target_model, dispatch_uid='denorm_target_%s_post_save'%target)

    target_graph = core.TARGET_GRAPH[target_model] = core.TARGET_GRAPH.get(target_model, {})

    for source, source_dict in options['sources'].iteritems():

        strategy = source_dict.get('strategy', 'cursor') # options are: [cursor, mapreduce]. defaults to cursor.

        # TODO: support storage options 'list' and 'dict'
        storage = source_dict.get('storage', 'scalar') # choices: [scalar, shared_dict]

        if storage == 'scalar':

            target_foreign_key = target_options.get_field(source)
            # if field did not exist, then get_field would have raised FieldDoesNotExist

            if not isinstance(target_foreign_key, ForeignKey):
                raise AttributeError('The source field %s.%s must be a ForeignKey' % (target, source))

            source_model = target_foreign_key.rel.to

        elif storage == 'shared_dict':

            target_foreign_key_list = target_options.get_field(Inflector().pluralize(source))
            # if field did not exist, then get_field would have raised FieldDoesNotExist

            if not isinstance(target_foreign_key_list, tb_fields.ListField):
                raise AttributeError('The target field %s.%s must be a ListField' % (target, source))

            # model must be explicitly configured, because target field does not specify it
            source_model = source_dict.get('model')

            # create denorm data field
            try:
                target_options.get_field('denorm_data')
            except FieldDoesNotExist:
                # field should not exist. now let's create it.

                denorm_data_field = JSONField(name='denorm_data', null=True, blank=True,
                                              decoder_kwargs={'cls': json_fields.JSONDecoder, 'parse_float':float})
                denorm_data_field.contribute_to_class(target_model, 'denorm_data')

            else:
                # field was already created on prior source field
                # TODO: do at beginning of target model configuration to make sure developer did not define it
                pass

        else:
            logging.error('[denorm.register] invalid storage option %s' % storage)

        source_options = source_model._meta

        # register signals for source. use dispatch_uid to prevent duplicates.
        db_signals.post_init.connect(receivers.source_model_post_init, sender=source_model, dispatch_uid='denorm_source_%s_post_init'%source)
        db_signals.pre_save.connect(receivers.source_model_pre_save, sender=source_model, dispatch_uid='denorm_source_%s_pre_save'%source)
        db_signals.post_save.connect(receivers.source_model_post_save, sender=source_model, dispatch_uid='denorm_source_%s_post_save'%source)

        # FIXME: it's quirky that label and throttles must be configured under each target-source in app's denorm_fields,
        # FIXME: but it gets applied here for entire source (not target dependent). it probably should be configured once
        # FIXME: per source, but how do accomplish that in the current configuration design?
        source_graph = core.SOURCE_GRAPH[source_model] = core.SOURCE_GRAPH.get(source_model, {
            'label': source_dict.get('label'),
            'throttles': source_dict.get('throttles'),
            'fields': {}
        })
        source_graph_fields = source_graph['fields']

        # mark model as registered for denormalization
        source_model._denorm_registered = True

        # clone list, so that if we add _id below, it doesn't corrupt original list
        denorm_field_names = list(source_dict['fields'])

        target_graph[source] = {
            'fields': denorm_field_names,
            'storage': storage,
            'source_model': source_model # important for shared_dict storage, because we don't know source model based on list field
        }

        for i, denorm_field_name in enumerate(denorm_field_names):

            source_field = source_options.get_field(denorm_field_name)
            # if field did not exist, then get_field would have raised FieldDoesNotExist

            target_field_name = '%s_%s' % (source, denorm_field_name)

            if storage == 'scalar':

                try:
                    target_options.get_field(target_field_name)
                except FieldDoesNotExist:
                    # field should not exist, so we're good
                    pass
                else:
                    raise AttributeError('The denorm field %s.%s must not already exist' % (target_model.__name__, target_field_name))

                # create target field of same type as source_field

                target_field = _copy_field(source_field, target_field_name, target)
                target_field.contribute_to_class(target_model, target_field_name)

                #print('added field %s with name %s, column %s' % (target_field, target_field_name, target_field.column))
                #print('added field %s with name %s' % (target_model._meta.get_field(target_field_name), target_field_name))

            else:
                assert(storage == 'shared_dict')

                # denorm_data field was already created outside this iteration loop
                pass

            # if source field is a foreign key, then we reference its key rather than the actual related field,
            # because we are not deferencing further than the key, and do not want to do an extra db lookup.
            if isinstance(source_field, ForeignKey):
                denorm_field_name += '_id'
                denorm_field_names[i] = denorm_field_name

            source_field_graph = source_graph_fields[denorm_field_name] = source_graph_fields.get(denorm_field_name, [])
            source_field_graph.append({
                'target_model': target_model,
                'source': source,
                'strategy': strategy,
                'storage': storage,
                'shards': source_dict.get('shards') and util.convert_func_to_string(source_dict['shards'])
            })