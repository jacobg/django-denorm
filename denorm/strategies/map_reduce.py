
import json, logging

from django.utils.timezone import now
from djangoappengine.mapreduce.pipeline import _convert_model_to_string
from inflector.inflector import Inflector
from mapreduce import context, mapper_pipeline, operation as op, output_writers

from denorm import util

class NullOutputWriter(output_writers.OutputWriter):

    @classmethod
    def validate(cls, mapper_spec):
        pass

    @classmethod
    def from_json(cls, state):
        return cls()

    def to_json(self):
        return {}

    @classmethod
    def _create(cls):
        return cls()

    @classmethod
    def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
        return cls()

    def write(self, data):
        pass

    def finalize(self, ctx, shard_state):
        pass

    @classmethod
    def get_filenames(cls, mapreduce_state):
        return []

    def _recover(self, mr_spec, shard_number, shard_attempt):
        return self._create()

class MapperPipeline(mapper_pipeline.MapperPipeline):

    def finalized(self):

        #
        # It's actually possible MapReduce job may not be done. If it takes longer than 10 minutes, a new
        # pipeline task will be spun up.
        # TODO: We may need to manage the entire process outside of a pipeline to handle that case.
        # More info in comment section of this web page:
        # http://sookocheff.com/posts/2015-05-19-app-engine-pipelines-part-three-fan-in-fan-out/#comment-2049741106
        #

        counters = self.outputs.counters.value

        logging.info(u'[MapperPipeline.finalized] job name %s denormalized %d instances in %d milliseconds'
                     % (self.args[0], counters.get('mapper-calls', 0), counters.get('mapper-walltime-ms', 0)))

# FIXME: batch save depends on djangoappengine customization to db compiler
def _batch_save(entity, save_params={}):

    entity.batch_op_class = op.db.Put
    entity.save(**save_params)
    return entity.batch_op # batch op set in db compiler so it can be executed later

def denorm_entity_mapper(entity):

    ctx = context.get()
    params = ctx.mapreduce_spec.mapper.params

    entity._denorm_values = params['denorm_values']

    # Instead of naive single save: entity.save(), do the following more efficient batch save:
    yield _batch_save(entity)

def denorm_instance(payload):
    logging.info('[map_reduce.denorm_instance] payload %s' % json.dumps(payload))

    source_model = util.get_model_by_name(payload['source_model'])
    target_model = util.get_model_by_name(payload['target_model'])
    related_field_name = payload['related_field']
    fields = payload['fields']
    storage = payload['storage']

    if storage == 'cursor':
        related_field_name_filter = related_field_name+'_id'
        denorm_values = fields
    else:
        assert(storage == 'shared_dict')

        # will look up source primary key in target's list field
        related_field_name_filter = Inflector.pluralize(related_field_name)

        denorm_values = {
            'denorm_data': {
                    related_field_name_filter: {
                        payload['instance_id']: fields
                    }
            }
        }

    pipeline = MapperPipeline(
        'denorm-target-%s-source-%s-instance-%s-at-%s' % (payload['target_model'], payload['source_model'], payload['instance_id'], now().isoformat()),
        handler_spec = util.convert_func_to_string(denorm_entity_mapper),
        input_reader_spec = 'djangoappengine.mapreduce.input_readers.DjangoModelInputReader', # FIXME: should be self-contained
        output_writer_spec = util.convert_func_to_string(NullOutputWriter),
        params = {
            # _convert_model_to_string is a different encoding than util.get_model_name, but we have to use because
            # we use djangoappengine's input reader
            'entity_kind': _convert_model_to_string(target_model),
            'queue_name': payload['queue_name'],
            'filters': [
                [related_field_name_filter, '=', payload['instance_id']],
            ],
            'denorm_values': denorm_values,
            #'storage': storage,
        },
        shards = payload['shards']
    )
    pipeline.start(queue_name=payload['queue_name'])
    pipeline_id = pipeline.pipeline_id

    logging.info('[map_reduce.denorm_instance] pipeline_id = %s' % str(pipeline_id))