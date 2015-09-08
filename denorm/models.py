
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django_extensions.db.models import TimeStampedModel

TASK_MODEL = getattr(settings, 'DENORM_TASK_MODEL', 'denorm.Task')

def get_task_model():
    """
    Returns the chosen model as a class.
    """
    try:
        klass = models.get_model(TASK_MODEL.split('.')[0], TASK_MODEL.split('.')[1])
    except:
        raise ImproperlyConfigured("Your denorm task class, {0}, is improperly defined".format(TASK_MODEL))
    return klass

class AbstractTask(TimeStampedModel):
    source_model = models.CharField(max_length=200)
    source_instance_id = models.PositiveIntegerField()

    # user set to request.user, but will be None if request.user.__model__ is not AUTH_USER_MODEL. this is so that applications
    # can use multiple authentication mechanisms, such as API keys tied to multi-user accounts.
    user = models.ForeignKey(settings.AUTH_USER_MODEL, blank=True, null=True, related_name='denorm_task_user', on_delete=models.DO_NOTHING)

    # label is used to manage control throttling.
    # it defaults to  <source_model>_<user_id>, but can be customized by source model registration
    label = models.CharField(max_length=500)

    class Meta:
        abstract = True

# default model
class Task(AbstractTask):

    # swappable is necessary to prevent model validation errors
    # coming from the ForeignKey fields' reverse relations, which
    # would be duplicate by both this class as well as the custom class.
    class Meta:
        swappable = 'DENORM_TASK_MODEL'