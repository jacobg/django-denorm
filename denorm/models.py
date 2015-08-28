
from django.conf import settings
from django.db import models
from django_extensions.db.models import TimeStampedModel

class Task(TimeStampedModel):
    source_model = models.CharField(max_length=200)
    source_instance_id = models.PositiveIntegerField()

    # user set to request.user, but will be None if request.user.__model__ is not AUTH_USER_MODEL. this is so that applications
    # can use multiple authentication mechanisms, such as API keys tied to multi-user accounts.
    user = models.ForeignKey(settings.AUTH_USER_MODEL, blank=True, null=True, related_name='denorm_task_user', on_delete=models.DO_NOTHING)

    # label is used to manage control throttling.
    # it defaults to  <source_model>_<user_id>, but can be customized by source model registration
    label = models.CharField(max_length=500)