
from django.conf.urls import patterns, url
import tasks

urlpatterns = patterns('',

    url(r'^run-tasks$', tasks.denorm_task_handler),

)