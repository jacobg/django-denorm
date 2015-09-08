'''
Created on Apr 24, 2013

@author: jacobgur
'''

from django.contrib import admin

from utils.admin import ModelAdmin
import models

class TaskAdmin(ModelAdmin):
    list_display = ['created', 'source_model', 'source_instance_id', 'label']
    raw_id_fields = ['user']
    readonly_fields = ['created', 'modified']
    ordering = ['created']

if models.Task == models.get_task_model():
    admin.site.register(models.Task, TaskAdmin)
