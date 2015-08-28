# django-denorm

This project is to be used on Google App Engine in conjunction with Django App Engine or Djangae projects to automatically add denormalized fields to configured models and update them. When source model instances are saved, it will kick off a task using either MapReduce or cursor-based batch updates to target models.

Additional features include:
1) denormalize either ForeignKey or ForeignKey lists fields
2) only denormalize if an affected value changed
3) throttling
4) special parameters you can use to require or disable denormalization on any particular model save, which is useful for migration setup and user override.

To use this application:
1) Add the denorm directory to your project.
2) In project settings, add it to INSTALLED_APPS.
3) In project settings, add the middleware LocalUserMiddleware to MIDDLEWARE_CLASSES. This middleware is used to add request.user as thread-local global variable for denorm throttling. If there is no request or authentication non-superuser user, then throttling will not be applied.
4) Add denorm.urls to your project root urls.
5) Currently requires django-autoload project to automatically register denormalization models (https://bitbucket.org/twanschik/django-autoload/)

To configure denormalization on an application's model(s), add a denorm_fields.py file to your application with your model registrations. An example:

	```python
	from denorm import register
	from . import models, util

	register(models.EmployeeActivity, {

	    'sources': {
	        'employee': {
	            'fields': ['first_name', 'last_name'],
	            'label': util.denorm_label,
	            'throttles': ['4/min', '25/hour', '50/day']
	        },

	        'jobs': {
	            'strategy': 'mapreduce',
	            'shards': util.shard_count_related,
	            'fields': ['name'],
	            # will store denormalized job data in a denorm_data JSONField keyed by job primary key
	            'storage': 'shared_dict',
	            # specify model, because list field is currently list of integers rather than list of ForeignKeys
    	        'model': costing_models.Job,
	            'label': util.denorm_label,
	            'throttles': ['2/min', '10/hour', '20/day']
	        },
	    }
	})

	# now, we can import admin changes which may reference newly-added denorm fields
	import denorm_admin
	```


One additional note is that you should not use admin.py to register your ModelAdmins, because the denormalized fields will not be setup by the time Django registers the ModelAdmin. Instead, define your ModelAdmins in a different file, and import it at the bottom of denorm_fields.py.