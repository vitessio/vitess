# Support for Django ORM

In order to use Vitess with Django ORM. 
1. You need to have mysqlclient installed. `pip install mysqlclient`  
2. You need to override the defined backend `django.db.backends.mysql` to disable savepoints as they are not supported.  
3. Foreign keys should also be disabled.  

Since django allows us to use custom backends for database connections, the framework is more flexible than most for adapting to 
various types of databases.

## Using Vitess with your django project.

1. Copy the custom_db_backends directory to your django project.  
2. Edit your `settings.py` to have the following;
```
"ENGINE": custom_db_backends.vitess,
```


## Notes
1. This has been tested with python 3.7 and django 2.2.   
2. Schema modifications using django migration tool are not yet fully tested. However, any errors that arise can be fixed by 
overriding the file [schema.py](https://github.com/django/django/master/django/db/backends/mysql/schema.py)   
3. The django-vitess adapter could be maintained as a separate project within Vitess in future so users can run 
`pip install django-vitess` for their projects.