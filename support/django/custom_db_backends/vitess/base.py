from django.db.backends.mysql.base import DatabaseWrapper as MysqlDatabaseWrapper
from .features import DatabaseFeatures

class DatabaseWrapper(MysqlDatabaseWrapper):
    vendor = 'vitess'

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.features = DatabaseFeatures(self)