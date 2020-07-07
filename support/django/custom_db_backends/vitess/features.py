from django.db.backends.mysql.features import DatabaseFeatures as MysqlBaseDatabaseFeatures

class DatabaseFeatures(MysqlBaseDatabaseFeatures):
    supports_transactions = False
    uses_savepoints = False
    supports_foreign_keys = False