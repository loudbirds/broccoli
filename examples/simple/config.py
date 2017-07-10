#from broccoli.storage import RedisBroccoli
from broccoli.contrib.sqlitedb import SqliteBroccoli

#broccoli = RedisBroccoli('simple.test')
broccoli = SqliteBroccoli('my_app', filename='broccoli.db')