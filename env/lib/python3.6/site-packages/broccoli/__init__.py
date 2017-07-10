__version__ = '0.1.0'

from broccoli.api import crontab
from broccoli.api import Broccoli

try:
	from broccoli.storage import RedisBroccoli
except ImportError:
	try:
		from broccoli.storage import SQLiteBroccoli
	except ImportError:
		class RedisOrSqliteBroccoli(object):
			def __init__(self, *args, **kwargs):
				raise RuntimeError("Error, neither redis or sqlite is installed. Install "
								   "using pip: 'pip install redis' or if that is not possible make"
								   "sure that sqlite support is available")
				