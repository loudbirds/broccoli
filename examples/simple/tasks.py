import random
import time
from broccoli import crontab

from config import broccoli


@broccoli.task()
def count_beans(num):
    print('-- counted %s beans --' % num)
    return 'Counted %s beans' % num

@broccoli.periodic_task(crontab(minute='*/5'))
def every_five_mins():
    print('Consumer prints this every 5 mins')

@broccoli.task(retries=3, retry_delay=10)
def try_thrice(msg):
    print('----TRY THRICE: %s----' % msg)
    if random.randint(1, 3) == 1:
        print('OK')
    else:
        print('About to fail, will retry in 10 seconds')
        raise Exception('Crap something went wrong')

@broccoli.task()
def slow(n):
    time.sleep(n)
    print('slept %s' % n)