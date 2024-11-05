import logging
import random
from logger import logger

def log_fail(whatfailed, reason, tablename):
    logger.info('Failure log', extra={
        'username': 'malvika',
        'log_id': random.randint(1000, 9999),
        'values': {
            'whatfailed': whatfailed,
            'reason': reason,
            'iserrorlog': 1,
            'table': tablename
        }
    })

def log_incrementalloading(tablename, count):
    logger.info('Incremental loading log', extra={
        'username': 'malvika',
        'log_id': random.randint(1000, 9999),
        'values': {
            'iserrorlog': 0,
            'table': tablename,
            'count': count
        }
    })

def log_update(tablename, scd_type,message):
    logger.info('Update log', extra={
        'username': 'malvika',
        'log_id': random.randint(1000, 9999),
        'values': {
            'iserrorlog': 0,
            'table': tablename,
            'scd_type': scd_type,
            'message':message
        }
    })

def log_overwrite(tablename):
    logger.info('Overwrite log', extra={
        'username': 'malvika',
        'log_id': random.randint(1000, 9999),
        'values': {
            'iserrorlog': 0,
            'table': tablename,
            'overwrite': 'Y'
        }
    })
