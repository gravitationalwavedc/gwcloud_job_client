import json
import os
from time import sleep


def working_directory(details, job_data):
    return os.path.join('/some/path/', str(details['job_id']))


def submit(details, job_data):
    sleep(1)
    return 4321


def status(details, job_data):
    with open('test_output', 'r') as f:
        return {
            'what': 'status',
            'expected': json.load(f),
            'details': details,
            'job_data': job_data
        }


def cancel(details, job_data):
    with open('test_output', 'r') as f:
        return {
            'what': 'cancel',
            'expected': json.load(f),
            'details': details,
            'job_data': job_data
        }


def delete(details, job_data):
    with open('test_output', 'r') as f:
        return {
            'what': 'delete',
            'expected': json.load(f),
            'details': details,
            'job_data': job_data
        }


try:
    from test_extra import *
except Exception:
    pass
