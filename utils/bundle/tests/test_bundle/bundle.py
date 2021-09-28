import json


def working_directory(details, job_data):
    with open('test_output', 'r') as f:
        return {
            'what': 'working_directory',
            'expected': json.load(f),
            'details': details,
            'job_data': job_data
        }


def submit(details, job_data):
    with open('test_output', 'r') as f:
        return {
            'what': 'submit',
            'expected': json.load(f),
            'details': details,
            'job_data': job_data
        }


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
