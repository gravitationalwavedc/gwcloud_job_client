import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, '..', 'db.sqlite3'),
        'OPTIONS': {
            'timeout': 30
        },
        'CONN_MAX_AGE': 10
    }
}

INSTALLED_APPS = ['db']

SECRET_KEY = 'none'
