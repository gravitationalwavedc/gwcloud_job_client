import inspect
import os
from functools import wraps

from asgiref.sync import sync_to_async as _sync_to_async

from settings import settings


# Code taken from https://github.com/django/asgiref/issues/142 for async iterators
def sync_to_async(sync_fn):
    is_gen = inspect.isgeneratorfunction(sync_fn)
    async_fn = _sync_to_async(sync_fn)

    if is_gen:

        @wraps(sync_fn)
        async def wrapper(*args, **kwargs):
            sync_iterable = await async_fn(*args, **kwargs)
            sync_iterator = await iter_async(sync_iterable)

            while True:
                try:
                    yield await next_async(sync_iterator)
                except StopAsyncIteration:
                    return

    else:

        @wraps(sync_fn)
        async def wrapper(*args, **kwargs):
            return await async_fn(*args, **kwargs)

    return wrapper


iter_async = sync_to_async(iter)


@sync_to_async
def next_async(it):
    try:
        return next(it)
    except StopIteration:
        raise StopAsyncIteration


async def sync_to_async_iterable(sync_iterable):
    sync_iterator = await iter_async(sync_iterable)
    while True:
        try:
            yield await next_async(sync_iterator)
        except StopAsyncIteration:
            return


def get_default_details():
    """
    Returns the default 'details' dictionary that is passed to the bundle.py file in each bundle

    :return: The default details dictionary
    """

    return {
        'cluster': settings.CLUSTER_NAME,
    }


def get_bundle_path():
    return os.path.join(os.path.dirname(__file__), "..", "bundles", "unpacked")
