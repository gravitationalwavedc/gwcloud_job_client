"""
Adapted from https://gist.github.com/grantjenks/095de18c51fa8f118b68be80a624c45a
"""
import importlib
import os
import socketserver
import stat
import sys
from types import ModuleType

from xmlrpc.server import SimpleXMLRPCDispatcher, SimpleXMLRPCRequestHandler

# Make sure the current working directory is added to the python path so the bundle can be found
sys.path.append(os.getcwd())


class UnixStreamXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    disable_nagle_algorithm = False

    def address_string(self):
        return self.client_address


class UnixStreamXMLRPCServer(socketserver.UnixStreamServer, SimpleXMLRPCDispatcher):
    def __init__(self, addr, log_requests=True, allow_none=True, encoding=None, bind_and_activate=True,
                 use_builtin_types=True):
        self.logRequests = log_requests

        SimpleXMLRPCDispatcher.__init__(self, allow_none, encoding, use_builtin_types)
        socketserver.UnixStreamServer.__init__(self, addr, UnixStreamXMLRPCRequestHandler, bind_and_activate)
        os.chmod(addr, stat.S_IRUSR | stat.S_IWUSR)


# Adapted from https://stackoverflow.com/a/58201660
def rreload(module, mdict=None, base_module=None):
    """
    Recursively reload modules.

    Only reloads modules that are not from packages.
    """
    if mdict is None:
        mdict = {}
    if module not in mdict:
        # modules reloaded from this module
        mdict[module] = []
    if base_module is None:
        base_module = module

    importlib.reload(module)
    for attribute_name in dir(module):
        attribute = getattr(module, attribute_name)
        if type(attribute) is ModuleType and attribute not in mdict[module] \
                and attribute.__name__ not in sys.builtin_module_names and not attribute.__package__:
            mdict[module].append(attribute)
            rreload(attribute, mdict, base_module)

        elif callable(attribute) and hasattr(attribute, '__module__') \
                and attribute.__module__ not in sys.builtin_module_names \
                and f"_{attribute.__module__}" not in sys.builtin_module_names \
                and sys.modules[attribute.__module__] != base_module \
                and sys.modules[attribute.__module__] not in mdict \
                and not sys.modules[attribute.__module__].__package__:
            mdict[sys.modules[attribute.__module__]] = [attribute]
            rreload(sys.modules[attribute.__module__], mdict, base_module)

    importlib.reload(module)


def working_directory(details, job_data):
    import bundle

    rreload(bundle)

    return bundle.working_directory(details, job_data)


def submit(details, job_data):
    import bundle

    rreload(bundle)

    return bundle.submit(details, job_data)


def status(details, job_data):
    import bundle

    rreload(bundle)

    return bundle.status(details, job_data)


def cancel(details, job_data):
    import bundle

    rreload(bundle)

    return bundle.cancel(details, job_data)


def delete(details, job_data):
    import bundle

    rreload(bundle)

    return bundle.delete(details, job_data)


# Create the RPC server
server = UnixStreamXMLRPCServer(sys.argv[1])

# Register the functions
server.register_function(working_directory)
server.register_function(submit)
server.register_function(status)
server.register_function(cancel)
server.register_function(delete)

# Run the server forever
server.serve_forever()
