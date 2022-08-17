"""
Adapted from https://gist.github.com/grantjenks/095de18c51fa8f118b68be80a624c45a
"""
import importlib
import os
import sys
from types import ModuleType

import aiohttp.web
import aiohttp_xmlrpc.handler

# Make sure the current working directory is added to the python path so the bundle can be found
sys.path.append(os.getcwd())


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


class XMLRPCServer(aiohttp_xmlrpc.handler.XMLRPCView):
    def rpc_working_directory(self, details, job_data):
        import bundle

        rreload(bundle)

        return bundle.working_directory(details, job_data)

    def rpc_submit(self, details, job_data):
        import bundle

        rreload(bundle)

        return bundle.submit(details, job_data)

    def rpc_status(self, details, job_data):
        import bundle

        rreload(bundle)

        return bundle.status(details, job_data)

    def rpc_cancel(self, details, job_data):
        import bundle

        rreload(bundle)

        return bundle.cancel(details, job_data)

    def rpc_delete(self, details, job_data):
        import bundle

        rreload(bundle)

        return bundle.delete(details, job_data)


# Create the RPC server
app = aiohttp.web.Application()
app.router.add_route('*', '/', XMLRPCServer)

# Run the RPC server
aiohttp.web.run_app(app, path=sys.argv[1])
