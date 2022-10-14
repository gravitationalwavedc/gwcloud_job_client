import os

# The working directory is the directory used by the daemon for writing log files
HPC_LOG_DIRECTORY = os.path.join(os.getcwd(), 'log')

# The remote web address of the websocket server (ws(s)://host:port/path)
HPC_WEBSOCKET_SERVER = 'ws://127.0.0.1:8001/job/ws/'

# The name of this cluster
CLUSTER_NAME = "ozstar"

try:
    from .local import *
except:
    pass