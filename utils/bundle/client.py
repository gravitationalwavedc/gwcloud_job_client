"""
Adapted from https://gist.github.com/grantjenks/095de18c51fa8f118b68be80a624c45a
"""

import http.client
import socket
import xmlrpc.client


class UnixStreamHTTPConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self.sock.connect(self.host)


class UnixStreamTransport(xmlrpc.client.Transport, object):
    def __init__(self, socket_path):
        self.socket_path = socket_path
        super().__init__()

    def make_connection(self, host):
        return UnixStreamHTTPConnection(self.socket_path)


class UnixStreamXMLRPCClient(xmlrpc.client.ServerProxy):
    def __init__(self, addr, **kwargs):
        transport = UnixStreamTransport(addr)
        super().__init__(
            "http://", transport=transport, **kwargs
        )
