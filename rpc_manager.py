from multiprocessing.connection import Listener, Client
from multiprocessing.connection import Connection, answer_challenge, deliver_challenge
import socket, struct
from threading import Thread


class RPCManager(object):
    def __init__(self, address=None, authkey=None):
        self._functions = {}
        self._server_c = Listener(address, authkey=authkey)
        self.connection_listener = None

    def register_function(self, func):
        self._functions[func.__name__] = func

    def register_listener(self, listener):
        self.connection_listener = listener

    def serve_forever(self):
        id = 0
        while True:
            client_c = self._server_c.accept()
            t = Thread(name=f"rpc-client{id}",target=self.handle_client, args=(client_c,))
            t.daemon = True
            t.start()

    def handle_client(self, client_c):
        # server_name = client_c.recv()
        # self.connection_listener.connected(server_name, client_c)
        while True:
            func_name, args, kwargs = client_c.recv()
            try:
                r = self._functions[func_name](*args, **kwargs)
                client_c.send(r)
            except Exception as e:
                client_c.send(e)
                self.connection_listener.disconnected(client_c)

    @property
    def address(self):
        return self._server_c.address


class RPCProxy(object):
    def __init__(self, address, authkey, timeout=0):
        self._conn = self.ClientWithTimeout(address, authkey=authkey, timeout=timeout)
        # self._conn = Client(address, authkey=authkey)

    def send(self, data):
        self._conn.send((data))

    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            self._conn.send((name, args, kwargs))
            result = self._conn.recv()
            if isinstance(result, Exception):
                raise result
            return result

        return do_rpc

    def ClientWithTimeout(self, address, authkey, timeout):
        with socket.socket(socket.AF_INET) as s:
            s.setblocking(True)
            s.connect(address)

            # We'd like to call s.settimeout(timeout) here, but that won't work.

            # Instead, prepare a C "struct timeval" to specify timeout. Note that
            # these field sizes may differ by platform.
            seconds = int(timeout)
            microseconds = int((timeout - seconds) * 1e6)
            timeval = struct.pack("@LL", seconds, microseconds)

            # And then set the SO_RCVTIMEO (receive timeout) option with this.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeval)

            # Now create the connection as normal.
            c = Connection(s.detach())

        # The following code will now fail if a socket timeout occurs.

        answer_challenge(c, authkey)
        deliver_challenge(c, authkey)

        return c