import socket
import time

class ClientError(Exception):
    pass

class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = socket.create_connection((self.host, self.port), self.timeout)

    def put(self, metric, value, timestamp=None):
        try:
            if timestamp == None:
                self.sock.sendall("put {metr} {val} {time}\n".format(metr=metric, val=str(value), time=str(int(time.time()))).encode("utf-8"))
            else:
                self.sock.sendall("put {metr} {val} {time}\n".format(metr=metric, val=str(value), time=str(timestamp)).encode("utf-8"))
        except socket.error as err:
            raise ClientError

        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.sock.recv(1024)
            except socket.error as err:
                raise ClientError("error recv data", err)

    def get(self, metric):
        try:
            return self.sock.sendall("get {metr}\n".format(metr=metric).encode("utf-8"))
        except socket.error as err:
                raise ClientError("error send", err)