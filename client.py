import socket
import time

class ClientError(Exception):
    pass

class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        try:
            self.sock = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error as err:
            raise ClientError("error create connection", err)

    def put(self, key, value, timestamp=None):
        try:
            if timestamp == None:
                self.sock.sendall("put {metric} {val} {time}\n".format(metric=key, val=str(value), time=str(int(time.time()))).encode())
            else:
                self.sock.sendall("put {metric} {val} {time}\n".format(metric=key, val=str(value), time=str(timestamp)).encode())
        except socket.error as err:
            raise ClientError("error send data", err)

        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.sock.recv(1024)
            except socket.error as err:
                raise ClientError("error recv data", err)
        
        decoded_data = data.decode()
 
        status, payload = decoded_data.split("\n", 1)
        payload = payload.strip()
 
        if status == "error":
            raise ClientError(payload)

    def get(self, key):
        try:
            self.sock.sendall("get {metric}\n".format(metric=key).encode())
        except socket.error as err:
            raise ClientError("error send", err)

        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.sock.recv(1024)
            except socket.error as err:
                raise ClientError("error recv data", err)

        decoded_data = data.decode()

        status, payload = decoded_data.split("\n", 1)
        payload = payload.strip()

        if status == "error":
            raise ClientError(payload)

        data = {}
        if payload == "":
            return data

        for row in payload.split("\n"):
            key, value, timestamp = row.split()
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data
