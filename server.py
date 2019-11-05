import asyncio
import json
from tempfile import gettempdir
from os import path

class ClientError(Exception):
    pass

class Server:
    def __init__(self):
        self.STORAGE = path.join(gettempdir(), 'storage.data')
        with open(self.STORAGE, "r") as f:
            self.dct = json.load(f)

    def put(self, key, value, timestamp):
        self.STORAGE = path.join(gettempdir(), 'storage.data')
        with open(self.STORAGE, "r") as f:
            self.dct = json.load(f)
        if key not in self.dct:
            self.dct[key] = {}
        self.dct[key][timestamp] = value
        with open(self.STORAGE, "w") as write_file:
            json.dump(self.dct, write_file)

    def get(self, key):
        self.STORAGE = path.join(gettempdir(), 'storage.data')
        with open(self.STORAGE, "r") as f:
            dct = json.load(f)

        if key=='*':
            new_dct = {}
            for key, dct_values in dct.items():
                lst_values = sorted(dct_values.items())
                new_dct[key] = lst_values
        else:
            new_dct = {}
            if key in dct:
                new_dct[key] = dct.get(key)
            else:
                new_dct[key] = {}
            for key, dct_values in new_dct.items():
                lst_values = sorted(dct_values.items())
                new_dct[key] = lst_values
        return new_dct

class ClientServerProtocol(asyncio.Protocol):
    server = Server()

    def process_data(self, data):
        lst = self.read(data) #список команд с параметрами
        lst_to_send = [] #список ответов сервера в виде словаря
        for com in lst:
            if com[0]=="put":
                self.server.put(com[1], com[2], com[3])
            elif com[0]=="get":
                r = self.server.get(com[1])
                lst_to_send.append(r)
        send = [] #список посылаемых ответов
        for dct in lst_to_send:
            for key, values in dct.items():
                for timestamp, value in values:
                    s = f"{key} {value} {timestamp}"
                    send.append(s)
        resp = "ok\n"
        if len(send)!=0:
            resp += "\n".join(send) + "\n\n"
        else:
            resp += "\n"
        return resp

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            resp = self.process_data(data.decode())
            self.transport.write(resp.encode())
        except:
            self.transport.write(f'error\nwrong command\n\n'.encode())

    def read(self, data):
        comms = []
        lst_commands = data.split("\n")
        for i in lst_commands:
            if i=='':
                lst_commands.remove(i)
        for com in lst_commands:
            lst = com.strip().split(' ')
            method = lst[0]
            if method=="put":
                key = lst[1]
                value = float(lst[2])
                timestamp = int(lst[3])
                comms.append((method, key, value, timestamp))
            elif method=="get":
                key = lst[1]
                comms.append((method, key))
            else:
                raise ClientError
        return comms


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol,host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server('127.0.0.1', 8888)
