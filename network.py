from socket import socket, AF_INET, SOCK_DGRAM, inet_aton, inet_ntoa
import random, time
import threading, queue
import numpy as np
from socketserver import ThreadingUDPServer

lock = threading.Lock()


def bytes_to_addr(bytes):
    return inet_ntoa(bytes[:4]), int.from_bytes(bytes[4:8], 'big')


def addr_to_bytes(addr):
    return inet_aton(addr[0]) + addr[1].to_bytes(4, 'big')


class Server(ThreadingUDPServer):
    def __init__(self, addr, rate=None, delay=None):
        super().__init__(addr, None)
        self.rate = rate
        self.buffer = 0
        self.delay = delay

    def verify_request(self, request, client_address):
        """
        request is a tuple (data, socket)
        data is the received bytes object
        socket is new socket created automatically to handle the request

        if this function returns False， the request will not be processed, i.e. is discarded.
        details: https://docs.python.org/3/library/socketserver.html
        """
        if self.buffer < 100000:  # some finite buffer size (in bytes)
            self.buffer += len(request[0])
            return True
        else:
            return False

    def finish_request(self, request, client_address):
        loss_rate = 0.05
        corrupt_rate = 1e-5

        data, socket = request

        with lock:
            if self.rate: time.sleep(len(data) / self.rate)
            self.buffer -= len(data)
            if random.random() < loss_rate:
                return
            
            for i in range(8, len(data)-1):
                if random.random() < corrupt_rate:
                    data = data[:i] + (data[i] + 1).to_bytes(1, 'big') + data[i+1:]

        tem = np.random.normal(loc=0,scale=pow(2048*2/10240,0.5),size=1)[0]
        time.sleep(abs(tem))

        to = bytes_to_addr(data[:8])
        print(client_address, to)  # observe tht traffic
        socket.sendto(addr_to_bytes(client_address) + data[8:], to)


server_address = ('127.0.0.1', 12345)

if __name__ == '__main__':
    with Server(server_address,10240) as server:
        server.serve_forever()