# -*- coding: utf-8 -*- 
# @Time : 2020.12.16 04:32:21
# @Author : Lloyd
# @File : rdt.py 
# Implement Reliable Data Transfer over Unreliable Transport Layer Using Go-Back-N Strategy
# Blocking recv and unblocking send.
# Refer to https://github.com/ziqin/CS305-Lab/blob/master/ReliableDataTransfer/ Created by Jeeken (Wang Ziqin)
# And the materials provided in CS305 Computer Network Class.
import logging
import socket
import struct

from collections import deque
from socket import timeout as TimeoutException
import threading
from typing import Union
from USocket import UnreliableSocket
import time


class RDTSocket(UnreliableSocket):
    """
    Connectionless Reliable Data Transfer Socket
    """
    MAX_RETRY_TIMES = 6
    TIMEOUT = 2
    WIN_SIZE = 2

    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self._rate = rate
        self._send_to = None
        self.debug = debug
        if self.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
        self.expected = 0
        self.base = 0
        self.ack_buffer = []
        self.data_buffer = []
        self.data_send = bytearray()
        self.send_thread = threading.Thread(target=self.unblocking_send)
        self.recv_thread = threading.Thread(target=self.recvfrom)
        logging.info('Sucessfully init a RDTSocket......')

    def close(self):
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        if self._send_to is None:
            super().close()
            logging.info('Connection Closed!')
            return
        fin_pkt = RDTSegment(None, seq_num=0, ack_num=0,fin=True).encode()
        while True:
            try:
                self.sendto(fin_pkt,self._send_to)
                logging.debug('Sending a fin packet......')
                segment_raw, remote_address = self._timeout_recvfrom(RDTSegment.SEGMENT_LEN, self.TIMEOUT * 2)
                segment = RDTSegment.parse(segment_raw)
                if segment.ack and segment.ack_num == 1 and remote_address == self._send_to:
                    while True:
                        fin_raw, addr = self._timeout_recvfrom(RDTSegment.SEGMENT_LEN, self.TIMEOUT * 2)
                        fin = RDTSegment.parse(fin_raw)
                        if fin.fin and fin.seq_num == 1 and addr == self._send_to:
                            ack = RDTSegment(None, seq_num=1,ack_num=1, ack=True).encode()
                            self.sendto(ack, self.set_send_to)
                            break
                self._send_to = None
                break
            except ValueError:
                logging.info('Error in the first shakehands')
            except TimeoutException:
                logging.info('Time out in the first shakehands')
            except Exception as e:
                logging.debug(e)
        self.send_thread.join()
        self.recv_thread.join()
        super().close()
        logging.info('Connection Closed!')

    def accept(self):
        """
        Accept a connection. The socket must be bound to an address and listening for 
        connections. The return value is a pair (conn, address) where conn is a new 
        socket object usable to send and receive data on the connection, and address 
        is the address bound to the socket on the other end of the connection.
        This function should be blocking.
        """
        conn, addr = RDTSocket(self._rate), None
        while True:
            try:
                segment_raw, remote_address = self._timeout_recvfrom(RDTSegment.SEGMENT_LEN, self.TIMEOUT * 2)
                segment = RDTSegment.parse(segment_raw)
                if segment.syn == 1:
                    logging.debug('Server received syn')
                    conn.set_send_to(remote_address)
                    ack = RDTSegment(None, seq_num=0, ack_num=0, syn=True, ack=True).encode()
                    while True:
                        try:
                            conn.sendto(ack, remote_address)
                            logging.debug('Server send syn&ack')
                            msg_raw, addr = conn._timeout_recvfrom(RDTSegment.SEGMENT_LEN, conn.TIMEOUT * 2)
                            msg = RDTSegment.parse(msg_raw)
                            if msg.ack_num == 0 and addr == conn._send_to:
                                logging.debug('Server received ack')
                                break
                        except ValueError:
                            logging.info('Error in the third shakehands')
                        except TimeoutException:
                            logging.info('Time out in the third shakehands')
                        except Exception as e:
                            logging.debug(e)
                    break
            except ValueError:
                logging.info('Error in the first shakehands')
            except TimeoutException:
                logging.info('Time out in the first shakehands')
            except Exception as e:
                logging.debug(e)
        addr = conn._send_to
        conn.send_thread.start()
        conn.recv_thread.start()
        return conn, addr

    def connect(self, address):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.
        """
        ack = RDTSegment(None, seq_num=0, ack_num=0, syn=True).encode()
        while True:
            try:
                self.sendto(ack, address)
                self.sendto(ack, address)
                self.sendto(ack, address)
                logging.debug('Client send syn')
                self._send_to = None
                self.setblocking(True)
                segment_raw, remote_address = self._timeout_recvfrom(RDTSegment.SEGMENT_LEN, self.TIMEOUT * 2)
                logging.debug('Client received ack&syn')
                segment = RDTSegment.parse(segment_raw)
                assert segment.syn == True and segment.ack == True, "Fail to establish a connection. Please wait a moment."
                ack = RDTSegment(None, seq_num=1, ack_num=0,ack=True).encode()
                self.set_send_to(remote_address)
                self.sendto(ack, remote_address)
                logging.debug('Client send ack')
                break
            except ValueError:
                logging.info('Error in the second shakehands')
            except TimeoutException:
                logging.info('Time out in the second shakehands')
            except Exception as e:
                logging.debug(e)
        logging.info('Connected to the server!')
        self.send_thread.start()
        self.recv_thread.start()

    def recv(self, buffer_size: int):
        """
        Receive data from the socket. 
        The return value is a bytes object representing the data received. 
        The maximum amount of data to be received at once is specified by bufsize. 
        
        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """
        assert self._send_to, "Connection not established yet. Use recvfrom instead."
        rcvd_data = bytearray()
        ack = RDTSegment(None, seq_num=0, ack_num=RDTSegment.SEQ_NUM_BOUND-1, ack=True)
        logging.info('Ready to receive...')
        duplicate_send = 0
        while True:
            try:
                segment = None
                for i in range(5):
                    segment = self.get_pkt('data', self.expected)
                    if segment is None:
                        time.sleep(0.2)
                    else:
                        break
                if segment is None:
                    raise ValueError
                if not segment.fin:
                    logging.debug('Receive #%d', self.expected)
                    if segment.seq_num == self.expected:
                        assert segment.len > 0
                        if len(rcvd_data) + segment.len <= buffer_size:
                            rcvd_data.extend(segment.payload)
                        else:
                            break
                        duplicate_send = 0
                        self.expected = (self.expected + 1) % RDTSegment.SEQ_NUM_BOUND
                        ack.ack_num = self.expected
                    else:
                        duplicate_send += 1
                        ack.ack_num = self.expected
                    if duplicate_send < 3:
                        self.sendto(ack.encode(), self._send_to)
                    # if the buffer is about to overflow, then send the ack segment again and return.
                    if len(rcvd_data) + RDTSegment.MAX_PAYLOAD_LEN > buffer_size:
                        self.sendto(ack.encode(), self._send_to)
                        break
                else:
                    # if receive fin, the send ack and fin.
                    ack = RDTSegment(None, seq_num=0, ack_num=1, ack=True)
                    self.sendto(ack.encode(), self._send_to)
                    self.sendto(ack.encode(), self._send_to)
                    self.sendto(ack.encode(), self._send_to)
                    fin = RDTSegment(None, seq_num=1, ack_num=1, fin=True)
                    self.sendto(fin.encode(), self._send_to)
                    self.sendto(fin.encode(), self._send_to)
                    self.sendto(fin.encode(), self._send_to)
                    logging.info('------------- oppsite side closed --------------')
                    logging.debug('Ready to close!')
                    self._send_to = None
                    self.recv_thread.join()
                    # self.setblocking(True)
                    super().close()
                    return None
            except ValueError:
                logging.info('wrong packet')
                ack.ack = False
                ack.ack_num = self.expected
                self.sendto(ack.encode(), self._send_to)
            except Exception as e:
                logging.debug(e)
                time.sleep(500)
            
        logging.info('------------- receipt finished --------------')
        logging.info('------------- %d --------------', len(rcvd_data))
        return bytes(bytes(rcvd_data).decode('utf-8').strip(b'\x00'.decode()).encode('utf-8'))

    def send(self, data: bytes):
        """
        Send data to the socket. 
        The socket must be connected to a remote socket, i.e. self._send_to must not be none.
        """
        assert self._send_to, "Connection not established yet. Use sendto instead."
        assert data, 'data is None'
        self.data_send.extend(data)
        length = len(data) % RDTSegment.MAX_PAYLOAD_LEN
        if length > 0:
            self.data_send.extend(b'\x00' * (RDTSegment.MAX_PAYLOAD_LEN - length))
        return len(data)

    def set_send_to(self, send_to):
        self._send_to = send_to

    def unblocking_send(self):
        address = self._send_to
        UNIT = RDTSegment.MAX_PAYLOAD_LEN
        cc = CongestionControl()
        newACK: bool = False
        duplicateACK: bool = False
        timeout: bool = False
        next = self.base  # to be sent
        max_send_num = self.base
        logging.info('ready to send...')

        # Send data
        while True:
            if len(self.data_send) == 0:
                continue
            if self._send_to is None:
                break
            # enqueue
            # win = deque(maxlen=cc.FSM(newACK,duplicateACK,timeout))
            win = deque(maxlen=RDTSocket.WIN_SIZE)
            logging.debug('window size %d', win.maxlen)
            newACK: bool = False
            duplicateACK: bool = False
            timeout: bool = False
            while next < self.base + win.maxlen:
                if next * UNIT >= len(self.data_send):
                    break
                pkt = RDTSegment(self.data_send[next * UNIT:next * UNIT + UNIT], seq_num=next, ack_num=0)
                win.append(pkt)
                next += 1

            if next > max_send_num:
                max_send_num = next
                
            for pkt in win:
                self.sendto(pkt.encode(), address)
                logging.debug('sent #%d', pkt.seq_num)

            # handle acknowledgements
            while True:
                try:
                    rcvd_ack = None
                    for i in range(10):
                        rcvd_ack = self.get_pkt('ack', next)
                        if rcvd_ack is None:
                            time.sleep(0.2)
                        else:
                            break
                    if rcvd_ack is not None:
                        if rcvd_ack.ack_num < next:
                            time.sleep(0.2)
                            rcvd_ack = self.get_pkt('ack', next)
                    else:
                        raise TimeoutException
                    if rcvd_ack is None:
                        raise TimeoutException
                    if not rcvd_ack.ack:
                        self.base = rcvd_ack.ack_num
                        next = self.base
                        break
                    if rcvd_ack.ack_num <= self.base:
                        raise AssertionError
                    newACK = True
                    logging.info('#%d acked', rcvd_ack.ack_num)
                    self.base = rcvd_ack.ack_num
                    next = self.base
                    break
                except AssertionError:
                    duplicateACK = True
                    logging.info('duplicate ack')
                except TimeoutException:
                    timeout = True
                    logging.debug('TimeOut!!!!!')
                    next = self.base
                    break        
        logging.info('----------- all data have been sent -----------')

    def get_pkt(self, mode, num):
        """Get the expected segment from buffer."""
        res = None
        if mode == 'ack':
            min = RDTSocket.WIN_SIZE
            for i in range(len(self.ack_buffer)):
                pkt = self.ack_buffer[i]
                if 0 <= num - pkt.ack_num < min:
                    res = pkt
                    min = num - pkt.ack_num
                    if min == 0:
                        break
            for pkt in self.ack_buffer:
                if pkt.ack_num + min <= num:
                    self.ack_buffer.remove(pkt)
        elif mode == 'data':
            for pkt in self.data_buffer:
                if pkt.seq_num == num:
                    res = pkt
                    break
            if res is None:
                for pkt in self.data_buffer:
                    if pkt.fin:
                        return pkt
            for pkt in self.data_buffer:
                if pkt.seq_num == num:
                    res = pkt
                    self.data_buffer.remove(pkt)
                elif pkt.seq_num < num:
                    self.data_buffer.remove(pkt)
        return res

    def recvfrom(self):
        """
        The runing function in recv threading, which will
        sostenuto recv segment from the other side until it is closed.
        And put the ack segment into ack_buffer, put the data segment or fin segment into data_buffer.
        """
        self.settimeout(self.TIMEOUT)
        while True:
            try:
                if self._send_to is None:
                    break
                ans_raw, address = super().recvfrom(RDTSegment.SEGMENT_LEN+8)
                assert address == self._send_to
                assert ans_raw
                ans = RDTSegment.parse(ans_raw)
                if ans.len == 0 and not ans.fin:
                    self.ack_buffer.append(ans)
                else:
                    self.data_buffer.append(ans)
            except socket.timeout:
                logging.debug('Receive time out')
            except ValueError:
                logging.debug('Receive a wrong packet')
            except Exception as e:
                logging.debug(e)

    def _timeout_recvfrom(self, buffer_size, timeout=TIMEOUT):
        try:
            self.settimeout(timeout)
            ans = super().recvfrom(buffer_size+8)
            return ans
        except socket.timeout:
            raise TimeoutException

class CongestionControl:
    """
    TCP CongestionControl FSM
    In order to simplify, let mss = 1
    the function FSM return the wind size.
    """

    def __init__(self):
        self.mss = 1
        self.cwnd = 1 * self.mss
        self.ssthresh = 64 * self.mss
        self.dupACKCount = 0
        self.status = 'SS'

    def FSM(self, newACK: bool = False, duplicateACK: bool = False, timeout: bool = False):
        if self.status == 'SS':
            if duplicateACK:
                self.dupACKCount += 1
            elif newACK:
                self.cwnd += self.mss
                self.dupACKCount = 0
            elif timeout:
                self.ssthresh = self.cwnd // 2
                self.cwnd = self.mss
                self.dupACKCount = 0
            if self.cwnd >= self.ssthresh:
                self.cwnd += self.mss
                self.dupACKCount = 0
                self.status = 'CA'
            if self.dupACKCount == 3:
                self.ssthresh = self.cwnd // 2
                self.cwnd = self.ssthresh + 3
                self.status = 'FR'
        elif self.status == 'CA':
            if newACK:
                self.cwnd += self.mss // self.cwnd
                self.dupACKCount = 0
            elif duplicateACK:
                self.dupACKCount += 1
            if timeout:
                self.ssthresh = self.cwnd//2
                self.cwnd = self.mss
                self.dupACKCount = 0
                self.status = 'SS'
            if duplicateACK == 3:
                self.ssthresh = self.cwnd // 2
                self.cwnd = self.ssthresh + 3
                self.status = 'FR'
        elif self.status == 'FR':
            if duplicateACK:
                self.cwnd += self.mss
            if newACK:
                self.cwnd = self.ssthresh
                self.dupACKCount = 0
                self.status = 'CA'
            if timeout:
                self.ssthresh = self.cwnd // 2
                self.cwnd = 1
                self.dupACKCount = 0
                self.status = 'SS'
        return self.cwnd
            

class RDTSegment:
    """
    Reliable Data Transfer Segment
    Segment Format:
      0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f  10  11  12  13  14  15  16  17  18  19  1a  1b  1c  1d  1e  1f
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |RDT VERSION|Pipelined protocol |SYN|FIN|ACk|      head len     |                            CHECKSUM                           |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                              SEQ                                                              |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                            SEQ ACK                                                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                              Len                                                              |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                                                                                               |
    /                                                            PAYLOAD                                                            /
    /                                                                                                                               /
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    RDT VERSION:                8(3.0)
    Pipelined protocol:         1(GO BACK N)
    Flags:
     - SYN                      Synchronize
     - FIN                      Finish
     - ACK                      Acknowledge
    Ranges:
     - Payload Length           0 - 1440  (append zeros to the end if length < 1440)
     - Sequence Number          0 - 4294967296
     - Acknowledgement Number   0 - 4294967296
    Checksum Algorithm:         32 bit one's complement of the one's complement sum
    """

    HEADER_LEN = 16
    MAX_PAYLOAD_LEN = 2048
    SEGMENT_LEN = MAX_PAYLOAD_LEN + HEADER_LEN
    SEQ_NUM_BOUND = 2147483647  #2^31-1

    def __init__(self,
                 payload: bytes,
                 seq_num: int,
                 ack_num: int,
                 syn: bool = False,
                 fin: bool = False,
                 ack: bool = False):
        self.len = len(payload) if payload else 0
        self.syn = syn
        self.fin = fin
        self.ack = ack
        self.seq_num = seq_num % RDTSegment.SEQ_NUM_BOUND
        self.ack_num = ack_num % RDTSegment.SEQ_NUM_BOUND
        if payload is not None and len(payload) > RDTSegment.MAX_PAYLOAD_LEN:
            logging.debug('A too long packet!')
            raise ValueError
        self.payload = payload

    def encode(self):
        """Returns packet in format."""
        head = 0xE1000000
        if self.syn:
            head |= 0x00800000
        if self.fin:
            head |= 0x00400000
        if self.ack:
            head |= 0x00200000
        head |= 0x00100000
        head |= RDTSegment.calc_checksum(self.payload)
        arr = bytearray(struct.pack('!IIII', head, self.seq_num, self.ack_num, self.len))
        if self.payload:
            arr.extend(self.payload)
        return bytes(arr)

    @staticmethod
    def calc_checksum(payload):
        """
        :param segment: raw bytes of a segment, with its checksum set to 0
        :return: 16-bit unsigned checksum
        """
        if payload is None:
            return 0xFFFF
        i = iter(payload)
        bytes_sum = sum(((a << 8) + b for a, b in zip(i, i)))  # for a, b: (s[0], s[1]), (s[2], s[3]), ...
        # pad zeros to form a 16-bit word for checksum
        if len(payload) % 2 == 1:  
            bytes_sum += payload[-1] << 8
        # add the overflow at the end (adding twice is sufficient)
        bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
        bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
        return (~bytes_sum & 0xFFFF)

    @staticmethod
    def parse(segment: Union[bytes, bytearray]):
        """Parse raw bytes into an RDTSegment object"""
        try:
            head, = struct.unpack('!I', segment[0:4])
            syn = (head & 0x00800000) != 0
            fin = (head & 0x00400000) != 0
            ack = (head & 0x00200000) != 0
            seq_num, ack_num, lenght = struct.unpack('!III', segment[4:16])
            payload = segment[16:16 + lenght] if lenght != 0 else None
            assert RDTSegment.calc_checksum(payload) == int.from_bytes(segment[2:4], byteorder='big', signed=False)
            return RDTSegment(payload, seq_num, ack_num, syn, fin, ack)
        except AssertionError:
            logging.debug('Corrupt!')
            raise ValueError