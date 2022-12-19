import struct
import concurrent.futures
import time
import hashlib

from threading import Timer
from lossy_socket import LossyUDP
from socket import INADDR_ANY

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        self.initial = 0x00000000
        self.small_time_val = 0.05
        self.binary_value = b''       
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.send_data_frame = self.initial
        self.target_ip = dst_ip
        self.target_port = dst_port
        self.recv_dataFrame = self.initial
        self.acks = set()
        self.recv_buffer = dict()
        self.send_buffer = dict()
        self.closed = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)
        
    def counter(self, time, algo, dataFrame):
        return Timer(time, algo, [dataFrame]).start()

    def get_hash_data(self, data, ack, dataFrame, fin):
        first_packet = struct.pack('iii' + str(len(data)) + 's', dataFrame, ack, fin, data)
        m = hashlib.md5()
        m.update(first_packet)
        code_hash = m.digest()
        return struct.pack('iii16s' + str(len(data))+ 's', dataFrame, ack, fin, code_hash, data)

    def send_to(self, encoded_data, ip, port):
        self.socket.sendto(encoded_data, (ip, port))

    def send(self, data_bytes: bytes):
        if len(self.binary_value) == 0:
            self.counter(self.small_time_val, self.n_algo, len(data_bytes))
            self.binary_value = data_bytes
        else:
            self.binary_value = self.binary_value + data_bytes
    
    def n_algo(self, currData):
        if currData < len(self.binary_value):
            return self.counter(1, self.n_algo, len(self.binary_value))
        splitData = (self.binary_value[0 + pack : 1200 + pack] for pack in range(0, len(self.binary_value), 1200))
        for data in splitData:
            self.send_buffer[self.send_data_frame] = self.get_hash_data(data,0, self.send_data_frame, 0)
            val = self.send_buffer[self.send_data_frame]
            self.send_to(val, self.target_ip, self.target_port)
            self.counter(self.small_time_val * 10, self.rerun, self.send_data_frame)
            self.send_data_frame = self.send_data_frame + 1
        self.binary_value = b''

    def rerun(self, dataFrame):
        if dataFrame not in self.acks:
            val = self.send_buffer[dataFrame] 
            self.send_to(val, self.target_ip, self.target_port)
            return self.counter(self.small_time_val * 10, self.rerun, dataFrame)

    def recv(self):
        while self.recv_dataFrame not in self.recv_buffer:
            time.sleep(self.small_time_val / 5)
        self.recv_dataFrame = self.recv_dataFrame + 1
        return self.recv_buffer.pop(self.recv_dataFrame - 1)

    def listener(self):
        while self.closed == False:
            try:
                packet = self.socket.recvfrom()[0]
                if packet:
                    dataFrame, ack, fin, stored_hash, data = struct.unpack('iii16s' + str(len(packet) - 28) + 's', packet)

                    comparison_packet = struct.pack('iii' + str(len(data)) + 's', dataFrame, ack, fin, data)
                    m = hashlib.md5()
                    m.update(comparison_packet)
                    code_hash = m.digest()
                    
                    hash_check = False if stored_hash == code_hash else True
                    ack_check = True if ack else False
                    fin_check = True if fin else False

                    if hash_check:
                        continue
                    elif ack_check:
                        self.acks.add(dataFrame)
                    elif fin_check:
                        self.send_to(self.get_hash_data(bytes(), 1, dataFrame, 0), self.target_ip, self.target_port)
                    else:
                        self.recv_buffer[dataFrame] = data
                        self.send_to(self.get_hash_data(bytes(), 1, dataFrame, 0), self.target_ip, self.target_port)
                
            except Exception as e:
                print(e)
                print("Stack trace")

    def close(self):
        while self.send_data_frame not in self.acks:
            self.send_to(self.get_hash_data(bytes(), 0, self.send_data_frame, 1), self.target_ip, self.target_port)
            time.sleep(self.small_time_val * 2)
        print("END MESSAGE")
        time.sleep(10)
        self.closed = True
        self.socket.stoprecv()
