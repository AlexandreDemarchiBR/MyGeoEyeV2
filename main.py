#!/usr/bin/python3
import socket
import os
import threading
import random
import time

MAIN_ADDR = ''
MAIN_PORT = 5555

CONTROL_MSG_SIZE_BYTES = 1024
MAX_CHUNK_SIZE_BYTES = 1024 * 4

REPLICATION_FACTOR = 2  # Pode ser alterado conforme necessário

class Main:
    def __init__(self, host: str, port: int, replication_factor: int) -> None:
        self.listen_addr = (host, port)
        self.workers = []
        self.replication_factor = replication_factor
        self.lock = threading.Lock()
        with open('main_dir/workers.txt', 'r') as f:
            workers = f.readlines()
            for line in workers:
                line = line.split()
                self.workers.append((line[0], int(line[1])))
        
        print(f"Main server initialized with {len(self.workers)} workers and replication factor {self.replication_factor}")

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(self.listen_addr)
            s.listen(400)
            print(f'Server is listening on {self.listen_addr}')
            while True:
                try:
                    conn, addr = s.accept()
                    print(f"Connected with {addr}")
                    t = threading.Thread(target=self.process_connection, args=(conn, addr), daemon=True)
                    t.start()
                except KeyboardInterrupt:
                    print('\tauf wiedersehen...')
                    s.close()
                    quit()

    def process_connection(self, conn: socket.socket, addr: tuple[str, int]):
        with conn:
            print(f"# main from client: recv control message from {addr}")
            control_msg = self.recvall(conn, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode()
            control_msg = control_msg.strip().split('$')
            if control_msg[0] == 'UPLOAD':
                print(f'{addr} requesting upload of {control_msg[1]}')
                file_name = control_msg[1]
                file_size = int(control_msg[2])
                self.upload_to_datanodes(conn, addr, file_name, file_size)
            elif control_msg[0] == 'LISTING':
                print(f'{addr} requesting listing')
                self.list_images(conn, addr)
            elif control_msg[0] == 'DOWNLOAD':
                print(f'{addr} requesting download of {control_msg[1]}')
                self.download_from_datanodes(conn, addr, control_msg[1])
            elif control_msg[0] == 'DELETE':
                print(f'{addr} requesting deletion of {control_msg[1]}')
                self.delete_in_datanodes(control_msg[1])

    def upload_to_datanodes(self, client_conn: socket.socket, client_addr: tuple[str, int], file_name: str, file_size: int):
        selected_datanodes = random.sample(self.workers, min(self.replication_factor, len(self.workers)))
        print(f"Selected datanodes for upload of {file_name}: {selected_datanodes}")
        
        start_time = time.time()
        #################################
        

        datanode_conn_list = []
        for datanode_addr in selected_datanodes:
            control_msg = f'UPLOAD${file_name}${file_size}'
            control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
            control_msg = control_msg.encode()
            #self.upload_to_datanode(client_conn, client_addr, datanode_addr, file_name, file_size)
            datanode_conn_temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            datanode_conn_temp.connect(datanode_addr)
            print(f"# main to datanode: send UPLOAD control message to {datanode_addr}")
            datanode_conn_temp.sendall(control_msg)

            print(f"# main from datanode: recv READY message from {datanode_addr}")
            control_msg = self.recvall(datanode_conn_temp, CONTROL_MSG_SIZE_BYTES)
            msg = control_msg.decode()
            msg = msg.strip().split('$')

            if msg[0] != 'READY':
                print(f"Datanode {datanode_addr} not ready")
                datanode_conn_temp.close()
                for datanode_conn in datanode_conn_list:
                    datanode_conn.close()
                return
            datanode_conn_list.append(datanode_conn_temp)

        print(f"# main to client: send READY message to {client_addr}")
        client_conn.sendall(control_msg)

        bytes_sent = 0
        while bytes_sent < file_size:
            chunk = self.recvall(client_conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
            #datanode_conn.sendall(chunk)
            for datanode_conn in datanode_conn_list:
                datanode_conn.sendall(chunk)
            bytes_sent += len(chunk)
            #print(f"Sent {bytes_sent}/{file_size} bytes of {file_name} to {datanode_addr}")

        

        #################################
        with open(f'main_dir/{file_name}', 'w') as f, self.lock:
            f.write(f"{file_size},{','.join([f'{addr[0]}:{addr[1]}' for addr in selected_datanodes])}")
        
        end_time = time.time()
        print(f"Upload of {file_name} completed in {end_time - start_time:.4f} seconds")

    def upload_to_datanode(self, client_conn: socket.socket, client_addr: tuple[str, int], datanode_addr: tuple[str, int], file_name: str, file_size: int):
        control_msg = f'UPLOAD${file_name}${file_size}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            print(f"# main to datanode: send UPLOAD control message to {datanode_addr}")
            datanode_conn.connect(datanode_addr)
            datanode_conn.sendall(control_msg)

            print(f"# main from datanode: recv READY message from {datanode_addr}")
            control_msg = self.recvall(datanode_conn, CONTROL_MSG_SIZE_BYTES)
            msg = control_msg.decode()
            msg = msg.strip().split('$')

            if msg[0] != 'READY':
                print(f"Datanode {datanode_addr} not ready")
                return
            
            print(f"# main to client: send READY message to {client_addr}")
            client_conn.sendall(control_msg)
            
            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = self.recvall(client_conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                datanode_conn.sendall(chunk)
                bytes_sent += len(chunk)
                #print(f"Sent {bytes_sent}/{file_size} bytes of {file_name} to {datanode_addr}")

    def download_from_datanodes(self, client_conn: socket.socket, client_addr: tuple[str, int], file_name: str):
        with open(f'main_dir/{file_name}', 'r') as f:
            file_info = f.read().split(',')
            file_size = int(file_info[0])
            datanode_addrs = [addr.split(':') for addr in file_info[1:]]
            datanode_addr = random.choice(datanode_addrs)
            datanode_addr = (datanode_addr[0], int(datanode_addr[1]))

        print(f"Selected datanode for download of {file_name}: {datanode_addr}")
        self.download_from_datanode(client_conn, client_addr, datanode_addr, file_name, file_size)

    def download_from_datanode(self, client_conn: socket.socket, client_addr: tuple[str, int], datanode_addr: tuple[str, int], file_name: str, file_size: int):
        control_msg = f'DOWNLOAD${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            print(f"# main to datanode: send DOWNLOAD control message to {datanode_addr}")
            datanode_conn.connect(datanode_addr)
            datanode_conn.sendall(control_msg)

            print(f"# main from datanode: recv SIZE_BYTES from {datanode_addr}")
            control_msg = self.recvall(datanode_conn, CONTROL_MSG_SIZE_BYTES)
            print(f"# main to client: send SIZE_BYTES to {client_addr}")
            client_conn.sendall(control_msg)

            print(f"# main from client: recv READY message from {client_addr}")
            control_msg = self.recvall(client_conn, CONTROL_MSG_SIZE_BYTES)
            print(f"# main to datanode: send READY message to {datanode_addr}")
            datanode_conn.sendall(control_msg)

            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = self.recvall(datanode_conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                client_conn.sendall(chunk)
                bytes_sent += len(chunk)
                #print(f"Sent {bytes_sent}/{file_size} bytes of {file_name} to {client_addr}")

    def delete_in_datanodes(self, file_name: str):
        with open(f'main_dir/{file_name}', 'r') as f:
            file_info = f.read().split(',')
            datanode_addrs = [addr.split(':') for addr in file_info[1:]]

        print(f"Deleting {file_name} from datanodes: {datanode_addrs}")
        for datanode_addr in datanode_addrs:
            datanode_addr = (datanode_addr[0], int(datanode_addr[1]))
            self.delete_in_datanode(datanode_addr, file_name)

        os.remove(f'main_dir/{file_name}')
        print(f"Deleted {file_name} from main server")

    def delete_in_datanode(self, datanode_addr: tuple[str, int], file_name: str):
        control_msg = f'DELETE${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            print(f"# main to datanode: send DELETE control message to {datanode_addr}")
            datanode_conn.connect(datanode_addr)
            datanode_conn.sendall(control_msg)

    def list_images(self, conn: socket.socket, client_addr: tuple[str, int]):
        with conn:
            msg = os.listdir('main_dir')
            msg.remove('workers.txt')
            msg.remove('main_endpoint.txt')
            msg = ' '.join(msg).encode()

            msg_size = len(msg)
            print(f"# main to client: send listing size {msg_size} to {client_addr}")
            conn.sendall(str(msg_size).ljust(1024, ' ').encode())

            print(f"# main to client: send listing to {client_addr}")
            conn.sendall(msg)
            print(f'Listing for {client_addr} done')

    def recvall(self, conn: socket.socket, msg_size: int) -> bytes:
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < msg_size:
            chunk = conn.recv(msg_size)
            chunks.append(chunk)
            bytes_recvd += len(chunk)
        return b''.join(chunks)

if __name__ == '__main__':
    s = Main(MAIN_ADDR, MAIN_PORT, REPLICATION_FACTOR)
    s.start()
