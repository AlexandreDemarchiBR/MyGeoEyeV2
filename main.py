import socket
import os
import threading

MAIN_ADDR = 'localhost'
MAIN_PORT = 5555

DATANODE_ADDR = 'localhost'
DATANODE_PORT = 6666

CONTROL_MSG_SIZE_BYTES = 1024
MAX_CHUNK_SIZE_BYTES = 1024 * 4

class Main:
    def __init__(self, host: str, port: int) -> None:
        self.listen_addr = (host,port)
        self.workers = []
        self.lock = threading.Lock()
        with open('main_dir/workers.txt', 'r') as f:
            workers = f.readlines()
            for line in workers:
                line = line.split()
                self.workers.append((line[0], int(line[1])))
        if not os.path.exists('main_dir/'):
            os.makedirs('main_dir/')

    
    # main server loop
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


    # runs in a thread to process a client connection
    def process_connection(self, conn: socket.socket, addr: tuple[str, int]):
        with conn:
            # receiving the type of request
            control_msg = self.recvall(conn, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode()
            # separating the fields
            control_msg = control_msg.strip().split('$')
            # interpreting the request
            if control_msg[0] == 'UPLOAD':
                print(f'{addr} requesting upload')
                file_name = control_msg[1]
                file_size = int(control_msg[2])
                self.upload_to_datanode(conn, addr, self.workers[0], file_name, file_size)
            elif control_msg[0] == 'LISTING':
                print(f'{addr} requesting listing')
                self.list_images(conn, addr)
            elif control_msg[0] == 'DOWNLOAD':
                print(f'{addr} requesting download of {control_msg[1]}')
                self.download_from_datanode(conn, addr, self.workers[0], control_msg[1])
            elif control_msg[0] == 'DELETE':
                print(f'{addr} requesting deletion of {control_msg[1]}')
                self.delete_in_datanode(self.workers[0], control_msg[1])


    def list_images(self, conn: socket.socket, client_addr: tuple[str, int]):
        with conn:
            # sending message size
            msg = os.listdir('main_dir')
            msg.remove('workers.txt')
            msg = ' '.join(msg).encode()

            msg_size = len(msg)
            conn.sendall(str(msg_size).ljust(1024, ' ').encode())

            # sending message
            conn.sendall(msg)
            print(f'listing for {client_addr} done')



    def upload_to_datanode(self, client_conn: socket.socket, client_addr: tuple[str, int], datanode_addr: tuple[str, int], file_name: str, file_size: int):
        
        control_msg = f'UPLOAD${file_name}${file_size}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            datanode_conn.connect(datanode_addr)
            datanode_conn.sendall(control_msg)

            control_msg = self.recvall(datanode_conn, CONTROL_MSG_SIZE_BYTES)
            msg = control_msg.decode()
            msg = msg.strip().split('$')

            if msg[0] != 'READY':
                print("Datanode not ready")
                return
            
            client_conn.sendall(control_msg)
            
            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = self.recvall(client_conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                datanode_conn.sendall(chunk)
                bytes_sent += len(chunk)
            
            with open(f'main_dir/{file_name}', 'w') as f, self.lock:
                f.write(str(file_size))
        print(f'upload for {client_addr} in datanode {datanode_addr} done')
    
    def download_from_datanode(self, client_conn: socket.socket, client_addr: tuple[str, int], datanode_addr: tuple[str, int], file_name: str):
        control_msg = f'DOWNLOAD${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            datanode_conn.connect(datanode_addr)
            # main to datanode: send DOWNLOAD$FILENAME
            datanode_conn.sendall(control_msg)

            # main from datanode: recv SIZE_BYTES
            control_msg = self.recvall(datanode_conn, CONTROL_MSG_SIZE_BYTES)
            msg = control_msg.decode()
            msg = msg.strip().split('$')
            file_size = int(msg[0])

            # main to client: send SIZE_BYTES
            client_conn.sendall(control_msg)

            # main from client: recv READY
            control_msg = self.recvall(client_conn, CONTROL_MSG_SIZE_BYTES)

            # main to datanode: send READY
            datanode_conn.sendall(control_msg)

            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = self.recvall(datanode_conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                client_conn.sendall(chunk)
                bytes_sent += len(chunk)
        print(f'download for {client_addr} from datanode {datanode_addr} done')

    def delete_in_datanode(self, datanode_addr: tuple[str, int], file_name: str):
        control_msg = f'DELETE${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        # main to datanode: send DELETE$FILENAME
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_conn:
            datanode_conn.connect(datanode_addr)
            datanode_conn.sendall(control_msg)
        os.remove(f'main_dir/{file_name}')
        print(f'deletion of {file_name} in datanode {datanode_addr} done')


    
    def recvall(self, conn: socket.socket, msg_size: int) -> bytes:
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < msg_size:
            chunk = conn.recv(msg_size)
            chunks.append(chunk)
            bytes_recvd += len(chunk)
        return b''.join(chunks)

if __name__ == '__main__':
    s = Main(MAIN_ADDR, MAIN_PORT)
    s.start()