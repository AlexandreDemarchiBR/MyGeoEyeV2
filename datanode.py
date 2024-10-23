import socket
import os
import threading
import hashlib



DATANODE_ADDR = 'localhost'
DATANODE_PORT = 6666

CONTROL_MSG_SIZE_BYTES = 1024
MAX_CHUNK_SIZE_BYTES = 1024 * 4

class Datanode:
    def __init__(self, host, port) -> None:
        self.listen_addr = (host,port)
        self.lock = threading.Lock()
        if not os.path.exists('datanode_dir/'):
            os.makedirs('datanode_dir/')
    
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
                self.save_image(conn, addr, file_name, file_size)
            elif control_msg[0] == 'LISTING':
                print(f'{addr} requesting listing')
                self.list_images(conn, addr)
            elif control_msg[0] == 'DOWNLOAD':
                print(f'{addr} requesting download of {control_msg[1]}')
                self.send_image(conn, addr, control_msg[1])
            elif control_msg[0] == 'DELETE':
                print(f'{addr} requesting deletion of {control_msg[1]}')
                self.delete_image(conn, addr, control_msg[1])

    def save_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str, file_size: int):
        control_msg = 'READY'.ljust(CONTROL_MSG_SIZE_BYTES ,' ').encode()
        conn.sendall(control_msg)
        file_path = f'datanode_dir/{file_name}'
        with open(file_path, 'wb') as f, self.lock:
            bytes_saved = 0
            while bytes_saved < file_size:
                chunk = self.recvall(conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_saved))
                chunk_size = f.write(chunk)
                bytes_saved += chunk_size
        print(f"md5 of {file_name}:\n{calculate_md5(file_path)}")
        print(f'upload for {file_name} from {addr} done')

    def send_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str):
        file_size = os.path.getsize(f'datanode_dir/{file_name}')
        control_msg = f'{file_size}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        # datanode to main: send SIZE_BYTES
        conn.sendall(control_msg)

        # datanode from main: redv READY
        control_msg = self.recvall(conn, CONTROL_MSG_SIZE_BYTES)

        file_path = f'datanode_dir/{file_name}'
        with open(file_path, 'rb') as f:
            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = f.read(min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                # datanode to main: send CHUNK
                conn.sendall(chunk)
                bytes_sent += len(chunk)
        print(f'sending of {file_name} to {addr} done')

    def delete_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str):
        print(f'deletion of {file_name} requested by {addr} done')
        os.remove(f'datanode_dir/{file_name}')

    def recvall(self, conn: socket.socket, msg_size: int) -> bytes:
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < msg_size:
            chunk = conn.recv(msg_size)
            chunks.append(chunk)
            bytes_recvd += len(chunk)
        return b''.join(chunks)

###############################################################################

def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

###############################################################################

if __name__ == '__main__':
    s = Datanode(DATANODE_ADDR, DATANODE_PORT)
    s.start()