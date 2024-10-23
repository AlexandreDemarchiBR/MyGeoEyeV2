import socket
import os
import threading
import hashlib
import time

CONTROL_MSG_SIZE_BYTES = 1024
MAX_CHUNK_SIZE_BYTES = 1024 * 4

class Datanode:
    def __init__(self, host, port) -> None:
        self.listen_addr = (host, port)
        self.lock = threading.Lock()
        if not os.path.exists('datanode_dir/'):
            os.makedirs('datanode_dir/')
    
    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(self.listen_addr)
            s.listen(400)
            print(f'Datanode is listening on {self.listen_addr}')
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
            print(f"# datanode from main: recv control message from {addr}")
            control_msg = self.recvall(conn, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode()
            control_msg = control_msg.strip().split('$')
            if control_msg[0] == 'UPLOAD':
                print(f'{addr} requesting upload')
                file_name = control_msg[1]
                file_size = int(control_msg[2])
                self.save_image(conn, addr, file_name, file_size)
            elif control_msg[0] == 'DOWNLOAD':
                print(f'{addr} requesting download of {control_msg[1]}')
                self.send_image(conn, addr, control_msg[1])
            elif control_msg[0] == 'DELETE':
                print(f'{addr} requesting deletion of {control_msg[1]}')
                self.delete_image(conn, addr, control_msg[1])

    def save_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str, file_size: int):
        print(f"# datanode to main: send READY message to {addr}")
        control_msg = 'READY'.ljust(CONTROL_MSG_SIZE_BYTES ,' ').encode()
        conn.sendall(control_msg)
        file_path = f'datanode_dir/{file_name}'
        start_time = time.time()
        with open(file_path, 'wb') as f, self.lock:
            bytes_saved = 0
            while bytes_saved < file_size:
                chunk = self.recvall(conn, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_saved))
                chunk_size = f.write(chunk)
                bytes_saved += chunk_size
                print(f"Saved {bytes_saved}/{file_size} bytes of {file_name}")
        end_time = time.time()
        print(f"md5 of {file_name}:\n{calculate_md5(file_path)}")
        print(f'Upload for {file_name} from {addr} completed in {end_time - start_time:.4f} seconds')

    def send_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str):
        file_size = os.path.getsize(f'datanode_dir/{file_name}')
        control_msg = f'{file_size}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        print(f"# datanode to main: send SIZE_BYTES to {addr}")
        conn.sendall(control_msg)

        print(f"# datanode from main: recv READY message from {addr}")
        control_msg = self.recvall(conn, CONTROL_MSG_SIZE_BYTES)

        file_path = f'datanode_dir/{file_name}'
        start_time = time.time()
        with open(file_path, 'rb') as f:
            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = f.read(min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                conn.sendall(chunk)
                bytes_sent += len(chunk)
                print(f"Sent {bytes_sent}/{file_size} bytes of {file_name}")
        end_time = time.time()
        print(f'Sending of {file_name} to {addr} completed in {end_time - start_time:.4f} seconds')

    def delete_image(self, conn: socket.socket, addr: tuple[str, int], file_name: str):
        start_time = time.time()
        os.remove(f'datanode_dir/{file_name}')
        end_time = time.time()
        print(f'Deletion of {file_name} requested by {addr} completed in {end_time - start_time:.4f} seconds')

    def recvall(self, conn: socket.socket, msg_size: int) -> bytes:
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < msg_size:
            chunk = conn.recv(msg_size)
            chunks.append(chunk)
            bytes_recvd += len(chunk)
        return b''.join(chunks)

def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python datanode.py <host> <port>")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    s = Datanode(host, port)
    s.start()
