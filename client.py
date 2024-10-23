import socket
import os
import hashlib

# endereço do main
MAIN_ADDR = 'localhost'
MAIN_PORT = 5555

# tamanho de mensagem de controle
CONTROL_MSG_SIZE_BYTES = 1024
# tamanho maximo de mensagem
MAX_CHUNK_SIZE_BYTES = 1024 * 4

class Client:
    # construtor
    def __init__(self, host: str, port: int) -> None:
        self.srv_addr = (host, port)
        if not os.path.exists('client_dir/'):
            os.makedirs('client_dir/')

    def upload_image(self, file_path: str) -> None:
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        control_msg = f'UPLOAD${file_name}${file_size}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with open(file_path, 'rb') as f, socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.srv_addr)
            s.sendall(control_msg)

            control_msg = self.recvall(s, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode()
            control_msg = control_msg.strip().split('$')

            if control_msg[0] != 'READY':
                print("Server not ready")
                quit()
            
            bytes_sent = 0
            while bytes_sent < file_size:
                chunk = f.read(min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_sent))
                s.sendall(chunk)
                bytes_sent += len(chunk)

    def list_images(self) -> str:
        control_msg = 'LISTING'.ljust(CONTROL_MSG_SIZE_BYTES, ' ').encode()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.srv_addr)
            s.sendall(control_msg)

            control_msg = self.recvall(s, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode().strip().split('$')
            answer_size = int(control_msg[0])

            answer = self.recvall(s, answer_size)
        return answer.decode()


    def download_image(self, file_name: str) -> None:
        control_msg = f'DOWNLOAD${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.srv_addr)
            # client to main: send DONWLOAD$FILENAME
            s.sendall(control_msg)

            # client from main: recv SIZE_BYTES
            control_msg = self.recvall(s, CONTROL_MSG_SIZE_BYTES)
            control_msg = control_msg.decode().strip().split('$')
            file_size = int(control_msg[0])

            control_msg = 'READY'
            control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
            control_msg = control_msg.encode()

            # client to main: send READY
            s.sendall(control_msg)

            with open(f'client_dir/{file_name}', 'wb') as f:
                bytes_saved = 0
                while bytes_saved < file_size:
                    # client from main: recv CHUNK
                    chunk = self.recvall(s, min(MAX_CHUNK_SIZE_BYTES, file_size - bytes_saved))
                    chunk_size = f.write(chunk)
                    bytes_saved += chunk_size


    def delete_image(self, file_name) -> None:
        control_msg = f'DELETE${file_name}'
        control_msg = control_msg.ljust(CONTROL_MSG_SIZE_BYTES, ' ')
        control_msg = control_msg.encode()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.srv_addr)
            # client to main: send DELETE$FILENAME
            s.sendall(control_msg)

    def recvall(self, conn: socket.socket, msg_size: int) -> bytes:
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < msg_size:
            chunk = conn.recv(msg_size)
            chunks.append(chunk)
            bytes_recvd += len(chunk)
        return b''.join(chunks)

###############################################################################

# retorna string com hash do arquivo para fins de verificação de integridade
def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

###############################################################################

if __name__ == '__main__':
    c = Client(MAIN_ADDR, MAIN_PORT)
    print(f"md5 of random_garbage BEFORE:\n{calculate_md5('client_dir/random_garbage')}")

    input('...')
    print('\nuploading')
    c.upload_image('client_dir/random_garbage')

    input('...')
    print('\nlisting')
    print(c.list_images())

    input('...')
    print('\nremoving local copy')
    os.remove('client_dir/random_garbage')

    input('...')
    print('\ndownloading')
    c.download_image('random_garbage')
    print(f"md5 of random_garbage AFTER:\n{calculate_md5('client_dir/random_garbage')}")

    input('...')
    print('deleting remote copy')
    c.delete_image('random_garbage')