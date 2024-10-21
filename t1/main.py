import socket
import threading
import time
from datetime import datetime
import os

# Função para registrar mensagens com timestamps
def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# Classe Peer
class Peer:
    def __init__(self, id, config, topology):
        self.id = id
        self.ip = config[id]['ip']
        self.udp_port = config[id]['udp_port']
        self.tcp_port = config[id]['tcp_port']
        self.speed = config[id]['speed']  # bytes por segundo
        self.neighbors = topology[id]
        self.files = self.load_files()  # Arquivos que o peer possui
        self.lock = threading.Lock()

    def load_files(self):
        # Carrega chunks de arquivos que o peer tem localmente
        files = {}
        dir_name = str(self.id)  # Cada peer tem um diretório com seu ID
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        for f in os.listdir(dir_name):
            files[f] = os.path.join(dir_name, f)
        return files

    def udp_listener(self):
        # Escuta requisições UDP para busca de arquivos
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.ip, self.udp_port))
        while True:
            data, addr = sock.recvfrom(1024)
            message = data.decode().split('|')
            log(f"Peer {self.id} recebeu mensagem {message} de {addr}")
            if message[0] == 'SEARCH':
                filename, ttl, origin_peer = message[1], int(message[2]), int(message[3])
                log(f"Peer {self.id} recebeu requisição de busca por {filename} de {origin_peer}")                
                # Verifica se o peer possui algum chunk do arquivo solicitado
                found_chunk = None
                for chunk in self.files:
                    if chunk.startswith(filename):  # Verifica se o chunk pertence ao arquivo buscado
                        found_chunk = chunk
                        break
                
                if found_chunk:
                    # Envia resposta diretamente para o peer de origem via UDP
                    # response_message = f'FOUND|{found_chunk}|{self.id}'
                    neighbor_port = 6000 + origin_peer + 1000
                    # sock.sendto(response_message.encode(), (self.ip, neighbor_port))
                    log(f"Peer {self.id} encontrou {found_chunk} e enviou resposta para Peer {origin_peer}")
                    self.tcp_transfer(self.files[found_chunk], self.ip, neighbor_port)
                    time.sleep(1)  # Simula a latência da rede
                elif ttl > 0:
                    # Repassa a busca para os vizinhos
                    self.flood_request(filename, ttl - 1, origin_peer)
                
    def flood_request(self, filename, ttl, origin_peer):
        # Envia requisição de busca para os peers vizinhos
        message = f'SEARCH|{filename}|{ttl}|{origin_peer}'
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for neighbor in self.neighbors:
            if (origin_peer == neighbor):
                continue
            neighbor_ip = '127.0.0.1'
            neighbor_port = 6000 + neighbor
            sock.sendto(message.encode(), (neighbor_ip, neighbor_port))
            log(f"Peer {self.id} enviou requisição para Peer {neighbor}")

    def tcp_transfer(self, chunk, target_peer_ip, target_peer_port):
        # Função que transfere chunks via TCP com limitação de velocidade
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_peer_ip, target_peer_port))
            # Envia o nome do arquivo
            with open(chunk, 'rb') as f:
                while True:
                    data = f.read(self.speed)
                    if not data:
                        break
                    # s.sendall(data)
                    data_to_send = chunk.split('/')[-1].encode() + b'|' + data
                    s.sendall(data_to_send)
                    time.sleep(1)  # Simula a velocidade de transferência
            log(f"Transferência de {chunk} para {target_peer_ip} concluída.")

    def tcp_listener(self):
        # Escuta requisições de TCP para transferência de chunks
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permitir reutilização de endereço
        sock.bind((self.ip, self.tcp_port))
        sock.listen()
        while True:
            conn, addr = sock.accept()
            with conn:
                data = conn.recv(1024)
                # 1. Dividir o dado com base no '|'
                parts = data.split(b'|')

                # 2. Extrair o nome do arquivo
                file_name = parts[0].decode('utf-8')  # Convertendo de bytes para string

                # 3. Extrair o conteúdo binário (dados após o '|')
                file_data = parts[1]  # Mantém os dados binários

                filename = file_name
                # salve o chunk recebido
                with open(os.path.join(str(self.id), filename), 'wb') as f:
                    while True:
                        if not file_data:
                            break
                        f.write(file_data)
                log(f"Peer {self.id} recebeu chunk {filename}")
                
                

    def request_file(self, filename, ttl):
        # Função para requisitar um arquivo com TTL inicial
        log(f"Peer {self.id} solicitando arquivo {filename} com TTL {ttl}")
        self.flood_request(filename, ttl, self.id)

    def start(self):
        # Inicia os threads para UDP e TCP listeners
        threading.Thread(target=self.udp_listener).start()
        threading.Thread(target=self.tcp_listener).start()

# Função para ler arquivos de configuração
def load_config(file):
    config = {}
    with open(file, 'r') as f:
        for line in f:
            parts = line.strip().split(':')  # Primeiro separa o id do resto da linha
            node_id = int(parts[0].strip())  # ID do nodo (antes dos dois-pontos)
            details = parts[1].strip().split(',')  # Separar IP, porta e velocidade
            config[node_id] = {
                'ip': details[0].strip(),
                'udp_port': int(details[1].strip()),
                'tcp_port': int(details[1].strip()) + 1000,  # TCP usa porta 1000 acima do UDP
                'speed': int(details[2].strip()) * 1000  # Velocidade de bytes por segundo
            }
    return config

# Função para ler a topologia da rede
def load_topology(file):
    topology = {}
    with open(file, 'r') as f:
        for line in f:
            parts = line.strip().split(':')
            node_id = int(parts[0])
            neighbors = list(map(int, parts[1].split(',')))
            topology[node_id] = neighbors
    return topology

if __name__ == "__main__":
    config = load_config('config.txt')
    topology = load_topology('topologia.txt')

    # Inicializa os peers
    peers = []
    for peer_id in config:
        peer = Peer(peer_id, config, topology)
        peers.append(peer)
        peer.start()

    # Exemplo de solicitação de arquivo
    time.sleep(2)  # Espera os peers inicializarem
    peers[0].request_file('image.png', ttl=7)
