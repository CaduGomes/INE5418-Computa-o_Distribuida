import socket
import threading
import time
from datetime import datetime
import os

request_file_lock = threading.Lock()

# Função para registrar mensagens com timestamps
def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# Classe Peer
class Peer:
    def __init__(self, id, config, topology):
        self.id = id
        self.ip = config[id]['ip']
        self.udp_port = config[id]['udp_port']
        self.tcp_port = int(config[id]['udp_port']) + (100 * id)
        self.speed = config[id]['speed'] 
        self.neighbors = [{'udp_address': (config[neighbor]['ip'], config[neighbor]['udp_port']), "id": neighbor} for neighbor in topology[id]]
        self.files = self.load_files()  # Arquivos que o peer possui
        self.request_file_chunks = {}
        self.timeout_occurred = False

    def load_files(self):
        # Carrega chunks de arquivos que o peer tem localmente
        files = {}
        dir_name = str(self.id)  # Cada peer tem um diretório com seu ID
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        for f in os.listdir(dir_name):
            files[f] = os.path.join(dir_name, f)
        self.files = files
        return files

    def udp_listener(self):
        # Escuta requisições UDP para busca de arquivos
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.ip, self.udp_port))
        while True:
            
            if self.timeout_occurred:
                break
            
            data, addr = sock.recvfrom(1024)
            message = data.decode().split('|')
            
            # log(f"Peer {self.id} recebeu mensagem {message} de {addr}")
            
            type = message[0]
            
            if type == 'SEARCH':
                filename, ttl, origin_peer_udp_address, origin_peer_id, request_peer_id =  message[1], int(message[2]), message[3], int(message[4]), int(message[5])
                
                self.load_files()  # Recarrega os arquivos locais
                
                log(f"Peer {self.id} recebeu requisição de busca por {filename} de {origin_peer_id}")                
                # Verifica se o peer possui algum chunk do arquivo solicitado
                founded_chunks = []
                for chunk in self.files:
                    if chunk.startswith(filename):  # Verifica se o chunk pertence ao arquivo buscado
                        founded_chunks.append(chunk)
                
                if founded_chunks:
                    for found_chunk in founded_chunks:
                        # Envia resposta diretamente para o peer de origem via UDP
                        response_message = f'FOUND|{found_chunk}|{self.ip}:{self.udp_port}|{self.id}'
                        origin_addr = tuple(origin_peer_udp_address.split(':'))
                        sock.sendto(response_message.encode(), (origin_addr[0], int(origin_addr[1])))

                        log(f"Peer {self.id} encontrou {found_chunk} e enviou resposta para Peer {origin_peer_id}")
                    
                if ttl > 0:
                    # Repassa a busca para os vizinhos
                    self.flood_request(filename, ttl, origin_peer_udp_address, origin_peer_id, request_peer_id)
                    
            elif type == 'FOUND':
                chunk, peer_udp_address, origin_id = message[1], message[2], int(message[3])
                if chunk in self.request_file_chunks:
                    if self.request_file_chunks[chunk]['found'] == False:
                        log(f"Peer {self.id} vai receber {chunk} de {origin_id}")
                        self.request_file_chunks[chunk]['sender_id'] = origin_id
                        self.request_file_chunks[chunk]['found'] = True
                        # Envia requisição UDP com o ip e porta para receber o chunk via TCP
                        
                        chunk_id = self.request_file_chunks[chunk]['id']
                        threading.Thread(target=self.tcp_listener, args=(chunk_id,)).start()

                        response_message = f'SEND|{chunk}|{self.ip}:{self.tcp_port + chunk_id}|{self.id}'
                        peer_addr = tuple(peer_udp_address.split(':'))
                        sock.sendto(response_message.encode(), (peer_addr[0], int(peer_addr[1])))

                    
            elif type == 'SEND':
                chunk, peer_tcp_address, origin_id = message[1], message[2], int(message[3])
                
                log(f"Peer {self.id} vai enviar {chunk} de {origin_id}")
                
                # Envia o chunk via TCP
                address = peer_tcp_address.split(':')
                
                threading.Thread(target=self.tcp_transfer, args=(chunk, (address[0], int(address[1])))).start()
                                   
    def flood_request(self, filename, ttl, origin_peer_udp_address, origin_peer_id, request_peer_id):
        # Envia requisição de busca para os peers vizinhos
        message = f'SEARCH|{filename}|{ttl - 1}|{origin_peer_udp_address}|{origin_peer_id}|{self.id}'
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        for neighbor in self.neighbors:
            if (origin_peer_id == neighbor['id'] or request_peer_id == neighbor['id']):
                continue
            time.sleep(1)  # Aguarda 1 segundo para evitar congestionamento
            log(f"Peer {self.id} enviou requisição de {filename} para Peer {neighbor['id']}")
            sock.sendto(message.encode(), neighbor['udp_address'])

    def tcp_transfer(self, chunk, target_peer):
        # Função que transfere chunks via TCP com limitação de velocidade
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)  # Define o timeout de 5 segundos para operações de socket
                s.connect(target_peer)
                log(f"Peer {self.id} conectado a {target_peer}")
                file_path = f"{self.id}/{chunk}"
                with open(file_path, 'rb') as f:
                    data = f.read()
                    
                    # filename + data
                    data_to_send = f"{chunk}|----|".encode() + data
                    
                    for i in range(0, len(data_to_send), self.speed):
                        s.sendall(data_to_send[i:i+self.speed])
                        time.sleep(1)
                        
                        percentage = (i + self.speed) / len(data_to_send) * 100
                        
                        if percentage > 100:
                            percentage = 100

                        log(f"Enviando {chunk} para {target_peer} ({percentage:.0f}%)")
                    
                log(f"Transferência de {chunk} para {target_peer} concluída.")
        except Exception as e:
            log(f"Erro ao transferir {chunk} para {target_peer}: {e}")
            
            log(f"Tentando novamente transferir {chunk} para {target_peer}")
            self.tcp_transfer(chunk, target_peer)

    def tcp_listener(self, chunk_id):
        if self.timeout_occurred:
            return
            
        log(f"Peer {self.id} esperando por transferência TCP")
        filename = None
        try:
            # Escuta requisições de TCP para transferência de chunks
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permitir reutilização de endereço
            sock.bind((self.ip, self.tcp_port + chunk_id))
            sock.listen()
            conn, addr = sock.accept()
            with conn:
               
                full_data = b''
                
                while True:
                    if self.timeout_occurred:
                        break
                    data = conn.recv(1024)
                    if not data:
                        break
                    full_data += data
               
                filename = full_data.split(b'|----|')[0].decode()

                file_data = full_data.split(b'|----|')[1]
                
                file_path = f"{self.id}/{filename}"
                
                if self.timeout_occurred:
                        return
                with open(file_path, 'ab') as f:
                    f.write(file_data)
            
            log (f"Peer {self.id} recebeu {filename} de {addr}")
        finally:
            # Liberar o lock_tcp
            if filename and filename in self.request_file_chunks:
                self.request_file_chunks[filename]['received'] = True
                self.calculate_chuncks_percentage(addr, filename)
                self.has_finished_receiving()
        
    def search_chunks_timeout(self, filename, ttl, chunks_quantity):
        # Deve finalizar a espera por chunks após um tempo
        timeout_calc = ttl * chunks_quantity * 2
        time.sleep(timeout_calc)
        for chunk in self.request_file_chunks:
            if chunk in self.request_file_chunks and self.request_file_chunks[chunk]['found'] == False:
                self.timeout_occurred = True
                log(f"[Timeout] Peer {self.id} não encontrou todos os chunks do arquivo {filename}. Abortando após {timeout_calc} segundos.")
                if request_file_lock.locked():
                    request_file_lock.release()
                    
                self.request_file_chunks = {}
            
    def request_file(self, filename, ttl, chunks):
        self.load_files() # Recarrega os arquivos locais
        
        request_file_lock.acquire()
        # Função para requisitar um arquivo com TTL inicial
        log(f"Peer {self.id} solicitando arquivo {filename} com TTL {ttl}")

        self.request_file_chunks = {}
        for i in range(chunks):
            self.request_file_chunks[f"{filename}.ch{i}"] = {'found': False, 'received': False, 'sender_id': None, 'id': i}
        
        # Verifica se já tem algum chunk do arquivo
        for chunk in self.files:
            if chunk.startswith(filename) and chunk in self.request_file_chunks:
                log(f"Peer {self.id} já possui {chunk}")
                self.request_file_chunks[chunk]['found'] = True
                self.request_file_chunks[chunk]['received'] = True
                self.request_file_chunks[chunk]['sender_id'] = self.id
                
        has_all_chunks = self.has_finished_receiving()
        if has_all_chunks == False:
            self.flood_request(filename, ttl, f"{self.ip}:{self.udp_port}", self.id, self.id)
            threading.Thread(target=self.search_chunks_timeout, args=(filename, ttl, chunks)).start()
            
    def has_finished_receiving(self) -> bool:
        # Função para verificar se todos os chunks foram recebidos    
        for chunk in self.request_file_chunks:
            if chunk in self.request_file_chunks and not self.request_file_chunks[chunk]['received']:
                return False
        
        log(f"Peer {self.id} recebeu todos os chunks")
        
        key = list(self.request_file_chunks.keys())[0]
        default_filename = f"{key.split('.')[0]}.{key.split('.')[1]}"
        
        # concatene os chunks para formar o arquivo
        for chunk in self.request_file_chunks:
                filepath = f"{self.id}/{chunk}"
                with open(filepath, 'rb') as f:
                    data = f.read()
                    with open(f"{self.id}/{default_filename}", 'ab') as f:
                        f.write(data)
        log(f"Arquivo {default_filename} salvo em {self.id}/{default_filename}")
        
        
        self.request_file_chunks = {}
        if request_file_lock.locked():
            request_file_lock.release()
        return True
                      
    def calculate_chuncks_percentage(self, sender_id, chunk):
        # Função para calcular a porcentagem de chunks recebidos
        chunks_received = 0
        for chunk in self.request_file_chunks:
            if chunk in self.request_file_chunks and self.request_file_chunks[chunk]['received']:
                chunks_received += 1
        percentage = (chunks_received / len(self.request_file_chunks)) * 100
        
        log(f"Peer {self.id} recebeu {chunk} de {sender_id} ({percentage:.0f}%)")

    def start(self):
        # Inicia o thread para UDP listener
        threading.Thread(target=self.udp_listener).start()

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
                'speed': int(details[2].strip())
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

# Função para ler a topologia da rede
def load_metadata(file):
    
    filename = None
    chunks = None
    ttl = None
    
    with open(file, 'r') as f:
        filename = f.readline().strip()
        chunks = int(f.readline().strip())
        ttl = int(f.readline().strip())
        
        return {'filename': filename, 'chunks': chunks, 'ttl': ttl}
            

if __name__ == "__main__":
    log("Menu de Configuração")
    config = load_config('config.txt')
    topology = load_topology('topologia.txt')

    # Pergunta 1: ID do nodo
    node_id = input("Qual o ID do nodo? (exemplo: 1): ")
    
    peer = Peer(int(node_id), config, topology)

    peer.start()
    log(f"Peer {peer.id} iniciado.")

    while True:
        if request_file_lock.locked():
            request_file_lock.release()
        metadata_file = input("Digite o nome do arquivo de metadados (exemplo: arquivo.p2p): \n")
        
        if not os.path.exists(metadata_file):
            log("Arquivo de metadados não encontrado")
            continue
        
        metadata = load_metadata(metadata_file)
        
        peer.request_file(metadata['filename'], metadata['ttl'], metadata['chunks'])
        request_file_lock.acquire()
        

        log("\n----- Operação concluída -----\n")
        
