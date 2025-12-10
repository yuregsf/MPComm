from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from socket import timeout # Importar o timeout é ESSENCIAL para a robustez

# ----------------------------------------------------------------------
# VARIÁVEIS GLOBAIS PARA ORDENAÇÃO CAUSAL (Relógio Vetorial e Fila de Entrega)
# ----------------------------------------------------------------------
# Variável global para o Relógio Vetorial (V[i] é a contagem para P_i)
vectorClock = [] 
# Fila de espera para mensagens que não podem ser entregues causalmente
# Stores tuples: (sender_id, received_vector, operation_data)
deliveryQueue = [] 
# ID do processo atual
myself = -1 
# O número de mensagens a enviar (definido em waitToStart)
nMsgs = 0
# ----------------------------------------------------------------------

operations = {
        0: "insert",
        1: "delete",
        2: "update",
        3: "query"
        }

handShakeCount = 0
PEERS = []

# Sockets de comunicação
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
    # Esta função agora tem uma variável local para evitar chamadas repetidas
    # e garantir que o IP local seja estável.
    try:
        # Tenta obter o IP público (funciona em cloud/EC2)
        ipAddr = get('https://api.ipify.org').content.decode('utf8')
    except Exception:
        # Fallback para IP privado local
        s = socket(AF_INET, SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ipAddr = s.getsockname()[0]
        s.close()

    # print('My public/private IP address é: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print ('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print ('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print ('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

# Função auxiliar para verificar a condição de entrega CAUSAL
# 
def can_deliver_causally(sender_id, received_vector, current_vector):
    global N
    # Condição 1: P_i está adiantado por 1
    if received_vector[sender_id] != current_vector[sender_id] + 1:
        return False
    
    # Condição 2: Todas as dependências anteriores foram vistas
    for k in range(N):
        if k != sender_id:
            if received_vector[k] > current_vector[k]:
                return False
                
    return True

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def process_queue_and_deliver(self, current_vector, queue, log):
        global myself
        global N
        
        delivered_count = 0
        i = 0
        # Itera sobre a fila
        while i < len(queue):
            sender_id, received_vector, operation = queue[i]
            
            if can_deliver_causally(sender_id, received_vector, current_vector):
                # Entrega (aplica na réplica e registra no log)
                delivered_msg = (received_vector, sender_id, operation)
                log.append(delivered_msg)
                
                print(f'*** DELIVERED FROM QUEUE ***: P{myself} delivered {delivered_msg[2]} from P{sender_id} with vector {received_vector}')
                
                # Regra 3 (a): Atualizar o relógio (máximo componente a componente) 
                for k in range(N):
                    current_vector[k] = max(current_vector[k], received_vector[k])
                
                # Regra 3 (b): Incrementar a própria entrada
                current_vector[myself] = current_vector[myself] + 1
                
                # Remove da fila e REINICIA a verificação (o novo vetor pode desbloquear outra mensagem)
                queue.pop(i)
                delivered_count += 1
                i = 0 
            else:
                i += 1
        return delivered_count

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        global handShakeCount
        global vectorClock
        global deliveryQueue
        global myself
        global N
        global nMsgs
        
        # Espera o handshake de todos os peers (incluindo si mesmo)
        # Conta o próprio handshake automaticamente
        handShakeCount = 1  # Conta a si mesmo
        
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        logList = [] 
        stopCount = 1  # Conta a si mesmo como "parado" quando não enviar mais
        expected_total_msgs = (N - 1) * nMsgs  # Recebe de N-1 peers (não de si mesmo)
        
        # -----------------------------------------------------------------
        # LOOP DE RECEBIMENTO ROBUSTO COM TIMEOUT
        # -----------------------------------------------------------------
        while True:
            # 1. Critério de Parada: Todas as paradas foram vistas E a fila está vazia
            if stopCount == N and len(deliveryQueue) == 0:
                print("All peers sent STOP messages and the delivery queue is empty. Terminating reception.")
                break
                
            # 2. Se todas as paradas chegaram, mas a fila não esvazia, tentamos liberar mensagens
            if stopCount == N and len(deliveryQueue) > 0:
                 self.process_queue_and_deliver(vectorClock, deliveryQueue, logList)
                 time.sleep(0.05) # Pequeno sleep para liberar CPU e reavaliar
                 continue

            try:
                # Usa um timeout curto (100ms) para evitar bloqueio e permitir reavaliação da fila.
                self.sock.settimeout(0.1) 
                msgPack = self.sock.recv(1024)   
                self.sock.settimeout(None) # Retorna ao modo bloqueante após sucesso
            except timeout:
                # 3. Em caso de timeout: Tenta processar a fila.
                self.process_queue_and_deliver(vectorClock, deliveryQueue, logList)
                continue # Volta para o topo do loop para re-avaliar a condição de parada
            except ConnectionResetError:
                 print("Connection reset error detected. Breaking reception loop.")
                 break
            
            # Se a recepção foi bem-sucedida (sem timeout):
            msg = pickle.loads(msgPack)
            
            if msg[0] == -1:
                # Só conta STOP de outros peers (não de si mesmo)
                if msg[1] != myself:
                    stopCount = stopCount + 1
                    print(f"Received STOP from P{msg[1]}. Current stopCount={stopCount}")
                    # Força a checagem da fila imediatamente.
                    self.process_queue_and_deliver(vectorClock, deliveryQueue, logList)
            
            elif len(msg) == 3:
                # O formato da mensagem recebida é: (remetente_id, received_vector, operacao)
                sender_id, received_vector, operation = msg
                
                # IGNORA mensagens de si mesmo (não deveria acontecer, mas por segurança)
                if sender_id == myself:
                    continue
                    
                received_vector = list(received_vector) 
                
                if can_deliver_causally(sender_id, received_vector, vectorClock):
                    # Entrega imediata
                    delivered_msg = (received_vector, sender_id, operation)
                    logList.append(delivered_msg)
                    
                    print(f'*** DELIVERED IMMEDIATE ***: P{myself} delivered {delivered_msg[2]} from P{sender_id} with vector {received_vector}')
                    
                    # Regra 3 (a) e (b) (após a entrega)
                    for k in range(N):
                        vectorClock[k] = max(vectorClock[k], received_vector[k])
                    vectorClock[myself] = vectorClock[myself] + 1
                    
                else:
                    # Coloca na fila de espera
                    deliveryQueue.append((sender_id, received_vector, operation))
                    print(f'--- WAITING ---: P{myself} put message from P{sender_id} in queue. Queue size: {len(deliveryQueue)}')
                
                # Tenta entregar mensagens presas na fila
                self.process_queue_and_deliver(vectorClock, deliveryQueue, logList)
        
        # -----------------------------------------------------------------
        # FINALIZAÇÃO
        # -----------------------------------------------------------------
        
        if len(logList) != expected_total_msgs:
            print(f"FATAL WARNING: Log size mismatch. Expected {expected_total_msgs} messages, but only {len(logList)} were delivered.")
            
        print(f'\nProcess: Total causally ordered messages delivered = {len(logList)}\n')

        # Write log file
        logFile = open('logfile.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()
        
        # Send the list of messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(logList)
        clientSock.send(msgPack)
        clientSock.close()
        
        handShakeCount = 0

        exit(0)

# Function to wait for start signal from comparison server:
def waitToStart():
    global myself
    global vectorClock
    global nMsgs
    
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    
    myself = msg[0]
    nMsgs = msg[1]
    
    # Inicializa o vetor clock (tamanho N)
    vectorClock = [0] * N
    
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
def main():
        global vectorClock
        global deliveryQueue
        while 1:
            print('Waiting for signal to start...')
            (myself, nMsgs) = waitToStart()
            print('I am up, and my ID is: ', str(myself))
        
            if nMsgs == 0:
                print('Terminating.')
                exit(0)
        
            time.sleep(5) 
        
            # Create receiving message handler
            msgHandler = MsgHandler(recvSocket)
            msgHandler.start()
            print('Handler started')
        
            PEERS = getListOfPeers()
            my_ip = get_public_ip()
            
            # Send handshakes (somente para outros peers)
            for addrToSend in PEERS:
                if addrToSend == my_ip:
                    continue
                print('Sending handshake to ', addrToSend)
                msg = ('READY', myself)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
        
            print('Main Thread: Sent all handshakes. Waiting for confirmation...')
        
            while (handShakeCount < N):
                pass
        
            # Send a sequence of data messages to all other processes 
            global vectorClock 
            
            for msgNumber in range(0, nMsgs):
                time.sleep(random.randrange(10,100)/1000)
                
                # Incrementar o relógio vetorial antes de enviar
                vectorClock[myself] = vectorClock[myself] + 1
                
                op_data = operations[msgNumber%4]
                msg = (myself, tuple(vectorClock), op_data) 
                
                msgPack = pickle.dumps(msg)
                
                # Enviar apenas para outros peers (NÃO para si mesmo)
                for addrToSend in PEERS:
                    if addrToSend == my_ip:
                        continue 
        
                    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                
                print(f'P{myself} Sent message {msgNumber} (Vector: {vectorClock})')
        
            # Enviar mensagem de parada para todos (incluindo si mesmo para contagem)
            for addrToSend in PEERS:
                msg = (-1, myself)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
if __name__ == "__main__":
        main()
