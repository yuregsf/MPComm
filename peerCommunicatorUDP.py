from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get

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
# N é o número total de peers (de constMP)
# O número de mensagens a enviar
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
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
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
def can_deliver_causally(sender_id, received_vector, current_vector):
    # Condição 1: P_i está adiantado por 1
    # O componente do remetente no vetor da mensagem deve ser exatamente um a mais 
    # do que o que P_j viu de P_i.
    if received_vector[sender_id] != current_vector[sender_id] + 1:
        return False
    
    # Condição 2: Todas as dependências anteriores foram vistas
    # Para todos os outros processos k != i, o que a mensagem viu de k
    # já deve ter sido visto (ou incorporado) por P_j.
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
        
        delivered_count = 0
        i = 0
        # Itera sobre a fila, verificando se alguma mensagem presa pode ser liberada
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
        
        # Espera o handshake de todos os peers
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        logList = [] # Lista de mensagens FINALMENTE ENTREGUES (ordenadas causalmente)
        stopCount=0 
        
        # O loop continua até que todos os peers enviem a mensagem de parada
        while stopCount < N:
            msgPack = self.sock.recv(1024)   
            msg = pickle.loads(msgPack)
            
            if msg[0] == -1:    # count the 'stop' messages
                stopCount = stopCount + 1
            elif len(msg) == 3:
                # O formato da mensagem recebida é: (remetente_id, received_vector, operacao)
                sender_id, received_vector, operation = msg
                # Necessário converter para lista para garantir mutabilidade e comparação
                received_vector = list(received_vector) 
                
                print(f'Message received (Vector: {received_vector}) from P{sender_id}. My current vector: {vectorClock}')
                
                # -----------------------------------------------------------------
                # IMPLEMENTAÇÃO DO RELÓGIO VETORIAL E ORDENAÇÃO CAUSAL
                # -----------------------------------------------------------------
                
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
                
                print(f"P{myself} updated vector: {vectorClock}")
                # -----------------------------------------------------------------
        
        # Impressão do Log Final Ordenado
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
        
        # Reset the handshake counter
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
        global logicalClocks
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
            
            # Send handshakes
            for addrToSend in PEERS:
                print('Sending handshake to ', addrToSend)
                msg = ('READY', myself)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
        
            print('Main Thread: Sent all handshakes. Waiting for confirmation...')
        
            while (handShakeCount < N):
                pass  # wait for the handshakes
        
            # Send a sequence of data messages to all other processes 
            global vectorClock 
            
            for msgNumber in range(0, nMsgs):
                # Wait some random time between successive messages
                time.sleep(random.randrange(10,100)/1000)
                
                # -----------------------------------------------------------------
                # IMPLEMENTAÇÃO DO RELÓGIO VETORIAL NO ENVIO
                # -----------------------------------------------------------------
                # 1. Incrementar a própria entrada do vetor antes de enviar
                vectorClock[myself] = vectorClock[myself] + 1
                
                # 2. Carimbar a mensagem com (process_id, vector_clock, operation_data)
                op_data = operations[msgNumber%4]
                # É importante enviar o vetor como uma tupla (immutable) para evitar 
                # referências, embora o pickle resolva isso. Usamos a lista localmente.
                msg = (myself, tuple(vectorClock), op_data) 
                # -----------------------------------------------------------------
                
                msgPack = pickle.dumps(msg)
                
                for addrToSend in PEERS:
                    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                    print(f'P{myself} Sent message {msgNumber} (Vector: {vectorClock})')
        
            # Tell all processes that I have no more messages to send
            for addrToSend in PEERS:
                msg = (-1,-1)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
if __name__ == "__main__":
        main()
