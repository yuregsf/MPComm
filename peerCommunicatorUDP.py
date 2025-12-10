from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get

# ----------------------------------------------------------------------
# VARIÁVEIS GLOBAIS PARA ORDENAÇÃO TOTAL (Relógio de Lamport e Fila de Entrega)
# ----------------------------------------------------------------------
# Variável global para o Relógio Lógico Escalar (Lamport)
logicalClock = 0
# Fila de prioridade para armazenar mensagens recebidas para ordenação total
# Armazena tuplas: (received_timestamp, sender_id, operation_data)
deliveryQueue = [] 
# ----------------------------------------------------------------------

operations = {
        0: "insert",
        1: "delete",
        2: "update",
        3: "query"
        }

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []

# UDP sockets to send and receive data messages:
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
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

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        # 1. CORREÇÃO: Declarações global no topo do run()
        global handShakeCount
        global logicalClock
        global deliveryQueue
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        logList = [] # Lista de mensagens FINALMENTE ENTREGUES (ordenadas)
        stopCount=0 
        
        # O número total de mensagens que esperamos é N * nMsgs. 
        # Precisamos esperar todas as mensagens para garantir a ordem total correta 
        # antes de enviar o log.
        expected_total_msgs = N * nMsgs
        
        while stopCount < N:
            try:
                msgPack = self.sock.recv(1024)   
                msg = pickle.loads(msgPack)
            except EOFError:
                # Caso a conexão feche antes de receber todas as mensagens, 
                # o que pode acontecer em cenários concorrentes.
                break 
            
            if msg[0] == -1:    # count the 'stop' messages
                stopCount = stopCount + 1
            elif len(msg) == 3:
                # O formato da mensagem recebida é: (remetente_id, timestamp, operacao)
                sender_id, received_timestamp, operation = msg
                
                # -----------------------------------------------------------------
                # IMPLEMENTAÇÃO DO RELÓGIO DE LAMPORT E ORDENAÇÃO
                # -----------------------------------------------------------------
                
                # 1. Atualizar o relógio local (Regra 3a)
                logicalClock = max(logicalClock, received_timestamp)
                
                # 2. Incrementar o relógio local (Regra 3b)
                logicalClock = logicalClock + 1

                # print(f'Message received (Clock: {received_timestamp}) from P{sender_id}. My clock updated to: {logicalClock}')
                
                # 3. Adicionar à fila de entrega (deliveryQueue)
                # Elemento: (timestamp, sender_id, operation)
                deliveryQueue.append((received_timestamp, sender_id, operation))
                
                # 4. Ordenar a fila de entrega (timestamp primário, sender_id como desempate)
                deliveryQueue.sort(key=lambda x: (x[0], x[1]))
        
        # -----------------------------------------------------------------
        # FASE DE ENTREGA FINAL (Após receber todas as mensagens 'stop')
        # -----------------------------------------------------------------
        # Neste ponto, todas as mensagens chegaram e estão ordenadas na fila.
        # Entregamos todas as mensagens da fila na ordem estrita para o log.
        
        logList = deliveryQueue
        
        print(f'\nProcess: Total ordered messages delivered = {len(logList)}\n')

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

def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    
    global nMsgs # Necessário para o loop de recebimento na MsgHandler
    nMsgs = msg[1]
    
    myself = msg[0]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
def main():
        global logicalClock
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
            global logicalClock 
            
            for msgNumber in range(0, nMsgs):
                # Wait some random time between successive messages
                time.sleep(random.randrange(10,100)/1000)
                
                # -----------------------------------------------------------------
                # IMPLEMENTAÇÃO DO RELÓGIO DE LAMPORT NO ENVIO
                # -----------------------------------------------------------------
                # 1. Incrementar o relógio antes de enviar (Regra 2)
                logicalClock = logicalClock + 1
                
                # 2. Carimbar a mensagem com (process_id, timestamp, operation_data)
                op_data = operations[msgNumber%4]
                msg = (myself, logicalClock, op_data) # NOVO FORMATO: (ID, TS, DATA)
                # -----------------------------------------------------------------
                
                msgPack = pickle.dumps(msg)
                
                for addrToSend in PEERS:
                    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                    print(f'P{myself} Sent message {msgNumber} (Clock: {logicalClock})')
        
            # Tell all processes that I have no more messages to send
            for addrToSend in PEERS:
                msg = (-1,-1)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
