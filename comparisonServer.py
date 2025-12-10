from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def mainLoop():
    while 1:
        nMsgs = promptUser()
        if nMsgs == 0:
            break
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
        req = {"op":"list"}
        msg = pickle.dumps(req)
        clientSock.send(msg)
        msg = clientSock.recv(2048)
        clientSock.close()
        peerList = pickle.loads(msg)
        print("List of Peers: ", peerList)
        startPeers(peerList,nMsgs)
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs)
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList,nMsgs):
    # Connect to each of the peers and send the 'initiate' signal:
    peerNumber = 0
    for peer in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        msg = (peerNumber,nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
    # Loop to wait for the message logs for comparison:
    numPeers = 0
    msgs = [] 

    # Receive the logs of messages from the peer processes
    while numPeers < N:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeers = numPeers + 1

    unordered = 0
    
    # O tamanho do log é o número total de mensagens enviadas no sistema (N * N_MSGS)
    expected_log_size = N * N_MSGS
    
    try:
        log_size = len(msgs[0])
    except IndexError:
        print("ERROR: No logs received or logs are empty.")
        return

    print(f"\n--- Starting Log Comparison. Expected log size: {expected_log_size} ---")

    # Verifica se todos os logs têm o tamanho esperado.
    if log_size != expected_log_size:
        print(f"WARNING: Expected log size ({expected_log_size}) does not match received log size ({log_size}).")
    
    # Compare the lists of messages (que devem estar totalmente ordenadas)
    for j in range(0, log_size):
        firstMsg = msgs[0][j]
        # Compara a j-ésima mensagem do Peer 0 com a j-ésima de todos os outros peers.
        for i in range(1, N): 
            if firstMsg != msgs[i][j]:
                unordered = unordered + 1
                print(f"Discrepancy at position {j}: Peer 0 has {firstMsg}, Peer {i} has {msgs[i][j]}")
                break
    
    print ('\n--- Comparison Result ---')
    print ('Found ' + str(unordered) + ' unordered message rounds')
    if unordered == 0:
        print('✅ CONSISTÊNCIA GARANTIDA: Todos os Peers têm a mesma ordem total de mensagens.')
    else:
        print('❌ INCONSISTÊNCIA DETECTADA: Logs de mensagens não correspondem.')
    print('---------------------------\n')


# Initiate server:
mainLoop()
