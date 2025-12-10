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
    # msgs armazena [Log do Peer 0, Log do Peer 1, ...]
    # Cada Log é uma lista de tuplas: (vector_clock, process_id, operation_data)
    msgs = [] 

    # Receive the logs of messages from the peer processes
    while numPeers < N:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeers = numPeers + 1

    # Cada peer recebe mensagens de N-1 outros peers (não de si mesmo)
    expected_log_size = (N - 1) * N_MSGS
    
    try:
        log_size = len(msgs[0])
    except IndexError:
        print("ERROR: No logs received or logs are empty.")
        return

    print(f"\n--- Starting Log Comparison. Expected log size: {expected_log_size} ---")
    print(f"Received {len(msgs)} logs from peers")
    
    # Verificar se todos os logs têm o tamanho esperado
    size_mismatch = False
    for i in range(N):
        if len(msgs[i]) != expected_log_size:
            print(f"WARNING: Peer {i} log size is {len(msgs[i])}, expected {expected_log_size}")
            size_mismatch = True
    
    if size_mismatch:
        print("ERROR: Not all peers received the expected number of messages!")
        return
        
    # Verificar consistência causal: todos os peers devem ter a MESMA sequência de mensagens
    # Formato de cada mensagem no log: (vector_clock, sender_id, operation)
    
    inconsistencies = 0
    first_peer_log = msgs[0]
    
    print("\nComparing all peer logs for causal consistency...")
    
    for i in range(1, N):
        if msgs[i] != first_peer_log:
            inconsistencies += 1
            print(f"\n❌ INCONSISTENCY: Peer {i} log differs from Peer 0")
            
            # Encontrar primeira diferença
            for j in range(min(len(first_peer_log), len(msgs[i]))):
                if first_peer_log[j] != msgs[i][j]:
                    print(f"  First difference at position {j}:")
                    print(f"    Peer 0: {first_peer_log[j]}")
                    print(f"    Peer {i}: {msgs[i][j]}")
                    break
    
    print('\n' + '='*50)
    print('--- CAUSAL CONSISTENCY VERIFICATION RESULT ---')
    print('='*50)
    
    if inconsistencies == 0:
        print('✅ SUCCESS: All peers delivered messages in the SAME CAUSAL ORDER')
        print('   Causal consistency is GUARANTEED!')
        print(f'   All {N} peers have identical logs with {expected_log_size} messages')
    else:
        print(f'❌ FAILURE: Found {inconsistencies} peer(s) with different message orders')
        print('   Causal consistency is VIOLATED!')
    
    print('='*50 + '\n')


# Initiate server:
mainLoop()
