from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

if len(sys.argv) != 2:
	print ('Usage: python3 comparisonServer.py <mode> <<<<where mode is either l or r (local or remote)>>>>')
	exit(0)

mode = ''
if sys.argv[1] == 'l':
	mode = 'l'
	PEERS = PEERS_SAME_REGION
else:
	if sys.argv[1] == 'r':
		mode = 'r'
		PEERS = PEERS_TWO_REGIONS
	else:
		print ('Usage: python3 comparisonServer.py <mode> <<<<where mode is either l or r (local or remote)>>>>')
		exit(0)

# Connect to each of the peers and send the 'initiate' signal:
peerNumber = 0
for peer in PEERS:
	clientSock = socket(AF_INET, SOCK_STREAM)
	clientSock.connect((PEERS[peerNumber], PEER_TCP_PORT))
	msg = (peerNumber,mode)
	msgPack = pickle.dumps(msg)
	clientSock.send(msgPack)
	msgPack = clientSock.recv(512)
	print(pickle.loads(msgPack))
	clientSock.close()
	peerNumber = peerNumber + 1

print('Now, wait for the message logs from the communicating peers...')

# Loop to wait for the message logs for comparison:
while (1):
	numMsgs = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Receive the lists of messages from the peer processes
	while numMsgs < N:
		(conn, addr) = serverSock.accept()
		msgPack = conn.recv(32768)
		conn.close()
		msgs.append(pickle.loads(msgPack))
		numMsgs = numMsgs + 1

	unordered = 0

	# Compare the lists of messages
	for j in range(0,N_MSGS-1):
		firstMsg = msgs[0][j]
		for i in range(1,N-1):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break
	
	print ('Found ' + str(unordered) + ' unordered message rounds')

serverSock.close()
