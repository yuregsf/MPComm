from socket import *
import pickle
from constMP import *

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind((SERVER_ADDR, SERVER_PORT))
serverSock.listen(5)

numMsgs = 0
msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

# Receive the lists of messages from the peer processes
while numMsgs < N:
	(conn, addr) = serverSock.accept()
	msgPack = conn.recv(2048)
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
	
	print ('Found ' + str(unordered) + ' messages')