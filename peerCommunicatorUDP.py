from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle

myAddresses = gethostbyname_ex(gethostname())
handShakes = [] # not used; only if we need to check whose handshake is missing
handShakeCount = 0
sendSocket = socket(AF_INET, SOCK_DGRAM)

i = 0
while i < N:
  handShakes.append(0)
  i = i + 1 

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting handshake...')
    
    global handShakes
    global handShakeCount
    
    logList = []
    
    # Wait until handshakes are received from all other processes
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        handShakes[msg[1]] = 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0 
    while True:                
      msgPack = self.sock.recv(1024)   # receive data from client
      msg = pickle.loads(msgPack)
      if msg[0] == -1:   # count the 'stop' messages from the other processes
        stopCount = stopCount + 1
        if stopCount == N:
          break  # stop loop when all other processes have finished
      else:
        print('Message ' + msg[1] + ' from process ' + msg[0])
        logList.append(msg)
        
    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    return

print('I am up, and my adddress is ', myAddresses[2])

#Find out who am I
myself = 0
for addr in HOSTS:
  if addr in myAddresses[2]:
    break
  myself = myself + 1
print('I am process ', str(myself))

#Create receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind((myAddresses[2][0], PORT))

# Wait for other processes to start
# To Do: fix bug that causes a failure when not all processes are started within this time
# (fully started processes start sending data messages, which the others try to interpret as control messages) 
time.sleep(10)

# Create receiving message handler
msgHandler = MsgHandler(recvSocket)
msgHandler.start()
print('Handler created')

# Send handshakes
# To do: Must continue sending until it gets a reply from each process
#        Send confirmation of reply
for addrToSend in HOSTS:
    print('Sending handshake to ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PORT))
    #data = recvSocket.recvfrom(128) # Confirmations have not yet been implemented

print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

while (handShakeCount < N):
  pass  # find a better way to wait

# Sending loop
if handShakeCount == N:
  for msgNumber in range(0, N_MSGS):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    #msgText = 'Message ' + str(msgNumber) + ' from process ' + str(myself)
    msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in HOSTS:
      sendSocket.sendto(msgPack, (addrToSend,PORT))

  # Tell all processes that I have no more messages to send
  for addrToSend in HOSTS:
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PORT))

