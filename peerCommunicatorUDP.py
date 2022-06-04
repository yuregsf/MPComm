from socket  import *
from constMP import * #-
import threading
import random
import time

myAddresses = gethostbyname_ex(gethostname())
#iolock = threading.Lock()
logList = []
handShakes = []
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
    
    while handShakeCount < N:
      rcv_data = self.sock.recv(1024)
      data = rcv_data.decode('utf-8')
      data = eval(data)
      if data[0] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        handShakes[data[1]] = 1
        print('--- Handshake received: ', data[1])

    print('Sec. Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0
    while True:                
      data = self.sock.recv(1024)   # receive data from client
      msg = bytes.decode(data)
      if msg == 'STOP':       # stop loop if all other processes finished
        stopCount = stopCount + 1
        if stopCount == N:
          break
      else:
        #iolock.acquire()
        print(msg+'\n')
        logList.append(str(bytes.decode(data)+'\n'))
        #iolock.release()
    
    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(logList)
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
    msg = str(('READY', myself))
    sendSocket.sendto(msg.encode(), (addrToSend,PORT))
    #data = recvSocket.recvfrom(128)

print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

while (handShakeCount < N):
  pass  # find a better way to wait

# Sending loop
if handShakeCount == N:
  for msgNumber in range(0, N_MSGS):
    time.sleep(random.randrange(10,100)/1000)
    msgText = 'Message ' + str(msgNumber) + ' from process ' + str(myself)
    msg = str.encode(msgText)
    for addrToSend in HOSTS:
      sendSocket.sendto(msg, (addrToSend,PORT))

  for addrToSend in HOSTS:
    sendSocket.sendto(b'STOP', (addrToSend,PORT))

