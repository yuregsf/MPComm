from socket import *
import pickle
from constMP import *

class GroupMngr:
  port = 0
  membership = []
  
  def __init__(self):
    self.port = GROUPMNGR_TCP_PORT
    self.serverLoop()

  def serverLoop(self):
    serverSock = socket(AF_INET, SOCK_STREAM)
    serverSock.bind(('0.0.0.0', self.port))
    serverSock.listen(6)
    while(1):
      (conn, addr) = serverSock.accept()
      msgPack = conn.recv(2048)
      conn.close()
      req = pickle.loads(msgPack)
      if req["op"] == "register":
        self.membership.append((req["ipaddr"],req["port"]))
      elif req["op"] == "list":
        list = []
        for m in self.membership:
          list.append(m[1])
        return list
      else:
        pass
        
        
