from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  while(1):
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(2048)
    conn.close()
    req = pickle.loads(msgPack)
    if req["op"] == "register":
      membership.append((req["ipaddr"],req["port"]))
    elif req["op"] == "list":
      list = []
      for m in membership:
        list.append(m[1])
      return list
    else:
      pass

serverLoop()
