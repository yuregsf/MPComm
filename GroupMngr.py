import constMP

class GroupMngr:
  int port
  def __init__(self, port):
    self.port = port
    self.serverLoop()

  def serverLoop(self):
    serverSock = socket(AF_INET, SOCK_STREAM)
    serverSock.bind(('0.0.0.0', SERVER_PORT))
    serverSock.listen(6)
    
