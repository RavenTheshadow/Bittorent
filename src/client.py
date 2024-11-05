import socket
import threading
import queue
from utils import *
THREAD_NUMS = 5

request_queue = queue.Queue()
cond = threading.Condition()



def connect_to_peer(peer):
  client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
  try:
    client.connect(peer)
    # WRITE CODE IN HERE


  except:
    client.close()

def thread_function():
  global request_queue
  with cond:
    while True:
      while request_queue.empty():
        cond.wait()
      peer = request_queue.get()
      connect_to_peer(peer)

def client_thread():
    global request_queue
    threads = [threading.Thread(target=thread_function) for _ in range(THREAD_NUMS)]
    [thread.start() for thread in threads]
    peers = []
    with cond:
      for peer in peers:
        request_queue.put(peer) # Peer is tuple (Peer_IP, Peer_Port) example: ('127.0.0.1',32456)
        cond.notify(1)
   
      
if __name__ == '__main__':
  pass