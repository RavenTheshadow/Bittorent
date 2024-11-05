import socket
import queue
import threading
THREAD_NUMS = 5

cond = threading.Condition()
request_queue = queue.Queue()


def handle_request(client):
  try:
    pass
  except:
    client.close()
  finally:
    client.close()

def thread_function():
  global request_queue
  with cond:
    while True:
      while request_queue.empty():
        cond.wait()
      client = request_queue.get()
      handle_request(client)

def thread_server(host,port):
  global request_queue
  server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
  server.bind((host,port))
  server.listen()
  threads = [threading.Thread(target=thread_function) for _ in range(THREAD_NUMS) ]
  [thread.start() for thread in threads]
  while True:
    try:
      client, address = server.accept()
      print(f"IP: {address[0]}, PORT: {address[1]}")
      with cond:
        request_queue.put(client)
        cond.notify(1)
    except:
      server.close()
      break

if __name__ =="__main__":
  pass