import socket
import threading
import os 
import requests
import time
from FileStructure import FileStructure
from PeerConnection import P2PConnection
from Torrent import TorrentInfo

DOWNLOAD_EVENT = 0
UPLOAD_EVENT = 1

def get_host_ip():
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    try:
      sock.connect(('8.8.8.8',1))
      ip = sock.getsockname()[0]
    except:
      ip='127.0.0.1'
    finally:
      sock.close()
    return ip
class Torrent:
  def __init__(self,torrent_file,port):
    self.torrent = P2PConnection(
      torrent_file_path=torrent_file,
      our_peer_id=get_host_ip(),
      peerList=[],
      port=port
    )
    self.downloader = threading.Thread(target=self.torrent.start_downloading,daemon=True)
    self.uploader = threading.Thread(target=self.torrent.listen_for_peers,daemon=True)
  def startTorrent(self):
    try:
      get_peers_list = threading.Thread(target=self._get_peers_list,daemon=True)
      get_peers_list.start()
      self.downloader.start()
      self.uploader.start()
    except:
      print("Error when start torrent")
  def startSeeder(self):
    try:
      self._send_message_to_tracker('completed')
      self.downloader.start()
      self.uploader.start()
    except:
      print("Error when seeder torrent")
  def stopTorrent(self):
    self._send_message_to_tracker('stopped')
  def _send_message_to_tracker(self,event_type):
    params = {
      'info_hash': self.get_info_hash(),
      'peer_id':get_host_ip(),
      'port':self.get_port(),
      'uploaded':self.get_bytes_uploaded(),
      'downloaded':self.get_bytes_downloaded(),
      'left':self.get_total_bytes() - self.get_bytes_downloaded(),
      'event':event_type
    }
    tracker_url = self.get_tracker_url()
    try:
      respose = requests.get(url=tracker_url,params=params)
      if respose.status_code == 200:
        return respose
      else: 
        return None
    except:
      return None
  def get_tracker_url(self):
    return self.torrent.downloader.torrent_info.announce
  def _get_peers_list(self):
    while not self.torrent.isEnoughPiece:
      response = self._send_message_to_tracker('started')
      torrent_info = response.json() if response != None else None
      if torrent_info:
        peers_list = [(peer['ip_address'],peer['port']) for peer in torrent_info['peers']]
        self.torrent.downloader.update_peer_list_from_tracker(peers_list)
      time.sleep(torrent_info['interval'])
  def get_port(self):
    return self.torrent.port
  def get_total_bytes(self):
    return self.torrent.downloader.torrent_info.total_bytes
  def get_bytes_downloaded(self):
    return self.torrent.downloader.number_of_bytes_downloaded
  def get_bytes_uploaded(self):
    return self.torrent.uploader.number_of_bytes_uploaded
  def get_info_hash(self):
    return self.torrent.downloader.torrent_info.info_hash

class Nodaemon:
  def __init__(self,lock_event : threading.Event):
    self.haveEvent = lock_event
    self.downloadEvent = threading.Event()
    self.uploadEvent = threading.Event()
    self.showEvent = threading.Event()
    self.stopEvent = threading.Event()
    self.torrentList = []
    self.args = None
  def run(self):
    print('Daemon is running')
    while True:
      self.haveEvent.wait()
      if self.downloadEvent.is_set():
        self.handleDownloadEvent()
        self.downloadEvent.clear()
      elif self.uploadEvent.is_set():
        self.handleUploadEvent()
        self.uploadEvent.clear()
      elif self.showEvent.is_set():
        self.handleShowEvent()
        self.showEvent.clear()
      elif self.stopEvent.is_set():
        print("Stop")
        self.handStopEvent()
        break
      self.haveEvent.clear()
  def isAvailablePort(self,port):
    ip = get_host_ip()
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
      sock.bind((ip,port))
      sock.close()
      return True
    except:
      sock.close()
      return False 
  def handStopEvent(self):
    for torrent in self.torrentList:
      torrent[1].stopTorrent()
  def handleUploadEvent(self):
    torrent_file = self.args.torrent_file
    port = self.args.port
    if not os.path.exists(path=torrent_file):
      print(f'upload: No such a file: {torrent_file}')
      return
    elif not self.isAvailablePort(port=int(port)):
      print(f'upload: Port {port} has already used')
      return
    else:
      torrent = Torrent(torrent_file=torrent_file,port=int(port))
      info_hash = torrent.get_info_hash()
      for torrent in self.torrentList:
        if torrent[0] == info_hash:
          print('upload: torrent has already started')
          return
      self.torrentList.append((info_hash,torrent))
      torrent.startSeeder()
        
  def handleDownloadEvent(self):
    torrent_file = self.args.torrent_file
    port = self.args.port
    if not os.path.exists(path=torrent_file):
      print(f'download: No such a file: {torrent_file}')
      return
    elif not self.isAvailablePort(port=int(port)):
      print(f'download: Port {port} has already used')
      return
    else:
      torrent = Torrent(torrent_file=torrent_file,port=int(port))
      info_hash = torrent.get_info_hash()
      for torrent in self.torrentList:
        if torrent[0] == info_hash:
          print('download: torrent has already started')
          return
      self.torrentList.append((info_hash,torrent))
      torrent.startTorrent()

  def handleShowEvent(self):
    torrent_file = self.args.file
    info_hash = self.args.info
    if not torrent_file and not info_hash:
      for torrent in self.torrentList:
        print(torrent)
    elif torrent_file:
      if os.path.exists(path=torrent_file):
        torrent_info = TorrentInfo(torrent_file_path=torrent_file)
        self._print_torrent_info(torrent_info=torrent_info)  
      else:
        print(f'show: No such a file: {torrent_file}')
        return
    elif info_hash:
      if info_hash in self.torrentList:
        print(self.torrentList[info_hash])
      else:
        print(f'show: No such a torrent: {info_hash}')
        return
    else:
      return
  def _print_torrent_info(self,torrent_info : TorrentInfo):
    print(f'\nINFO HASH: {torrent_info.info_hash}')
    print(f'DIRECTORY NAME: {torrent_info.name}')
    print(f'TRAKCER URL: {torrent_info.announce}')
    print('LIST OF FILE:')
    files = torrent_info.files
    for file in files:
      print(f' - file name: {file["path"][-1]} - file size: {file["length"]}')