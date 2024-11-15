from multiprocessing import Process, Pipe
import time
import threading
import requests
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from Cli.utils import *
from Peer.PeerConnection import P2PConnection
import dotenv


class Nodaemon:
    def __init__(self, receiver):
        self.receiver = receiver
        self.running = False
    def run(self):
        print("Daemon is running")
        self.running = True
        while self.running:
            if self.receiver.poll():  
                message = self.receiver.recv()
                commands = [command.strip() for command in message.split()]
                if commands[0] == "DOWNLOAD":
                    self.download(commands[1])
                elif commands[0] == "STOP":
                    self.running = False
                    for torrent, torrent_info in self.torrents:
                        self.send_message_to_tracker(info_hash=torrent,event_type="stopped")
                elif commands[0] == "UPLOAD":
                    self.upload(commands[1])
                elif commands[0] == "LIST":
                    self.list_torrent()
                else:
                    print(f"Daemon received unknown command: {message}")
        print("Daemon has stopped")
    def send_message_to_tracker(self,info_hash,event_type,compact_flag=0):
        dotenv.load_dotenv("../.env")
        tracker_url = os.getenv("TRACKER")
        params = {
            "info_hash": info_hash,
            "peer_id": f"{get_host_ip()}".strip(),
            "port":os.getenv("PORT"),
            "uploaded":0,
            "downloaded":0,
            "left":0,
            "event":event_type,
            "compact":compact_flag
        }
        try:
            response = requests.get(tracker_url,params=params)
            if response.status_code == 200:
                torrent_info = response.json()
                if "peers" in torrent_info:
                    return torrent_info
                else:
                    return None
            else:
                print("Torrent is not founded")
                return None
        except:
            print("Connecting to server is failed !!")
            return None
    def download(self,torrent_file):
        info_hash = get_info_hash(torrent_file)
        torrent_info = self.send_message_to_tracker(info_hash=info_hash,event_type="started")
        if torrent_info != None:
            peer = P2PConnection(
                torrent_file_path=torrent_file,
                our_peer_id=get_host_ip(),
                peerList=torrent_info["peers"]
            )
            downloader = threading.Thread(target=peer.start_downloading,daemon=True)
            uploader = threading.Thread(target=peer.listen_for_peers,daemon=True)
            downloader.start()
            uploader.start()
        else:
            print(f"{torrent_file} is download failed")
            return
    def upload(self,torrent_file):
        response =  response = self.send_message_to_tracker(info_hash=get_info_hash(torrent_file),event_type="completed")
        if response != None:
            print(f"{torrent_file} is uploaded succesfully")
            return
        else:
            print(f"{torrent_file} is uploaded failed")
            return
    def list_torrent(self):
        print("List all torrents")

def start_daemon():
    sender, receiver = Pipe()
    daemon = Nodaemon(receiver)
    daemon_process = Process(target=daemon.run,daemon=True)
    daemon_process.start()
    return sender, daemon_process
