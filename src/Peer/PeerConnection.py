import socket
import struct
import logging
from threading import Lock, Thread
from Download.downloader import Downloader
from Upload.upload import Upload

logging.basicConfig(level=logging.INFO)

class P2PConnection:
    def __init__(self, torrent_file_path, our_peer_id="192.168.56.1", peerList=[], port= 9000):
        self.lock = Lock()
        self.our_peer_id = our_peer_id
        self.peerList = peerList

        self.torrent_file_path = torrent_file_path
        self.our_peer_id = our_peer_id
    
        self.isEnoughPiece = False
        self.number_of_bytes_downloaded = 0
        self.uploader = Upload(torrent_file_path,r'DownloadFolder/mapping_file.json',our_peer_id)
        self.downloader = Downloader(self.torrent_file_path, self.our_peer_id, self.peerList, self.uploader, self.number_of_bytes_downloaded)
        self.port = port

    # Phần này lâm viết 

    def start_downloading(self):
        self.downloader.multi_download_manage()

    def listen_for_peers(self):
        """Continuously listens for incoming peer connections."""
        try:
            listen_port = self.port
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind(('', listen_port))
                server_socket.listen(5)
                logging.info(f"Listening for incoming connections on port {listen_port}")

                while True:
                    conn, addr = server_socket.accept()
                    logging.info(f"Accepted connection from {addr}")

                    # Start a new thread to handle each incoming peer
                    Thread(target=self.handle_incoming_peer, args=(conn, addr)).start()
        except socket.error as e:
            logging.error(f"Error while listening for peers: {e}")

    def handle_incoming_peer(self, conn, addr):
        """Handles an incoming peer connection."""
        try:
            self.uploader.upload_flow(conn)
        except (socket.error, struct.error) as e:
            logging.error(f"Error handling peer {addr}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    # my_IP = get_my_IP()
    # print(my_IP)

    our_Peer_ID = "10.229.197.210"

    peerList = [("10.229.197.223", 6000)]
    peer = P2PConnection(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\hello.torrent',
                          our_Peer_ID, peerList)
    
    peer.start_downloading()
    print(f"{peer.downloader.number_of_bytes_downloaded} bytes downloaded")