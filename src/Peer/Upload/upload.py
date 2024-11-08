import os
import hashlib
import struct
import logging
import threading
import time
import random
import struct
import logging
import socket
import sys
from Message.sendMessage import SendMessageP2P
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from MOCKTorrent import TorrentInfo
import json

class Upload:
    def __init__(self, torrent_file_path, mapping_file_path):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.piece_folder = self._get_piece_folder(mapping_file_path)
        self.contribution_rank = {}  # Bảng xếp hạng đóng góp
        self.unchoke_list = []       # Danh sách peer được unchoke
        self.lock = threading.Lock()
        self.msg_sender = SendMessageP2P()

    def _get_piece_folder(self, mapping_file_path):
        """Retrieve the piece folder path based on the info_hash from the mapping file."""
        try:
            with open(mapping_file_path, 'r') as f:
                mapping = json.load(f)
            
            # Look up folder path using info_hash
            info_hash = self.torrent_info.info_hash
            piece_folder = mapping.get(info_hash)

            if piece_folder:
                return piece_folder
            else:
                logging.error(f"No piece folder found for info_hash: {info_hash}")
                return None
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error reading mapping file: {e}")
            return None

    def send_bitfield(self, conn):
        """Sends the bitfield message to a connected peer."""
        try:
            # Read bitfield data from disk
            parent_folder = os.path.dirname(self.piece_folder)
            bitfield_path = os.path.join(parent_folder, 'bitfield')
            with open(bitfield_path, 'rb') as f:
                bitfield = f.read()

            # Prepare bitfield message
            bitfield_message = struct.pack('>IB', len(bitfield) + 1, 5) + bitfield

            # Send bitfield message
            conn.sendall(bitfield_message)
            logging.info(f"Sent bitfield to peer {conn.getpeername()}")
        except (FileNotFoundError, IOError) as e:
            logging.error(f"Error reading bitfield from disk: {e}")
        except socket.error as e:
            logging.error(f"Error sending bitfield to peer {conn.getpeername()}: {e}")

    def check_info_hash(self, received_info_hash):
        check = self.torrent_info.info_hash
        if received_info_hash == check:
            logging.info("Info hash matches.")
        else:
            logging.error(f"Info hash mismatch: expected {check}, received {received_info_hash}")
        return received_info_hash == check


    def send_handshake_message(self, s, info_hash: str, peer_id: str):
        self.msg_sender.send_handshake_message(s, info_hash, peer_id)

    def send_handshake_response(self, socket, peer_id):
        pstr = "BitTorrent protocol"
        pstrlen = len(pstr)
        reserved = bytes(8)
        handshake = struct.pack(f"!B{pstrlen}s8s20s20s", pstrlen, pstr.encode('utf-8'), reserved, self.torrent_info.info_hash.encode('utf-8'), peer_id.encode('utf-8'))
        socket.send(handshake)

    def handle_request(self, socket, request_data):
        index, begin, length = struct.unpack('>III', request_data)
        piece_path = os.path.join(self.piece_folder, f"piece_{index}")
        
        try:
            with open(piece_path, 'rb') as f:
                f.seek(begin)
                block_data = f.read(length)
                block_length_prefix = struct.pack('>I', len(block_data) + 9)
                piece_message = struct.pack('>BIII', 7, index, begin, len(block_data)) + block_data
                socket.send(block_length_prefix + piece_message)
                logging.info(f"Sent block {begin}-{begin + length} of piece {index} to peer.")
        except FileNotFoundError:
            logging.error(f"Piece {index} not found in {self.piece_folder}.")

    def start_periodic_tasks(self):
        # Thread cập nhật trạng thái choke/unchoke mỗi 10 giây
        threading.Thread(target=self.update_choke_status, daemon=True).start()
        # Thread chọn ngẫu nhiên một peer ngoài top 5 mỗi 30 giây
        threading.Thread(target=self.random_unchoke_peer, daemon=True).start()

    def handle_interested(self, conn, peer):
        """Handles an interested message from a peer."""
        with self.lock:
            self.interested_peers.add(peer)

            if peer in self.unchoke_list:
                self.msg_sender.send_unchoke_message(conn)
            else:
                self.msg_sender.send_choke_message(conn)


    def update_choke_status(self):
        """Updates the unchoke list based on contribution ranking every 10 seconds."""
        while True:
            time.sleep(10)
            with self.lock:
                # Sort peers based on contribution and select the top 5 for unchoking
                sorted_peers = sorted(self.contribution_rank.items(), key=lambda x: x[1], reverse=True)
                
                # Clear the unchoke list and add top 5 peers
                self.unchoke_list.clear()
                self.unchoke_list = [peer for peer, _ in sorted_peers[:5]]
                
                # Send choke/unchoke messages based on updated list
                for peer in self.contribution_rank.keys():
                    if peer in self.unchoke_list:
                        self.msg_sender.send_unchoke_message(peer)  
                    # else:
                    #     self.msg_sender.send_choke_message(peer)    # Implement this method to send choke

                logging.info(f"Updated unchoke list: {self.unchoke_list}")

    def random_unchoke_peer(self):
        """Randomly unchokes a peer outside the top 5 every 30 seconds."""
        while True:
            time.sleep(30)
            with self.lock:
                # Select peers that are not in the top 5
                peers_outside_top5 = [peer for peer in self.contribution_rank if peer not in self.unchoke_list]
                
                # Randomly unchoke one peer outside the top 5 if any are available
                if peers_outside_top5:
                    random_peer = random.choice(peers_outside_top5)
                    if random_peer not in self.unchoke_list:
                        self.unchoke_list.append(random_peer)
                        self.msg_sender.send_unchoke_message(random_peer) # Send unchoke message
                        logging.info(f"Randomly unchoked peer: {random_peer}")
