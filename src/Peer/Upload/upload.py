import os
import hashlib
import struct
import logging
import threading
import time
import random
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from MOCKTorrent import TorrentInfo


class Upload:
    def __init__(self, torrent_file_path, piece_folder):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.piece_folder = piece_folder
        self.contribution_rank = {}  # Bảng xếp hạng đóng góp
        self.unchoke_list = []       # Danh sách peer được unchoke
        self.lock = threading.Lock()

    def check_info_hash(self, received_info_hash):
        check = self.torrent_info.info_hash
        return received_info_hash == check

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

    def update_choke_status(self):
        while True:
            time.sleep(10)
            with self.lock:
                # Sắp xếp peers dựa trên đóng góp
                sorted_peers = sorted(self.contribution_rank.items(), key=lambda x: x[1], reverse=True)
                # Giữ top 5 trong danh sách unchoke
                self.unchoke_list = [peer for peer, _ in sorted_peers[:5]]
                logging.info(f"Updated unchoke list: {self.unchoke_list}")

    def random_unchoke_peer(self):
        while True:
            time.sleep(30)
            with self.lock:
                # Lấy tất cả các peer không nằm trong top 5
                peers_outside_top5 = [peer for peer in self.contribution_rank if peer not in self.unchoke_list]
                if peers_outside_top5:
                    random_peer = random.choice(peers_outside_top5)
                    self.unchoke_list.append(random_peer)
                    logging.info(f"Randomly unchoked peer: {random_peer}")
