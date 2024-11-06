import os
import hashlib
import struct
import logging

class Upload:
    def __init__(self, torrent_info, piece_folder):
        self.torrent_info = torrent_info
        self.piece_folder = piece_folder
        self.contribution_rank = {}  # Bảng xếp hạng đóng góp
        self.unchoke_list = []       # Danh sách peer được unchoke

    def check_info_hash(self, received_info_hash):
        return received_info_hash == self.torrent_info.info_hash

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
