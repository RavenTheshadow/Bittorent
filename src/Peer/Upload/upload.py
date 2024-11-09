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
    def __init__(self, torrent_file_path, mapping_file_path, peer_id):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.piece_folder = self._get_piece_folder(mapping_file_path)
        self.peer_id = peer_id
        self.contribution_rank = {}  # Đóng góp của các peer
        self.unchoke_list = []       # Danh sách các peer được unchoke
        self.peer_sockets = {}       # Lưu socket của mỗi peer
        self.lock = threading.Lock()
        self.msg_sender = SendMessageP2P()
        
        # Khởi chạy các thread định kỳ
        self.start_global_periodic_tasks()

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
            bitfield_length = struct.pack('>I', len(bitfield) + 1)
            bitfield_message_id = struct.pack('B', 5)
            bitfield_message = bitfield_length + bitfield_message_id + bitfield

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

    def handle_request(self, conn, payload):
        """Handles a request message from a peer."""
        try:
            # Unpack the request data
            index, begin, length = struct.unpack('>III', payload)
            piece_info_hash = self.torrent_info.get_piece_info_hash(index).decode('utf-8')
            piece_path = os.path.join(self.piece_folder, f'{piece_info_hash}')

            # Read the requested block from the piece file
            with open(piece_path, 'rb') as f:
                f.seek(begin)
                block_data = f.read(length)

            logging.info(f"Block hash info: {hashlib.sha1(block_data).hexdigest()}")

            # Prepare the piece message
            block_length_prefix = struct.pack('>I', len(block_data) + 9)
            piece_message_id = struct.pack('B', 7)
            piece_index = struct.pack('>I', index)
            piece_begin = struct.pack('>I', begin)
            piece_message = piece_message_id + piece_index + piece_begin + block_data

            # Send the piece message to the peer
            conn.sendall(block_length_prefix + piece_message)
            logging.info(f"Sent block {begin}-{begin + length} of piece {index} to peer.")
            logging.info(f"Message Size: {len(block_length_prefix + piece_message)}")
        except FileNotFoundError:
            logging.error(f"Piece {index} not found in {self.piece_folder}.")
        except IOError as e:
            logging.error(f"IOError reading piece {index} from disk: {e}")
        except socket.error as e:
            logging.error(f"Socket error sending piece {index} to peer: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling request for piece {index}: {e}")

    def start_global_periodic_tasks(self):
        """Khởi động các thread toàn cục."""
        threading.Thread(target=self.update_choke_status, daemon=True).start()
        threading.Thread(target=self.random_unchoke_peer, daemon=True).start()

    def handle_interested(self, conn, peer):
        """Handles an interested message from a peer."""
        with self.lock:
        # Check if the peer is in the unchoke list and send the appropriate message.
            if peer in self.unchoke_list:
                self.send_unchoke_message_conn(conn)
            else:
                self.send_choke_message_conn(conn)
    
    def send_unchoke_message_conn(self, conn):
        """Sends an unchoke message to a connected peer."""
        try:
            length_prefix = struct.pack('>I', 1)  # 4 bytes for length prefix
            unchoke_message = struct.pack('B', 1)  # 1 for unchoke
            conn.sendall(length_prefix + unchoke_message)

            logging.info(f"Sent unchoke message to {conn.getpeername()}")
        except socket.error as e:
            logging.error(f"Error sending unchoke message: {e}")

    def send_choke_message_conn(self, conn):
        """Sends a choke message to a connected peer."""
        try:
            length_prefix = struct.pack('>I', 1)  # 4 bytes for length prefix
            choke_message = struct.pack('B', 0)  # 0 for choke
            conn.sendall(length_prefix + choke_message)
            logging.info(f"Sent choke message to {conn.getpeername()}")
        except socket.error as e:
            logging.error(f"Error sending choke message: {e}")




    def update_choke_status(self):
        """Cập nhật danh sách unchoke dựa trên bảng xếp hạng đóng góp mỗi 10 giây."""
        while True:
            time.sleep(10)
            with self.lock:
                # Sắp xếp và chọn top 5 peer
                sorted_peers = sorted(self.contribution_rank.items(), key=lambda x: x[1], reverse=True)
                self.unchoke_list = [peer for peer, _ in sorted_peers[:5]]
                for peer in self.unchoke_list:
                    self.send_unchoke_message(peer)
                logging.info(f"Updated unchoke list: {self.unchoke_list}")

    def random_unchoke_peer(self):
        """Ngẫu nhiên unchoke một peer ngoài top 5 mỗi 30 giây."""
        while True:
            time.sleep(30)
            with self.lock:
                peers_outside_top5 = [peer for peer in self.contribution_rank if peer not in self.unchoke_list]
                
                if peers_outside_top5:
                    random_peer = random.choice(peers_outside_top5)
                    if random_peer not in self.unchoke_list:
                        self.unchoke_list.append(random_peer)
                        self.send_unchoke_message(random_peer)
                        logging.info(f"Randomly unchoked peer: {random_peer}")

    def send_unchoke_message(self, peer):
        """Gửi thông điệp unchoke đến một peer."""
        conn = self.peer_sockets.get(peer)
        if conn:
            self.send_unchoke_message_conn(conn)

    def send_choke_message(self, peer):
        """Gửi thông điệp choke đến một peer."""
        conn = self.peer_sockets.get(peer)
        if conn:
            self.send_choke_message_conn(conn)

    def update_contribution_rank(self, peer):
        # If peer is not in the contribution rank, add it with a contribution score of 0
        if peer not in self.contribution_rank:
            self.contribution_rank[peer] = 0

    def upload_flow(self, conn):
        """Thực hiện các bước để upload cho một kết nối `peer`."""
        try:
            # 1. Nhận và kiểm tra info_hash từ peer
            handshake = conn.recv(88)
            received_info_hash = handshake[28:68].decode('utf-8')
            received_peer_ip   = handshake[68:88].decode('utf-8')

            if not self.check_info_hash(received_info_hash):
                logging.error("Info hash mismatch. Closing connection.")
                conn.close()
                return

            # 2. Gửi handshake response
            self.send_handshake_message(conn, self.torrent_info.info_hash, self.peer_id)

            # 3. Gửi bitfield
            self.send_bitfield(conn)

            message_response = struct.unpack('B', conn.recv(1))[0]

            while message_response != 1:
                self.send_bitfield(conn)
                message_response = struct.unpack('B', conn.recv(1))[0]

            # 4. Thêm peer vào danh sách với thông tin socket
            with self.lock:
                self.peer_sockets[received_peer_ip] = conn
                self.update_contribution_rank(received_peer_ip)

            # 5. Xử lý yêu cầu từ peer
            while True:
                request_data = conn.recv(1024)
                if not request_data:
                    logging.info("Peer disconnected.")
                    break

                # Phân tích yêu cầu và xử lý
                length_prefix = struct.unpack('>I', request_data[:4])[0]    # Giải mã length_prefix
                message_id = struct.unpack('B', request_data[4:5])[0]       # Giải mã message_id
                payload = request_data[5:]                                  # Payload
                if message_id == 2:  # interested message
                    self.handle_interested(conn, received_peer_ip)
                
                elif message_id == 6:  # request message
                    self.handle_request(conn, payload)
                
                else:
                    logging.warning(f"Không hỗ trợ message_id: {message_id}")

        except (socket.error, struct.error) as e:
            logging.error(f"Lỗi trong upload flow: {e}")
        finally:
            with self.lock:
                # Loại bỏ peer khỏi danh sách khi kết nối đóng
                if received_peer_ip in self.peer_sockets:
                    del self.peer_sockets[received_peer_ip]
                
                if received_peer_ip in self.contribution_rank:
                    del self.contribution_rank[received_peer_ip]
            conn.close()
