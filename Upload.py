import os
import hashlib
import struct
import logging
import threading
import time
import random
import socket
from sendMessage import SendMessageP2P
from Torrent import TorrentInfo
import json

class Upload:
    def __init__(self, torrent_file_path, mapping_file_path, peer_id):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.piece_folder = self._get_piece_folder(mapping_file_path)
        self.mapping = mapping_file_path
        self.peer_id = peer_id
        self.contribution_rank = {}
        self.unchoke_list = []
        self.peer_sockets = {}
        self.lock = threading.Lock()
        self.msg_sender = SendMessageP2P()
        self.downloader = None
        self.start_global_periodic_tasks()
        self.number_of_bytes_uploaded = 0
    def _set_downloader(self, downloader):
        self.downloader = downloader

    def _get_piece_folder(self, mapping_file_path):
        try:
            with open(mapping_file_path, 'r') as f:
                mapping = json.load(f)
            info_hash = self.torrent_info.info_hash
            piece_folder = mapping[info_hash]
            if piece_folder:
                return piece_folder
            else:
                logging.error(f"No piece folder found for info_hash: {info_hash}")
                return None
        except (FileNotFoundError, json.JSONDecodeError) as e:
            os.makedirs(os.path.dirname(mapping_file_path), exist_ok=True)
            return None

    def send_bitfield(self, conn):
        try:
            self.piece_folder = self._get_piece_folder(self.mapping)
            print(f"piece_folder: {self.piece_folder}")
            parent_folder = os.path.dirname(self.piece_folder)
            bitfield_path = os.path.join(parent_folder, 'bitfield')
            with open(bitfield_path, 'rb') as f:
                bitfield = f.read()
                bitfield_length = struct.pack('>I', len(bitfield) + 1)
                bitfield_message_id = struct.pack('B', 5)
                bitfield_message = bitfield_length + bitfield_message_id + bitfield
                conn.sendall(bitfield_message)
        except (FileNotFoundError, IOError) as e:
            logging.error(f"Error reading bitfield from disk: {e}")
        except socket.error as e:
            logging.error(f"Error sending bitfield to peer {conn.getpeername()}: {e}")

    def check_info_hash(self, received_info_hash):
        check = self.torrent_info.info_hash
        if received_info_hash == check:
            # logging.info("Info hash matches.")
            pass
        else:
            logging.error(f"Info hash mismatch: expected {check}, received {received_info_hash}")
        return received_info_hash == check

    def send_handshake_message(self, s, info_hash: str, peer_id: str):
        self.msg_sender.send_handshake_message(s, info_hash, peer_id)

    def handle_request(self, conn, payload):
        try:
            index, begin, length = struct.unpack('>III', payload)
            piece_info_hash = self.torrent_info.get_piece_info_hash(index).decode('utf-8')
            piece_path = os.path.join(self.piece_folder, f'{piece_info_hash}')
            with open(piece_path, 'rb') as f:
                f.seek(begin)
                block_data = f.read(length)
            # logging.info(f"Block hash info: {hashlib.sha1(block_data).hexdigest()}")
            block_length_prefix = struct.pack('>I', len(block_data) + 9)
            piece_message_id = struct.pack('B', 7)
            piece_index = struct.pack('>I', index)
            piece_begin = struct.pack('>I', begin)
            piece_message = piece_message_id + piece_index + piece_begin + block_data
            time.sleep(len(block_data) / 1e6)
            conn.sendall(block_length_prefix + piece_message)
            self.number_of_bytes_uploaded += len(block_data)
            # logging.info(f"Sent block {begin}-{begin + length} of piece {index} to peer.")
            # logging.info(f"Message Size: {len(block_length_prefix + piece_message)}")
        except FileNotFoundError:
            logging.error(f"Piece {index} not found in {self.piece_folder}.")
        except IOError as e:
            logging.error(f"IOError reading piece {index} from disk: {e}")
        except socket.error as e:
            logging.error(f"Socket error sending piece {index} to peer: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling request for piece {index}: {e}")

    def start_global_periodic_tasks(self):
        threading.Thread(target=self.update_choke_status, daemon=True).start()
        threading.Thread(target=self.random_unchoke_peer, daemon=True).start()

    def handle_interested(self, conn, peer):
        with self.lock:
            if peer in self.unchoke_list:
                self.send_unchoke_message_conn(conn)
            else:
                self.send_choke_message_conn(conn)

    def send_unchoke_message_conn(self, conn):
        try:
            length_prefix = struct.pack('>I', 1)
            unchoke_message = struct.pack('B', 1)
            conn.sendall(length_prefix + unchoke_message)
            # logging.info(f"Sent unchoke message to {conn.getpeername()}")
        except socket.error as e:
            logging.error(f"Error sending unchoke message: {e}")

    def send_choke_message_conn(self, conn):
        try:
            length_prefix = struct.pack('>I', 1)
            choke_message = struct.pack('B', 0)
            conn.sendall(length_prefix + choke_message)
            # logging.info(f"Sent choke message to {conn.getpeername()}")
        except socket.error as e:
            logging.error(f"Error sending choke message: {e}")

    def update_choke_status(self):
        while True:
            time.sleep(10)
            with self.lock:
                sorted_peers = sorted(self.contribution_rank.items(), key=lambda x: x[1], reverse=True)
                self.unchoke_list = [peer for peer, _ in sorted_peers[:5]]
            for peer in self.unchoke_list:
                self.send_unchoke_message(peer)
            # logging.info(f"Updated unchoke list: {self.unchoke_list}")

    def random_unchoke_peer(self):
        while True:
            time.sleep(30)
            with self.lock:
                peers_outside_top5 = [peer for peer in self.contribution_rank if peer not in self.unchoke_list]
            if peers_outside_top5:
                random_peer = random.choice(peers_outside_top5)
                if random_peer not in self.unchoke_list:
                    with self.lock:
                        self.unchoke_list.append(random_peer)
                    self.send_unchoke_message(random_peer)
                    # logging.info(f"Randomly unchoked peer: {random_peer}")

    def send_unchoke_message(self, peer):
        conn = self.peer_sockets.get(peer)
        if conn:
            self.send_unchoke_message_conn(conn)

    def send_choke_message(self, peer):
        conn = self.peer_sockets.get(peer)
        if conn:
            self.send_choke_message_conn(conn)

    def update_contribution_rank(self, peer):
        if peer not in self.contribution_rank:
            self.contribution_rank[peer] = 0

    # def request_listen_port(self, conn):
    #     if not self.downloader.is_having_all_pieces():
    #         conn.sendall(struct.pack('>I', 1) + struct.pack('B', 13))
    #         response = conn.recv(9)
    #         length_prefix = struct.unpack('>I', response[:4])[0]
    #         if len(response[4:]) != length_prefix:
    #             raise Exception("Invalid response length")
    #         message_id = struct.unpack('B', response[4:5])[0]
    #         if message_id != 9:
    #             raise Exception(f"Expected message ID 9, received {message_id}")
    #         port = struct.unpack('>H', response[5:7])[0]
    #         logging.info(f"Received listen port: {port}")
    #         return port

    def upload_flow(self, conn):
        try:
            handshake = conn.recv(88)
            received_info_hash = handshake[28:68].decode('utf-8')
            received_peer_ip = handshake[68:88].decode('utf-8')
            if not self.check_info_hash(received_info_hash):
                logging.error("Info hash mismatch. Closing connection.")
                conn.close()
                return
            self.send_handshake_message(conn, self.torrent_info.info_hash, self.peer_id)
            self.send_bitfield(conn)
            message_response = struct.unpack('B', conn.recv(1))[0]
            while message_response != 1:
                self.send_bitfield(conn)
                message_response = struct.unpack('B', conn.recv(1))[0]
            with self.lock:
                self.peer_sockets[received_peer_ip] = conn
                self.update_contribution_rank(received_peer_ip)
            # retry_count = 5
            # attempt = 0
            # while attempt < retry_count:
            #     try:
            #         port = self.request_listen_port(conn)
            #         self.downloader.update_peer_list((received_peer_ip, port))
            #         break
            #     except Exception as e:
            #         logging.error(f"Error requesting listen port: {e}")
            #         attempt += 1
            #         if attempt >= retry_count:
            #             raise Exception("Error requesting listen port")
            #         continue
            while True:
                request_data = conn.recv(1024)
                if not request_data:
                    break
                length_prefix = struct.unpack('>I', request_data[:4])[0]
                message_id = struct.unpack('B', request_data[4:5])[0]
                payload = request_data[5:]
                if len(payload) != length_prefix - 1:
                    continue
                if message_id == 2:
                    self.handle_interested(conn, received_peer_ip)
                elif message_id == 6:
                    self.handle_request(conn, payload)
                else:
                    pass
                    # logging.info(f"Received message ID {message_id} from peer {received_peer_ip}")
        except (socket.error, struct.error) as e:
            logging.error(f"Error in upload flow: {e}")
        finally:
            with self.lock:
                if 'received_peer_ip' in locals() and received_peer_ip in self.peer_sockets:
                    del self.peer_sockets[received_peer_ip]
                if 'received_peer_ip' in locals() and received_peer_ip in self.contribution_rank:
                    del self.contribution_rank[received_peer_ip]
            conn.close()