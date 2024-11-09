import socket
import struct
import hashlib
import logging
import queue
import random
from threading import Lock, Barrier, Event, Thread
from concurrent.futures import ThreadPoolExecutor
from Message.sendMessage import SendMessageP2P
from Download.downloader import Downloader
from Upload.upload import Upload

logging.basicConfig(level=logging.INFO)

class P2PConnection:
    def __init__(self, torrent_file_path, our_peer_id="192.168.56.1", peerList=[]):
        self.lock = Lock()
        self.our_peer_id = our_peer_id
        self.peerList = peerList
        
        self.piece_data = {}
        
        self.isEnoughPiece = False
        self.isDownloadComplete = False

        self.torrent_file_path = torrent_file_path
        self.our_peer_id = our_peer_id
        
        self.barrier = Barrier(len(peerList) + 1)
        self.peer_event = {peer: Event() for peer in peerList}
        self.peer_block_requests = {peer: [] for peer in peerList}
        self.unchoke = {peer: False for peer in peerList}
    
        self.isEnoughPiece = False
        self.uploader = Upload(torrent_file_path,r'DownloadFolder/mapping_file.json',our_peer_id)

    def receive_bitfield(self, s, peer):
        try:
            # Read the length prefix (4 bytes)
            length_prefix = self._recv_exactly(s, 4)
            bitfield_length = struct.unpack('>I', length_prefix)[0]

            # Read the message ID (1 byte)
            message_id = self._recv_exactly(s, 1)
            bitfield_message_id = struct.unpack('B', message_id)[0]

            if bitfield_message_id == 5:
                # Read the bitfield data
                bitfield_data = self._recv_exactly(s, bitfield_length - 1)
                with self.lock:
                    for i in range(len(bitfield_data) * 8):
                        if bitfield_data[i // 8] & (1 << (7 - (i % 8))):
                            if i < self.downloader.pieces_length:
                                if peer not in self.downloader.having_pieces_list[i]:
                                    self.downloader.having_pieces_list[i].append(peer)
                logging.info(f"Received bitfield from {peer[0]}:{peer[1]}")
        except (struct.error, socket.error) as e:
            logging.error(f"Error processing bitfield from {peer[0]}:{peer[1]} - {e}")

    def _recv_exactly(self, s, num_bytes):
        """Helper method to receive exactly num_bytes from the socket."""
        data = b''
        while len(data) < num_bytes:
            packet = s.recv(num_bytes - len(data))
            if not packet:
                raise socket.error("Connection closed unexpectedly")
            data += packet
        return data


    def connect_to_peer(self, peer):
        peer_ip, peer_port = peer
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(60)
                s.connect((peer_ip, peer_port))
                
                send_message = SendMessageP2P()
                send_message.send_handshake_message(s, self.downloader.torrent_info.info_hash, self.our_peer_id)

                handshake_response = s.recv(88)
                handshake_response = handshake_response.decode('utf-8')

                if handshake_response[28:68] != self.downloader.torrent_info.info_hash:
                    return

                logging.info(f"Handshake successful with {peer_ip}:{peer_port}")
                
                # self.receive_bitfield(s, peer)

                while True:
                    bitfield_msg = s.recv(1024)

                    bitfield_length = struct.unpack('>I', bitfield_msg[:4])[0]
                    bitfield_message_id = struct.unpack('B', bitfield_msg[4:5])[0]
                    logging.info(f"bitfield_length: {bitfield_length}")
                    logging.info(f"message ID: {bitfield_message_id}")
                    
                    if bitfield_message_id == 5:
                        bitfield = bitfield_msg[5:]
                        with self.lock:
                            for i, has_piece in enumerate(bitfield):
                                if has_piece and i < self.downloader.pieces_length:
                                    if peer not in self.downloader.having_pieces_list[i]:
                                        self.downloader.having_pieces_list[i].append(peer)

                        msg = struct.pack('>B', 1)
                        s.send(msg)
                        break
                    else:
                        msg = struct.pack('>B', 0)
                        s.send(msg)

                logging.info(f"Received bitfield from {peer[0]}:{peer[1]}")
        
                self.barrier.wait()

                message_queue = queue.Queue()

                listen_thread = Thread(target=self._listen_thread, args=(s, peer, message_queue))
                processor_thread = Thread(target=self._processor_thread, args=(s, peer, message_queue))

                listen_thread.start()
                processor_thread.start()

                
                
                while not self.isEnoughPiece:
                    logging.info(f"Wait for unlock: ")
                    self.peer_event[peer].clear()
                    self.peer_event[peer].wait()
                    
                    self._send_block_request(s, peer)

                listen_thread.join()
                processor_thread.join()
                
        except (socket.timeout, socket.error) as e:
            logging.error(f"Failed to connect to {peer_ip}:{peer_port} - {e}")
        finally:
            if peer in self.peerList:
                self.peerList.remove(peer)

    def _listen_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                message = s.recv(256 * 1024 + 10)
                if not message:
                    break
                message_queue.put(message)
        except socket.error as e:
            logging.error(f"Error receiving message from {peer} - {e}")
        finally: 
            logging.info(f"Disconnected from {peer}")

    def _processor_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        while not self.isEnoughPiece:
            try:
                message = message_queue.get(timeout=60)
                self._handle_message(message, peer)
            except queue.Empty:
                pass
    
    def _send_block_request(self, s, peer):
        try:                
            send_message = SendMessageP2P()
            if not self.peer_block_requests[peer]:
                logging.info(f"No block requests for {peer}")
                return

            index, start, end = self.peer_block_requests[peer].pop(0)
            send_message.send_interested_message(s)
            
            ### Wait for unchoke message
            while self.unchoke[peer] == False:
                pass
            ## Wait for unchoke message
            send_message.send_request_message(index, start, end - start, s)

        except (socket.timeout, socket.error, IndexError) as e:
            logging.error(f"Error: {e}")
            pass

    def _handle_message(self, message, peer):
        if len(message) < 1:
            return
        length_prefix = struct.unpack('>I', message[:4])[0]
        message_id = struct.unpack('B', message[4:5])[0]
        payload = message[5:]
        
        if message_id == 0:
            self._handle_choke_message(peer)
        elif message_id == 1:
            self._handle_unchoke_message(peer)
        elif message_id == 4:
            self._handle_have_message(payload, peer)
        elif message_id == 7:
            self._handle_piece_message(payload, peer)

    def _handle_choke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = False

    def _handle_unchoke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = True

    def _handle_have_message(self, payload, peer):
        piece_index = struct.unpack('>I', payload)[0]
        if self.downloader.bit_field[piece_index] == 0:
            self.downloader.having_pieces_list[piece_index].append(peer)

    def _handle_piece_message(self, payload, peer):
        
        index = struct.unpack('>I', payload[:4])[0]         # Index: 4 bytes
        begin = struct.unpack('>I', payload[4:8])[0]        # Begin: 4 bytes
        block_data = payload[8:]                            # Block data: Rest of the payload   

        with self.lock:
            if index not in self.piece_data:
                piece_size = self.downloader.torrent_info.get_piece_sizes()[index]
                self.piece_data[index] = bytearray(piece_size)
            self.piece_data[index][begin:begin + len(block_data)] = block_data

        self.barrier.wait()

    def _send_block_request(self, s, peer):
        try:
            send_message = SendMessageP2P()
            index, start, end = self.peer_block_requests[peer].pop(0)
            send_message.send_interested_message(s)
            
            while self.unchoke[peer] == False:
                pass
            logging.info(f"Wait for unchoke message")
            ## Wait for unchoke message

            send_message.send_request_message(index, start, end - start, s)
        except (socket.timeout, socket.error, IndexError) as e:
            logging.error(f"Error: {e}")
            pass

    def create_connection(self,listen_port):
        self.downloader = Downloader(self.torrent_file_path, self.our_peer_id)

        with ThreadPoolExecutor(max_workers=len(self.peerList)) as executor:
            futures = [executor.submit(self.connect_to_peer, peer) for peer in self.peerList]

            self.barrier.wait()
            
            for peer in self.peerList:
                self.uploader.contribution_rank[peer[0]] = 0
                
            self._request_rarest_pieces()

            for future in futures:
                future.result()

    def _request_rarest_pieces(self):
        while True:
            rarest_piece = self.downloader.get_rarest_pieces()
            if rarest_piece is None:
                if (self.downloader.is_having_all_pieces()):
                    self.isEnoughPiece = True
                    self.isDownloadComplete = True
                else:
                    self.isDownloadComplete = True
                return
            
            torrent_info_hash = self.downloader.torrent_info.get_piece_info_hash(rarest_piece)
            while True:
                self.isDownloadComplete = False

                request_blocks = self.downloader.download_piece(rarest_piece)
                selected_peers = random.sample(self.downloader.having_pieces_list[rarest_piece], min(5, len(self.downloader.having_pieces_list[rarest_piece])))

                for i, block in enumerate(request_blocks):
                    peer = selected_peers[i % len(selected_peers)]
                    self.peer_block_requests[peer].append(block)
                    logging.info(f"Unlock peer from request racest pieces.")
                    self.peer_event[peer].set()

                # Wait for peer event complete
                self.barrier.wait()
                
                custom = hashlib.sha1(self.piece_data[rarest_piece]).hexdigest()
                if torrent_info_hash == custom:
                    self.downloader.update_pieces(rarest_piece, self.piece_data[rarest_piece], rarest_piece)
                    
                    for i, block in enumerate(request_blocks):
                        peer = selected_peers[i % len(selected_peers)]
                        index, start, end = block

                        self.uploader.contribution_rank[peer[0]] += (end - start) 

                    del self.piece_data[rarest_piece]
                    break
                else:
                    self.piece_data = {}





    # Phần này lâm viết 


    def listen_for_peers(self, listen_port):
        """Continuously listens for incoming peer connections."""
        try:
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

import time


if __name__ == "__main__":
    # my_IP = get_my_IP()
    # print(my_IP)

    our_Peer_ID = "192.168.56.1"

    peerList = []
    peer = P2PConnection(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\hello.torrent',
                          our_Peer_ID, peerList)
    
    peer.listen_for_peers(6868)