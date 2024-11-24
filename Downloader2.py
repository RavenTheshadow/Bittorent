from collections import defaultdict
from pathlib import Path
import sys, socket, logging, struct, queue, time, random, hashlib
from threading import Lock, Thread, Condition

from Torrent import TorrentInfo
from FileStructure import FileStructure
from sendMessage import SendMessageP2P
from Upload import Upload
class Downloader:
    def __init__(self, torrent_file_path, our_peer_id, peerList, uploader: Upload, number_of_bytes_downloaded=0, listen_port=9000):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.pieces_length = self.torrent_info.get_number_of_pieces()
        self.having_pieces_list = defaultdict(list)
        self.our_peer_id = our_peer_id
        self.file_structure = FileStructure("DownloadFolder", self.torrent_info.info_hash, self.pieces_length, "DownloadFolder/mapping_file.json", torrent_info=self.torrent_info)
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)
        self.lock = Lock()
        self.peerList = peerList
        self.unchoke = {}
        self.pieces_data = set()
        self.peerConnection = {}
        self.uploader = uploader
        self.number_of_bytes_downloaded = number_of_bytes_downloaded
        self.task_done = False
        self.listen_port = listen_port
        self.condition = Condition()
        self.dataQueue = queue.Queue()
    
    def download_piece(self, piece_index):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]
            block_size = 1011
            return [(piece_index, offset, offset + min(block_size, piece_size - offset)) for offset in range(0, piece_size, block_size)]
        except Exception as e:
            logging.error(f"Error downloading piece {piece_index}: {e}")
            return None

    def get_rarest_pieces(self):
        rarest_pieces = []
        min_peers_count = float('inf')
        for piece_index, peers in self.having_pieces_list.items():
            if self.bit_field[piece_index] == 0:
                peers_count = len(peers)
                if peers_count < min_peers_count:
                    min_peers_count = peers_count
                    rarest_pieces = [piece_index]
                elif peers_count == min_peers_count:
                    rarest_pieces.append(piece_index)
        return random.choice(rarest_pieces) if rarest_pieces else None

    def update_pieces(self, index, piece, info_hash):
        self.bit_field[index] = 1
        self.file_structure.bitfield[index] = 1
        self.file_structure.save_bitfield(self.download_dir / 'bitfield')
        piece_path = self.download_dir / 'pieces' / f"{info_hash}"
        self.file_structure.save_piece_data(piece_path, piece)
        self.having_pieces_list[index] = []

    def is_having_all_pieces(self):
        return all(self.bit_field)


    def _listen_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            is_completed = False
            buffer = b''
            while not is_completed:
                message = s.recv(1024)
                if not message:
                    is_completed = True
                    break
                else:
                    buffer += message
                message_queue.put(buffer)
        except socket.error as e:
            logging.error(f"Error receiving message from {peer} - {e}")
        finally:
            logging.info(f"Disconnected from {peer}")
            with self.lock:
                if peer in self.peerConnection:
                    del self.peerConnection[peer]
            s.close()

    def _processor_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                message = message_queue.get(timeout=3600)
                self._handle_message(message, peer)
        except queue.Empty:
            logging.error(f"Timeout waiting for message from {peer}")
        finally:
            with self.lock:
                if peer in self.peerConnection:
                    del self.peerConnection[peer]
            s.close()

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
            self._handle_piece_message(payload)
        elif message_id == 11:
            self._handle_get_peers_list_message(payload)
        elif message_id == 13:
            self._handle_send_sever_information(peer)

    def _handle_choke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = False

    def _handle_unchoke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = True

    def _handle_have_message(self, payload, peer):
        piece_index = struct.unpack('>I', payload)[0]
        if self.bit_field[piece_index] == 0:
            with self.lock:
                self.having_pieces_list[piece_index].append(peer)

    def _handle_piece_message(self, payload):
        index = struct.unpack('>I', payload[:4])[0]
        begin = struct.unpack('>I', payload[4:8])[0]
        block_data = payload[8:]
        hash_block = hashlib.sha1(block_data).hexdigest()
        piece_sizes = self.torrent_info.get_piece_sizes()
        if index >= len(piece_sizes):
            logging.error(f"Invalid piece index: {index}")
            return
        with self.lock:
            self.dataQueue.put((index, begin, block_data))
            with self.condition:
                self.condition.notify(1)

    def start_a_connection(self, peer, s: socket.socket):
        try:
            peer_ip, peer_port = peer
            s.connect((peer_ip, peer_port))
            send_message = SendMessageP2P()
            send_message.send_handshake_message(s, self.torrent_info.info_hash, self.our_peer_id)
            handshake_response = s.recv(88)
            if handshake_response[28:68].decode('utf-8') != self.torrent_info.info_hash:
                return
            logging.info(f"Handshake successful with {peer_ip}:{peer_port}")
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
                            if has_piece and i < self.pieces_length:
                                if peer not in self.having_pieces_list[i]:
                                    self.having_pieces_list[i].append(peer)
                    msg = struct.pack('>B', 1)
                    s.send(msg)
                    break
                else:
                    msg = struct.pack('>B', 0)
                    s.send(msg)
            logging.info(f"Received bitfield from {peer[0]}:{peer[1]}")
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error connecting to peer {peer}: {e}")
            if peer in self.peerList:
                self.peerList.remove(peer)
                self.unchoke.pop(peer, None)

    def _connect_peer(self, peer):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3600)
            self.start_a_connection(peer, s)
            with self.lock:
                if not s.getpeername():
                    return None
                else:
                    self.peerConnection[peer] = s
                    if peer[0] not in self.uploader.contribution_rank:
                        self.uploader.contribution_rank[peer[0]] = 0
        except (socket.timeout, socket.error) as e:
            logging.error(f"Error connecting to peer {peer}: {e}")
            if peer in self.peerConnection:
                del self.peerConnection[peer]
            return None

    def _downloader_flow(self, peer, s):
        try:
            message_queue = queue.Queue(maxsize=10000)
            self.start_downloading_handling_thread(s, peer, message_queue)
        except Exception as e:
            logging.error(f"Error in _downloader_flow: {e}")

    def _send_get_peer_list(self, peerList):
        try:
            send_message = SendMessageP2P()
            for peer in peerList:
                conn = self.peerConnection[peer]
                send_message.send_get_peers_list_message(conn)
        except Exception as e:
            logging.error(f"Error in _send_get_peer_list: {e}")

    def _broadcast_have_message(self, piece_index):
        try:
            send_message = SendMessageP2P()
            for peer, conn in self.uploader.peer_sockets.items():
                send_message.send_have_message(conn, piece_index)
        except Exception as e:
            logging.error(f"Error in _broadcast_have_message: {e}")

    def _handle_block(self, index, start, block_data, request_blocks, selected_peers):
        with self.lock:
            if index not in self.pieces_data:
                self.pieces_data[index] = []
            self.pieces_data[index][start:start + len(block_data)] = block_data
            piece_size = self.torrent_info.get_piece_sizes()[index]
            if len(self.pieces_data[index]) == piece_size:
                torrent_info_hash = self.torrent_info.get_piece_info_hash(index).decode('utf-8')
                self.pieces_data[index] = bytes(self.pieces_data[index])
                custom = hashlib.sha1(self.pieces_data[index]).hexdigest()
                if torrent_info_hash == custom:
                    self.update_pieces(index, self.pieces_data[index], custom)
                    for i, block in enumerate(request_blocks):
                        peer = selected_peers[i % len(selected_peers)]
                        index, start, end = block
                        self.uploader.contribution_rank[peer[0]] += end - start
                        self.number_of_bytes_downloaded += end - start
                    logging.info(f"Get piece: {index} hashValue: {torrent_info_hash}")
                    self._broadcast_have_message(index)
                else:
                    self.pieces_data.pop(index)
                self.dataQueue.put(None)

    def request_blocks_from_peers(self, block, peer):
        index, start, end = block
        s = self.peerConnection[peer]
        message = SendMessageP2P()
        message.send_interested_message(s)
        
        while self.unchoke[peer] == False:
            recv = s.recv(1024)
            message_id = struct.unpack('B', recv[4:5])[0]
            if message_id == 1:
                self.unchoke[peer] = True
                break
        
        
        while True:
            try:
                message.send_request_message(index, start, end - start, s)
                recv = s.recv(1024)
                message_id = struct.unpack('B', recv[4:5])[0]
                if message_id == 7:
                    payload = recv[5:]
                    break
            except Exception as e:
                continue
 

        with self.lock:
            self.pieces_data.add((start, payload))
    def key_sort(self, x):
        return x[0]

    def download_blocks(self, request_blocks, selected_peers):
        index = request_blocks[0][0]
        worker_list = []
        for i, block in enumerate(request_blocks):
                if block is not None:
                    worker = Thread(target=self.request_blocks_from_peers, args=(block, selected_peers[i % min(len(selected_peers), 5)]))
                    worker_list.append(worker)
                    time.sleep(0.1)
                    worker.start()
        for worker in worker_list:
            worker.join()


        self.pieces_data = sorted(self.pieces_data, key=self.key_sort)
        
        byte_data = b''
        for block in self.pieces_data:
            byte_data += block[1]
        
        
        piece_hash = hashlib.sha1(byte_data).hexdigest()
        hash_info = self.torrent_info.get_piece_info_hash(index).decode('utf-8')

        if piece_hash == hash_info:
            self.update_pieces(index, byte_data, piece_hash)
            self.pieces_data = []
            return True
        return False 

    def download_rarest_piece(self):
        MAX_RETRIED = 3
        retried_count = 0
        retried_delay = 1
        while not self.is_having_all_pieces():
            rarest_piece = self.get_rarest_pieces()
            if rarest_piece is None:
                break
            request_blocks = self.download_piece(rarest_piece)
            selected_peers = random.sample(self.having_pieces_list[rarest_piece], min(5, len(self.having_pieces_list[rarest_piece])))
            while retried_count < MAX_RETRIED:
                if (self.download_blocks(request_blocks, selected_peers)):
                    break
                else:
                    retried_count += 1
                    time.sleep(retried_delay)
                    retried_delay *= 2
        logging.info("Download processing completed")

    def multi_download_manage(self):
        worker_list = []
        for peer in self.peerList:
            worker = Thread(target=self._connect_peer, args=(peer,))
            worker_list.append(worker)
            worker.start()
        for worker in worker_list:
            worker.join()

        # OK
        self.download_rarest_piece()

    def _download(self):
        while not self.is_having_all_pieces():
            if self.peerList:
                self.multi_download_manage()
            else:
                time.sleep(10)
        self.file_structure.merge_pieces(self.torrent_info)


    def update_peer_list(self, peer):
        if peer not in self.peerList:
            with self.lock:
                self.peerList.append(peer)
                if peer not in self.unchoke:
                    self.unchoke[peer] = False
                

    def update_peer_list_from_tracker(self, peer_list):
        for peer in peer_list:
            self.update_peer_list(peer)
    
    def send_request(self, s: socket, peer, index, start, end):
        send_message = SendMessageP2P()
        send_message.send_interested_message(s)
        while not self.unchoke[peer]:
            time.sleep(0.25)
        send_message.send_request_message(index, start, end - start, s)