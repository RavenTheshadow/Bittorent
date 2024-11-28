from collections import defaultdict, OrderedDict
import socket, logging, struct, queue, time, random, hashlib
from threading import Lock, Thread, Condition
from Torrent import TorrentInfo
from FileStructure import FileStructure
from sendMessage import SendMessageP2P
from Upload import Upload
import tqdm
logging.basicConfig(level=logging.INFO)

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
        self.unchoke = {peer: False for peer in peerList}
        self.pieces_data = OrderedDict()
        self.peerConnection = {}
        self.uploader = uploader
        self.number_of_bytes_downloaded = number_of_bytes_downloaded
        self.task_done = False
        self.listen_port = listen_port
        self.condition = Condition()
        self.dataQueue = queue.Queue()
        self.listener_list = []
        self.received_blocks = defaultdict(set)
        self.progress = tqdm.tqdm(unit='B', unit_scale=True, unit_divisor=1024, total=self.torrent_info.total_bytes, initial=self.number_of_bytes_downloaded)
    def start_a_connection(self, peer, s: socket.socket):
        try:
            peer_ip, peer_port = peer
            s.connect((peer_ip, peer_port))
            self._perform_handshake(s, peer)
            self._receive_bitfield(s, peer)
        except (socket.timeout, socket.error) as e:
            # logging.error(f"Error connecting to peer {peer}: {e}")
            self._remove_peer(peer)

    def _perform_handshake(self, s, peer):
        send_message = SendMessageP2P()
        send_message.send_handshake_message(s, self.torrent_info.info_hash, self.our_peer_id)
        handshake_response = s.recv(88)
        if handshake_response[28:68].decode('utf-8') != self.torrent_info.info_hash:
            raise ConnectionError("Handshake failed")

    def _receive_bitfield(self, s, peer):
        while True:
            bitfield_msg = s.recv(1024)
            if not bitfield_msg:
                if s.fileno() != -1:
                    s.close()
                    self._remove_peer(peer)
                    break
                s.send(struct.pack('>B', 0))
                continue
            bitfield_message_id = struct.unpack('B', bitfield_msg[4:5])[0]
            if bitfield_message_id == 5:
                self._process_bitfield(bitfield_msg[5:], peer)
                s.send(struct.pack('>B', 1))
                break
            else:
                s.send(struct.pack('>B', 0))
        # logging.info(f"Received bitfield from {peer[0]}:{peer[1]}")

    def _process_bitfield(self, bitfield, peer):
        for i, has_piece in enumerate(bitfield):
            if has_piece and i < self.pieces_length:
                if peer not in self.having_pieces_list[i]:
                    self.having_pieces_list[i].append(peer)

    def _remove_peer(self, peer):
        with self.lock:
            if peer in self.peerList:
                self.peerList.remove(peer)
                self.unchoke.pop(peer, None)

    def _connect_peer(self, peer):
        if peer in self.peerConnection:
            return self.peerConnection[peer]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Time out for a section = 1200
            s.settimeout(1200)
            self.start_a_connection(peer, s)
            self._validate_connection(s, peer)
            return s
        except (socket.timeout, socket.error) as e:
            # logging.error(f"Error connecting to peer {peer}: {e}")
            self._remove_peer(peer)
            return None

    def _validate_connection(self, s, peer):
        if not s.getpeername():
            raise ConnectionError(f"Failed to start a connection to peer {peer} within 10 seconds")
        with self.lock:
            self.peerConnection[peer] = s
            if peer[0] not in self.uploader.contribution_rank:
                self.uploader.contribution_rank[peer[0]] = 0
            self.unchoke[peer] = False

    def download_piece(self, piece_index):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]
            block_size = 1024
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

    def start_downloading_handling_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            listen_thread = Thread(target=self._listen_thread, daemon=True, args=(s, peer, message_queue))
            processor_thread = Thread(target=self._processor_thread, daemon=True, args=(s, peer, message_queue))
            listen_thread.start()
            processor_thread.start()
        except (socket.timeout, socket.error) as e:
            if peer in self.peerList:
                self.peerList.remove(peer)

    def _listen_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                if s.fileno() == -1:
                    break
                message = s.recv(1024 + 13)
                if not message:
                    break
                message_queue.put(message)
        except socket.error as e:
            logging.error(f"Error receiving message from {peer} - {e}")
        finally:
            # logging.info(f"Disconnected from {peer}")
            with self.lock:
                if peer in self.peerConnection:
                    del self.peerConnection[peer]
            if s.fileno() != -1:
                s.close()

    def _processor_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                message = message_queue.get()
                self._handle_message(message, peer)
        except queue.Empty:
            logging.error(f"Timeout waiting for message from {peer}")
        except Exception as e:
            logging.error(f"Error processing message from {peer}: {e}")
        finally:
            if s.fileno() != -1:
                s.close()

    def _handle_message(self, message, peer):
        if len(message) < 1:
            return
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

    def _handle_choke_message(self, peer):
        with self.lock:
            if peer in self.unchoke:
                self.unchoke[peer] = False

    def _handle_unchoke_message(self, peer):
        with self.lock:
            if peer in self.unchoke:
                self.unchoke[peer] = True

    def _handle_have_message(self, payload, peer):
        piece_index = struct.unpack('>I', payload)[0]
        if self.bit_field[piece_index] == 0:
            with self.lock:
                self.having_pieces_list[piece_index].append(peer)

    def _downloader_flow(self, peer, s):
        try:
            message_queue = queue.Queue(maxsize= 1024)
            self.start_downloading_handling_thread(s, peer, message_queue)
        except Exception as e:
            logging.error(f"Error in _downloader_flow: {e}")

    def _handle_piece_message(self, payload):
        index = struct.unpack('>I', payload[:4])[0]
        begin = struct.unpack('>I', payload[4:8])[0]
        block_data = payload[8:]
        piece_sizes = self.torrent_info.get_piece_sizes()
        if index >= len(piece_sizes):
            logging.error(f"Invalid piece index: {index}")
            return
        self.number_of_bytes_downloaded+=len(block_data)
        self.progress.update(len(block_data))
        with self.lock:
            self.dataQueue.put((index, begin, block_data))
            self.received_blocks[index].add((begin, begin + len(block_data)))
        with self.condition:
            self.condition.notify_all()

    def _send_block_request(self, s, peer, index, start, end):
        send_message = SendMessageP2P()
        send_message.send_interested_message(s)
        while True:
            with self.lock:
                if self.unchoke.get(peer, False):
                    break
            time.sleep(0.1)
        send_message.send_request_message(index, start, end - start, s)

    def request_blocks_from_peers(self, request_blocks, selected_peers):
        for i, block in enumerate(request_blocks):
            index, start, end = block
            peer_request_block = selected_peers[i % len(selected_peers)]
            with self.lock:
                s = self.peerConnection.get(peer_request_block)
            if s:
                self._send_block_request(s, peer_request_block, index, start, end)
                time.sleep(0.1)

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

    def _handle_block(self, index, start, block_data):
        with self.lock:
            piece_size = self.torrent_info.get_piece_sizes()[index]
            self.pieces_data[start] = block_data
            length = sum(len(data) for data in self.pieces_data.values())
            if length >= piece_size:
                self.dataQueue.put(None)

    def process_downloaded_blocks(self, request_blocks, selected_peers):
        missing_blocks = set(request_blocks)
        while True:
            try:
                block = self._wait_for_block()
                if block is None:
                    return
                missing_blocks.discard((block[0], block[1], block[1] + len(block[2])))
                self._handle_block(*block)
            except Exception as e:
                if missing_blocks:
                    self.request_blocks_from_peers(list(missing_blocks), selected_peers)

    def _wait_for_block(self):
        with self.condition:
            if self.dataQueue.empty():
                if not self.condition.wait(timeout=1):
                    raise TimeoutError("No data received for 5 seconds")
            return self.dataQueue.get()

    def download_rarest_piece(self):
        MAX_RETRIES = 3

        rarest_piece = self.get_rarest_pieces()
        if rarest_piece is None:
            return None

        request_blocks = self.download_piece(rarest_piece)
        selected_peers = random.sample(self.having_pieces_list[rarest_piece], min(5, len(self.having_pieces_list[rarest_piece])))

        for _ in range(MAX_RETRIES):
            try:
                self.request_blocks_from_peers(request_blocks, selected_peers)
                self.process_downloaded_blocks(request_blocks, selected_peers)
                self._verify_and_update_piece(rarest_piece)
                return
            except ConnectionError as e:
                logging.error(f"Connection error while processing piece {rarest_piece}: {e}")
    
    def sort_pieces_data(self):
        with self.lock:
            self.pieces_data = OrderedDict(sorted(self.pieces_data.items()))

    def _verify_and_update_piece(self, rarest_piece):
        torrent_info_hash = self.torrent_info.get_piece_info_hash(rarest_piece).decode('utf-8')
        if self.pieces_data:
            # Sort the pieces data by offset
            self.sort_pieces_data()
            piece_data = b''.join(self.pieces_data.values())
            current_info_hash = hashlib.sha1(piece_data).hexdigest()
        else:
            raise ConnectionError("No data received for piece")
        if torrent_info_hash == current_info_hash:
            self.update_pieces(rarest_piece, piece_data, current_info_hash)
            logging.info(f"Get piece: {rarest_piece} hashValue: {torrent_info_hash}")
            self._broadcast_have_message(rarest_piece)
        else:
            logging.error(f"Hash mismatch for piece {rarest_piece}. Re-requesting missing blocks.")
            self._re_request_missing_blocks(rarest_piece)
        self.pieces_data.clear()

    def _re_request_missing_blocks(self, piece_index):
        piece_size = self.torrent_info.get_piece_sizes()[piece_index]
        block_size = 1024
        expected_blocks = {(offset, offset + min(block_size, piece_size - offset)) for offset in range(0, piece_size, block_size)}
        missing_blocks = expected_blocks - self.received_blocks[piece_index]
        if missing_blocks:
            logging.info(f"Re-requesting {len(missing_blocks)} missing blocks for piece {piece_index}")
            selected_peers = random.sample(self.having_pieces_list[piece_index], min(5, len(self.having_pieces_list[piece_index])))
            self.request_blocks_from_peers(list(missing_blocks), selected_peers)
        else:
            logging.info(f"All blocks received for piece {piece_index}")
        self.received_blocks[piece_index].clear()

        request_blocks = self.download_piece(rarest_piece)
        selected_peers = random.sample(self.having_pieces_list[rarest_piece], min(5, len(self.having_pieces_list[rarest_piece])))

        for _ in range(MAX_RETRIES):
            try:
                self.request_blocks_from_peers(request_blocks, selected_peers)
                self.process_downloaded_blocks(request_blocks, selected_peers)
                self._verify_and_update_piece(rarest_piece)
                return
            except ConnectionError as e:
                logging.error(f"Connection error while processing piece {rarest_piece}: {e}")
    
    def sort_pieces_data(self):
        with self.lock:
            self.pieces_data = OrderedDict(sorted(self.pieces_data.items()))

    def _verify_and_update_piece(self, rarest_piece):
        torrent_info_hash = self.torrent_info.get_piece_info_hash(rarest_piece).decode('utf-8')
        if self.pieces_data:
            # Sort the pieces data by offset
            self.sort_pieces_data()
            piece_data = b''.join(self.pieces_data.values())
            current_info_hash = hashlib.sha1(piece_data).hexdigest()
        else:
            raise ConnectionError("No data received for piece")
        if torrent_info_hash == current_info_hash:
            self.update_pieces(rarest_piece, piece_data, current_info_hash)
            # logging.info(f"Get piece: {rarest_piece} hashValue: {torrent_info_hash}")
            self._broadcast_have_message(rarest_piece)
        else:
            logging.error(f"Hash mismatch for piece {rarest_piece}. Re-requesting missing blocks.")
            self._re_request_missing_blocks(rarest_piece)
        self.pieces_data.clear()

    def _re_request_missing_blocks(self, piece_index):
        piece_size = self.torrent_info.get_piece_sizes()[piece_index]
        block_size = 1024
        expected_blocks = {(offset, offset + min(block_size, piece_size - offset)) for offset in range(0, piece_size, block_size)}
        missing_blocks = expected_blocks - self.received_blocks[piece_index]
        if missing_blocks:
            # logging.info(f"Re-requesting {len(missing_blocks)} missing blocks for piece {piece_index}")
            selected_peers = random.sample(self.having_pieces_list[piece_index], min(5, len(self.having_pieces_list[piece_index])))
            self.request_blocks_from_peers(list(missing_blocks), selected_peers)
        else:
            # logging.info(f"All blocks received for piece {piece_index}")
            pass
        self.received_blocks[piece_index].clear()
    def downloadThread(self):
        pass
    def _download(self):
        while not self.is_having_all_pieces():
            # Thread(target=self.downloadThread,daemon=True).start()
            if self.peerList:
                try:
                    self.download_rarest_piece()
                except ConnectionError as e:
                    logging.error(f"Download error: {e}")
            else:
                time.sleep(5)
        self.file_structure.merge_pieces(self.torrent_info)
        # if self.progress:
        #     self.progress.close()
        #     self.progress.clear()
        #     print(f"Finish downloading !!")
    def update_peer_list(self, peer):
        with self.lock:
            if peer not in self.peerList:
                self.peerList.append(peer)
        conn = self._connect_peer(peer)
        if conn:
            listener = Thread(target=self._downloader_flow, args=(peer, conn))
            listener.start()
            self.listener_list.append(listener)

    def update_peer_list_from_tracker(self, peer_list):
        for peer in peer_list:
            self.update_peer_list(peer)