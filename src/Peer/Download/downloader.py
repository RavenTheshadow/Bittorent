from collections import defaultdict
from pathlib import Path
import sys, socket, logging, struct, queue, time, random, hashlib
sys.path.append(str(Path(__file__).resolve().parent.parent))
from MOCKTorrent import TorrentInfo
from FileStruct.fileStructure import FileStructure
from Message.sendMessage import SendMessageP2P
from threading import Lock, Thread, Condition
from Upload.upload import Upload
logging.basicConfig(level=logging.INFO)
class Downloader:
    def __init__(self, torrent_file_path, our_peer_id, peerList, uploader: Upload, number_of_bytes_downloaded=0, listen_port= 9000):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.pieces_length = self.torrent_info.get_number_of_pieces()
        self.having_pieces_list = defaultdict(list)
        self.our_peer_id = our_peer_id

        self.file_structure = FileStructure("DownloadFolder", self.torrent_info.info_hash,
                                             self.pieces_length, "DownloadFolder/mapping_file.json", torrent_info= self.torrent_info)
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)

        self.lock = Lock()
        self.peerList = peerList
        self.unchoke = {peer: False for peer in peerList}
        self.pieces_data = {}
        self.peerConnection = {}

        self.uploader = uploader
        self.number_of_bytes_downloaded = number_of_bytes_downloaded
        self.task_done = False
        self.listen_port = listen_port
        ### Get data treatment
        self.condition = Condition()
        self.dataQueue = queue.Queue()

    def download_piece(self, piece_index):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]
            
            block_size = 1430
            return [(piece_index, offset, offset + min(block_size, piece_size - offset)) for offset in range(0, piece_size, block_size)]
        except Exception as e:
            print(f"Error downloading piece {piece_index}: {e}")
            return None

    def get_rarest_pieces(self):
        rarest_piece = None
        min_peers_count = float('inf')

        for piece_index, peers in self.having_pieces_list.items():
            if self.bit_field[piece_index] == 0:
                peers_count = len(peers)
                if peers_count < min_peers_count:
                    min_peers_count = peers_count
                    rarest_piece = piece_index

        return rarest_piece
    
    def update_pieces(self, index, piece, info_hash):
        self.bit_field[index] = 1
        self.file_structure.bitfield[index] = 1
        self.file_structure.save_bitfield(self.download_dir / 'bitfield')
        piece_path = self.download_dir / 'pieces' / f"{info_hash}"
        self.file_structure.save_piece_data(piece_path, piece)
        self.having_pieces_list[index] = [] # None
    
    def is_having_all_pieces(self):
            return all(self.bit_field)

    def start_downloading_handling_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            listen_thread = Thread(target=self._listen_thread, daemon= True, args=(s, peer, message_queue))
            processor_thread = Thread(target=self._processor_thread, daemon= True, args=(s, peer, message_queue))

            listen_thread.start()
            processor_thread.start()

        except (socket.timeout, socket.error) as e:
            logging.error(f"")
            if peer in self.peerList:
                self.peerList.remove(peer)
            listen_thread.join()
            processor_thread.join()

    def _listen_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                # TCP get max length of 1460 bytes
                message = s.recv(1460)
                if not message:
                    break
                message_queue.put(message)
        except socket.error as e:
            logging.error(f"Error receiving message from {peer} - {e}")
        finally: 
            logging.info(f"Disconnected from {peer}")

    def _processor_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
            try:
                while True:
                    message = message_queue.get(timeout=60)
                    self._handle_message(message, peer)
            except queue.Empty:
                logging.error(f"Timeout waiting for message from {peer}")

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

    def _handle_send_sever_information(self, peer):
        try:
            send_message = SendMessageP2P()
            conn = self.peerConnection[peer]
            send_message.send_server_information(conn, self.our_peer_id, self.listen_port)
        except Exception as e:
            pass

    def _handle_choke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = False

    def _handle_unchoke_message(self, peer):
        with self.lock:
            self.unchoke[peer] = True

    def _handle_get_peers_list_message(self, payload):
        try:
            peer_list = payload.decode('utf-8')
            for peer in peer_list:
                with self.lock:
                    if peer not in self.peerList:
                        self.peerList.append(peer)
                        self._connect_peer(peer)
        except Exception as e:
            logging.error(f"Error handling get peers list message: {e}")

    def _handle_have_message(self, payload, peer):
        piece_index = struct.unpack('>I', payload)[0]
        if self.bit_field[piece_index] == 0:
            with self.lock:
                self.having_pieces_list[piece_index].append(peer)

    def _handle_piece_message(self, payload):
        index = struct.unpack('>I', payload[:4])[0]         # Index: 4 bytes
        begin = struct.unpack('>I', payload[4:8])[0]        # Begin: 4 bytes
        block_data = payload[8:]                            # Block data: Rest of the payload   
        
        piece_sizes = self.torrent_info.get_piece_sizes()
    
        if index >= len(piece_sizes):
            logging.error(f"Invalid piece index: {index}")
            return
    
        with self.lock:
            self.dataQueue.put((index, begin, block_data))
            with self.condition:
                self.condition.notify_all()

    def start_a_connection(self, peer, s: socket.socket):
        try:
            peer_ip, peer_port = peer
            s.connect((peer_ip, peer_port))
                
            send_message = SendMessageP2P()
            send_message.send_handshake_message(s, self.torrent_info.info_hash, self.our_peer_id, self.listen_port)

            handshake_response = s.recv(88)
            handshake_response = handshake_response.decode('utf-8')

            if handshake_response[28:68] != self.torrent_info.info_hash:
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
            if peer in self.peerList:
                self.peerList.remove(peer)
                self.unchoke.pop(peer, None)

    def send_request(self, s: socket, peer,  index, start, end):
        send_message = SendMessageP2P()
        
        # index, start, end = self.peer_block_requests[peer].pop(0)
        send_message.send_interested_message(s)
            
        ### Wait for unchoke message
        while not self.unchoke[peer]:
            time.sleep(0.25)  # Add a small sleep to avoid busy waiting
        ## Wait for unchoke message
        send_message.send_request_message(index, start, end - start, s)

    def _send_block_request(self, s, peer, index, start, end):
        MAX_RETRIED     = 3
        retried_count   = 0
        retried_delay   = 2
        while retried_count < MAX_RETRIED:
            try:                
                self.send_request(s, peer, index, start, end)
                break
            except socket.timeout as e:
                logging.error(f"Socket is time out, trying to reconnection...")
                retried_count += 1
                time.sleep(retried_delay)
                continue
            except IndexError as e:
                raise IndexError("Not valid index")
            except socket.error as e:
                self.peerList.remove(peer)

                for piece in self.having_pieces_list:
                    if peer in piece:
                        piece.remove(peer)
                self.unchoke.pop(peer)
        if retried_count == MAX_RETRIED:
            raise TimeoutError(f"Cannot reconnection after tried {MAX_RETRIED}")
    
    def _connect_peer(self, peer):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(600)
            self.start_a_connection(peer, s)
            with self.lock:
                if not s.getpeername():
                    return None
                else:
                    self.peerConnection[peer] = s
                    if peer[0] not in self.uploader.contribution_rank:
                        self.uploader.contribution_rank[peer[0]] = 0
        except Exception as e:
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
            for conn in self.uploader.peer_sockets:
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

    def request_blocks_from_peers(self, request_blocks, selected_peers):
        try:
            for i, block in enumerate(request_blocks):
                index, start, end = block
                peer_request_block = selected_peers[i % len(selected_peers)]
                s = self.peerConnection[peer_request_block]
                self._send_block_request(s, peer_request_block, index, start, end)
                time.sleep(0.1)
        except Exception as e:
            logging.error(f"Error in request_blocks_from_peers: {e}")

    def process_downloaded_blocks(self, request_blocks, selected_peers):
        while True:
            with self.condition:
                while self.dataQueue.empty():
                    if not self.condition.wait(timeout=5):
                        raise TimeoutError(f"Timeout while waiting for data in dataQueue")

                block = self.dataQueue.get()
                if block is None:
                    break

                index, start, block_data = block
                self._handle_block(index, start, block_data, request_blocks, selected_peers)

    def download_rarest_piece(self):
        MAX_RETRIED     = 3
        retried_count   = 0
        retried_delay   = 1

        while not self.is_having_all_pieces():
            rarest_piece = self.get_rarest_pieces()

            if rarest_piece is None:
                break

            request_blocks = self.download_piece(rarest_piece)
            selected_peers = random.sample(self.having_pieces_list[rarest_piece], min(5, len(self.having_pieces_list[rarest_piece])))

            while retried_count < MAX_RETRIED:
                try:
                    self.request_blocks_from_peers(request_blocks, selected_peers)
                    self.process_downloaded_blocks(request_blocks, selected_peers)
                    retried_count = 0
                    break
                except TimeoutError as e:
                    retried_count += 1
                    time.sleep(retried_delay)
                    continue
            
            if retried_count == MAX_RETRIED:
                raise ConnectionError(f"Failed to processing after retried... {MAX_RETRIED}")
            else:
                retried_count = 0

        logging.info("Download processing completed")
            

    def multi_download_manage(self):
        """Manage multi-threaded downloads from the peer list."""
        worker_list = []
        for peer in self.peerList:
            worker = Thread(target=self._connect_peer, args=(peer, ))
            worker_list.append(worker)
            worker.start()

        for worker in worker_list:
            worker.join()

        listener_list = []
        for peer in self.peerList:
            conn = self.peerConnection[peer]
            listener = Thread(target=self._downloader_flow, args=(peer, conn))
            listener_list.append(listener)
            listener.start()
        try:
            self.download_rarest_piece()
        except ConnectionError as e:
            logging.error(f"{e}")

        for listener in listener_list:
            listener.join()

        self.file_structure.merge_pieces(self.torrent_info)
        self.task_done = True            
            
if __name__ == "__main__":
    tester = Downloader(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\hello.torrent', "127.119.128.1", [], None)
    tester.file_structure.merge_pieces(tester.torrent_info)