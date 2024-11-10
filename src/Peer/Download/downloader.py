from collections import defaultdict
from pathlib import Path
import sys, socket, logging, struct, queue, time, random, hashlib
sys.path.append(str(Path(__file__).resolve().parent.parent))
from MOCKTorrent import TorrentInfo
from FileStruct.fileStructure import FileStructure
from Message.sendMessage import SendMessageP2P
from threading import Lock, Thread, Barrier

logging.basicConfig(level=logging.INFO)
class Downloader:
    def __init__(self, torrent_file_path, our_peer_id, peerList, contribution_rank):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.pieces_length = self.torrent_info.get_number_of_pieces()
        self.having_pieces_list = defaultdict(list)
        self.our_peer_id = our_peer_id
        self.file_structure = FileStructure("DownloadFolder", self.torrent_info.info_hash, self.pieces_length, "DownloadFolder/mapping_file.json")
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)
        self.lock = Lock()
        self.peerList = peerList
        self.unchoke = {peer: False for peer in peerList}
        self.contribution_rank = contribution_rank
        self.pieces_data = {}
        self.peerConnection = {}
        self.barrier = None

    def download_piece(self, piece_index):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]
            
            block_size = 2048
            return [(piece_index, offset, offset + min(block_size, piece_size - offset)) for offset in range(0, piece_size, block_size)]
        except Exception as e:
            print(f"Error downloading piece {piece_index}: {e}")
            return None

    def get_rarest_pieces(self):
        rarest_pieces = None
        for piece_index, peers in self.having_pieces_list.items():
            if self.bit_field[piece_index] == 0:
                if rarest_pieces is None or len(peers) < len(self.having_pieces_list[rarest_pieces]):
                    rarest_pieces = piece_index
        return rarest_pieces
    
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
                self.peerList.remove()
            listen_thread.join()
            processor_thread.join()

    def _listen_thread(self, s: socket.socket, peer, message_queue: queue.Queue):
        try:
            while True:
                message = s.recv(2048 + 30)
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
        # elif message_id == 4:
        #     self._handle_have_message(payload, peer)
        elif message_id == 7:
            logging.info(f"Len message: {len(message)}")
            self._handle_piece_message(payload, peer)

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

    def _handle_piece_message(self, payload, peer):
        index = struct.unpack('>I', payload[:4])[0]         # Index: 4 bytes
        begin = struct.unpack('>I', payload[4:8])[0]        # Begin: 4 bytes
        block_data = payload[8:]                            # Block data: Rest of the payload   
        logging.info(f"Size of block_data: {len(block_data)}")
    
        piece_sizes = self.torrent_info.get_piece_sizes()
        logging.info(f"Received piece index: {index}, Total pieces: {len(piece_sizes)}")
    
        if index >= len(piece_sizes):
            logging.error(f"Invalid piece index: {index}")
            return
    
        with self.lock:
            if index not in self.pieces_data:
                piece_size = piece_sizes[index]
                self.pieces_data[index] = bytearray(piece_size)
            self.pieces_data[index][begin:begin + len(block_data)] = block_data
        self.barrier.wait()

    def start_a_connection(self, peer, s: socket.socket):
        try:
            peer_ip, peer_port = peer
            s.connect((peer_ip, peer_port))
                
            send_message = SendMessageP2P()
            send_message.send_handshake_message(s, self.torrent_info.info_hash, self.our_peer_id)

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
            logging.error(f"Failed to connect to {peer_ip}:{peer_port} - {e}")
            if peer in self.peerList:
                self.peerList.remove(peer)

    def _send_block_request(self, s, peer, index, start, end):
        try:                
            send_message = SendMessageP2P()
        
            # index, start, end = self.peer_block_requests[peer].pop(0)
            send_message.send_interested_message(s)
            
            ### Wait for unchoke message
            while not self.unchoke[peer]:
                time.sleep(1)  # Add a small sleep to avoid busy waiting
            ## Wait for unchoke message
            send_message.send_request_message(index, start, end - start, s)

        except (socket.timeout, socket.error, IndexError) as e:
            logging.error(f"Error: {e}")
            pass

    def _connect_peer(self, peer):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(60)
            self.start_a_connection(peer, s)
            with self.lock:
                self.peerConnection[peer] = s
                if peer[0] not in self.contribution_rank:
                    self.contribution_rank[peer[0]] = 0
        except Exception as e:
            logging.error(f"Error in _connect_peer: {e}")
            if peer in self.peerConnection:
                del self.peerConnection[peer]
            return None

    def _downloader_flow(self, peer, s):
        try:        
            message_queue = queue.Queue()
            self.start_downloading_handling_thread(s, peer, message_queue)
        except Exception as e:
            logging.error(f"Error in _downloader_flow: {e}")

    def rarest_pieces_algorithm(self):
        while not self.is_having_all_pieces():
            rarest_piece = self.get_rarest_pieces()
            torrent_info_hash = self.torrent_info.get_piece_info_hash(rarest_piece).decode('utf-8')
            request_blocks = self.download_piece(rarest_piece)
            selected_peers = random.sample(self.having_pieces_list[rarest_piece], min(5, len(self.having_pieces_list[rarest_piece])))

            self.barrier = Barrier(len(selected_peers) + 1)
            for block in request_blocks:
                for peer in selected_peers:
                    try:
                        index, start, end = block
                        s = self.peerConnection[peer]
                        self._send_block_request(s, peer, index, start, end)
                    except Exception as e:
                        logging.error(f"Error sending block request to peer {peer}: {e}")
                self.barrier.wait()

                for peer in selected_peers:
                    logging.info(f"Get {rarest_piece} {start} {end} from {peer}")
                    logging.info(f"Block hash: {hashlib.sha1(self.pieces_data[rarest_piece][start:end]).hexdigest()}")
            
            custom = hashlib.sha1(self.pieces_data[rarest_piece]).hexdigest()
            if torrent_info_hash == custom:
                self.update_pieces(rarest_piece, self.pieces_data[rarest_piece], custom)
                    
                for i, block in enumerate(request_blocks):
                    peer = selected_peers[i % len(selected_peers)]
                    index, start, end = block
                    self.contribution_rank[peer[0]] += (end - start) 
            else:
                del self.pieces_data[rarest_piece]
            

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

        self.rarest_pieces_algorithm()

        for listener in listener_list:
            listener.join()

            
            
if __name__ == "__main__":
    tester = Downloader(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\hello.torrent', "127.119.128.1:6681")
    for i in range(52):
        tester.update_pieces(i, b"Hello", tester.torrent_info.get_piece_info_hash(i).decode('utf-8'))
        
    tester.having_pieces_list[0].append(("192.168.1.1", 6681))
    tester.having_pieces_list[1].append(("191.129.12.1", 6682))
    tester.having_pieces_list[1].append(("10.0.2.3", 8000))
    tester.having_pieces_list[2].append(("10.0.2.3", 8000))
    print(tester.get_rarest_pieces())