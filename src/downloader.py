from threading import Thread, Lock
from collections import Counter, defaultdict
import socket, struct, os, time
from FileStruct.fileStructure import FileStructure

class TorrentInfo:
    def __init__(self, info_hash = 'info_hash', pieces_length= 10):
        self.info_hash      = info_hash
        self.pieces_length  = pieces_length

class PeerNode:
    def __init__(self, peer_ip, peer_port):
        self.peer_ip    = peer_ip
        self.peer_port  = peer_port


class Downloader:
    def __init__(self, torrent_info: TorrentInfo,
                 our_peer_id,
                 peerList=[]):
        
        self.torrent_info = torrent_info
        self.info_hash = torrent_info.info_hash
        self.pieces_length = torrent_info.pieces_length
        self.pieces_count = [0] * self.pieces_length
        self.having_pieces_list = defaultdict(list)

        self.our_peer_id = our_peer_id
        self.peerList = peerList
        self.others_peer_bitfield = []
        self.file_structure = FileStructure("DownloadFolder",
                                             self.info_hash,
                                             self.pieces_length,
                                             "DownloadFolder/mapping_file.json")
        
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)
        self.lock = Lock()

    def send_handshake_message(self, s: socket.socket):
        pstrlen = struct.pack('B', 19)
        pstr = b'BitTorrent protocol'
        reserved = struct.pack('8B', *[0]*8)
        info_hash = self.info_hash.encode('utf-8')
        peer_id = self.our_peer_id.encode('utf-8')
        s.send(pstrlen + pstr + reserved + info_hash + peer_id)
    
    def send_keep_alive_message(self, s: socket.socket):
        s.send(struct.pack('>I', 0))

    def send_interested_message(self, s: socket.socket):
        length_prefix = struct.pack('>I', 1)
        message_id = struct.pack('B', 2)  # 2 for interested
        s.send(length_prefix + message_id)

    def wait_for_unchoke_message(self, s:socket.socket):
        while True:
            length_prefix = struct.unpack('>I', s.recv(4))[0]
            message_id = struct.unpack('B', s.recv(1))[0]
            if message_id == 1:  # 1 for unchoke
                return True
            elif length_prefix == 0:
                print("Received keep-alive message")
            else:
                s.recv(length_prefix - 1)
            time.sleep(0.01)

    def send_request_message(self, index, begin, length, s: socket.socket):
        length_prefix = struct.pack('>I', 13)  # 4 bytes for length prefix
        message_id = struct.pack('B', 6)  # 1 byte for message ID (6 for request)
        index = struct.pack('>I', index)  # 4 bytes for piece index
        begin = struct.pack('>I', begin)  # 4 bytes for begin offset
        length = struct.pack('>I', length)  # 4 bytes for length
        s.send(length_prefix + message_id + index + begin + length)

    def download_piece(self, s, piece_index, piece_length):
        pass

    def connect_to_peer(self, peer: PeerNode):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer.peer_ip, peer.peer_port))
            
            # Send the handshake message
            self.send_handshake_message(s)

            # Receive the handshake message
            handshake_response = s.recv(68)
            if handshake_response[28:48] == self.info_hash.encode('utf-8'):
                print(f"Handshake successful with {peer.peer_ip}:{peer.peer_port}")

                # Receive bitfield message
                bitfield_length = struct.unpack('>I', s.recv(4))[0]
                bitfield_message_id = struct.unpack('B', s.recv(1))[0]
                if bitfield_message_id == 5:  # Message ID for "bitfield" is 5
                    bitfield = s.recv(bitfield_length - 1)

                    # Update shared resources
                    with self.lock:
                        self.others_peer_bitfield.append(bitfield)
                        for i, has_piece in enumerate(bitfield):
                            if has_piece and i < self.pieces_length:
                                self.pieces_count[i] += 1
                                peer_info = f"{peer.peer_ip}:{peer.peer_port}"
                                if peer_info not in self.having_pieces_list[i]:
                                    self.having_pieces_list[i].append(peer_info)

                    print(f"Received bitfield from {peer.peer_ip}:{peer.peer_port}")
                while True:
                    time.sleep(30)
                    self.send_keep_alive_message(s)
            else:
                print(f"Handshake failed with {peer.peer_ip}:{peer.peer_port}")
        except socket.error as e:
            print(f"Failed to connect to {peer.peer_ip}:{peer.peer_port} - {e}")
            # Remove the peer from the list
            self.peerList.remove(peer)
        finally:
            s.close()


    def create_connection(self):
        threads = []
        for peer in self.peerList:
            thread = Thread(target=self.connect_to_peer, args=(peer,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def get_rarest_pieces_list(self):
        piece_counter = Counter()

        for bitfield in self.others_peer_bitfield:
            for index, bit in enumerate(bitfield):
                if bit == 1 and self.bit_field[index] == 0:
                    piece_counter[index] += 1

        rarest_pieces = sorted(
            piece_counter.keys(), key=lambda piece: piece_counter[piece]
        )
        
        return rarest_pieces

    def update_bitfield(self, piece_index):
        with self.lock:
            self.bit_field[piece_index] = 1
            self.file_structure.save_bitfield(os.path.join(self.download_dir, 'bitfield'), self.bit_field)

# Test Downloader class functionality
# tester = Downloader(TorrentInfo(), '12345', [PeerNode('192.168.1.1', 6681)])
# tester.update_bitfield(2)
# print(tester.get_rarest_pieces_list())