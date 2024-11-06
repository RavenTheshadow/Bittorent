import socket, struct, random
from threading import Lock, Barrier, Event
from concurrent.futures import ThreadPoolExecutor
from Message.sendMessage import SendMessageP2P
from Download.downloader import Downloader

class P2PConnection:
    def __init__(self, torrent_file_path, our_peer_id="Hello", peerList=[]):
        self.lock = Lock()
        self.our_peer_id = our_peer_id
        self.peerList = peerList
        self.downloader = Downloader(torrent_file_path, our_peer_id, peerList)
        self.barrier = Barrier(len(peerList) + 1)
        self.peer_event = {peer: Event() for peer in peerList}
        self.peer_block_requests = {peer: [] for peer in peerList}
        self.piece_data = {}
    def connect_to_peer(self, peer):
        try:
            peer_ip, peer_port = peer

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # Set timeout to avoid infinite waiting
                s.connect((peer_ip, peer_port))
                
                # Send the handshake message
                send_message = SendMessageP2P()
                send_message.send_handshake_message(s, self.downloader.info_hash, self.our_peer_id)

                # Receive the handshake message
                handshake_response = s.recv(68)
                if handshake_response[28:48] == self.downloader.info_hash.encode('utf-8'):
                    print(f"Handshake successful with {peer_ip}:{peer_port}")

                    # Receive bitfield message
                    bitfield_length_bytes = s.recv(4)
                    if len(bitfield_length_bytes) == 4:
                        bitfield_length = struct.unpack('>I', bitfield_length_bytes)[0]

                        bitfield_message_id_bytes = s.recv(1)
                        if len(bitfield_message_id_bytes) == 1:
                            bitfield_message_id = struct.unpack('B', bitfield_message_id_bytes)[0]

                            if bitfield_message_id == 5:  # Message ID for "bitfield" is 5
                                bitfield = s.recv(bitfield_length - 1)

                                # Update shared resources
                                with self.lock:
                                    for i, has_piece in enumerate(bitfield):
                                        if has_piece and i < self.downloader.pieces_length:
                                            peer_info = (peer_ip, peer_port)
                                            if peer_info not in self.downloader.having_pieces_list[i]:
                                                self.downloader.having_pieces_list[i].append(peer_info)

                            print(f"Received bitfield from {peer_ip}:{peer_port}")

                    # Wait for all peers to get the bitfield
                    self.barrier.wait()

                    piece_index     = None
                    piece_size      = None
                    piece_data      = None
                    
                    
                    while True:
                        # Wait for the first rarest piece to be ready
                        self.peer_event[peer].wait()

                        if self.peer_block_requests[peer]:
                            index, start, length = self.peer_block_requests[peer].pop(0)

                            if piece_index is None:
                                piece_index = index
                                piece_size  = self.downloader.torrent_info.get_piece_sizes()[piece_index]
                                piece_data  = bytearray(piece_size)
                                self.piece_data[piece_index] = piece_data

                            send_message.send_interested_message(s)
                            send_message.wait_for_unchoke_message(s)
                            send_message.send_request_message(index, start, length, s)
                            
                            # Receive the block
                            block_data = s.recv(length)
                            piece_data[start:(start + length)] = block_data

                            if all(piece_data):
                                print(f"Piece {piece_index} downloaded successfully")
                                self.peer_event[peer].clear()
                                piece_index    = None
                                piece_size     = None
                                piece_data     = None

                        send_message.send_keep_alive_message(s)
                else:
                    print(f"Handshake failed with {peer_ip}:{peer_port}")
        except socket.error as e:
            print(f"Failed to connect to {peer_ip}:{peer_port} - {e}")
            self.peerList.remove(peer)

    def create_connection(self):
        with ThreadPoolExecutor(max_workers=len(self.peerList)) as executor:
            futures = [executor.submit(self.connect_to_peer, peer) for peer in self.peerList]

            self.barrier.wait()
            rarest_piece = self.downloader.get_rarest_pieces()

            if rarest_piece is not None:
                request_blocks = self.downloader.download_piece(rarest_piece)
                # Select random 4 peer in having pieces list
                peers_selected = random.sample(self.downloader.having_pieces_list[rarest_piece], min(4, len(self.downloader.having_pieces_list[rarest_piece])))

                for i, block in enumerate(request_blocks):
                    peer = peers_selected[i % len(peers_selected)]
                    self.peer_block_requests[peer].append(block)
                    self.peer_event[peer].set()
                

            for future in futures:
                future.result()