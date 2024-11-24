import struct, socket, time

class SendMessageP2P:

    def send_have_message(self, s: socket.socket, piece_index):
        length_prefix = struct.pack('>I', 5)  # 4 bytes for length prefix
        message_id = struct.pack('B', 4)  # 1 byte for message ID (4 for have)
        piece_index = struct.pack('>I', piece_index)  # 4 bytes for piece index
        s.send(length_prefix + message_id + piece_index)
        
    def send_handshake_message(self, s: socket.socket, info_hash: str, peer_id: str):
        pstrlen = struct.pack('B', 19)
        pstr = b'BitTorrent protocol'
        reserved = struct.pack('8B', *[0]*8)
        info_hash = info_hash.encode('utf-8')
        peer_id = peer_id.encode('utf-8')
        s.send(pstrlen + pstr + reserved + info_hash + peer_id)
    
    def send_keep_alive_message(self, s: socket.socket):
        s.send(struct.pack('>I', 0))

    def send_interested_message(self, s: socket.socket):
        length_prefix = struct.pack('>I', 1)
        message_id = struct.pack('B', 2)  # 2 for interested
        s.send(length_prefix + message_id)

    def send_choke_message(self, s: socket.socket):
        """Send a choke message to the peer."""
        length_prefix = struct.pack('>I', 1)
        message_id = struct.pack('B', 0)  # 0 for choke
        s.send(length_prefix + message_id)
    
    def send_unchoke_message(self, s: socket.socket):
        """Send an unchoke message to the peer."""
        length_prefix = struct.pack('>I', 1)
        message_id = struct.pack('B', 1)  # 1 for unchoke
        s.send(length_prefix + message_id)

    def wait_for_unchoke_message(self, s: socket.socket):
        try:
            while True:
                length_prefix = struct.unpack('>I', s.recv(4))[0]
                if length_prefix == 0:
                    continue

                message_id = struct.unpack('B', s.recv(1))[0]
                if message_id == 1:  # 1 for unchoke
                    return True

                s.recv(length_prefix - 1)
        except (socket.error, struct.error) as e:
            # logging.error(f"Error while waiting for unchoke message - {e}")
            return False

    def send_request_message(self, index, begin, length, s: socket.socket):
        length_prefix = struct.pack('>I', 13)  # 4 bytes for length prefix
        message_id = struct.pack('B', 6)  # 1 byte for message ID (6 for request)
        index = struct.pack('>I', index)  # 4 bytes for piece index
        begin = struct.pack('>I', begin)  # 4 bytes for begin offset
        length = struct.pack('>I', length)  # 4 bytes for length
        s.send(length_prefix + message_id + index + begin + length)

    def send_get_peers_list_message(self, s: socket.socket):
        length_prefix = struct.pack('>I', 1)
        message_id = struct.pack('B', 10)
        s.send(length_prefix + message_id)

    def send_server_information(self, s: socket.socket, listen_port):
        length_prefix = struct.pack('>I', 3)
        message_id = struct.pack('B', 9)
        port = struct.pack('>H', listen_port)
        s.send(length_prefix + message_id + port)