import socket, struct
from threading import Thread, Lock
from Message.sendMessage import SendMessageP2P
from collections import defaultdict
from Download.downloader import Downloader
class PeerNode:
    def __init__(self, peer_ip, peer_port):
        self.peer_ip    = peer_ip
        self.peer_port  = peer_port
        

class P2PConnection:
    def __init__(self, info_hash= "info_hash", our_peer_id="Hello", peerList=[]):
        self.lock = Lock()
        self.info_hash = info_hash
        self.our_peer_id = our_peer_id
        self.peerList = peerList
        self.downloader = Downloader()

    def connect_to_peer(self, peer: PeerNode):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)  # Thiết lập timeout để tránh chờ đợi vô hạn
            s.connect((peer.peer_ip, peer.peer_port))
            
            # Send the handshake message
            SendMessageP2P().send_handshake_message(s, self.info_hash, self.our_peer_id)

            # Receive the handshake message
            handshake_response = s.recv(68)
            if handshake_response[28:48] == self.info_hash.encode('utf-8'):
                print(f"Handshake successful with {peer.peer_ip}:{peer.peer_port}")

                # Receive bitfield message
                bitfield_length_bytes = s.recv(4)
                if len(bitfield_length_bytes) == 4:
                    bitfield_length = struct.unpack('>I', bitfield_length_bytes)[0]

                    bitfield_message_id_bytes = s.recv(1)
                    if len(bitfield_message_id_bytes) == 1:
                        bitfield_message_id = struct.unpack('B', bitfield_message_id_bytes)[0]

                        if bitfield_message_id == 5:  # Message ID cho "bitfield" là 5
                            bitfield = s.recv(bitfield_length - 1)

                            # Cập nhật tài nguyên chia sẻ
                            with self.lock:
                                self.downloader.others_peer_bitfield.append(bitfield)
                                for i, has_piece in enumerate(bitfield):
                                    if has_piece and i < self.downloader.pieces_length:
                                        peer_info = f"{peer.peer_ip}:{peer.peer_port}"
                                    if peer_info not in self.downloader.having_pieces_list[i]:
                                        self.downloader.having_pieces_list[i].append(peer_info)

                        print(f"Received bitfield from {peer.peer_ip}:{peer.peer_port}")

                while True:
                    SendMessageP2P.send_keep_alive_message(s)
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
