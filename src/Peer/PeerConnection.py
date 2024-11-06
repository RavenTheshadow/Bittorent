import socket
import struct
import random
import hashlib
import logging
from threading import Lock, Barrier, Event
from concurrent.futures import ThreadPoolExecutor
from Message.sendMessage import SendMessageP2P
from Download.downloader import Downloader

logging.basicConfig(level=logging.INFO)

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
        self.contributor = {peer: 0 for peer in peerList}
        self.isEnoughPiece = False

    def connect_to_peer(self, peer):
        peer_ip, peer_port = peer
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(60)
                s.connect((peer_ip, peer_port))

                send_message = SendMessageP2P()
                send_message.send_handshake_message(s, self.downloader.info_hash, self.our_peer_id)

                handshake_response = s.recv(68)
                if handshake_response[28:48] != self.downloader.info_hash.encode('utf-8'):
                    logging.error(f"Handshake failed with {peer_ip}:{peer_port}")
                    return

                logging.info(f"Handshake successful with {peer_ip}:{peer_port}")
                self._receive_bitfield(s, peer_ip, peer_port)
                self.barrier.wait()

                while not self.isEnoughPiece:
                    self.peer_event[peer].wait()
                    self._download_block(s, peer)
                
        except (socket.timeout, socket.error) as e:
            logging.error(f"Failed to connect to {peer_ip}:{peer_port} - {e}")
        finally:
            if peer in self.peerList:
                self.peerList.remove(peer)

    def _receive_bitfield(self, s, peer_ip, peer_port):
        try:
            bitfield_length = struct.unpack('>I', s.recv(4))[0]
            bitfield_message_id = struct.unpack('B', s.recv(1))[0]
            if bitfield_message_id == 5:
                bitfield = s.recv(bitfield_length - 1)
                with self.lock:
                    for i, has_piece in enumerate(bitfield):
                        if has_piece and i < self.downloader.pieces_length:
                            peer_info = (peer_ip, peer_port)
                            if peer_info not in self.downloader.having_pieces_list[i]:
                                self.downloader.having_pieces_list[i].append(peer_info)
                logging.info(f"Received bitfield from {peer_ip}:{peer_port}")
        except (struct.error, socket.error) as e:
            logging.error(f"Error processing bitfield from {peer_ip}:{peer_port} - {e}")

    def _download_block(self, s, peer):
        send_message = SendMessageP2P()

        while True:
            self.peer_event[peer].wait()
            if not self.peer_block_requests[peer]:
                continue

            index, start, end = self.peer_block_requests[peer].pop(0)
            
            send_message.send_interested_message(s)
            send_message.wait_for_unchoke_message(s)
            send_message.send_request_message(index, start, end - start, s)

            block_data = s.recv(end - start)
            with self.lock:
                if index not in self.piece_data:
                    piece_size = self.downloader.torrent_info.get_piece_sizes()[index]
                    self.piece_data[index] = bytearray(piece_size)
                self.piece_data[index][start:end] = block_data

            if len(self.piece_data[index]) == self.downloader.torrent_info.get_piece_sizes()[index]:
                piece_hash = hashlib.sha1(self.piece_data[index]).hexdigest()
                expected_hash = self.downloader.torrent_info.get_piece_info_hash(index)
                if piece_hash == expected_hash:
                    self.contributor[peer] += (end - start)
                else:
                    logging.error(f"Piece {index} hash mismatch. Expected {expected_hash}, got {piece_hash}")

            self.peer_event[peer].clear()

    def create_connection(self):
        with ThreadPoolExecutor(max_workers=len(self.peerList)) as executor:
            futures = [executor.submit(self.connect_to_peer, peer) for peer in self.peerList]

            self.barrier.wait()
            self._request_rarest_pieces()

            for future in futures:
                future.result()

    def _request_rarest_pieces(self):
        while True:
            rarest_piece = self.downloader.get_rarest_pieces()
            if rarest_piece is None:
                self.isEnoughPiece = True
                return
            
            torrent_info_hash = self.downloader.torrent_info.get_piece_info_hash(rarest_piece)
            custom = None
            while True:
                request_blocks = self.downloader.download_piece(rarest_piece)
                selected_peers = random.sample(self.downloader.having_pieces_list[rarest_piece], min(4, len(self.downloader.having_pieces_list[rarest_piece])))

                for i, block in enumerate(request_blocks):
                    peer = selected_peers[i % len(selected_peers)]
                    self.peer_block_requests[peer].append(block)
                    self.peer_event[peer].set()
                
                custom = hashlib.sha1(self.piece_data[rarest_piece]).hexdigest()
                if torrent_info_hash == custom:
                    self.downloader.update_pieces(rarest_piece, self.piece_data[rarest_piece], torrent_info_hash)
                    for i, block in enumerate(request_blocks):
                        peer = selected_peers[i % len(selected_peers)]
                        index, start, end = block
                        self.contributor[peer] += end - start

                    del self.piece_data[rarest_piece]
                    break