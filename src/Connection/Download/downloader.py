from threading import Lock
from collections import defaultdict
from FileStruct.fileStructure import FileStructure
import os

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
        self.having_pieces_list = defaultdict(list)
             # Danh sách các peer có pieces mà chúng ta chưa có
        # Mock data
        self.having_pieces_list[0].append(f"192.168.1.1:6681")
        self.having_pieces_list[1].append(f"191.111.2.4:6682")
        self.having_pieces_list[2].append(f"192.168.1.1:6681")
        self.having_pieces_list[8].append(f"191.111.2.4:6682")
        self.having_pieces_list[0].append(f"192.168.1.1:6681")


        self.our_peer_id = our_peer_id
        self.peerList = peerList
        self.others_peer_bitfield = []

        self.file_structure = FileStructure("DownloadFolder",
                                             self.info_hash,
                                             self.pieces_length,
                                             "DownloadFolder/mapping_file.json")
        
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)
        

    def download_piece(self, s, piece_index, piece_length):
        pass


    def get_rarest_pieces(self):
        # Find the rarest pieces
        rarest_pieces = None
        for piece in self.having_pieces_list:
            if self.bit_field[piece] == 0:
                if rarest_pieces is None or rarest_pieces['len'] > len(self.having_pieces_list[piece]):
                    rarest_pieces = {
                        'index': piece,
                        'len': len(self.having_pieces_list[piece])
                    }
        if rarest_pieces is not None:
            return rarest_pieces['index']
        else:
            return None

    # def update_bitfield(self, piece_index):
    #     with self.lock:
    #         self.bit_field[piece_index] = 1
    #         self.file_structure.save_bitfield(os.path.join(self.download_dir, 'bitfield'), self.bit_field)

# Test Downloader class functionality
# tester = Downloader(TorrentInfo(), '12345', [PeerNode('192.168.1.1', 6681)])
# tester.update_bitfield(2)
# print(tester.get_rarest_pieces())