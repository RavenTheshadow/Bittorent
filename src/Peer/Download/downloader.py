from collections import defaultdict
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from MOCKTorrent import TorrentInfo
from FileStruct.fileStructure import FileStructure



class Downloader:
    def __init__(self, torrent_file_path= r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\SubFolder.torrent',
                       our_peer_id= "127.119.128.1:6681", peerList=[]):
        # Torrent_Info path to torrent file
        self.torrent_info = TorrentInfo(torrent_file_path)

        # Info hash 
        self.info_hash = self.torrent_info.info_hash
        
        # Number of pieces in file torrent
        self.pieces_length = self.torrent_info.get_number_of_pieces()
        
        # List of peer that contain pieces[i]
        self.having_pieces_list = defaultdict(list)

        # PeerID = peer_ip:peer_port
        self.our_peer_id = our_peer_id

        # PeerList
        self.peerList = peerList

        self.file_structure = FileStructure("DownloadFolder",
                                             self.info_hash,
                                             self.pieces_length,
                                             "DownloadFolder/mapping_file.json")
        
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)

        self.piece_data = None

    def download_piece(self, piece_index: int):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]

            block_size = 8 * 1024

            return [(piece_index, offset, min(block_size, piece_size - offset))
                for offset in range(0, piece_size, block_size)]
            
        except:
            return None
        # Explain:
        # if piece_index < self.pieces_length:

        #     piece_size = self.torrent_info.get_piece_sizes()[piece_index]

        #     # Defile block size
        #     block_size = 8 * 1024
        #     request_block = []

        #     for offset in range(0, piece_size, block_size):
        #         block_length = min(block_size, piece_size - offset)
        #         request_block.append(block_length)

        #     self.piece_data = bytearray(piece_size)
        #     return request_block
        # else:
        #     return None

    def get_rarest_pieces(self):
        # Find the rarest pieces
        rarest_pieces = None
        for piece_index, peers in self.having_pieces_list.items():
            if self.bit_field[piece_index] == 0:
                if rarest_pieces is None or len(peers) < len(self.having_pieces_list[rarest_pieces]):
                    rarest_pieces = piece_index
        return rarest_pieces

if __name__== "__main__":
# Test Downloader class functionality
    tester = Downloader()
    # Mock data
    tester.having_pieces_list[0].append(("192.168.1.1", 6681))
    tester.having_pieces_list[1].append(("191.129.12.1", 6682))
    tester.having_pieces_list[1].append(("10.0.2.3", 8000))
    tester.having_pieces_list[2].append(("10.0.2.3", 8000))
    print(tester.get_rarest_pieces())