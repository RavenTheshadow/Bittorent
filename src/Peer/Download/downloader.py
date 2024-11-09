from collections import defaultdict
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from MOCKTorrent import TorrentInfo
from FileStruct.fileStructure import FileStructure

class Downloader:
    def __init__(self, torrent_file_path, our_peer_id):
        self.torrent_info = TorrentInfo(torrent_file_path)
        self.pieces_length = self.torrent_info.get_number_of_pieces()
        self.having_pieces_list = defaultdict(list)
        self.our_peer_id = our_peer_id
        self.file_structure = FileStructure("DownloadFolder", self.torrent_info.info_hash, self.pieces_length, "DownloadFolder/mapping_file.json")
        self.download_dir = self.file_structure.get_info_hash_folder()
        self.bit_field = self.file_structure.get_bitfield_info(self.download_dir)
        self.piece_data = None

    def download_piece(self, piece_index):
        try:
            piece_size = self.torrent_info.get_piece_sizes()[piece_index]
            
            block_size = 256 * 1024
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

if __name__ == "__main__":
    tester = Downloader(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\hello.torrent', "127.119.128.1:6681")
    for i in range(52):
        tester.update_pieces(i, b"Hello", tester.torrent_info.get_piece_info_hash(i).decode('utf-8'))
        
    tester.having_pieces_list[0].append(("192.168.1.1", 6681))
    tester.having_pieces_list[1].append(("191.129.12.1", 6682))
    tester.having_pieces_list[1].append(("10.0.2.3", 8000))
    tester.having_pieces_list[2].append(("10.0.2.3", 8000))
    print(tester.get_rarest_pieces())