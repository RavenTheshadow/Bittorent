from pathlib import Path
import bencodepy
import hashlib

class TorrentInfo:
    def __init__(self, torrent_file_path):
        self.torrent_file_path = torrent_file_path
        with open(self.torrent_file_path, 'rb') as f:
            torrent_data = bencodepy.decode(f.read())

        info = torrent_data[b'info']
        self.piece_length = info[b'piece_length']
        self.pieces = info[b'pieces']
        self.name = info[b'name'].decode('utf-8')
        self.files = [
            {
                "length": file[b'length'],
                "path": [p.decode('utf-8') for p in file[b'path']]
            }
            for file in info[b'files']
        ] if b'files' in info else [{"length": info[b'length'], "path": [self.name]}]

        self.announce = torrent_data[b'announce'].decode('utf-8')

        # Calculate info_hash
        info_bencoded = bencodepy.encode(info)
        self.info_hash = hashlib.sha1(info_bencoded).hexdigest()

    def get_piece_sizes(self):
        piece_sizes = []
        for file in self.files:
            number_of_pieces = file['length'] // self.piece_length
            piece_sizes.extend([self.piece_length] * number_of_pieces)
            if file['length'] % self.piece_length > 0:
                piece_sizes.append(file['length'] % self.piece_length)
        
        return piece_sizes
    
    def get_piece_info_hash(self, piece_index):
        start = piece_index * 40
        end = start + 40
        return self.pieces[start:end]

    def get_number_of_pieces(self):
        return len(self.pieces) // 40

if __name__ == "__main__":
    # Example usage
    torrent_info = TorrentInfo(r'C:\Users\MyClone\OneDrive\Desktop\SharingFolder\SubFolder.torrent')
    print(f"Info Hash: {torrent_info.info_hash}")
    print(f"Piece Length: {torrent_info.piece_length}")
    print(f"Name: {torrent_info.name}")
    print(f"Files: {torrent_info.files}")
    print(f"Announce: {torrent_info.announce}")
    print(f"Pieces: {torrent_info.pieces}")
    print(f"Get Piece Size: {torrent_info.get_piece_sizes()}")
    print(f"Get Piece info hash: {torrent_info.get_piece_info_hash(0)}")