import json
from Torrent import TorrentInfo
from pathlib import Path

class FileStructure:
    def __init__(self, download_dir="DownloadFolder", info_hash="info_hash_02",
                  pieces_length=10, mapping_file_path="DownloadFolder/mapping_file.json", torrent_info: TorrentInfo = None):
        self.download_dir = Path(download_dir)
        self.info_hash = info_hash
        self.pieces_length = pieces_length
        self.mapping_file_path = Path(mapping_file_path)
        self.bitfield = [0] * self.pieces_length
        self.torrent_info = torrent_info

    def save_bitfield(self, bitfield_path):
        with open(bitfield_path, 'wb') as f:
            f.write(bytes(self.bitfield))

    def save_piece_data(self, piece_path, piece):
        with open(piece_path, 'wb') as f:
            f.write(piece)

    def update_mapping_file(self):
        mapping_file = {self.info_hash: str(self.download_dir / self.info_hash / 'pieces')}

        if self.mapping_file_path.exists():
            with open(self.mapping_file_path, 'r') as f:
                mp = json.load(f)
            mp.update(mapping_file)
        else:
            mp = mapping_file

        with open(self.mapping_file_path, 'w') as f:
            json.dump(mp, f, indent=4)

    def get_info_hash_folder(self):
        info_hash_folder = self.download_dir / self.info_hash

        if not info_hash_folder.exists():
            info_hash_folder.mkdir(parents=True)
            self.save_bitfield(info_hash_folder / 'bitfield')
            (info_hash_folder / 'pieces').mkdir()
            self.update_mapping_file()
            
        elif not (info_hash_folder / 'bitfield').exists():
            if (info_hash_folder / 'pieces').exists():
                for piece in (info_hash_folder / 'pieces').iterdir():
                    piece_info_hash = piece.name.encode('utf-8')
                    index = self.torrent_info.get_piece_index(piece_info_hash)-1
                    self.bitfield[index] = 1
            else:
                (info_hash_folder / 'pieces').mkdir()
            self.save_bitfield(info_hash_folder / 'bitfield')
        else:
            self.bitfield = self.get_bitfield_info(info_hash_folder)

        return info_hash_folder

    def get_bitfield_info(self, info_hash_folder=None):
        if info_hash_folder is None:
            info_hash_folder = self.download_dir / self.info_hash
        else:
            info_hash_folder = Path(info_hash_folder)

        with open(info_hash_folder / 'bitfield', 'rb') as f:
            return list(f.read())

    def get_pieces_folder(self):
        with open(self.mapping_file_path, 'r') as f:
            mp = json.load(f)
            return mp[self.info_hash]
    
    def has_all_pieces(self, torrent_info: TorrentInfo):
        pieces_folder = Path(self.get_pieces_folder()).resolve()
        for piece_index in range(torrent_info.get_number_of_pieces()):
            piece_info_hash = torrent_info.get_piece_info_hash(piece_index).decode('utf-8')
            piece_path = pieces_folder / piece_info_hash
            if not piece_path.exists():
                return False
        return True

    def merge_pieces(self, torrent_info: TorrentInfo):
        try:
            if not self.has_all_pieces(torrent_info):
                print("Not all pieces are available yet.")
                return

            pieces_folder = Path(self.get_pieces_folder()).resolve()
        
            files = torrent_info.files  # Metadata các file từ torrent

            piece_index = 0

            for file_info in files:
                # Tạo đường dẫn file theo metadata
                file_path = Path(*file_info['path'])
                # Tạo thư mục cha của file nếu chưa có
                file_path.parent.mkdir(parents=True, exist_ok=True)

                with open(file_path, 'wb') as f:
                    remaining_length = file_info['length']
    
                    while remaining_length > 0:
                        piece_info_hash = torrent_info.get_piece_info_hash(piece_index).decode('utf-8')
                    
                        piece_path = pieces_folder / piece_info_hash

                        with open(piece_path, 'rb') as piece_file:
                            piece_data = piece_file.read()
                            f.write(piece_data)
                            remaining_length -= len(piece_data)
                        piece_index += 1
                
        except Exception as e:
            pass


if __name__ == "__main__":
    fs = FileStructure()
    print(fs.get_info_hash_folder())