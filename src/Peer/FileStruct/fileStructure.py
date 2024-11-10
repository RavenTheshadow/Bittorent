import json
from pathlib import Path

class FileStructure:
    def __init__(self, download_dir="DownloadFolder", info_hash="info_hash_02", pieces_length=10, mapping_file_path="DownloadFolder/mapping_file.json"):
        self.download_dir = Path(download_dir)
        self.info_hash = info_hash
        self.pieces_length = pieces_length
        self.mapping_file_path = Path(mapping_file_path)
        self.bitfield = [0] * self.pieces_length

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
    

if __name__ == "__main__":
    fs = FileStructure()
    print(fs.get_info_hash_folder())