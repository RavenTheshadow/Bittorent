import os
import json

class FileStructure:
    def __init__(self, download_dir = "DownloadFolder",
                  info_hash= "info_hash", pieces_length=10, mapping_file_path = "DownloadFolder/mapping_file.json"):
        self.download_dir = download_dir
        self.info_hash = info_hash
        self.pieces_length = pieces_length

        self.mapping_file_path = mapping_file_path

    def create_bitfield(self):
        self.bitfield = [0] * self.pieces_length

    def save_bitfield(self, bitfield_path, bitfield):
        with open(bitfield_path, 'wb') as f:
            f.write(bytes(bitfield))

    def add_infohash_folder_to_mapping_file(self, info_hash, mapping_file_path):
        mapping_file = {
            info_hash: mapping_file_path
        }

        # Read the mapping file
        if os.path.exists(self.mapping_file_path):
            with open(self.mapping_file_path, 'r') as f:
                mp = json.load(f)

            mp.update(mapping_file)

            # Update the mapping file
            with open(self.mapping_file_path, 'w') as f:
                f.write(json.dumps(mp, indent=4))
        else:
            # Create the mapping file
            mp = mapping_file
            with open(self.mapping_file_path, 'w') as f:
                f.write(json.dumps(mp, indent=4))
        
    def get_info_hash_folder(self):
        
        info_hash_folder = os.path.join(self.download_dir, self.info_hash)

        if not os.path.exists(info_hash_folder):
            # Create the folder
            os.makedirs(info_hash_folder)

            # Create the bitfield file
            self.create_bitfield()
            self.save_bitfield(os.path.join(info_hash_folder, 'bitfield'), self.bitfield)

            # Create the pieces folder
            os.makedirs(os.path.join(info_hash_folder, 'pieces'))

            # Add the info_hash folder to the mapping file
            self.add_infohash_folder_to_mapping_file(self.info_hash, os.path.join(info_hash_folder, 'pieces'))

        # else:
        #     # Load the bitfield file
        #     with open(os.path.join(info_hash_folder, 'bitfield'), 'rb') as f:
        #         self.bitfield = list(f.read())
        #         print(self.bitfield)

        return info_hash_folder

    def get_bitfield_info(self, info_hash_folder):
        with open(os.path.join(info_hash_folder, 'bitfield'), 'rb') as f:
            return list(f.read())
        

    def get_pieces_folder(self, info_hash):
        with open(self.mapping_file_path, 'r') as f:
            mp = json.load(f)
            return mp[info_hash]

if __name__ == "__main__":
    fs = FileStructure()
    fs.get_info_hash_folder()