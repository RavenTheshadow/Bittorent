import hashlib
from pathlib import Path
import dotenv
from utils import *
import os
import bencodepy

def create(args,sender):
  dotenv.load_dotenv("../.env")
  directory = args.directory
  directory_path = Path(directory)
  if os.path.exists(directory):
    piece_length = int(os.getenv("PIECE_LENGTH"))
    pieces=""
    piece_byte_list = []
    files = []
    for file_path in directory_path.rglob('*'):
      if file_path.is_file():
        file_size = os.path.getsize(str(file_path))
        piece_list = split_file_to_pieces(str(file_path),piece_length)
        for piece in piece_list:
          pieces+=hash_piece(piece)
          piece_byte_list+=[piece]
        files.append({
          "length": file_size,
          "path": str(file_path).split("/")
        })
    meta_info = {
      "info": {
        "piece_length": piece_length,
        "pieces":pieces,
        "name":directory,
        "files": files
      },
      "announce": os.getenv("TRACKER")
    }
    meta_info_hash = bencodepy.encode(meta_info)
    with open(f"{directory}.torrent","wb") as f:
      f.write(meta_info_hash)
    info_hash = hashlib.sha1(bencodepy.encode(meta_info["info"])).hexdigest()
    if not os.path.exists(".torrent"):
      os.mkdir(".torrent")
    os.mkdir(f".torrent/{info_hash}")
    os.mkdir(f".torrent/{info_hash}/pieces")
    for piece in piece_byte_list:
      with open(f".torrent/{info_hash}/pieces/{hash_piece(piece)}","wb") as f:
        f.write(piece)
  else:
    print(f"create: No such a directory: {directory}")
    return