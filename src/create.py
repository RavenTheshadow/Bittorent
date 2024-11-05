import click
from pathlib import Path
import dotenv
from utils import *
import os
import bencodepy
@click.command()
@click.argument("directory")
def create(directory):
  dotenv.load_dotenv()
  if os.path.exists(directory):
    directory_path = Path(directory)
    piece_length = int(os.getenv("PIECE_LENGTH"))
    pieces=""
    files = []
    for file_path in directory_path.rglob('*'):
      if file_path.is_file():
        file_size = os.path.getsize(str(file_path))
        piece_list = split_file_to_pieces(str(file_path),piece_length)
        for piece in piece_list:
          pieces+=hash_piece(piece)
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
    meta_info = bencodepy.encode(meta_info)
    with open(f"{directory}.torrent","wb") as f:
      f.write(meta_info)
  else:
    print(f"create: No such a directory: directory")
    return