import argparse
import threading
import os
import dotenv
from pathlib import Path
from prompt_toolkit import PromptSession
from Nodaemon import Nodaemon
from FileStructure import FileStructure
from Torrent import TorrentInfo
from utils import *

def createTorrentFile(args):
  directory = args.directory
  dotenv.load_dotenv()
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
        "name":dirbitectory,
        "files": files
      },
      "announce": os.getenv("TRACKER")
    }
    meta_info_hash = bencodepy.encode(meta_info)
    torrent_file_name = args.output or directory
    with open(f"{torrent_file_name}.torrent","wb") as f:
      f.write(meta_info_hash)
    info_hash = hashlib.sha1(bencodepy.encode(meta_info["info"])).hexdigest()
    fileStructure = FileStructure(
      pieces_length=int(len(pieces)//40),
      info_hash=info_hash,
      torrent_info=TorrentInfo(torrent_file_path=f'{torrent_file_name}.torrent')
    )
    if not os.path.exists("DownloadFolder"):
      os.mkdir("DownloadFolder")
    os.mkdir(f"DownloadFolder/{info_hash}")
    os.mkdir(f"DownloadFolder/{info_hash}/pieces")
    for piece in piece_byte_list:
      with open(f"DownloadFolder/{info_hash}/pieces/{hash_piece(piece)}","wb") as f:
        f.write(piece)
    fileStructure.update_mapping_file()
    fileStructure.get_info_hash_folder()
  else:
    print(f"create: No such a directory: {directory}")
    return
class Cli:
  def __init__(self):
    self.session = PromptSession()
    self.parser = argparse.ArgumentParser(
      prog='Bitorrent',
      description='CLI for interacting with Nodaemon'
    )
    self.lock_event = threading.Event()
    self.daemon = Nodaemon(lock_event=self.lock_event)
    self.daemon_thread = threading.Thread(target=self.daemon.run,daemon=True)
    self.daemon_thread.start()
    self.add_subParsers()
  def add_subParsers(self):
    subParsers = self.parser.add_subparsers(prog="bitorrent")
    # create command
    createParser = subParsers.add_parser(name='create',help='Create torrent file from source directory')
    createParser.add_argument('directory')
    createParser.add_argument('-o','--output',help='Specify name of output torrent file')
    createParser.set_defaults(func=createTorrentFile)
    # download command
    downloadParser = subParsers.add_parser(name='download',help='Download files from a torrent')
    downloadParser.add_argument('torrent_file')
    downloadParser.add_argument('-p','--port',required=True)
    downloadParser.set_defaults(func=self.downloadFiles)
    #upload command
    uploadParser = subParsers.add_parser(name='upload',help='Download files from a torrent')
    uploadParser.add_argument('torrent_file')
    uploadParser.add_argument('-p','--port',required=True)
    uploadParser.set_defaults(func=self.uploadFiles)
    # show command
    showParser = subParsers.add_parser(name='show',help='Shows all torrents')
    showParser.add_argument('-f','--file',help="Show a torrent file meta data")
    showParser.add_argument('-i','--info',help="Show details of the torrent")
    showParser.set_defaults(func=self.showInfo)
  def run(self):
    while True:
      try:
        if not self.lock_event.is_set():
          useInput = self.session.prompt("> ")
          if useInput != '':
            try:
              commands = [command.strip() for command in useInput.split()]
              if commands[0] == 'bitorrent':
                args = self.parser.parse_args(commands[1:])
                args.func(args)
                self.lock_event.set()
              else:
                print(f'bitorrent-cli: command not found: {commands[0]}')
            except SystemExit:
              self.parser.print_usage()
      except (KeyboardInterrupt, EOFError):
        self.daemon.stopEvent.set()
        self.lock_event.set()
        self.daemon_thread.join()
        break
  def showInfo(self,args):
    if args.file and args.info:
      self.parser.print_usage()
      return
    self.daemon.args = args
    self.daemon.showEvent.set()
  def downloadFiles(self,args):
    self.daemon.args = args
    self.daemon.downloadEvent.set()
  def uploadFiles(self,args):
    self.daemon.args = args
    self.daemon.uploadEvent.set()    

if __name__ == "__main__":
  cli = Cli()
  cli.run()