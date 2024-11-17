import argparse
from pathlib import Path
from utils import *
from old_version.Nodaemon import start_daemon
from prompt_toolkit import PromptSession
import dotenv
import os
import time
from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText

def formatted_title(title : str):
  text = []
  for word in title.split():
    text+=[('#ff0066', word),('', ' ')]
  return FormattedText(text)
def formatted_content(title : str):
  text = []
  for word in title.split():
    text+=[('#44ff00', word),('', ' ')]
  return FormattedText(text)
def show(args,sender):
  torrent_file = args.torrent_file
  if os.path.exists(torrent_file):
    meta_info = read_torrent_file(torrent_file)
    print_formatted_text(formatted_title("INFO HASH:"),end="")
    print_formatted_text(formatted_content(f"{get_info_hash(torrent_file)}"))
    print_formatted_text(formatted_title("DIRECTORY NAME:"),end="")
    print_formatted_text(formatted_content(f"{meta_info['info']['name']}"))
    print_formatted_text(formatted_title("TRACKER URL:"),end="")
    print_formatted_text(formatted_content(f"{meta_info['announce']}"))
    print_formatted_text(formatted_title("LIST OF FILES:"))
    files = meta_info["info"]["files"]
    for file in files:
      print_formatted_text(formatted_content(f"* {file['path'][-1]} - {file['length']}B"))
  else:
    print(f"show: No such a file: {torrent_file}")
    return
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
def download(args,sender):
  torrent_file = args.torrent_file
  if os.path.exists(torrent_file):
    sender.send(f"DOWNLOAD {torrent_file}")
  else:
    print(f"download: No such a torrent file: {torrent_file}")
    return
def upload(args,sender):
  torrent_file = args.torrent_file
  if os.path.exists(torrent_file):
    sender.send(f"UPLOAD {torrent_file}")
  else:
    print(f"upload: No such a torrent filev: {torrent_file}")
    return
def command_list(args,sender):
  sender.send("LIST")
def main():
    sender, daemon_process = start_daemon()
    time.sleep(1)
    parser = argparse.ArgumentParser(prog="bitorrent",description="CLI Application with argparse and Prompt Toolkit")
    subparser = parser.add_subparsers()
    #-----------------Create Command-----------------#
    create_command = subparser.add_parser("create",help="create torrent file")
    create_command.add_argument("directory",help="name of directory to create torrent file")
    create_command.add_argument("-o","--output",help="name of output file")
    create_command.set_defaults(func=create)
    #-----------------Download Command-----------------#
    download_command = subparser.add_parser("download",help="Download files from a torrent")
    download_command.add_argument("torrent_file")
    download_command.set_defaults(func=download)
    #-----------------Show Command-----------------#
    show_command = subparser.add_parser("show",help="show .torrent file metadata")
    show_command.add_argument("torrent_file")
    show_command.set_defaults(func=show)
    #-----------------List Command-----------------#
    list_command = subparser.add_parser("list",help="List all torrents")
    list_command.set_defaults(func=command_list)
    #-----------------Upload Command-----------------#
    upload_command = subparser.add_parser("upload",help="Upload files to a torrent")
    upload_command.add_argument("torrent_file")
    upload_command.set_defaults(func=upload)

    session = PromptSession()
    print("Welcome to the interactive CLI. Type 'exit' to quit.")
    while True:
        try:
            user_input = session.prompt("> ")
            try:
                commands = [ command.strip() for command in user_input.split()]
                if commands[0] == "bitorrent":
                  args = parser.parse_args(commands[1:])
                  args.func(args,sender)
                else:
                  parser.print_help()
            except SystemExit:
                parser.print_usage()
        except (KeyboardInterrupt, EOFError):
            sender.send("STOP")
            print("\nExiting...")
            break
    daemon_process.join()


if __name__ == "__main__":
    main()
