import os
from utils import *
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