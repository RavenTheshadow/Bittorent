import os
def download(args,sender):
  torrent_file = args.torrent_file
  if os.path.exists(torrent_file):
    sender.send(f"DOWNLOAD {torrent_file}")
  else:
    print(f"download: No such a torrent file: {torrent_file}")
    return
