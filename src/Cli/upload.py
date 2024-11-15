import os
def upload(args,sender):
  torrent_file = args.torrent_file
  if os.path.exists(torrent_file):
    sender.send(f"UPLOAD {torrent_file}")
  else:
    print(f"upload: No such a torrent filev: {torrent_file}")
    return