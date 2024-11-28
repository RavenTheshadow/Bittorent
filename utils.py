import re
import socket
import hashlib
import bencodepy
import os
import mmap
decimal_match = re.compile('\d')

def get_info_hash(torrent):
    info = read_torrent_file(torrent)["info"]
    hash_info = hashlib.sha1(bencodepy.encode(info)).hexdigest()
    return hash_info
def bdecode(data):
    chunks = list(data)
    chunks.reverse()
    root = _dechunk(chunks)
    return root
def _dechunk(chunks):
    item = chunks.pop()
    if item == 'd': 
        item = chunks.pop()
        hash = {}
        while item != 'e':
            chunks.append(item)
            key = _dechunk(chunks)
            hash[key] = _dechunk(chunks)
            item = chunks.pop()
        return hash
    elif item == 'l':
        item = chunks.pop()
        list = []
        while item != 'e':
            chunks.append(item)
            list.append(_dechunk(chunks))
            item = chunks.pop()
        return list
    elif item == 'i':
        item = chunks.pop()
        num = ''
        while item != 'e':
            num  += item
            item = chunks.pop()
        return int(num)
    elif decimal_match.search(item):
        num = ''
        while decimal_match.search(item):
            num += item
            item = chunks.pop()
        line = ''
        for i in range(int(num)):
            line += chunks.pop()
        return line
    raise "Invalid input!"


def read_torrent_file(torrent_file):
    if os.path.exists(torrent_file):
        with open(torrent_file,'r') as f:  
            meta_info = bdecode(f.read())
            return meta_info
    else:
        print(f"No such a file: {torrent_file}")
        return None
    
def split_file_to_pieces(file_path,piece_length):
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        with open(file_path,'rb') as f:
            mm = mmap.mmap(f.fileno(),length=0,access=mmap.ACCESS_READ,offset=0)
            pieces = [mm[p : p+piece_length] for p in range(0,file_size,piece_length)]
            mm.close()
            return pieces
    else:
        print(f"No such a file: {file_path}")
        return None
    
def reassemble_file(file_path, pieces):
    if not os.path.exists(file_path):
        file_bytes = b''
        for piece in pieces:
            file_bytes+=piece
        with open(file_path,'wb') as f:
            f.write(file_bytes)

    else:
        print(f"File {file_path} has already existed")
        return None
# def write_piece(index,torrent,piece):
#     if os.path.exists(torrent):
#         info_hash = get_info_hash(torrent)
#         if not os.path.exists("pieces"):
#             os.mkdir("pieces")
#         if not os.path.exists(f"pieces/{info_hash}"):
#             os.mkdir(f"pieces/{info_hash}")
#         with open(f"pieces/{info_hash}/{index}",'wb') as f:
#             f.write(piece)
# def read_piece(index,torrent):
#     if os.path.exists(torrent):
#         info_hash = get_info_hash(torrent)
#         if os.path.exists("pieces"):
#             with open(f"pieces/{info_hash}/{index}",'rb') as f:
#                 return f.read()
#     else:
#         "You don't have any piece"
#         return None

def hash_piece(piece):
    return hashlib.sha1(piece).hexdigest()  

def get_host_ip():
  sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
  try:
    sock.connect(('8.8.8.8',1))
    ip = sock.getsockname()[0]
  except:
    ip='127.0.0.1'
  finally:
    sock.close()
  return ip

def readBitFiled(info_hash):
    with open(f'DownloadFolder/{info_hash}/bitfield','rb') as f:
        print(f.read())
if __name__ == "__main__":
  readBitFiled(get_info_hash('hello.torrent'))