import argparse
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from Nodaemon.nodaemon import start_daemon
from prompt_toolkit import PromptSession
from create import create
from show import show
from download import download
from command_list import command_list
from upload import upload
import time

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
