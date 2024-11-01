import click
from create import create

@click.group()
def bitorrent():
  pass

bitorrent.add_command(create)
if __name__ == "__main__":
  bitorrent()