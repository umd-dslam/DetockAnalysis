import argparse
import os
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument("path", help="Recursively untar all files under this path")
parser.add_argument("-e", default="tar.gz", help="Extension of tar files")
args = parser.parse_args()


if __name__ == '__main__':
    for root, dirs, files in os.walk(args.path):
        for f in files:
            if f.endswith(args.e):
                name, ext = f[:-len(args.e)-1], f[-len(args.e):]

                src = os.path.join(root, f)
                dest = os.path.join(root, name)

                os.makedirs(dest, exist_ok=True)
                tar = tarfile.open(src)
                tar.extractall(dest)

                print(f'{src}: extracted to {dest}')
