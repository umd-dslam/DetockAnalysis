import argparse
import os
import re
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["untar", "rm"], help="Action to perform on each tar file")
parser.add_argument("path", help="Recursively perform the action to all files under this path")
parser.add_argument("-e", default="tar.gz", help="Extension of tar files")
parser.add_argument("-f", nargs="*", help="List of file names used in \"rm\" action")


if __name__ == '__main__':
    args = parser.parse_args()
    
    pat = re.compile(f'^([\w,\s-]+)\.{re.escape(args.e)}$')

    for root, dirs, files in os.walk(args.path):
        for f in files:
            match = pat.fullmatch(f)
            if match is not None:
                name = match.group(1)

                src = os.path.join(root, f)
                dest = os.path.join(root, name)

                if args.action == "untar":
                    os.makedirs(dest, exist_ok=True)
                    tar = tarfile.open(src)
                    tar.extractall(dest)
                    tar.close()

                    print(f'{src}: extracted to {dest}')
                elif args.action == "rm":
                    assert args.f, "A list of file names must be provided with -f"
                    old = tarfile.open(src)
                    found = False
                    for item in old.getmembers():
                        if item.name in args.f:
                            found = True
                            break
                    if found:
                        tmp = os.path.join(root, f"{f}.tmp")
                        new = tarfile.open(tmp, 'w:gz')
                        for item in old.getmembers():
                            if item.name in args.f:
                                print(f'Removed {item.name} from {src}')
                                continue
                            extracted = old.extractfile(item)
                            if extracted:
                                new.addfile(item, extracted)
                        old.close()
                        new.close()
                        os.rename(tmp, src)
                    else:
                        old.close()

