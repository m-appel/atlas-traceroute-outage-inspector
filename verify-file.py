import argparse
import bz2
import json
import logging
import os
import sys


def main() -> None:
    desc = """Verify integrity of .json.bz2 files and optionally delete broken files."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('file')
    parser.add_argument('-d', '--delete', action='store_true', help='delete broken files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('verify-file.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    input_file = args.file

    try:
        with bz2.open(input_file, 'rt') as f:
            for l in f:
                json.loads(l)
    except Exception as e:
        logging.info(f'{input_file} is broken.')
        logging.info(e)
        if args.delete:
            os.remove(input_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
