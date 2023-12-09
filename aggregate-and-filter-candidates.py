import argparse
import bz2
import logging
import os
import pickle
import sys
from collections import defaultdict

MIN_TR = 24
NON_CANDIDATE_FILE_SUFFIX = '.non-candidates.pickle.bz2'
CANDIDATE_FILE_SUFFIX = '.csv'
CANDIDATE_DATA_DELIMITER = ','


def read_candidate_file(candidate_file: str, candidate_dict: dict) -> None:
    with open(candidate_file, 'r') as f:
        f.readline()
        for line in f:
            prb_id, dst_addr, num_tr = line.strip().split(CANDIDATE_DATA_DELIMITER)
            prb_id = int(prb_id)
            num_tr = int(num_tr)
            identifier = (prb_id, dst_addr)
            candidate_dict[identifier] += num_tr


def read_non_candidate_file(non_candidate_file, non_candidate_set: set) -> None:
    with bz2.open(non_candidate_file, 'r') as f:
        new_non_candidates = set(pickle.load(f))
    non_candidate_set.update(new_non_candidates)


def aggregate_candidates(candidate_dir: str) -> dict:
    """Aggregate candidates into a single file.

    Read all candidate (and non-candidate) files from the specified directory and sum
    the number of traceroutes per (prb_id, dst_addr) tuple. Ignore all non-candidates.
    """
    candidates = defaultdict(int)
    non_candidates = set()
    for entry in os.scandir(candidate_dir):
        if not entry.is_file():
            continue
        file = os.path.join(candidate_dir, entry.name)
        if entry.name.endswith(CANDIDATE_FILE_SUFFIX):
            read_candidate_file(file, candidates)
        elif entry.name.endswith(NON_CANDIDATE_FILE_SUFFIX):
            read_non_candidate_file(file, non_candidates)

    # It is possible that a (prb_id, dst_addr) tuple is a candidate based on one
    # measurement, but a non-candidate based on another. Since we want to have the most
    # conservative estimate, remove all non-candidates.
    for non_candidate in non_candidates:
        if non_candidate in candidates:
            candidates.pop(non_candidate)

    return candidates


def main() -> None:
    desc = """Aggregate all candidate (and non-candidate) files into a single list.
    Only include candidates with a minimum number of traceroutes.
    There might be (prb_id, dst_addr) tuples that are candidates based on some
    measurements, but non-candidates based on others. Ignore all non-candidates for the
    most conservative selection."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('candidates_dir')
    parser.add_argument('output_file')
    parser.add_argument('--min-traceroutes', type=int, default=MIN_TR,
                        help=f'minimum number of traceroutes required for each candidate (default: {MIN_TR})')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('aggregate-and-filter-candidates.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    min_tr_threshold = args.min_traceroutes
    logging.debug(f'Requiring {min_tr_threshold} traceroutes for valid candidates.')

    # Read and aggregate candidates.
    candidates = aggregate_candidates(args.candidates_dir)
    # Apply traceroute threshold.
    filtered_candidates = [(*identifier, tr_count)
                           for identifier, tr_count in candidates.items() if tr_count >= min_tr_threshold]

    # Write candidate list to file.
    output_file = args.output_file
    if not output_file.endswith(CANDIDATE_FILE_SUFFIX):
        logging.warning(f'Expecting output file ending with {CANDIDATE_FILE_SUFFIX}.')
    with open(output_file, 'w') as f:
        headers = ['prb_id', 'dst_addr', 'num_tr']
        f.write(CANDIDATE_DATA_DELIMITER.join(headers) + '\n')
        for prb_id, dst_addr, num_tr in sorted(filtered_candidates):
            f.write(CANDIDATE_DATA_DELIMITER.join(map(str, [prb_id, dst_addr, num_tr])) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
