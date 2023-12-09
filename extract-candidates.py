import argparse
import bz2
import json
import logging
import os
import pickle
import sys
from collections import defaultdict
from typing import Tuple

from shared_functions import addr_in_hop, get_monitored_prefixes, load_asn_to_pfxs, prefixes_in_hop

INPUT_FILE_SUFFIX = '.jsonl.bz2'
NON_CANDIDATE_FILE_SUFFIX = '.non-candidates.pickle.bz2'
CANDIDATE_FILE_SUFFIX = '.candidates.csv'
CANDIDATE_DATA_DELIMITER = ','


def is_candidate(tr: dict, monitored_prefixes: set) -> bool:
    """Check if the traceroute is a candidate.

    A candidate traceroute must:
      - reach its target
      - traverse one of the monitored prefixes
    """
    dst_addr = tr['dst_addr']
    last_hop = tr['result'][-1]
    # Candidates have to always reach their target.
    if not addr_in_hop(last_hop, dst_addr):
        return False
    # Candidates need to traverse one of the monitored prefixes.
    for hop in tr['result']:
        if prefixes_in_hop(hop, monitored_prefixes):
            return True
    return False


def extract_candidates(msm_file: str, monitored_prefixes: set) -> Tuple[dict, set]:
    """Extract candidates (and also non-candidates) in form of (prb_id, dst_addr) tuples from the
    measurement results.

    A candidate must:
      - always reach its target
      - always traverse one of the monitored prefixes
    """
    # Potential candidates with traceroute count.
    candidates = defaultdict(int)
    # Non-candidates that failed to reach the destination or did not pass through the
    # monitored prefixes.
    non_candidates = set()
    with bz2.open(msm_file, 'rt') as f:
        for line in f:
            try:
                tr = json.loads(line)
            except json.decoder.JSONDecodeError as e:
                logging.error(f'Measurement file is broken: {msm_file}: {line.strip()}')
                logging.error(line.strip())
                logging.error(e)
                sys.exit(1)
            if 'dst_addr' not in tr:
                continue
            identifier = (tr['prb_id'], tr['dst_addr'])
            if identifier in non_candidates:
                continue
            if is_candidate(tr, monitored_prefixes):
                candidates[identifier] += 1
                continue
            # Not a candidate.
            non_candidates.add(identifier)
            if identifier in candidates:
                candidates.pop(identifier)
    return candidates, non_candidates


def main() -> None:
    desc = """Extract potential candidates (and non-candidates) from the specified measurement
           results file.
           Candidates are (prb_id, dst_addr) tuples whose traceroutes always reach their
           target and always pass through at least one of the monitored prefixes.
           The 'filter' argument can either contain a single prefix (IPv4 / IPv6) or ASN, or
           reference a file containing a list of prefixes and/or ASNs, one per line.
           If an AS number is specified, an rtree must be provided to build an
           AS-to-prefix mapping.
           """
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('msm_result_file')
    parser.add_argument('output_dir')
    parser.add_argument('filter',
                        help='filter for candidates. Can be a single IPv4/6 prefix, ASN, or a file containing a '
                        'mixture of the three.')
    parser.add_argument('--rtree', help='rtree for AS-to-prefix mapping')
    parser.add_argument('-f', '--force', action='store_true', help='overwrite existing files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('extract-candidates.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.debug(f'Started: {sys.argv}')

    # Argument sanity checks.
    msm_result_file = args.msm_result_file
    if not msm_result_file.endswith(INPUT_FILE_SUFFIX):
        logging.warning(f'Expected measurement result file ending with {INPUT_FILE_SUFFIX}')

    # Generate output file names
    output_dir = args.output_dir
    file_prefix = os.path.basename(msm_result_file)[:-len(INPUT_FILE_SUFFIX)]
    candidate_file_name = f'{file_prefix}{CANDIDATE_FILE_SUFFIX}'
    non_candidate_file_name = f'{file_prefix}{NON_CANDIDATE_FILE_SUFFIX}'
    output_file_candidates = os.path.join(output_dir, candidate_file_name)
    output_file_non_candidates = os.path.join(output_dir, non_candidate_file_name)

    if not args.force and (os.path.exists(output_file_candidates) or os.path.exists(output_file_non_candidates)):
        sys.exit(0)

    # Load AS-to-prefix mapping (if applicable).
    asn_to_pfxs = None
    if args.rtree:
        asn_to_pfxs = load_asn_to_pfxs(args.rtree)

    # Load/parse monitored prefixes.
    monitored_prefixes = get_monitored_prefixes(args.filter, asn_to_pfxs)
    if not monitored_prefixes:
        logging.warning('Invalid prefix specified or no prefixes found for ASN.')
        sys.exit(1)

    logging.debug(f'Filtering for {len(monitored_prefixes)} prefixes:')
    for pfx in monitored_prefixes:
        logging.debug(f'  {pfx}')

    # Extract candidates from measurement results.
    candidates, non_candidates = extract_candidates(args.msm_result_file, monitored_prefixes)
    if not candidates and not non_candidates:
        sys.exit(0)

    # Create output directory and write data to file(s).
    os.makedirs(output_dir, exist_ok=True)
    if candidates:
        with open(output_file_candidates, 'w') as f:
            headers = ['prb_id', 'dst_addr', 'num_tr']
            f.write(CANDIDATE_DATA_DELIMITER.join(headers) + '\n')
            for (prb_id, dst_addr), num_tr in sorted(candidates.items()):
                f.write(CANDIDATE_DATA_DELIMITER.join(map(str, [prb_id, dst_addr, num_tr])) + '\n')
    if non_candidates:
        with bz2.open(output_file_non_candidates, 'w') as f:
            pickle.dump(list(non_candidates), f)


if __name__ == '__main__':
    main()
    sys.exit(0)
