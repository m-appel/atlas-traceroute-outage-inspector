import argparse
import bz2
import json
import logging
import os
import sys

INPUT_FILE_SUFFIX = '.jsonl.bz2'
AGGREGATED_CANDIDATES_DATA_DELIMITER = ','


def load_aggregated_candidates(input_file: str) -> set:
    """Load probe/destination candidates from file.

    Return a set of (prb_id, dst_addr) tuples.
    """
    ret = set()
    with open(input_file, 'r') as f:
        f.readline()
        for l in f:
            prb_id, dst_addr, tr_count = l.split(AGGREGATED_CANDIDATES_DATA_DELIMITER)
            ret.add((int(prb_id), dst_addr))
    return ret


def main() -> None:
    desc = """Filter a measurement results file extracting only traceroutes between the
           candidate probe and destination pairs.

           The filtered data is written to a file with the same name, so a separate
           output directory is required.
           """
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('msm_result_file')
    parser.add_argument('candidate_file')
    parser.add_argument('output_dir')
    parser.add_argument('-f', '--force', action='store_true', help='overwrite existing files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('filter-msm-results.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Argument sanity checks
    msm_result_file = args.msm_result_file
    if not msm_result_file.endswith(INPUT_FILE_SUFFIX):
        logging.error(f'Expected measurement results file ending with {INPUT_FILE_SUFFIX}')
        sys.exit(1)

    output_dir = args.output_dir
    output_file = os.path.join(output_dir, os.path.basename(msm_result_file))
    if output_file == msm_result_file:
        logging.error('Output file would overwrite input file.')
        sys.exit(1)

    if os.path.exists(output_file) and not args.force:
        sys.exit(0)

    # Load candidates.
    candidates = load_aggregated_candidates(args.candidate_file)

    # Read and filter measurement results.
    filtered_results = list()
    with bz2.open(msm_result_file, 'rt') as f:
        for line in f:
            try:
                tr = json.loads(line)
            except json.decoder.JSONDecodeError as e:
                logging.error(f'Measurement file is broken: {msm_result_file}: {line.strip()}')
                logging.error(line.strip())
                logging.error(e)
                sys.exit(1)
            if 'dst_addr' not in tr or not tr['dst_addr']:
                continue
            identifier = (tr['prb_id'], tr['dst_addr'])
            if identifier not in candidates:
                continue
            filtered_results.append(tr)

    # Do not create empty files.
    if not filtered_results:
        return

    # Write filtered data.
    os.makedirs(output_dir, exist_ok=True)
    with bz2.open(output_file, 'wt') as f:
        for tr in filtered_results:
            f.write(json.dumps(tr) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
