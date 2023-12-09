import argparse
import bz2
import json
import logging
import os
import sys
from collections import defaultdict, namedtuple
from multiprocessing import Pool

from shared_functions import addr_in_hop, get_monitored_prefixes, load_asn_to_pfxs, prefixes_in_hop

INPUT_FILE_SUFFIX = '.jsonl.bz2'
OUTPUT_FILE_SUFFIX = '.csv'
OUTPUT_DATA_DELIMITER = ','
BIN_SIZE_IN_S = 300

# Status attached to each traceroute. Keeps track of:
#   - if the traceroute reached its target
#   - if the traceroute traversed at least one of the monitored prefixes
#   - (if available) the RTT to the destination
Status = namedtuple('Status', 'target_reached prefix_in_path rtt')


def get_result_files(results_dir: str) -> list:
    """Get a list of measurement result files from the specified directory."""
    logging.info(f'Reading results from {results_dir}')
    ret = list()
    for entry in os.scandir(results_dir):
        if not entry.is_file() or not entry.name.endswith(INPUT_FILE_SUFFIX):
            continue
        ret.append(os.path.join(results_dir, entry.name))
    logging.info(f'Found {len(ret)} result files.')
    return ret


def get_avg_hop_rtt(hop: dict) -> float:
    """Calculate the average RTT from hop results.

    Return 0 if no RTT values are present in hop.
    """
    acc = 0
    count = 0
    for result in hop['result']:
        if 'rtt' in result:
            acc += result['rtt']
            count += 1
    if count == 0:
        return 0
    return acc / count


def process_traceroute(tr: dict, monitored_prefixes: set) -> Status:
    """Process a single traceroute and return its status.

    Status indicates
      - if the target was reached
      - if at least one of the monitored prefixes was traversed
      - (if available) the RTT to the target
    """
    dst_addr = tr['dst_addr']
    last_hop = tr['result'][-1]
    target_reached = addr_in_hop(last_hop, dst_addr)
    rtt = 0
    if target_reached:
        rtt = get_avg_hop_rtt(last_hop)
    prefix_in_path = False
    for hop in tr['result']:
        if prefixes_in_hop(hop, monitored_prefixes):
            prefix_in_path = True
            break
    return Status(target_reached, prefix_in_path, rtt)


def process_result_file(params: tuple) -> list:
    """Process an entire measurement results file and return a list of (timestamp,
    Status) tuples where each entry represents one traceroute."""
    result_file, monitored_prefixes = params
    ret = list()
    with bz2.open(result_file, 'rt') as f:
        for line in f:
            try:
                tr = json.loads(line)
            except json.decoder.JSONDecodeError as e:
                logging.error(f'Measurement file is broken: {result_file}: {line.strip()}')
                logging.error(line.strip())
                logging.error(e)
                return list()
            tr_status = process_traceroute(tr, monitored_prefixes)
            ret.append((tr['timestamp'], tr_status))
    return ret


def bin_data(results: list, bin_size: int) -> dict:
    """Sort traceroutes into bins of bin_size and return a dict mapping the bin
    timestamp to a dict of statistics."""
    bins = defaultdict(lambda: {
        'num_tr': 0,                      # Total number of traceroutes
        'target_pfx': 0,                  # Target reached and monitored prefix traversed
        'target_no_pfx': 0,               # Target reached but monitored prefix not traversed
        'no_target_pfx': 0,               # Target not reached but monitored prefix traversed
        'no_target_no_pfx': 0,            # Target not reached and monitored prefix not traversed
        'target_pfx_rtt_count': 0,        # Number of traceroutes with RTT values and monitored prefix traversed
        'target_pfx_rtt_avg': list(),     # RTT values for traceroutes above
        'target_no_pfx_rtt_count': 0,     # Number of traceroutes with RTT values and monitored prefix not traversed
        'target_no_pfx_rtt_avg': list(),  # RTT values for traceroutes above
    })
    # Iterate through lists of results from different measurement result files.
    for l in results:
        # Iterate through traceroutes from one file.
        for ts, tr_status in l:
            bin = ts - (ts % bin_size)
            bins[bin]['num_tr'] += 1
            if tr_status.target_reached:
                rtt = tr_status.rtt
                if tr_status.prefix_in_path:
                    bins[bin]['target_pfx'] += 1
                    if rtt > 0:
                        bins[bin]['target_pfx_rtt_count'] += 1
                        bins[bin]['target_pfx_rtt_avg'].append(rtt)
                else:
                    bins[bin]['target_no_pfx'] += 1
                    if rtt > 0:
                        bins[bin]['target_no_pfx_rtt_count'] += 1
                        bins[bin]['target_no_pfx_rtt_avg'].append(rtt)
            else:
                if tr_status.prefix_in_path:
                    bins[bin]['no_target_pfx'] += 1
                else:
                    bins[bin]['no_target_no_pfx'] += 1
    return bins


def make_consecutive_bins_and_compute_rtt(bins: dict, bin_size: int) -> list:
    """Fill gaps between existing bins with empty bins and convert list of RTT values to
    a single average value.

    Return a list of bins and statistics ordered by bin timestamp.
    """
    first_bin = min(bins)
    last_bin = max(bins)
    curr_bin = first_bin
    output_data = list()
    while curr_bin <= last_bin:
        if curr_bin not in bins:
            # Fill gap with empty bin
            output_data.append((curr_bin, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        else:
            # Calculate average RTT for each class (no target obviously has no RTT).
            target_pfx_rtt_count = bins[curr_bin]['target_pfx_rtt_count']
            target_pfx_rtt_avg = 0
            if target_pfx_rtt_count > 0:
                target_pfx_rtt_avg = sum(bins[curr_bin]['target_pfx_rtt_avg']) / target_pfx_rtt_count

            target_no_pfx_rtt_count = bins[curr_bin]['target_no_pfx_rtt_count']
            target_no_pfx_rtt_avg = 0
            if target_no_pfx_rtt_count > 0:
                target_no_pfx_rtt_avg = sum(bins[curr_bin]['target_no_pfx_rtt_avg']) / target_no_pfx_rtt_count

            output_data.append((curr_bin,
                                bins[curr_bin]['num_tr'],
                                bins[curr_bin]['target_pfx'],
                                bins[curr_bin]['target_no_pfx'],
                                bins[curr_bin]['no_target_pfx'],
                                bins[curr_bin]['no_target_no_pfx'],
                                target_pfx_rtt_count,
                                target_pfx_rtt_avg,
                                target_no_pfx_rtt_count,
                                target_no_pfx_rtt_avg,
                                ))
        curr_bin += bin_size
    return output_data


def main() -> None:
    desc = """Process traceroutes in parallel and sort results into fixed-size bins.
    Output file contains different categories described in the bin_data function."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('results_dir')
    parser.add_argument('output_file')
    parser.add_argument('filter',
                        help='filter for candidates. Can be a single IPv4/6 prefix, ASN, or a file containing a '
                        'mixture of the three.')
    parser.add_argument('--rtree', help='rtree for IP-to-AS mapping')
    parser.add_argument('-b', '--bin-size', type=int, default=BIN_SIZE_IN_S,
                        help=f'bin size in seconds (default: {BIN_SIZE_IN_S})')
    parser.add_argument('-n', '--num-workers', type=int, default=4, help='number of parallel workers')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('bin-msm-results.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    # Get a list of measurement result files.
    msm_result_files = get_result_files(args.results_dir)

    # Load AS-to-prefix mapping (if applicable)
    asn_to_pfxs = None
    if args.rtree:
        asn_to_pfxs = load_asn_to_pfxs(args.rtree)

    # Load monitored prefixes.
    monitored_prefixes = get_monitored_prefixes(args.filter, asn_to_pfxs)

    # Prepare parameters for parallel processing.
    params = [(result_file, monitored_prefixes) for result_file in msm_result_files]

    # Process measurement result files.
    logging.info('Processing measurement result files.')
    with Pool(args.num_workers) as p:
        results = p.map(process_result_file, params)

    # Combine and bin data.
    bin_size = args.bin_size
    logging.info(f'Binning data into {bin_size} second bins.')
    bins = bin_data(results, bin_size)
    output_data = make_consecutive_bins_and_compute_rtt(bins, bin_size)

    # Write bin data to file.
    output_file = args.output_file
    if not output_file.endswith(OUTPUT_FILE_SUFFIX):
        logging.warning(f'Expected output file ending with {OUTPUT_FILE_SUFFIX}')
    with open(output_file, 'w') as f:
        headers = ['bin_timestamp', 'num_tr', 'target_pfx', 'target_no_pfx', 'no_target_pfx', 'no_target_no_pfx',
                   'target_pfx_rtt_count', 'target_pfx_rtt_avg', 'target_no_pfx_rtt_count', 'target_no_pfx_rtt_avg']
        f.write(OUTPUT_DATA_DELIMITER.join(headers) + '\n')
        for entry in output_data:
            f.write(OUTPUT_DATA_DELIMITER.join(map(str, entry)) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
