import argparse
import bz2
import logging
import os
import pickle
import sys
import math
from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime, timezone
from multiprocessing import Pool

from requests.adapters import HTTPAdapter, Response
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

DEFAULT_METADATA_FILE = 'metadata.pickle.bz2'
OUTPUT_FILE_SUFFIX = '.jsonl.bz2'
INPUT_DATE_FMT = '%Y-%m-%dT%H:%M:%S'
API_URL = 'https://atlas.ripe.net/api/v2/measurements/{msm_id}/results/'
EMPTY_RESULTS_FILE = '{start}--{end}-empty-msm-ids.log'


def parse_timestamp(timestamp: str) -> int:
    """Parse timestamp from input date format (in UTC) to UNIX epoch."""
    try:
        dt = datetime.strptime(timestamp, INPUT_DATE_FMT).replace(tzinfo=timezone.utc)
    except ValueError as e:
        logging.error(f'Invalid timestamp specified : {timestamp}')
        logging.error(e)
        return 0
    return int(dt.timestamp())


def load_metadata(metadata_file: str) -> dict:
    """Load data from file and return the actual metadata part."""
    logging.info(f'Loading metadata from file: {metadata_file}')
    if not os.path.exists(metadata_file):
        logging.error('Metadata file does not exist.')
        return dict()
    try:
        with bz2.open(metadata_file, 'rb') as f:
            data = pickle.load(f)
    except ValueError as e:
        logging.error(f'Failed to load metadata: {e}')
        return dict()
    if 'metadata' not in data:
        logging.error('Malformed metadata file. Expected "metadata" key.')
        return dict()
    metadata = data['metadata']
    logging.info(f'Loaded metadata for {len(metadata):,d} measurements.')
    return metadata


def filter_metadata(metadata: dict, start_time: int, stop_time: int, af: int = None) -> list:
    """Filter the metadata by timeframe and IP version.

    Return a list of measurement ids.
    """
    filtered_msm_ids = list()
    for msm_id, msm_metadata in metadata.items():
        if af and msm_metadata['af'] != af:
            continue
        if (
            # One-off measurements need to be started within the timeframe.
            (msm_metadata['is_oneoff']
                and msm_metadata['start_time'] >= start_time
                and msm_metadata['start_time'] < stop_time)
            or
            # Repeated measurements need to be started before the end of the timeframe
            # and either have not stopped yet, or stop within or after the timeframe.
            (not msm_metadata['is_oneoff']
             and msm_metadata['start_time'] < stop_time
             and (msm_metadata['stop_time'] is None or msm_metadata['stop_time'] > start_time))
        ):
            filtered_msm_ids.append((msm_id, msm_metadata['af']))
    logging.info(f'Filtered {len(filtered_msm_ids):,d}/{len(metadata):,d} measurements.')
    return filtered_msm_ids


def init_session(num_workers: int) -> FuturesSession:
    session = FuturesSession(executor=ProcessPoolExecutor(num_workers))
    retry = Retry(
        backoff_factor=0.1,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry,
                          pool_maxsize=max(num_workers, 10))
    session.mount('https://', adapter)
    return session


# Async function
def store_response(params: tuple) -> str:
    """Generate output file name from parameters and write response to file."""
    msm_id, af, resp, output_dir = params
    output_file = os.path.join(output_dir, str(af), f'{msm_id}{OUTPUT_FILE_SUFFIX}')
    with bz2.open(output_file, 'wt') as f:
        f.write(resp.text)
    return output_file


# Callbacks for async worker.
def store_success_cb(file: str) -> None:
    logging.info(f'Stored {file}')


def store_fail_cb(msg) -> None:
    logging.error(f'Store of some response failed: {msg}')


def process_chunk(chunk: list,
                  interval_start_ts: int,
                  interval_end_ts: int,
                  output_dir: str,
                  parallel_downloads: int,
                  compression_workers: int,
                  force: bool,
                  empty_results: set,
                  empty_results_file: str) -> None:
    """Download and store a chunk of measurement results.

    Parameters:
      chunk: list of (msm_id, af) tuples
      interval_start_ts: start timestamp (as UNIX epoch) to fetch measurement results
      from
      interval_end_ts: end timestamp (as UNIX epoch) to fetch measurement results to
      output_dir: output directory for downloaded results
      parallel_downloads: number of parallel downloads
      compression_workers: number of workers used for parallel data compression and
      storage
      force: overwrite existing files
      empty_results: set of measurement ids that were previously fetched but have no
      results within the specified interval
      empty_results_file: filename to store measurement ids with no results
    """
    session = init_session(parallel_downloads)

    # Start fetching measurement results in parallel.
    queries = list()
    for msm_id, af in chunk:
        # Skip previously fetched measurements with no results within specified
        # interval. Useful in combination with force == False to restart interrupted
        # fetching processes, since we do not generate empty files and would send
        # unnecessary requests each time.
        if msm_id in empty_results:
            continue
        output_file = os.path.join(output_dir, str(af), f'{msm_id}{OUTPUT_FILE_SUFFIX}')
        if os.path.exists(output_file) and not force:
            continue

        # 'txt' format gives us one JSON structure per result, which we can
        # incrementally store/load.
        future = session.get(url=API_URL.format(msm_id=msm_id),
                             params={'start': interval_start_ts,
                                     'stop': interval_end_ts,
                                     'format': 'txt'},
                             timeout=60)
        # Add measurement id and address family to future since we need these later to
        # generate the output file name.
        future.msm_id = msm_id
        future.af = af
        queries.append(future)

    # Compress and store measurement results in parallel.
    with Pool(compression_workers) as p:
        store_results = list()

        # Wait for downloads to complete and begin writing to disk.
        for future in as_completed(queries):
            msm_id = future.msm_id
            try:
                resp: Response = future.result()
            except Exception as e:
                logging.error(f'Request for measurement {msm_id} failed: {e}')
                continue
            if not resp.ok:
                logging.error(f'Request to {resp.url} failed with code {resp.status_code}')
                continue
            if not resp.text:
                logging.info(f'No results for measurement {msm_id}')
                # Write measurement ids without results directly to disk so that we can
                # skip then should we want to restart / resume the process later.
                with open(empty_results_file, 'a') as f:
                    f.write(f'{msm_id}\n')
                continue

            # Add the response to the pool's queue.
            store_params = (future.msm_id, future.af, resp, output_dir)
            store_results.append(p.apply_async(
                                 store_response,
                                 [store_params],
                                 callback=store_success_cb,
                                 error_callback=store_fail_cb))

        # Wait for all writes to finish.
        logging.info('Chunk download complete, waiting for file store to finish.')
        for res in store_results:
            res.wait()
    session.close()


def main() -> None:
    desc = """Fetch measurement results for a specific time interval."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('interval_start')
    parser.add_argument('interval_end')
    parser.add_argument('output_dir')
    parser.add_argument('-af', '--address-family', type=int, choices=[4, 6],
                        help='only fetch results for IPv4/6 measurements')
    parser.add_argument('-m', '--metadata', help='use this metadata file instead of the default')
    parser.add_argument('-n', '--parallel-downloads', type=int, default=4, help='number of parallel downloads')
    parser.add_argument('--compression-workers', type=int,
                        help='number of workers used for parallel compression. Default is same as parallel downloads')
    parser.add_argument('-f', '--force', action='store_true', help='overwrite existing files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('fetch-msm-results-for-interval.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    # Parse timestamps.
    interval_start_ts = parse_timestamp(args.interval_start)
    interval_end_ts = parse_timestamp(args.interval_end)
    if interval_start_ts == 0 or interval_end_ts == 0:
        sys.exit(1)

    # Load metadata.
    metadata_file = DEFAULT_METADATA_FILE
    if args.metadata:
        metadata_file = args.metadata
    metadata = load_metadata(metadata_file)
    if not metadata:
        sys.exit(1)

    # Get ids of measurements active within the specified interval and belonging to the
    # requested address family (if applicable).
    af = args.address_family
    msm_ids = filter_metadata(metadata, interval_start_ts, interval_end_ts, af)

    parallel_downloads = args.parallel_downloads
    logging.info(f'Downloading with {parallel_downloads} threads in parallel.')
    compression_workers = parallel_downloads
    if args.compression_workers:
        compression_workers = args.compression_workers
    logging.info(f'Compressing with {compression_workers} workers in parallel.')

    # Create output directories.
    output_dir = args.output_dir
    if af:
        os.makedirs(os.path.join(output_dir, str(af)), exist_ok=True)
    else:
        os.makedirs(os.path.join(output_dir, '4'), exist_ok=True)
        os.makedirs(os.path.join(output_dir, '6'), exist_ok=True)

    # Read measurement ids that were fetched previously but yield no results within the
    # specified interval.
    start_ts_formatted = datetime.fromtimestamp(interval_start_ts, tz=timezone.utc).strftime(INPUT_DATE_FMT)
    end_ts_formatted = datetime.fromtimestamp(interval_end_ts, tz=timezone.utc).strftime(INPUT_DATE_FMT)
    empty_results_file = EMPTY_RESULTS_FILE.format(start=start_ts_formatted, end=end_ts_formatted)
    empty_results = set()
    if os.path.exists(empty_results_file):
        with open(empty_results_file, 'r') as f:
            empty_results = {int(l.strip()) for l in f}

    # We fetch results in chunks and reset the session in between, since it does not
    # free resources otherwise for some reason, which can fill up the memory in case of
    # large timeframes...
    CHUNK_SIZE = 100
    chunks = math.ceil(len(msm_ids) / CHUNK_SIZE)
    for chunk_idx in range(chunks):
        logging.info(f'Chunk {chunk_idx}')
        chunk = msm_ids[chunk_idx * CHUNK_SIZE: (chunk_idx + 1) * CHUNK_SIZE]
        process_chunk(chunk,
                      interval_start_ts,
                      interval_end_ts,
                      output_dir,
                      parallel_downloads,
                      compression_workers,
                      args.force,
                      empty_results,
                      empty_results_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
