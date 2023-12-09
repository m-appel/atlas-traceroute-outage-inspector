import argparse
import bz2
import logging
import os
import pickle
import sys
from typing import Tuple

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, JSONDecodeError
from urllib3.util.retry import Retry

DEFAULT_FILE = 'metadata.pickle.bz2'
TMP_FILE = 'metadata.tmp'
API_BASE = 'https://atlas.ripe.net/api/v2/measurements/'


def load_metadata(metadata_file: str) -> Tuple[int, dict]:
    """Load metadata and last included measurement id from file."""
    if not os.path.exists(metadata_file):
        logging.info('Metadata file does not exist. Fetching everything.')
        return 0, dict()

    with bz2.open(metadata_file, 'r') as f:
        data = pickle.load(f)
    last_msm_id = data['last_id']
    metadata = data['metadata']
    return last_msm_id, metadata


def store_metadata(metadata: dict, metadata_file: str) -> None:
    """Update the last_id field and write the metadata to the metadata_file.

    Write the data to a temporary file first, to not end up with a half-overwritten
    broken file in case there is a problem. Then remove the existing file and rename the
    temporary file.
    """
    last_id = max(metadata.keys())
    logging.info(f'Writing to temporary file {TMP_FILE}')
    with bz2.open(TMP_FILE, 'w') as f:
        pickle.dump({'last_id': last_id,
                     'metadata': metadata},
                    f)
    logging.info('Removing existing file and renaming temporary file.')
    if os.path.exists(metadata_file):
        os.remove(metadata_file)
    os.rename(TMP_FILE, metadata_file)


def merge_metadata(metadata: dict, new_metadata: list) -> dict:
    """Merge new metadata into the existing dict and return the updated dict."""
    logging.info(f'Merging {len(new_metadata)} new entries.')
    for entry in new_metadata:
        msm_id = entry['id']
        status = entry.pop('status')
        if (status['id'] != 2 and status['when']
                and (entry['stop_time'] is None or status['when'] < entry['stop_time'])):
            # If the measurement is not running (status id 2) and a system stop time is
            # specified, prefer this over the requested stop time if it is earlier.
            entry['stop_time'] = status['when']
        value = entry.copy()
        value.pop('id')
        value.pop('type')
        if msm_id in metadata and metadata[msm_id] != value:
            # This should not happen during normal operation, since no overlapping ids
            # should be fetched. However, I can imagine a scenario where one would
            # manually decrement the last_id field of the existing dump to update /
            # confirm a chunk of metadata.
            logging.warning(f'Metadata for measurement {msm_id} changed:')
            logging.warning(f'Old: {metadata[msm_id]}')
            logging.warning(f'New: {value}')
        metadata[msm_id] = value
    return metadata


def init_session() -> Session:
    session = Session()
    retry = Retry(
        backoff_factor=0.1,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session


def fetch_url(url: str, session: Session, params: dict = dict()) -> dict:
    """Fetch (JSON) contents from the URL and return decoded data."""
    r = session.get(url, params=params)
    try:
        r.raise_for_status()
    except HTTPError as e:
        logging.error(f'Request {r.url} failed with status {r.status_code}: {e}')
        logging.error(r.text)
        return dict()
    try:
        res = r.json()
    except JSONDecodeError as e:
        logging.error(f'Request {r.url} returned invalid JSON: {e}')
        return dict()
    return res


def main() -> None:
    desc = """Update (or create) the metadata index. The index contains the IP version and
    start/endtime of all traceroute measurements in RIPE Atlas."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-f', '--file', help='use this file instead of default location')
    parser.add_argument('-o', '--overlap', type=int,
                        help='fetch metadata for the last OVERLAP measurements in the index again to check for an '
                             'updated status')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('update-metadata.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Get filename from default parameter, or specified argument.
    metadata_file = DEFAULT_FILE
    if args.file:
        metadata_file = args.file
    logging.info(f'Loading/writing metadata from/to {metadata_file}')

    # Load existing metadata and the last measurement id contained in the metadata.
    last_id, metadata = load_metadata(metadata_file)
    logging.info(last_id)
    overlap = args.overlap
    if overlap:
        msm_ids = list(metadata.keys())
        msm_ids.sort()
        last_id = msm_ids[-overlap]
        logging.info(f'Adjusting last measurement id by -{overlap}')
    logging.info(f'Loaded {len(metadata)} existing entries. Fetching from measurement id {last_id}')

    # Default parameters:
    # - 500 results per request (maximal possible value)
    # - only get metadata for public traceroute measurements
    # - only retrieve IP version, measurement id, one-off flag, start time, stop time,
    #   status
    # - fetch measurements with id greater than last_id
    params = {'page_size': 500,
              'format': 'json',
              'type': 'traceroute',
              'is_public': 'true',
              'fields': 'af,id,is_oneoff,start_time,stop_time,status',
              'id__gt': last_id}

    session = init_session()

    # Fetch first page to get a 'next' URL, then fetch in a loop.
    new_metadata = list()
    res = fetch_url(API_BASE, session, params)
    if not res:
        sys.exit(1)
    new_metadata.extend(res['results'])
    logging.info(f'Loading {res["count"]} results')

    # Fetch remaining pages.
    while res['next']:
        logging.debug(res['next'])
        res = fetch_url(res['next'], session)
        if not res:
            break
        new_metadata.extend(res['results'])

    # Merge new and existing metadata.
    metadata = merge_metadata(metadata, new_metadata)

    # Write metadata.
    logging.info(f'Writing {len(metadata)} entries to {metadata_file}')
    store_metadata(metadata, metadata_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
