import bz2
import ipaddress
import logging
import os
import pickle
from collections import defaultdict

import radix


def addr_in_hop(hop: dict, addr: str) -> bool:
    """Check if addr is in any result of hop."""
    if 'error' in hop or 'result' not in hop:
        return False
    for result in hop['result']:
        if 'from' in result and result['from'] == addr:
            return True
    return False


def prefixes_in_hop(hop: dict, prefixes: set) -> bool:
    """Check if any result of hop contains an address that is in any of the specified
    prefixes.
    """
    if 'error' in hop or 'result' not in hop:
        return False
    for result in hop['result']:
        if 'from' in result:
            reply_addr = ipaddress.ip_address(result['from'])
            for prefix in prefixes:
                if reply_addr in prefix:
                    return True
    return False


def parse_filter_entry(entry: str, asn_to_pfxs: dict):
    """Parse a single filter entry to a set of prefixes.

    If entry is an ASN, an AS-to-prefix(es) mapping is required.
    """
    if entry.isdigit():
        if asn_to_pfxs is None:
            logging.error('Filtering by ASN requires ASN-to-prefixes mapping.')
            return set()
        if entry not in asn_to_pfxs:
            return set()
        return {ipaddress.ip_network(pfx) for pfx in asn_to_pfxs[entry]}
    try:
        return {ipaddress.ip_network(entry)}
    except ValueError as e:
        logging.error(f'Entry does not seem to be an ASN, but is also not a valid IPv4/6 prefix: {e}')
    return set()


def get_monitored_prefixes(filter: str, asn_to_pfxs: dict) -> set:
    """Get monitored prefixes from filter string.

    The filter string can be a single prefix or ASN, or a file containing one prefix or
    ASN per line.
    """
    if os.path.exists(filter):
        logging.debug('Interpreting filter as file')
        ret = set()
        with open(filter, 'r') as f:
            for l in f:
                ret.update(parse_filter_entry(l.strip(), asn_to_pfxs))
        return ret
    return parse_filter_entry(filter, asn_to_pfxs)


def load_asn_to_pfxs(rtree_file: str) -> dict:
    """Create an AS-to-prefix mapping from an rtree.

    rtree nodes are expected to have an 'as' data field containing the AS number.
    """
    logging.info(f'Loading rtree from: {rtree_file}')
    with bz2.open(rtree_file, 'rb') as f:
        rtree: radix.Radix = pickle.load(f)
    asn_to_pfx = defaultdict(set)
    for n in rtree.nodes:
        asn_to_pfx[n.data['as']].add(n.prefix)
    return asn_to_pfx
