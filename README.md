# Inspect network outages with RIPE Atlas

This repository contains a pipeline to easily inspect network outages using traceroute
data from [RIPE Atlas](https://atlas.ripe.net/).
It can be used to analyze one or more IP prefixes, both IPv4 and IPv6.

The basic idea is to infer a set of probe-destination pairs that reliably traverses the
monitored prefix during a reference timeframe, and then analyze the data for the same
pairs during an outage. The detailed methodology and steps are described below the Quick
Start.

This technique is based on the blog posts from Emile Aben. It is recommended to check
these out to get more context about RIPE Atlas and the data this pipeline processes:

- [Does the Internet Route Around Damage? A Case Study Using RIPE
Atlas](https://labs.ripe.net/author/emileaben/does-the-internet-route-around-damage-a-case-study-using-ripe-atlas/)
- [Does The Internet Route Around Damage in
2018?](https://labs.ripe.net/author/emileaben/does-the-internet-route-around-damage-in-2018/)
- [Does The Internet Route Around Damage? - Edition
2021](https://labs.ripe.net/author/emileaben/does-the-internet-route-around-damage-edition-2021/)
- [Does the Internet Route Around Damage? - Edition
2023](https://labs.ripe.net/author/emileaben/does-the-internet-route-around-damage-edition-2023/)

## Setup

Install required dependencies:

```bash
pip install -r requirements.txt
```

If you want to monitor an entire AS, check out
[rib-explorer](https://github.com/m-appel/rib-explorer) to create an appropriate radix
tree, or check out the expected data format below.

## Quick Start

Below is an example sequence of commands required to inspect the [AMS-IX
outage](https://www.ams-ix.net/ams/outage-on-amsterdam-peering-platform) that occurred
in November of 2023.

**Warning:** These commands fetch around 100GB of data and take a long time to complete
by default. There are several parameters related to parallel downloads and processing,
so it is recommended to check out the detailed description of the scripts below.

```bash
# Create data directory
mkdir amsix2023
# Create a file with monitored prefixes
echo "2001:7f8:1::/64" > amsix2023/monitored-prefixes.txt
echo "80.249.208.0/21" >> amsix2023/monitored-prefixes.txt

# Update metadata
python3 update-metadata.py
# NOTE: This metadata is periodically updates in this repository as well

# Fetch reference data from the day before the outage
python3 fetch-msm-results-for-interval.py 2023-11-21T00:00:00 2023-11-22T00:00:00 amsix2023/reference/
# Fetch data that covers the outage
python3 fetch-msm-results-for-interval.py 2023-11-22T00:00:00 2023-11-24T00:00:00 amsix2023/outage/

# Extract candidates
# IPv4
for MSM_RESULT in amsix2023/reference/4/*jsonl.bz2; do
    python3 extract-candidates.py "$MSM_RESULT" amsix2023/candidates/4/ amsix2023/monitored-prefixes.txt
done
# IPv6
for MSM_RESULT in amsix2023/reference/6/*jsonl.bz2; do
    python3 extract-candidates.py "$MSM_RESULT" amsix2023/candidates/6/ amsix2023/monitored-prefixes.txt
done

# Aggregate candidates
# IPv4
python3 aggregate-and-filter-candidates.py amsix2023/candidates/4/ amsix2023/candidates-v4.csv
# IPv6
python3 aggregate-and-filter-candidates.py amsix2023/candidates/6/ amsix2023/candidates-v6.csv

# Filter outage results based on candidates
# IPv4
for MSM_RESULT in amsix2023/outage/4/*jsonl.bz2; do
    python3 filter-msm-results.py "$MSM_RESULT" amsix2023/candidates-v4.csv amsix2023/outage-filtered/4/
done
# IPv6
for MSM_RESULT in amsix2023/outage/6/*jsonl.bz2; do
    python3 filter-msm-results.py "$MSM_RESULT" amsix2023/candidates-v6.csv amsix2023/outage-filtered/6/
done

# Analyze and bin filtered measurement results
# IPv4
python3 bin-msm-results.py amsix2023/outage-filtered/4/ amsix2023/bins-v4.csv amsix2023/monitored-prefixes.txt
# IPv6
python3 bin-msm-results.py amsix2023/outage-filtered/6/ amsix2023/bins-v6.csv amsix2023/monitored-prefixes.txt
```

### Parallelization

All scripts that take time either have a parameter (`-n`) for parallelization built in,
or are working on a single file and can thus be run in parallel externally (e.g., by
using [parallel](https://www.gnu.org/software/parallel/)).

`fetch-msm-results-for-interval.py` accepts a number of parallel *downloads* with `-n`
and a number of parallel *compression workers* with `--compression-workers`.

Scripts that are run in a *for* loop above, should be run with parallel:

```bash
ls amsix2023/reference/4/*.jsonl.bz2 | parallel python3 extract-candidates.py {} amsix2023/candidates/4/ amsix2023/monitored-prefixes.txt
```

## Methodology

The basic idea of this pipeline is simple: Select a set of good candidates (in form of
probe-destination pairs) that reliably traverse the monitored prefix during a reference
timeframe and then analyze their behavior during the outage.

A *candidate* is identified by a probe id and destination IP.
A candidate is selected if *during the reference timeframe*:

1. All traceroutes reached the destination
1. All traceroutes traversed the monitored prefix
1. A sufficient number of traceroutes is available

The reference timeframe should be long enough to increase confidence in the extracted
candidates.

While requirements 1. and 2. are currently hard coded, requirement 3. can be modified by the
`--min-traceroutes` parameter of `aggregate-and-filter-candidates.py`.
By default a reference timeframe of 24h is assumed and one traceroute per hour (i.e., 24 within the
timeframe) is required.

For the analysis of the *outage timeframe* (`bin-msm-results.py`), each traceroute is labelled with
two attributes:

1. Destination reached (true/false)
1. Prefix traversed (true/false)

Finally, the traceroutes and labels are grouped into bins to simplify visualization.
The default bin size is 5 minutes (300s), but can be adjusted with the `--bin-size` parameter.

## Detailed command description

In the following, each command is described in a bit more detail.
They are sorted by order of expected execution.
For even more detail, check out the code.

### `update-metadata.py`

The [REST API](https://atlas.ripe.net/docs/apis/rest-api-reference/#measurements) does
not offer any method to simply fetch all traceroute results for a specific timeframe.
The `metadata.pickle.bz2` file contains the start and stop times (if available) of all
traceroute measurements up to a certain point.
The file structure is a `dict` with the following keys:

```python
{
    'last_id': int,
    'metadata': {
        msm_id: {
            'af': int,          # 4 or 6
            'is_oneoff': bool,
            'start_time': int,  # Unix time
            'stop_time': int    # Can be None
        },
        ...
    }
}
```

The `last_id` field keeps track of the last id contained in the current metadata file to
prevent unnecessary requests.

The `stop_time` can be `None` is some cases, e.g., an ongoing one-off measurement, or an
ongoing recurring measurement with no requested stop time. This script checks if a
system stop time is present (in the `when` field of `status`) and prefers this over the
requested stop time.

Note that it might be useful to re-fetch some of the metadata already contained in the
index to check for updated stop times.
The `--overlap` parameters can be used to update the last measurement metadata.

### `fetch-msm-results-for-interval.py`

This script takes the metadata index (optionally from a custom file with `--metadata`),
extracts the measurements that are active during the specified time frame, and downloads
results for them in parallel

By default results for both IPv4 and IPv6 are fetched, but this can be adjusted with the
`--address-family` parameter.

Results are downloaded in parallel (configured with `--parallel-downloads`) and
stored/compressed in parallel (`--compression-workers`).
Results are compressed as soon as each download is finished, so downloading and
compression happens in parallel as well, something to keep in mind when configuring
these parameters.

Since this process can take a long time, there are some mechanisms to enable fast pickup
after the script was interrupted:

- The script does not fetch data for existing results. This can be overruled with the
  `--force` flag.
- The script automatically creates (and reads) a file keeping track of measurement ids
  that yield no results for the requested timeframe. If the script is restarted, this
  prevents a lot of unnecessary requests. The file name is `{interval_start}--{interval_end}-empty-msm-ids.log`.

In principle, this should enable a seamless restart, however, it might happen that there
are some broken output files that will not get downloaded again (because they exist).
There is a helper script `verify-file.py` to test (and delete) broken files.

### `extract-candidates.py`

This script extracts potential candidates (as described in [Methodology](#methodology))
from a single measurement result file.
It creates both candidates and non-candidates files, which are later merged into one
candidate list.
The non-candidate files are used to get the most conservative estimate. It a candidate
failed in *any* measurement, it will not be included in the final list.

The `filter` argument can either be a single IPv4/6 prefix, an AS number, or a file
containing a list of these.
If an AS number is specified as the filter or in the filter file, a radix tree with a
specific format (see below) must be specified with the `--rtree` parameter.

If `filter` is a file, the file should contain one prefix / AS number per line.

#### AS-to-prefix mapping

The script extracts announced prefixes of an AS from a radix tree as created by
[rib-explorer](https://github.com/m-appel/rib-explorer).
The radix tree is based on [py-radix](https://pypi.org/project/py-radix/) and the script
expects an `as` field in the data component:

```python
asn_to_pfx = defaultdict(set)
for n in rtree.nodes:
    asn_to_pfx[n.data['as']].add(n.prefix)
```

### `aggregate-and-filter-candidates.py`

The per-measurement candidate and non-candidate lists are merged into one final list.
This script sums the number of valid traceroutes per candidate over all result files and
excludes invalid candidates.

Only candidates with a minimum number of traceroutes are included in the final list.
By default this number is 24, but can be adjusted with the `--min-traceroutes` parameter.

### `filter-msm-results.py`

This script extracts the traceroutes that belong to the candidates from a single
measurement file.
It creates a file with the same name as the input file in the output directory.

### `bin-msm-results.py`

Finally, the filtered measurement results need to be analyzed and sorted into bins for
easier visualization.

Each traceroute gets two labels, based on if it reached the destination and if it
traversed the monitored prefix.
In addition, the script calculates the average RTT for traceroutes that reached the
target and had RTT values available.

The columns of the output table are as follows:

- `bin_timestamp`: Beginning of bin (unix time)
- `num_tr`: Total number of traceroutes
- `target_pfx`: Target reached and monitored prefix traversed
- `target_no_pfx`: Target reached but monitored prefix not traversed
- `no_target_pfx`: Target not reached but monitored prefix traversed
- `no_target_no_pfx`: Target not reached and monitored prefix not traversed
- `target_pfx_rtt_count`: Number of traceroutes with RTT values and monitored prefix traversed
- `target_pfx_rtt_avg`: Average RTT of above traceroutes
- `target_no_pfx_rtt_count`: Number of traceroutes with RTT values and monitored prefix not traversed
- `target_no_pfx_rtt_avg`: Average RTT of above traceroute

The default bin size is five minutes (300s), but can be adjusted with the `--bin-size`
parameter.
