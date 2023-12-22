from __future__ import annotations

import multiprocessing
import math
import os
import typing

import requests

import options

max_8byte_int = int(math.pow(2, 63)) - 1
min_8byte_int = -int(math.pow(2, 63))

def is_valid_amount(amount:str) -> bool:
    if amount.isdigit():
        return (min_8byte_int <= int(amount) <= max_8byte_int)
    return True

def feed_csv_blobs(ending_key:str,
                   queue: multiprocessing.Queue[str|None],
                   args: list[str],
                   opts: options.GkgOptions):
    i:int = 0
    line: str|None = None
    try:
        with open(opts.masterfile, 'r') as fp:
            while 1:
                last_line = line
                line = fp.readline()
                i += 1
                if not line:
                    break
                vec = line.rstrip().split(' ')
                if len(vec) != 3:
                    if not opts.quiet:
                        print (f"ill-formed line: '{line.rstrip()} @ {i}'")
                    continue
                assert len(vec)==3, f"Bad format '{line}'."
                size, hash, url = vec
                if not url.endswith(ending_key):
                    continue
                zip_name = url.rsplit('/', 1)[1]
                timestamp_part = zip_name[:14]
                # print ('L', timestamp_part, opts.lower_limit)
                if timestamp_part < opts.lower_limit_ymdhms:
                    #if opts.verbose:
                    #    print (f'Ignoring {zip_name} because it\'s too old.')
                    continue
                if opts.upper_limit_ymdhms <= timestamp_part:
                    if opts.verbose:
                        print (f'Found {zip_name} which is newer than ' +
                               opts.upper_limit_ymdhms + '. Exiting')
                        break
                    continue
                year = timestamp_part[:4]
                gzcsv_path = f'/opt/gdelt/csv/{year}/{zip_name}'
                if not os.path.exists(gzcsv_path):
                    if opts.dry_run:
                        print ("Not fetching {url} in dry-run.")
                        continue
                    if not opts.quiet:
                        print (f'Fetching from {url}')
                    with requests.get(url) as r:
                        if r.status_code == 200:
                            with open(gzcsv_path+'.tmp', 'wb') as wfp:
                                wfp.write(r.content)
                            os.rename(gzcsv_path+'.tmp', gzcsv_path)
                        else:
                            print (f"Bad HTTP-STATUS '{r.status_code}'")
                            continue
                else:
                    if opts.verbose:
                        print (f'Loading from {zip_name}')
                    if opts.dry_run:
                        continue

                if not opts.no_store:
                    queue.put(gzcsv_path)

        if not opts.quiet:
            print ('pushed all csv files!')
            print ('The last-line seen was: ', last_line)
    except KeyboardInterrupt:
        pass
