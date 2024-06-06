from __future__ import annotations

import multiprocessing
import math
import os
import typing

import requests

import options

max_8byte_int = int(math.pow(2, 63)) - 1
min_8byte_int = -int(math.pow(2, 63))

def is_valid_amount(amount:bytes) -> bool:
    if 16 < len(amount):
        return False
    try:
        if amount.isdigit():
            return min_8byte_int <= int(amount) <= max_8byte_int
        float(amount)
        return True
    except ValueError:
        return False

def make_csv_path_generator(args:list[str],
                            opts:options.GkgOptions,
                            ) -> typing.Generator:
    if 0 < len(args):
        def nextrow_g() -> typing.Generator:
            for gzfile_path in args:
                yield gzfile_path
    else:
        def nextrow_g() -> typing.Generator:
            with open('/var/log/gdelt/csv_fetch_errors.csv', 'a') as fp:
                def error_out(msg:str):
                    print (msg)
                    print (msg, file=fp)
                for gzfile_path in walk_on_csv_rows(
                    ".gkg.csv.zip", error_out, opts):
                    yield gzfile_path
    return nextrow_g()


# def feed_csv_blobs(ending_key:str,
#                    queue: multiprocessing.Queue[str|None],
#                    opts: options.GkgOptions):
#     for line in walk_on_csv_rows(ending_key, args, opts):
#         queue.put(line)


def walk_on_csv_rows(ending_key:str,
                     msgout:typing.Callable[[str], None],
                     opts: options.GkgOptions) -> typing.Generator:
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
                        msgout(f"ill-formed line: '{line.rstrip()} @ {i}'")
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
                        msgout("Not fetching {url} in dry-run.")
                        continue
                    if not opts.quiet:
                        print (f'Fetching from {url}')
                    with requests.get(url) as r:
                        if r.status_code == 200:
                            with open(gzcsv_path+'.tmp', 'wb') as wfp:
                                wfp.write(r.content)
                            os.rename(gzcsv_path+'.tmp', gzcsv_path)
                        else:
                            msgout(f"Bad HTTP-STATUS '{r.status_code}' from {url}")
                            continue
                else:
                    if opts.verbose:
                        print (f'Loading from {zip_name}')
                    if opts.dry_run:
                        continue

                yield gzcsv_path

        if not opts.quiet:
            print ('pushed all csv files!')
            print ('The last-line seen was: ', last_line)
    except KeyboardInterrupt:
        pass
