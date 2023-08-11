#! /usr/local/bin/python3

from __future__ import annotations

from datetime import timedelta
from functools import partial
from multiprocessing import Queue, Process
import os
from queue import Empty
import random
import re
import requests
from subprocess import PIPE, Popen
import sys
import time
import zipfile

MAX_BATCH_SIZE:int = 32


def do_import(blobs:list[tuple[str,bytes]]) -> None:
    script_dir = os.path.dirname(__file__)
    p = Popen('mongoimport '
              '--collection=eventscsv '
              '--mode=upsert '
              "--writeConcern '{w:1}' "
              '--type=tsv '
              '--columnsHaveTypes '
              '--parseGrace=skipField '
              f'--fieldFile={script_dir}/gdelt_field_file.ff '
              '--uri=mongodb://root:xyz32jml@mongo:27017/gdelt '
              '--authenticationDatabase=admin',
              shell=True,
              bufsize=65536,
              stdin=PIPE,
              stdout=PIPE,
              stderr=PIPE,
              close_fds=True)
#    p = Popen('wc', shell=True, bufsize=65536,
#              stdin=PIPE, close_fds=True)

    last_gzcsv_path: str | None = None
    try:
        assert p.stdin is not None
        for gzcsv_path, blob in blobs:
            last_gzcsv_path = gzcsv_path
            p.stdin.write(blob)
        p.stdin.close()
    except:
        print (f'An error occured while-processing {last_gzcsv_path}')
        assert p.stdout is not None
        print ('stdout:', p.stdout.read())
        assert p.stderr is not None
        print ('stderr:', p.stderr.read())
        p.wait()
        raise

    assert p.stdout is not None
    print ('stdout:', p.stdout.read())
    assert p.stderr is not None
    print ('stderr:', p.stderr.read())
    p.wait()


def importer(queue:Queue[tuple[str, bytes] | None]) -> None:
    exit_flag = False
    while exit_flag is False:
        items:list[tuple[str, bytes]] = []
        while 1:
            try:
                item: tuple[str, bytes] | None = queue.get_nowait()
            except Empty:
                break
            if item is None:
                exit_flag = True
                print ('Exiting...')
                break
            items.append(item)
        if 0 < len(items):
            do_import(items)


def find_nth_item(line, start_pos, nth):
    pos = start_pos
    while 0 < nth:
        pos = line.find(b'\t', pos + 1)
        if pos < 0:
            return (None, None)
        nth -= 1
    next_pos = line.find(b'\t', pos+1)
    return pos+1, next_pos


with open(os.path.join(os.path.dirname(__file__), 'GDELT.ff')) as fp:
    lines = fp.readlines()
GOLDSTEINSCALE_INDEX = [x for x in enumerate(lines)
                        if x[1].startswith('[GoldsteinScale]')][0][0] // 2
def find_bad_lines(blob):
    bad_lines = []
    i = 1
    start_pos = 0
    while 1:
        start, end = find_nth_item(blob, start_pos, GOLDSTEINSCALE_INDEX)
        if start is None:
            break
        if blob[start:end] == b'':
            bad_lines.append(i)
        start_pos = blob.find(b'\n', end+1)
        i += 1
    return bad_lines


def remove_bad_lines(blob, bad_lines):
    lines = [l for l in blob.split(b'\n') if l != b'']
    bad_lines_rev = bad_lines[:]
    bad_lines_rev.reverse()
    good_lines = []
    for index, line in enumerate(lines):
        if 0 < len(bad_lines_rev):
            if bad_lines_rev[-1] == index+1:
                bad_lines_rev.pop()
                continue
        good_lines.append(line)
    return b'\n'.join(good_lines) + b'\n'


def feed_csv_blobs(queue, args, opts) -> None:
    i:int = 0
    line: str|None = None
    try:
        with open(opts.masterfile, 'r') as fp:
            while 1:
                last_line = line
                line = fp.readline()
                if not line:
                    break
                vec = line.rstrip().split(' ')
                if len(vec) != 3:
                    print (f"ill-formed line: '{line.rstrip()}'")
                    continue
                assert len(vec)==3, f"Bad format '{line}'."
                size, hash, url = vec
                if not url.endswith('.export.CSV.zip'):
                    continue
                base_gzname = url.rsplit('/', 1)[1]
                timestamp_part = base_gzname[:14]
                # print ('L', timestamp_part, opts.lower_limit)
                if timestamp_part < opts.lower_limit:
                    # print (f'Ignoring {base_gzname} because it\'s too old.')
                    continue
                # print ('U', opts.upper_limit, timestamp_part)
                if opts.upper_limit <= timestamp_part:
                    # print (f'Ignoring {base_gzname} because it\'s too new.')
                    continue
                base_name = base_gzname[:-4]
                gzcsv_path = os.path.join('/opt/gdelt/csv', base_gzname)
                if not os.path.exists(gzcsv_path):
                    if opts.dry_run:
                        print ("Not fetching {url} in dry-run.")
                        continue
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
                    print (f'Loading from {base_gzname}')
                    if opts.dry_run:
                        continue

                if not opts.no_store:
                    with zipfile.ZipFile(gzcsv_path, 'r') as archive:
                        blob = archive.read(base_name)
#                    bad_lines = find_bad_lines(blob)
#                    print ('Bad lines: ', bad_lines)
#                    if 0 < len(bad_lines):
#                        blob = remove_bad_lines(blob, bad_lines)
                    queue.put((gzcsv_path, blob))
                i += 1
                # if 20 <= i:
                #    break
        print ('last-line', last_line)
        print ('finished all processing!')
    except KeyboardInterrupt:
        pass
    


def main(args, opts) -> None:
    
    queue:Queue[tuple[str,bytes]|None] = Queue(MAX_BATCH_SIZE)

    proc = Process(target=importer, args=(queue,))
    proc.start()

    try:
        feed_csv_blobs(queue, args, opts)
    finally:
        queue.put(None)
        proc.join()


def flatten_time_str(time_in_readable_format:str) -> str:
    pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})')
    m = pattern.match(time_in_readable_format)
    assert m, f"'{time_in_readable_format}' is unparsable."
    return ''.join([m.group(i) for i in range(1,7)])
    
if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-m', '--masterfile', type=str,
                      default='/opt/gdelt/csv/masterfilelist.txt')
    parser.add_option('-l', '--lower-limit', type=str,
                      default='1980-01-01T00-00-00')
    parser.add_option('-u', '--upper-limit', type=str,
                      default='2050-01-01T00-00-00')
    parser.add_option('-d', '--dry-run', default=False, action='store_true')
    parser.add_option('-n', '--no-store', default=False, action='store_true')
    parser.add_option('-v', '--verbose', default=False, action='store_true')

    opts, args = parser.parse_args()
    opts.lower_limit = flatten_time_str(opts.lower_limit)
    opts.upper_limit = flatten_time_str(opts.upper_limit)
    main(args, opts)
