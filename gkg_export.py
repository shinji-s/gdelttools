from __future__ import annotations

import datetime
import json
import math
import multiprocessing
import optparse
import os
import pathlib
import sys
import traceback
import typing
import zipfile

import pymongo

import import_util
import options

from mymongo import with_mongo

from gkg import GKG, SourceCollectionID, V1Count, V21Count, V15Tone



def do_main(mongo_conn:pymongo.MongoClient,
            opts:options.GkgOptions) -> None:


    x = mongo_conn.gdelt.gkg.find_one({})
    print (x)
    gkg = GKG.deserialize(x)
    src_columns = open('gdelttools/20230701000000.gkg.csv').readline().strip().split('\t')
    dst_columns = gkg.to_csv().split('\t')

    for i, (src, dst) in enumerate(zip(src_columns, dst_columns)):
        print (i, src == dst, f"[{src}]", f"[{dst}]")


@with_mongo()
def main(mongo_conn: pymongo.MongoClient,
         args: list[str],
         opts: options.GkgOptions
         ) -> None:
    do_main(mongo_conn, opts)

if __name__ == '__main__':
    
    parser = optparse.OptionParser(usage="How to use %p!")
    parser.add_option('-v', '--verbose', action='store_true', default=False)
    parser.add_option('-q', '--quiet', action='store_true', default=False)
    parser.add_option('-w', '--num-workers', type=int, default=2)
    parser.add_option('-m', '--masterfile', type=str,
                      default='/opt/gdelt/csv/masterfilelist.txt')
    parser.add_option('-l', '--lower-limit', type=str,
                      default='1980-01-01T00:00')
    parser.add_option('-u', '--upper-limit', type=str,
                      default='2050-01-01T00:00')
    parser.add_option('-d', '--dry-run', default=False, action='store_true')
    parser.add_option('-n', '--no-store', default=False, action='store_true')

    
    opts, args = parser.parse_args()
    print (opts)
    opts.upper_limit = options.make_ymdhms_string(opts.upper_limit)
    opts.lower_limit = options.make_ymdhms_string(opts.lower_limit)

    typed_opts = options.GkgOptions(
        opts.quiet,
        opts.verbose,
        opts.num_workers,
        opts.masterfile,
        opts.lower_limit,
        opts.upper_limit,
        opts.dry_run,
        opts.no_store,
        )
    if len(args) == 0:
        args = ['/opt/gdelt/csv/20230714133000.gkg.csv']
        
    main(args, typed_opts)
