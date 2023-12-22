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


def remove_bad_locations(row: list[bytes]) -> None:
    for row_index, expected_count in ((9, 6), (10, 8)):
        ok_locations = (loc_block for loc_block in row[row_index].split(b';')
                        if loc_block.count(b'#') == expected_count)
        row[row_index] = b';'.join(ok_locations)

def first_field_is_sane_int(block:bytes) -> bool:
    amount = block.split(b',', 1)[0]
    return import_util.is_valid_amount(amount)

def remove_insane_ints(row: list[bytes]) -> None:
    ok_blocks = (v21amount_block for v21amount_block in row[24].split(b';')
                 if first_field_is_sane_int(v21amount_block))
    row[24] = b';'.join(ok_blocks)


def do_main(mongo_conn:pymongo.MongoClient,
            csvgz_path: str,
            opts:options.GkgOptions,
            ) -> None:
    with zipfile.ZipFile(csvgz_path, 'r') as archive:
        base_name = os.path.basename(csvgz_path)[:-4] # name without '.zip'
        blob = archive.read(base_name)

    pos, gkg_count, line_count = 0, 0, 0
    while pos < len(blob):
        eol_pos = blob.find(b'\n', pos)
        if eol_pos < 0:
            break
        line_count += 1

        break_flag = False
        while blob[eol_pos-13:eol_pos] != b'</PAGE_TITLE>':
            eol_pos = blob.find(b'\n', eol_pos + 1)
            if eol_pos < 0:
                print(f"Found dangling line. {line_count}@{csvgz_path}:[{line}]")
                break_flag = True
                break
            print(f"Joining line {line_count}@{csvgz_path}")
            line_count += 1
        if break_flag:
            break
        line = blob[pos:eol_pos]
        pos = eol_pos + 1
        src_columns = line.rstrip().split(b'\t')
        if len(src_columns) != 27:
            print (f"Short line {line_count}@{csvgz_path}:[{line}]")
            continue
        remove_bad_locations(src_columns)
        remove_insane_ints(src_columns)
        gkg_son_obj = mongo_conn.gdelt.gkg.find_one(
            {'gkg_record_id': src_columns[0]}
            )
        # print (f"{gkg_son_obj=}")
        try:
            gkg = GKG.deserialize(gkg_son_obj)
        except:
            print ('OFFENDING SON:', gkg_son_obj)
            raise
        dst_columns = gkg.to_csv().split(b'\t')
        column_matches = [(i, src == dst, f"{src}", f"{dst}")
                          for i, (src, dst)
                          in enumerate(zip(src_columns, dst_columns))]
        if all([m[1] == True for m in column_matches]):
            if not opts.quiet:
                print (f"{src_columns[0]} OK")
        else:
            print ("Ouch!")
            print (column_matches)
            print (gkg_son_obj)
            sys.exit(1)

@with_mongo()
def main(mongo_conn: pymongo.MongoClient,
         args: list[str],
         opts: options.GkgOptions
         ) -> None:
    for csv_path in args:
        do_main(mongo_conn, csv_path, opts)

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
    # print (opts)
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
        # args = ['./gdelttools/20230701000000.gkg.csv']
        args = ['/opt/gdelt/csv/20230701000000.gkg.csv.zip']

    main(args, typed_opts)
