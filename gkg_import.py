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


def as_is(x:typing.Any) -> typing.Any:
    return x


def int_or_float(s:str) -> int|float:
    if s.isdigit():
        return int(s)
    return float(s)

def  convert_to_source_collection_identifier(
    column_value:str
    ) -> SourceCollectionID:
    ivalue = int(column_value)
    if ivalue in typing.get_args(SourceCollectionID):
        return typing.cast(SourceCollectionID, ivalue)
    raise Exception(f'Bad value {ivalue} for a SourceCollectionID')


def double_split(
    first_level_delim:str,
    second_level_delim:str,
    column_value: str,
    converters: typing.Iterable[typing.Callable[[str], typing.Any]] = []
) -> list[typing.Any]:
    
    if column_value.strip() == '':
        return []
    chunks_list = [
        block.split(second_level_delim) for block
        in column_value.strip(first_level_delim).split(first_level_delim)]
    num_chunks = len(chunks_list[0])
    if len(converters) < num_chunks:
        converters = converters + [str] * (num_chunks - len(converters))
    return [[f(x) for f, x in zip(converters, chunks, strict=True)]
            for chunks in chunks_list]


def single_split(delim:str,
                 column_value:str,
                 converters: typing.Iterable[typing.Callable[[str], typing.Any]] = []
                 ) -> list[typing.Any]:
    chunks = column_value.rstrip(delim).split(delim)
    if converters is None:
        return chunks
    if len(converters) < len(chunks):
        converters = converters + [str] * (len(chunks)-len(converters))
    return [f(x) for f, x in zip(converters, chunks, strict=True)]


def float_or_none(s:str) -> float | None:
    if len(s) == 0:
        return None
    return float(s)

def int_or_none(s:str) -> int | None:
    if len(s) == 0:
        return None
    return int(s)

def int_or_str(s:str) -> int | str:
    if s.isdigit():
        return int(s)
    return s
    

def run_non_empty_check(columns:list[str], nonempties:set[int]) -> None:
    for i, col in enumerate(columns):
        if i in nonempties or col.strip()=='':
            continue
        print (f"Column {i} found non-empty for the 1st time. "
               f"{len(nonempties)}/{len(columns)}")
        nonempties.add(i)

max_8byte_int = int(math.pow(2, 63)) - 1
min_8byte_int = -int(math.pow(2, 63))

def filter_insane_int_in_v21amount(v21amounts:list[list[typing.Any]],
                                   csv_gz:str,
                                   line_pos:int):
    for row in v21amounts:
        if not (min_8byte_int <= row[0] <= max_8byte_int):
            print (f"*** Insane int value in '{row}' at {line_pos}@{csv_gz} "
                   " isgnored the entry.", file=sys.stderr)
            break
    else:
        return v21amounts
    return [row for row in v21amounts
            if min_8byte_int <= row[0] <= max_8byte_int]
    

def import_gkg(mongo_conn:pymongo.MongoClient,
               gkg_csv:str,
               columns_found_nonempty:set[int],
               opts:options.GkgOptions) -> None:

    print (f"Processing {gkg_csv}")
    # print (mongo_conn.gdelt.list_collection_names())

    with zipfile.ZipFile(gkg_csv, 'r') as archive:
        base_name = os.path.basename(gkg_csv)[:-4] # name without '.zip'
        blob = str(archive.read(base_name), 'latin')
    pos, gkg_count, line_count = 0, 0, 0
    while pos < len(blob):
        end_pos = blob.find('\n', pos)
        if end_pos < 0:
            break
        line = blob[pos:end_pos]
        pos = end_pos + 1
        line_count += 1
        vec = line.rstrip().split('\t')
        if len(vec) != 27:
            print (f"Short line {line_count}@{gkg_csv}:[{line}]")
            continue
        # print (f"{vec[6]=}")
        # run_non_empty_check(vec, columns_found_nonempty)
        gkg = GKG(
            # gkg-record-id
            vec[0],

            # v1_date
            datetime.datetime.strptime(vec[1], '%Y%m%d%H%M%S'),

            # v2_source_collection_identifier
            convert_to_source_collection_identifier(vec[2]),

            # v2_source_common_name
            vec[3],

            # v2_document_identifier
            vec[4],
            
            # v1_counts
            [V1Count(*args) for args
             in double_split(";", "#", vec[5],
                             (str,   # Count Type
                              int, # Count
                              str,   # Object Type
                              str,   # Location Type
                              str,   # Location FullName
                              str,   # Location CountryCode
                              str,   # Location ADM1Code
                              float_or_none, # Location Latitude
                              float_or_none, # Location Longitude
                              str,   # Location FeatureID
                              ))],
            
            # v21_counts
            [V21Count(*args) for args
             in double_split(";", "#", vec[6],
                             (str,   # Count Type
                              int,   # Count
                              str,   # Object Type
                              int,   # Location Type
                              str,   # Location FullName
                              str,   # Location CountryCode
                              str,   # Location ADM1Code
                              float_or_none, # Location Latitude
                              float_or_none, # Location Longitude
                              int_or_str,   # Location FeatureID
                              int,   # Location Offset in document
                              ))],

            # v1_themes
            double_split(';', ',', vec[7]),

            # v2_enhanced_themes
            double_split(';', '#', vec[8]),

            # v1_locations
            double_split(';', '#', vec[9]),

            # v2_enhanced_locations
            double_split(';', '#', vec[10]),

            # v1_persons
            single_split(';', vec[11]),

            # v2_enhanced_persons
            double_split(';', '#', vec[12]),

            # v1_organizations
            single_split(';', vec[13]),

            # v2_enhanced_organizations
            double_split(';', ',', vec[14]),

            # v1.5_tone
            V15Tone(*single_split(',', vec[15], (float, float, float, float, float, float, int))),

            # v2.1_enhanced_dates
            double_split(';', '#', vec[16], (int, int, int, int, int)),

            # v2_gcam
            {k: int_or_float(v) for (k, v) in double_split(',', ':', vec[17])},

            # v2_sharing_image
            vec[18],

            # v21_related_images
            single_split(';', vec[19]),

            # v21_social_image_embeds
            single_split(';', vec[20]),

            # v21_social_video_embeds
            single_split(';', vec[21]),
            
            # v21_quotations
            double_split('#', '|', vec[22]),
            
            # v21_all_names
            double_split(';', ',', vec[23]),

            # v21_amounts
            filter_insane_int_in_v21amount(
              double_split(';', ',', vec[24], (int_or_float, as_is, int)),
              gkg_csv,
              line_count
            ),

            # v21_translation_info
            double_split(';', ',', vec[25], (int_or_float, as_is)),

            # v2_extras_xml
            vec[26],
        )
        if not opts.no_store:
            try:
                mongo_conn.gdelt.gkg.insert_one(gkg.to_bson())
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print ('Offending GKG:', gkg, file=sys.stderr)
                raise
                
            gkg_count += 1
        # value_list = list(mongo_conn.gdelt.gkg.find({}))
        # gkg = GKG.deserialize(value_list[0])
        # print (value_list[0]['_id'])
    if not opts.quiet:
        print (f"Inserted {gkg_count} gkg objects");
        


@with_mongo()
def importer(mongo_conn:pymongo.MongoClient,
             queue: multiprocessing.Queue[str|None],
             opts:options.GkgOptions
             ) -> None:
    columns_found_nonempty:set[int] = set()
    while True:
        gzfile_path: str | None = queue.get()
        if gzfile_path is None:
            break
        import_gkg(mongo_conn,
                   gzfile_path,
                   columns_found_nonempty,
                   opts)


def main(args:list[str], opts:options.GkgOptions) -> None:
    queue:multiprocessing.Queue[str|None] = (
        multiprocessing.Queue(opts.num_workers * 2))
    workers = []
    for i in range(opts.num_workers):
        w = multiprocessing.Process(target=importer, args=(queue, opts))
        w.start()
        workers.append(w)
    try:
        import_util.feed_csv_blobs(".gkg.csv.zip", queue, args, opts)
    finally:
        for w in workers:
            queue.put(None)
        for w in workers:
            w.join()


if __name__ == '__main__':
    
    parser = optparse.OptionParser(usage="How to use %p!")
    parser.add_option('-v', '--verbose', action='store_true', default=False)
    parser.add_option('-q', '--quiet', action='store_true', default=False)
    parser.add_option('-w', '--num-workers', type=int, default=2)
    parser.add_option('-m', '--masterfile', type=str,
                      default='/opt/gdelt/csv/masterfilelist.txt')
    parser.add_option('-l', '--lower-limit', type=str,
                      default='1980-01-01T00-00-00')
    parser.add_option('-u', '--upper-limit', type=str,
                      default='2050-01-01T00-00-00')
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
