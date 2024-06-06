from __future__ import annotations

import datetime
import json
import math
import multiprocessing
import optparse
import os
import pathlib
import re
import sys
import traceback
import typing
import zipfile

import pymongo

import chunk_splitter
import import_util
import options

from mymongo import with_mongo

from gkg import (GKG, SourceCollectionID, V15Tone, V1Count, V21Amount,
                 V21Count, V2EnhancedTheme, V1Location, V21EnhancedDate,
                 V2EnhancedLocation, V2EnhancedPerson, V2EnhancedOrganization,
                 V2GCAM)


def as_is(x:typing.Any) -> typing.Any:
    return x

def convert_to_source_collection_identifier(
    column_value:bytes
    ) -> SourceCollectionID:
    ivalue = int(column_value)
    if ivalue in typing.get_args(SourceCollectionID):
        return typing.cast(SourceCollectionID, ivalue)
    raise Exception(f'Bad value {ivalue} for a SourceCollectionID')

def write_to_stderr(s:str) -> None:
    print (s, file=sys.stderr)

def double_split(
    first_level_delim:bytes,
    second_level_delim:bytes,
    column_value: bytes,
    converters: tuple[typing.Callable[[bytes], typing.Any],...] = (),
    csvgz_path: str | None = None,
    line_number: int | None = None,
    warn_out: typing.Callable[[str], None] | None = None,
) -> list[typing.Any]:
    
    if column_value.strip() == b'':
        return []
    chunks_list = [
        block.split(second_level_delim) for block
        in column_value.strip(first_level_delim).split(first_level_delim)]
    try:
        f: typing.Callable[[bytes], typing.Any] = bytes
        len_diff = len(chunks_list[0]) - len(converters)
        if 0 < len_diff:
            conververs = converters + (bytes,) * len_diff
        elif len_diff < 0:
            raise ValueError(f"There are more converters than data chunks.")
        L = []
        for chunks in chunks_list:
            if len(chunks) != len(converters):
                if chunks[:4] != [b'0', b'Georgia, , Georgia', b'GG', b'GG']:
                    out = warn_out or write_to_stderr
                    out("The number of converters and elements does not match "
                        f"at {line_number}:{csvgz_path}. Ignoring the offending entry.")
                    out(f"{converters=}")
                    out(f"{chunks=}")
                continue
            try:
                L.append([f(x) for f, x in zip(converters, chunks, strict=True)])
            except ValueError:
                print(f"{converters=}")
                print(f"{chunks=}")
                raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print (f"{converters=}")
        print (f"{chunks_list=}")
        raise
    return L

# def double_split_debug(
#     first_level_delim:bytes,
#     second_level_delim:bytes,
#     column_value: bytes,
#     converters: tuple[typing.Callable[[bytes], typing.Any]] = (bytes,),
# ) -> list[typing.Any]:

#     if column_value.strip() == b'':
#         return []
#     chunks_list = [
#         block.split(second_level_delim) for block
#         in column_value.strip(first_level_delim).split(first_level_delim)]
#     try:
#         num_chunks = len(chunks_list[0])
#         if len(converters) < num_chunks:
#             converters = converters + [bytes] * (num_chunks - len(converters))
#         return [[f(x) for f, x in zip(converters, chunks, strict=True)]
#                 for chunks in chunks_list]
#     except (KeyboardInterrupt, SystemExit):
#         raise
#     except:
#         print (f"{converters=}")
#         print (f"{chunks_list=}")
#         raise


def single_split(
    delim:bytes,
    column_value:bytes,
    converters: tuple[typing.Callable[[bytes], typing.Any],...] = (),
    ) -> list[typing.Any]:
    chunks = column_value.rstrip(delim).split(delim)
    len_diff = len(chunks) - len(converters)
    if 0 < len_diff:
        converters = converters + (as_is,) * len_diff
    try:
        return [f(x) for f, x in zip(converters, chunks, strict=True)]
    except ValueError:
        print(f"{converters=}")
        print(f"{chunks=}")
        raise

URL_DELIMITER = re.compile(b';https?:')
def url_split(column_value:bytes) -> list[bytes]:
    if len(column_value) == 0:
        return []
    pos =0
    out: list[bytes] = []
    while 1:
        m = URL_DELIMITER.search(column_value, pos)
        if not m:
            break
        end = m.span()[0]
        out.append(column_value[pos:end])
        pos = end + 1
    if pos < len(column_value):
        out.append(column_value[pos:])
    return out

def int_float_or_bytes(s: bytes) -> int | float | bytes:
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s

def int_or_float(s:bytes) -> int | float:
    if s.isdigit():
        return int(s)
    if s[0] == ord('-') and s[1:].isdigit():
        return int(s)
    return float(s)

def int_float_or_none(s:bytes) -> float | None:
    if len(s) == 0:
        return None
    if s.isdigit():
        return int(s)
    if s[0] == ord('-') and s[1:].isdigit():
        return int(s)
    return float(s)

def int_float_or_none_debug(s:bytes) -> float | None:
    if len(s) == 0:
        return None
    if s.isdigit():
        return int(s)
    if s[0] == ord('-') and s[1:].isdigit():
        return int(s)
    return float(s)


def float_or_none(s:bytes) -> float | None:
    if len(s) == 0:
        return None
    return float(s)


def int_or_none(s:bytes) -> int | None:
    if len(s) == 0:
        return None
    return int(s)

def int_or_str(s:bytes) -> int | bytes:
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

def filter_invalid_amount_in_v21amount(v21amounts:list[list[typing.Any]],
                                       csv_gz:str,
                                       line_pos:int,
                                       warn_out: typing.Any
                                       ) -> list[list[typing.Any]]:
    out: list[list[typing.Any]] = []
    for row in v21amounts:
        if not import_util.is_valid_amount(row[0]):
            msg = f"*** Insane int value in '{row}' at {line_pos}@{csv_gz} " \
                   " ignored the entry."
            if opts.bailout_on_exception:
                raise Exception(msg)
            print (msg, file=sys.stderr)
            warn_out(msg)
        else:
            out.append(row)
    return out

max_8byte_int = int(math.pow(2, 63)) - 1
min_8byte_int = -int(math.pow(2, 63))

def convert_amount_in_v21amount_to_numeric_if_possible(
    v21amounts:list[list[typing.Any]],
    csv_gz:str,
    line_pos:int,
    warn_out: typing.Any) -> list[list[typing.Any]]:
    out: list[list[typing.Any]] = []
    for row in v21amounts:
        try:
            ival = int(row[0])
            if min_8byte_int <= ival <= max_8byte_int:
                out.append([ival] + row[1:])
            else:
                # store it as it is, bytes.
                out.append(row)
            continue
        except ValueError:
            pass
        try:
            out.append([float(row[0])] + row[1:])
            continue
        except ValueError:
            pass
        out.append(row)
    return out

def is_valid_v1_location_args(args, warn_out):
    if len(args) != 7:
        msg = f"Bad V1Location args: ({args})"
        print (msg)
        warn_out(msg)
        return False
    return True


def makeGKGfromColumns(vec: list[bytes],
                       gkg_csv: str,
                       line_count: int,
                       warn_out: typing.Any,
                       ) -> GKG:
    gkg = GKG(
        # gkg-record-id
        vec[0],

        # v1_date
        datetime.datetime.strptime(str(vec[1], 'utf-8'), '%Y%m%d%H%M%S'),

        # v2_source_collection_identifier
        convert_to_source_collection_identifier(vec[2]),

        # v2_source_common_name
        vec[3],

        # v2_document_identifier
        vec[4],

        # v1_counts
        [V1Count(*args) for args
         in double_split(b";", b"#", vec[5],
                         (bytes,   # Count Type
                          # 'Count' can't be 'int' due to '081'
                          # in '20230701000000-562'
                          bytes,   # Count
                          bytes,   # Object Type
                          bytes,   # Location Type
                          bytes,   # Location FullName
                          bytes,   # Location CountryCode
                          bytes,   # Location ADM1Code
                          int_float_or_none, # Location Latitude
                          int_float_or_none, # Location Longitude
                          bytes,   # Location FeatureID
                          ))],

        # v21_counts
        [V21Count(*args) for args
         in double_split(b";", b"#", vec[6],
                         (bytes,   # Count Type
                          # 'Count' can't be 'int' due to '081'
                          # in '20230701000000-562'
                          bytes,   # Count
                          bytes,   # Object Type
                          int,   # Location Type
                          bytes,   # Location FullName
                          bytes,   # Location CountryCode
                          bytes,   # Location ADM1Code
                          int_float_or_none, # Location Latitude
                          int_float_or_none, # Location Longitude
                          int_or_str,   # Location FeatureID
                          int,   # Location Offset in document
                          ))],

        # v1_themes
        double_split(b';', b',', vec[7], (bytes,)),

        # v2_enhanced_themes
        [V2EnhancedTheme(*args) for args
         in double_split(b';', b',', vec[8], (bytes, int))],

        # v1_locations
        [V1Location(*args) for args
         in double_split(b';', b'#', vec[9],
                         (int, bytes, bytes, bytes,
                          int_float_or_none, int_float_or_none,
                          int_float_or_bytes),
                         csvgz_path=gkg_csv,
                         line_number=line_count,
                         warn_out=warn_out)
         if is_valid_v1_location_args(args, warn_out)],

        # v2_enhanced_locations
        [V2EnhancedLocation(*args) for args
        in double_split(b';', b'#', vec[10],
                        (int, bytes, bytes, bytes, bytes,
                         float_or_none, float_or_none, int_float_or_bytes, int),
                        csvgz_path=gkg_csv,
                        line_number=line_count,
                        warn_out=warn_out)],

        # v1_persons
        single_split(b';', vec[11], (as_is,)),

        # v2_enhanced_persons
        [V2EnhancedPerson(*args) for args
        in double_split(b';', b',', vec[12], (bytes, int))],

        # v1_organizations
        single_split(b';', vec[13], (as_is,)),

        # v2_enhanced_organizations
        [V2EnhancedOrganization(*args) for args
        in double_split(b';', b',', vec[14], (bytes, int))],

        # v1.5_tone
        V15Tone(*single_split(b',', vec[15],
                              (int_or_float, int_or_float, int_or_float,
                               int_or_float, int_or_float, int_or_float,
                               int))),

        # v2.1_enhanced_dates
        [V21EnhancedDate(*args) for args
         in double_split(b';', b'#', vec[16],
                         (int, bytes, bytes, bytes, int))],

        # v2_gcam
        [V2GCAM(*args) for args
         in double_split(b',', b':', vec[17],
                         (bytes, int_or_float))],

        # v2_sharing_image
        vec[18],

        # v21_related_images
        url_split(vec[19]),

        # v21_social_image_embeds
        url_split(vec[20]),

        # v21_social_video_embeds
        url_split(vec[21]),

        # v21_quotations
        double_split(b'#', b'|', vec[22], (int, int, bytes, bytes)),

        # v21_all_names
        double_split(b';', b',', vec[23], (bytes, int)),

        # v21_amounts
        # The first converter is 'bytes' to preserve '00000' in '00000488'
        [V21Amount(*args) for args
         in convert_amount_in_v21amount_to_numeric_if_possible(
              double_split(b';', b',', vec[24], (as_is, as_is, int)),
              gkg_csv,
              line_count,
              warn_out
              )],

        # v21_translation_info
        double_split(b';', b',', vec[25], (int_or_float, as_is)),

        # v2_extras_xml
        vec[26],
    )
    return gkg


@with_mongo()
def import_gkg(mongo_conn:pymongo.MongoClient,
               gkg_csv:str,
               columns_found_nonempty:set[int],
               opts:options.GkgOptions,
               warn_out: typing.Any,
               ) -> None:
    request_file = '/tmp/gkg_show_progress'
    do_reporting = ((request_file_exists := os.path.exists(request_file))
                    or not opts.quiet)
    if do_reporting:
        print (f"Processing {gkg_csv}...")
    # print (mongo_conn.gdelt.list_collection_names())
    try:
        with zipfile.ZipFile(gkg_csv, 'r') as archive:
            base_name = os.path.basename(gkg_csv)[:-4] # name without '.zip'
            blob = archive.read(base_name)
    except zipfile.BadZipFile:
        print (f"Corrupt zip file? [{gkg_csv}]")
        warn_out(f"Corrupt zip file? [{gkg_csv}]")
        return
    pos, gkg_count, line_count = 0, 0, 0
    for line in chunk_splitter.split_to_chunks(blob):
        if 16 * 1024 * 1024 <= len(line):
            warn_out('Line too long: ' + str(line[:32]) + '...')
            continue
        line_count += line.count(b'\n') + 1
        vec = line.split(b'\t')
        if len(vec) != 27:
            warn_out(f"Short line {line_count}@{gkg_csv}:[{line!r}]")
            continue
        # run_non_empty_check(vec, columns_found_nonempty)
        #if vec[0] != b'20230701000000-66':
        #    continue
        # print (f"{vec[0]=} {vec[9]=}")
        try:
            x = mongo_conn.gdelt.gkg.find_one({'gkg_record_id':vec[0]})
            if x is None:
                gkg = makeGKGfromColumns(vec, gkg_csv, line_count, warn_out)
            else:
                gkg = None
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print (f"Offending row:{vec[0]!r}")
            raise
        if gkg is not None and not opts.no_store:
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
    if do_reporting:
        print (f"Inserted {gkg_count} gkg objects");


def logger(logging_queue: multiprocessing.Queue[tuple[str,str]|None],
           opts:options.GkgOptions):
    fp_cache: dict[str, typing.IO[str]] = {}
    while True:
        t = logging_queue.get()
        if t is None:
            break
        year_month_part, msg = t
        fp = fp_cache.get(year_month_part)
        if fp is None:
            if 16 <= len(fp_cache):
                first_key = next(iter(fp_cache))
                fp_cache.pop(first_key).close()
            fp = open(f"/var/log/gdelt/{year_month_part}_warns.log", 'a')
            fp_cache[year_month_part] = fp
        print (msg, file=fp)
    for year_month_part, fp in fp_cache.items():
        fp.close()


def importer(queue: multiprocessing.Queue[str|None],
             logging_queue: multiprocessing.Queue[tuple[str,str]|None],
             opts:options.GkgOptions
             ) -> None:
    columns_found_nonempty:set[int] = set()
    while True:
        gzfile_path: str | None = queue.get()
        if gzfile_path is None:
            break
        year_month_part = os.path.basename(gzfile_path)[:6]
        def warn_out(msg:str):
            logging_queue.put((year_month_part, msg))
        import_gkg(gzfile_path,
                   columns_found_nonempty,
                   opts,
                   warn_out)


def main(nextrow_g:typing.Generator, opts:options.GkgOptions) -> None:
    queue:multiprocessing.Queue[str|None] = (
        multiprocessing.Queue(opts.num_workers * 2))
    logging_queue:multiprocessing.Queue[tuple[str,str]|None] = (
        multiprocessing.Queue(1))
    logger_ = multiprocessing.Process(target=logger,
                                      args=(logging_queue, opts))
    logger_.start()
    workers = []
    for i in range(opts.num_workers):
        w = multiprocessing.Process(target=importer,
                                    args=(queue, logging_queue, opts))
        w.start()
        workers.append(w)
    try:
        for csv_gz_path in nextrow_g:
            queue.put(csv_gz_path)
    finally:
        print ("Requesting workers to quit...")
        logging_queue.put(None)
        for w in workers:
            queue.put(None)
        logger_.join()
        for w in workers:
            w.join()


def make_csv_storage_dir(opts:options.GkgOptions) -> None:
    start_year = int(opts.lower_limit[:4])
    end_year = int(opts.upper_limit[:4])
    for y in range(start_year, end_year + 1):
        dirpath = f'/opt/gdelt/csv/{y}'
        if not os.path.exists(dirpath):
            os.mkdir(dirpath)


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
    parser.add_option('-x', '--bailout-on-exception', default=False,
                      action='store_true')
    
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

    make_csv_storage_dir(opts)

    main(import_util.make_csv_path_generator(args, typed_opts),
         typed_opts)

