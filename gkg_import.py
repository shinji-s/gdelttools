from __future__ import annotations

import datetime
import json
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

from gkg import (GKG, SourceCollectionID, V15Tone, V1Count, V21Amount,
                 V21Count, V2EnhancedTheme, V1Location, V21EnhancedDate)


def as_is(x:typing.Any) -> typing.Any:
    return x

def  convert_to_source_collection_identifier(
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
    converters: typing.Iterable[typing.Callable[[str], typing.Any]] = (),
    csvgz_path: str | None = None,
    line_number: int | None = None,
    warn_out: typing.Callable[[str], None] | None = None,
) -> list[typing.Any]:
    
    if column_value.strip() == b'':
        return []
    chunks_list = [
        block.split(second_level_delim) for block
        in column_value.strip(first_level_delim).split(first_level_delim)]
    num_chunks = len(chunks_list[0])
    try:
        if len(converters) < num_chunks:
            converters = converters + (bytes,) * (num_chunks - len(converters))
        L = []
        for chunks in chunks_list:
            if len(chunks) != len(converters):
                if line_number is None or csvgz_path is None:
                    print (f"{converters=}", file=sys.stderr)
                    print (f"{chunks=}", file=sys.stderr)
                    raise Exception(
                        "The number of converters and elements does not match!\n")
                out = warn_out or write_to_stderr
                out("The number of converters and elements does not match "
                    f"at {line_number}:{csvgz_path}")
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

def double_split_debug(
    first_level_delim:bytes,
    second_level_delim:bytes,
    column_value: bytes,
    converters: typing.Iterable[typing.Callable[[str], typing.Any]] = []
) -> list[typing.Any]:

    if column_value.strip() == b'':
        return []
    chunks_list = [
        block.split(second_level_delim) for block
        in column_value.strip(first_level_delim).split(first_level_delim)]
    try:
        num_chunks = len(chunks_list[0])
        if len(converters) < num_chunk:
            converters = converters + [bytes] * (num_chunks - len(converters))
        return [[f(x) for f, x in zip(converters, chunks, strict=True)]
                for chunks in chunks_list]
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print (f"{converters=}")
        print (f"{chunks_list=}")
        raise


def single_split(delim:bytes,
                 column_value:bytes,
                 converters: typing.Iterable[typing.Callable[[bytes], typing.Any]] = []
                 ) -> list[typing.Any]:
    chunks = column_value.rstrip(delim).split(delim)
    if converters is None:
        return chunks
    if len(converters) < len(chunks):
        converters = converters + [bytes] * (len(chunks)-len(converters))
    return [f(x) for f, x in zip(converters, chunks, strict=True)]


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

def int_or_float(s:bytes) -> int|float:
    if s.isdigit():
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

def int_or_str(s:bytes) -> int | str:
    if s.isdigit():
        return int(s)
    return s
    
def int_or_float(s:bytes) -> int | float:
    if s.isdigit():
        return int(s)
    if s[0] == ord('-') and s[1:].isdigit():
        return int(s)
    return float(s)

def run_non_empty_check(columns:list[str], nonempties:set[int]) -> None:
    for i, col in enumerate(columns):
        if i in nonempties or col.strip()=='':
            continue
        print (f"Column {i} found non-empty for the 1st time. "
               f"{len(nonempties)}/{len(columns)}")
        nonempties.add(i)

def filter_insane_int_in_v21amount(v21amounts:list[list[typing.Any]],
                                   csv_gz:str,
                                   line_pos:int,
                                   warn_out: typing.Any):
    for row in v21amounts:
        if not import_util.is_valid_amount(row[0]):
            msg = f"*** Insane int value in '{row}' at {line_pos}@{csv_gz} "
                   " ignored the entry."
            print (msg, file=sys.stderr)
            warn_out(msg)
            break
    else:
        return v21amounts
    return [row for row in v21amounts if is_valid_amount(row[0])]


def is_valid_v1_location_args(args, warn_out):
    if len(args) != 7:
        msg = f"Bad V1Location args: ({args})"
        print (msg)
        warn_out(msg)
        return False
    return True


def makeGKGfromColumns(vec: list[bytes],
                       gkg_csv: string,
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
        double_split(b';', b',', vec[7]),

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
        double_split(b';', b'#', vec[10],
                     csvgz_path=gkg_csv,
                     line_number=line_count,
                     warn_out=warn_out),

        # v1_persons
        single_split(b';', vec[11]),

        # v2_enhanced_persons
        double_split(b';', b'#', vec[12]),

        # v1_organizations
        single_split(b';', vec[13]),

        # v2_enhanced_organizations
        double_split(b';', b',', vec[14]),

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
        {str(k, 'utf-8'): int_or_float(v) for (k, v)
         in double_split(b',', b':', vec[17])},

        # v2_sharing_image
        vec[18],

        # v21_related_images
        single_split(b';', vec[19]),

        # v21_social_image_embeds
        single_split(b';', vec[20]),

        # v21_social_video_embeds
        single_split(b';', vec[21]),

        # v21_quotations
        double_split(b'#', b'|', vec[22]),

        # v21_all_names
        double_split(b';', b',', vec[23]),

        # v21_amounts
        # The first converter is 'as_is' because of '00000488'
        [V21Amount(*args) for args
         in filter_insane_int_in_v21amount(
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


def import_gkg(mongo_conn:pymongo.MongoClient,
               gkg_csv:str,
               columns_found_nonempty:set[int],
               opts:options.GkgOptions,
               warn_out: typing.Any,
               ) -> None:

    if not opts.quiet:
        print (f"Processing {gkg_csv}...")
    # print (mongo_conn.gdelt.list_collection_names())
    with zipfile.ZipFile(gkg_csv, 'r') as archive:
        base_name = os.path.basename(gkg_csv)[:-4] # name without '.zip'
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
                print(f"Found dangling line. {line_count}@{gkg_csv}:[{line}]")
                break_flag = True
                break
            print(f"Joining line {line_count}@{gkg_csv}")
            line_count += 1
        if break_flag:
            break
        line = blob[pos:eol_pos]
        pos = eol_pos + 1
        vec = line.rstrip().split(b'\t')
        if len(vec) != 27:
            warn_out(f"Short line {line_count}@{gkg_csv}:[{line}]")
            continue
        # run_non_empty_check(vec, columns_found_nonempty)
        #if vec[0] != b'20230701000000-66':
        #    continue
        # print (f"{vec[0]=} {vec[9]=}")
        try:
            gkg = makeGKGfromColumns(vec, gkg_csv, line_count, warn_out)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print (f"Offending row:{vec[0]}")
            raise
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
        yearmonth_part = os.path.basename(gzfile_path)[:6]
        logfile = f"/var/log/gdelt/{yearmonth_part}_warns.log"
        with open(logfile, 'a') as fp:
            def warn_out(s:str):
                print ('warnout: ' + s, file=fp)
            import_gkg(mongo_conn,
                       gzfile_path,
                       columns_found_nonempty,
                       opts,
                       warn_out)


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
        print ("Requesting workers to quit...")
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
