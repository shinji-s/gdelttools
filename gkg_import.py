import datetime
import dataclasses
import json
import optparse
import pathlib
import sys
import typing

import bson # type: ignore
import pymongo

import options

from mymongo import with_mongo

SourceCollectionID = typing.Literal[
    1, # WEB
    2, # CITATION ONLY,
    3, # CORE
    4, # DTIC
    5, # JSOR
    6, # NONTEXTUALSOURCE
    ]

SOURCE_WEB, SOURCE_CITATION_ONLY, SOURCE_CORE, SOURCE_DTIC, \
SOURCE_JSTOR, SOURCE_NON_TEXTUAL = typing.get_args(SourceCollectionID)


@dataclasses.dataclass
class V1Count:
    count_type: str
    count: int
    object_type: str
    location_type: str
    location_fullname: str
    location_countrycode: str
    location_adm1code: str
    location_latitude: float
    location_longitude: float
    location_feature_id: str

    def serialize(self):
        values = [1] # version
        for field in dataclasses.fields(self.__class__):
            values.append(getattr(self, field.name))
        return values

@dataclasses.dataclass
class V21Count(V1Count):
    location_offset_within_document: int

@dataclasses.dataclass
class V15Tone:
    tone: float
    positive_score: float
    negative_score: float
    polarity: float
    active_reference_dencity: float
    self_or_group_reference_dencity: float
    word_count: int

    def serialize(self):
        values = [1] # version
        for field in dataclasses.fields(self.__class__):
            values.append(getattr(self, field.name))
        return values

    @staticmethod
    def deserialize(values:list[typing.Any]) -> 'V15Tone':
        if values[0] != 1:  # check version
            raise Exception(f"Unexpected version '{values[0]}' on serialized V15Tone.")
        return V15Tone(*values[1:])


@dataclasses.dataclass
class GKG:
    gkg_record_id: str
    v1_date: datetime.date
    v2_source_collection_identifier: SourceCollectionID
    v2_source_common_name: str
    v2_document_identifier: str
    v1_counts: list[V1Count]
    v21_counts: list[V21Count]
    v1_themes: list[str]
    v2_enhanced_themes: list[str]
    v1_locations: list[list[str]]
    v2_enhanced_locations: list[list[str]]
    v1_persons: list[str]
    v2_enhanced_persons: list[str]
    v1_organizations: list[str]
    v2_enhanced_organizations: list[str]
    # v15_tone: list[str]
    # Tone, Positive Score, Negative Score, Polarity,
    # Activity Reference Dencity, Self/Group Reference Dencity, word-count
    v15_tone: V15Tone
    v21_enhanced_dates: list[datetime.datetime]
    v2_gcam: dict[str, float]
    v2_sharing_image: str
    v21_related_images: list[str]
    v21_social_image_embeds: list[str]
    v21_social_video_embeds: list[str]
    v21_quotations: list[list[str]]
    v21_all_names: list[tuple[str, int]]
    v21_amounts: list[tuple[int|float, str, int]]
    v21_translation_info: list[tuple[str, str]]
    v2_extras_xml: str
    

    def to_bson(self) -> bson.son.SON:
        # print (dataclasses.fields(self.__class__))
        row: bson.son.SON = bson.son.SON()
        value_list:list[typing.Any] = []
        for field in dataclasses.fields(self.__class__):
            value = getattr(self, field.name)
            if field.name == 'v1_date':
                value = value.isoformat()
            elif field.name == 'v1_counts':
                value = [v1count.serialize() for v1count in value]
            elif field.name == 'v21_counts':
                value = [v21count.serialize() for v21count in value]
            elif field.name == 'v15_tone':
                value = value.serialize()
            value_list.append(value)
        row['version'] = 1
        row['value_list'] = value_list
        return row

    @staticmethod
    def deserialize(bson_value:bson.son.SON) -> 'GKG':
        assert bson_value['version'] == 1
        return GKG(*bson_value['value_list'])


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
    if converters == []:
        return chunks_list
    return [[f(x) for f, x in zip(converters, chunks)]
            for chunks in chunks_list]


def single_split(delim:str,
                 column_value:str,
                 converters: typing.Iterable[typing.Callable[[str], typing.Any]] = []
                 ) -> list[typing.Any]:
    chunks = column_value.rstrip(delim).split(delim)
    if converters is None:
        return chunks
    return [f(x) for f, x in zip(converters, chunks)]


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


def import_gkg(mongo_conn:pymongo.MongoClient,
               gkg_csv:pathlib.Path,
               columns_found_nonempty:set[int],
               opts:options.Options) -> None:

    print (f"Processing {gkg_csv}")
    # print (mongo_conn.gdelt.list_collection_names())
    with gkg_csv.open('r') as fp:
        vec = fp.readline().rstrip().split('\t')
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
            {k: float(v) for (k, v) in double_split(',', ':', vec[17])},

            # v2_sharing_image
            vec[18],

            # v21_related_images
            single_split(';', vec[19]),

            # v21_social_image_embeds
            single_split(';', vec[20]),

            # v21_social_video_embdes
            single_split(';', vec[21]),
            
            # v21_quotations
            double_split('#', '|', vec[22]),
            
            # v21_allnames
            double_split(';', ',', vec[23]),

            # v21_amounts
            double_split(';', ',', vec[24], (int_or_float, as_is, int)),

            # v21_translation_info
            double_split(';', ',', vec[25], (int_or_float, as_is)),
            
            vec[26],
        )
        mongo_conn.gdelt.gkg.insert_one(gkg.to_bson())

        # value_list = list(mongo_conn.gdelt.gkg.find({}))
        # gkg = GKG.deserialize(value_list[0])
        # print (value_list[0]['_id'])



@with_mongo()
def main(mongo_conn:pymongo.MongoClient,
         args:list[str],
         opts:options.Options) -> None:
    columns_found_nonempty:set[int] = set()
    for fname in args:
        import_gkg(mongo_conn,
                   pathlib.Path(fname),
                   columns_found_nonempty,
                   opts)


if __name__ == '__main__':
    
    parser = optparse.OptionParser(usage="How to use %p!")
    parser.add_option('-v', '--verbose', action='store_true', default=False)
    parser.add_option('-q', '--quiet', action='store_true', default=False)
    parser.add_option('-n', '--num-threads', type=int, default=4)
    
    opts, args = parser.parse_args()
    typed_opts = options.Options(opts.quiet, opts.verbose, opts.num_threads)
    if len(args) == 0:
        args = ['/opt/gdelt/csv/20230714133000.gkg.csv']
        
    main(args, typed_opts)
