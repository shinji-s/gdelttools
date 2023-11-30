from __future__ import annotations
import dataclasses
import datetime
import typing

import bson # type: ignore

SourceCollectionID = typing.Literal[
    1, # WEB
    2, # CITATION ONLY,
    3, # CORE
    4, # DTIC
    5, # JSOR
    6, # NONTEXTUALSOURCE
    ]
def validate_as_SurceCollectionID(n: int) -> SourceCollectionID:
    assert 1 <= n <= 6
    return typing.cast(SourceCollectionID, n)

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
        return [getattr(self, field.name)
                for field in dataclasses.fields(self.__class__)]

    @staticmethod
    def deserialize(version:int, values:list[typing.Any]) -> V1Count:
        if version != 1:  # check version
            raise Exception(f"Unexpected version '{values[0]}' on serialized V15Tone.")
        return V1Count(*values)

@dataclasses.dataclass
class V21Count(V1Count):
    location_offset_within_document: int

@dataclasses.dataclass
class V15Tone:
    tone: float
    positive_score: float
    negative_score: float
    polarity: float
    active_reference_density: float
    self_or_group_reference_density: float
    word_count: int

    def serialize(self):
        return [getattr(self, field.name)
                for field in dataclasses.fields(self.__class__)]

    @staticmethod
    def deserialize(version:int, values:list[typing.Any]) -> 'V15Tone':
        if version != 1:  # check version
            raise Exception(f"Unexpected version '{values[0]}' on serialized V15Tone.")
        return V15Tone(*values)


def single_join(delim: str,
                value_list: list[str|int|float]
                ) -> str:
    return delim.join(map(str, value_list))

def single_join_with_excessdelim(delim: str,
                                 value_list: list[str|int|float]
                                 ) -> str:
    joined = delim.join(map(str, value_list))
    if len(joined) == 0:
        return ''
    return joined + delim

def double_join(first_delim: str,
                second_delim: str,
                value_list: list[list[str|int|float]]
                ) -> str:
    return first_delim.join(
        [second_delim.join(map(str, inner_value_list))
         for inner_value_list in value_list])

def double_join_with_excessdelim(first_delim: str,
                                 second_delim: str,
                                 value_list: list[list[str|int|float]]
                                 ) -> str:
    joined = first_delim.join(
        [second_delim.join(map(str, inner_value_list))
         for inner_value_list in value_list])
    if len(joined) == 0:
        return ''
    return joined + first_delim


def double_join_dict(first_delim: str,
                     second_delim: str,
                     value_list: list[list[str|int|float]]
                     ) -> str:
    return first_delim.join(
        [second_delim.join(map(str, inner_value_list))
         for inner_value_list in value_list.items()])

@dataclasses.dataclass
class GKG:
    gkg_record_id: str                                  #0
    v1_date: datetime.date                              #1
    v2_source_collection_identifier: SourceCollectionID #2
    v2_source_common_name: str                          #3
    v2_document_identifier: str                         #4
    v1_counts: list[V1Count]                            #5
    v21_counts: list[V21Count]                          #6
    v1_themes: list[str]                                #7
    v2_enhanced_themes: list[str]                       #8
    v1_locations: list[list[str]]                       #9
    v2_enhanced_locations: list[list[str]]              #10
    v1_persons: list[str]                               #11
    v2_enhanced_persons: list[str]                      #12
    v1_organizations: list[str]                         #13
    v2_enhanced_organizations: list[str]                #14
    # v15_tone: list[str]
    # Tone, Positive Score, Negative Score, Polarity, 
    # Activity Reference Density, Self/Group Reference Density, word-count
    v15_tone: V15Tone                                   #15
    v21_enhanced_dates: list[int]                       #16
    v2_gcam: dict[str, float]                           #17
    v2_sharing_image: str                               #18
    v21_related_images: list[str]                       #19
    v21_social_image_embeds: list[str]                  #20
    v21_social_video_embeds: list[str]                  #21
    v21_quotations: list[list[str]]                     #22
    v21_all_names: list[tuple[str, int]]                #23
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
        version = bson_value['version']
        assert version == 1
        vl = bson_value['value_list']
        gkg = GKG(
            vl[0],  # record id
            datetime.datetime.fromisoformat(vl[1]), # v1_date
            validate_as_SurceCollectionID(vl[2]),   # v2_source_collection_id
            vl[3],                                  # v2_source_common_name
            vl[4],                                  # document identifier
            vl[5],                                  # v1_counts
            [V21Count.deserialize(version, args)
             for args in vl[6]],                    # v21_counts
            vl[7],                                  # v1_themes
            vl[8],                                  # v2_enhanced_themes
            vl[9],                                  # v1_locations
            vl[10],                                 # v2_enhanced_locations
            vl[11],                                 # v1_persons
            vl[12],                                 # v2_enhanced_persons
            vl[13],                                 # v1_organizations
            vl[14],                                 # v2_enhanced_organizations
            V15Tone.deserialize(version, vl[15]),   # v15_tone
            vl[16],                                 # v21_enhanced_dates
            vl[17],                                 # v2_gcam
            vl[18],                                 # v2_sharing_image
            vl[19],                                 # v2_related_image
            vl[20],                                 # v21_social_image_embdeds
            vl[21],                                 # v21_social_video_embdeds
            vl[22],                                 # v21_quotations
            vl[23],                                 # v21_all_names
            vl[24],                                 # v21_amounts
            vl[25],                                 # v21_translation_info
            vl[26],                                 # v2_extras_xml
            )
            
        return gkg


    def to_csv(self) -> str:
        return '\t'.join([
            self.gkg_record_id,
            self.v1_date.strftime('%Y%m%d%H%M%S'),
            str(self.v2_source_collection_identifier),
            self.v2_source_common_name,
            self.v2_document_identifier,
            double_join(';', '#', self.v1_counts),
            double_join(';', '#', self.v21_counts),
            double_join(';', ',', self.v1_themes),
            double_join(';', '#', self.v2_enhanced_themes),
            double_join(';', '#', self.v1_locations),
            double_join(';', '#', self.v2_enhanced_locations),
            single_join(';', self.v1_persons),
            double_join(';', '#', self.v2_enhanced_persons),
            single_join(';', self.v1_organizations),
            double_join(';', ',', self.v2_enhanced_organizations),
            single_join(',', self.v15_tone.serialize()),
            double_join(';', '#', self.v21_enhanced_dates),
            double_join_dict(',', ':', self.v2_gcam),
            self.v2_sharing_image,
            single_join_with_excessdelim(';', self.v21_related_images),
            single_join_with_excessdelim(';', self.v21_social_image_embeds),
            single_join_with_excessdelim(';', self.v21_social_video_embeds),
            double_join('#', '|', self.v21_quotations),
            double_join(';', ',', self.v21_all_names),
            double_join_with_excessdelim(';', ',', self.v21_amounts),
            double_join(';', ',', self.v21_translation_info),
            self.v2_extras_xml,
            ])
        
