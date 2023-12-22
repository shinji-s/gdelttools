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

class GdeltObject:
    def serialize(self):
        return {field.name:getattr(self, field.name)
                for field in dataclasses.fields(self.__class__)}

    def value_list(self):
        return [getattr(self, field.name)
                for field in dataclasses.fields(self.__class__)]

    @staticmethod
    def create(class_: typing.Any,
               row: typing.Mapping[typing.Any]
               ) -> typing.Any:
        args = [row[field.name] for field in dataclasses.fields(class_)]
        return class_(*args)


@dataclasses.dataclass
class V1Count(GdeltObject):
    count_type: bytes
    count_as_bytes: bytes
    object_type: bytes
    location_type: bytes
    location_fullname: bytes
    location_countrycode: bytes
    location_adm1code: bytes
    location_latitude: float | int
    location_longitude: float | int
    location_feature_id: bytes

    @property
    def count(self):
        return int(self.count_as_bytes)

    @staticmethod
    def deserialize(version:int, row:list[typing.Any]) -> V1Count:
        return GdeltObject.create(V1Count, row)


@dataclasses.dataclass
class V21Count(V1Count):
    location_offset_within_document: int

    @staticmethod
    def deserialize(version:int, row:list[typing.Any]) -> V21Count:
        return GdeltObject.create(V21Count, row)


@dataclasses.dataclass
class V15Tone(GdeltObject):
    tone: bytes
    positive_score: float
    negative_score: float
    polarity: float
    active_reference_density: float
    self_or_group_reference_density: float
    word_count: int

    @staticmethod
    # def deserialize(version:int, row:typing.Mapping[typing.Any]) -> 'V15Tone':
    #     args = [row[field.name] for field in dataclasses.fields(V15Tone)]
    #     return V15Tone(*args)
    @staticmethod
    def deserialize(version:int,
                    row:typing.Mapping[typing.Any]
                    ) -> V15Tone:
        return GdeltObject.create(V15Tone, row)


@dataclasses.dataclass
class V21Amount(GdeltObject):
    amount: int | float
    object_: bytes
    offset: int

    @staticmethod
    def deserialize(version:int,
                    row:typing.Mapping[typing.Any]
                    ) -> V21Amount:
        return GdeltObject.create(V21Amount, row)


@dataclasses.dataclass
class V1Location(GdeltObject):
    type: int
    fullname: bytes
    country_code: bytes
    adm1_code: bytes
    latitude: float
    longitude: float
    feature_id: str | float

    @staticmethod
    def deserialize(version:int,
                    row:typing.Mapping[typing.Any]
                    ) -> V1Location:
        return GdeltObject.create(V1Location, row)


@dataclasses.dataclass
class V2EnhancedTheme(GdeltObject):
    theme: str
    offset: int

    @staticmethod
    def deserialize(version:int,
                    row:typing.Mapping[typing.Any]
                    ) -> V2EnhancedTheme:
        return GdeltObject.create(V2EnhancedTheme, row)


@dataclasses.dataclass
class V21EnhancedDate(GdeltObject):
    resolution: int
    month_as_bytes: bytes
    day_as_bytes: bytes
    year_as_bytes: bytes
    offset: int

    @property
    def month(self):
        return int(self.month_as_bytes)

    @property
    def day(self):
        return day(self.day_as_bytes)

    @property
    def year(self):
        return year(self.year_as_bytes)

    @staticmethod
    def deserialize(version:int,
                    row:typing.Mapping[typing.Any]
                    ) -> V21EnhancedDate:
        return GdeltObject.create(V21EnhancedDate, row)


def to_bytes(x:typing.Any) -> bytes:
    if type(x) is bytes:
        return x
    return bytes(str(x), 'ascii')

def single_join(delim: bytes,
                value_list: list[bytes|int|float]
                ) -> bytes:
    return delim.join(map(to_bytes, value_list))

def single_join_with_excessdelim(delim: bytes,
                                 value_list: list[bytes|int|float]
                                 ) -> bytes:
    joined = delim.join(map(to_bytes, value_list))
    if len(joined) == 0:
        return b''
    return joined + delim

def double_join(first_delim: bytes,
                second_delim: bytes,
                value_list: list[list[bytes|int|float]]
                ) -> str:
    return first_delim.join(
        [second_delim.join(map(to_bytes, inner_value_list))
         for inner_value_list in value_list])

def double_join_with_excessdelim(first_delim: str,
                                 second_delim: str,
                                 value_list: list[list[str|int|float]]
                                 ) -> str:
    joined = first_delim.join(
        [second_delim.join(map(to_bytes, inner_value_list))
         for inner_value_list in value_list])
    if len(joined) == 0:
        return b''
    return joined + first_delim


def double_join_dict(first_delim: bytes,
                     second_delim: bytes,
                     value_list: list[list[str|int|float]]
                     ) -> bytes:
    return first_delim.join(
        [second_delim.join(map(to_bytes, inner_value_list))
         for inner_value_list in value_list.items()])


def csv_bytes(v:typing.Any) -> str:
    if v is None:
        return b''
    return to_bytes(v)

def object_list_join_with_excessdelim(outer_delim: str,
                                      obj_field_delim: str,
                                      object_list:list[typing.Any]) -> str:
    if len(object_list) == 0:
        return b''
    return outer_delim.join(
        obj_field_delim.join([csv_bytes(v) for v in o.value_list()])
        for o in object_list) + outer_delim

def object_list_join(outer_delim: str,
                     obj_field_delim: str,
                     object_list:list[typing.Any]) -> str:
    return outer_delim.join(
        obj_field_delim.join([csv_bytes(v) for v in o.value_list()])
        for o in object_list)

@dataclasses.dataclass
class GKG:
    gkg_record_id: bytes                                #0
    v1_date: datetime.date                              #1
    v2_source_collection_identifier: SourceCollectionID #2
    v2_source_common_name: bytes                        #3
    v2_document_identifier: bytes                       #4
    v1_counts: list[V1Count]                            #5
    v21_counts: list[V21Count]                          #6
    v1_themes: list[bytes]                              #7
    v2_enhanced_themes: list[V2EnhancedTheme]           #8
    v1_locations: list[V1Location]                      #9
    v2_enhanced_locations: list[list[V2EnhancedLocation]] #10
    v1_persons: list[bytes]                             #11
    v2_enhanced_persons: list[bytes]                    #12
    v1_organizations: list[bytes]                       #13
    v2_enhanced_organizations: list[bytes]              #14
    # v15_tone: list[str]
    # Tone, Positive Score, Negative Score, Polarity, 
    # Activity Reference Density, Self/Group Reference Density, word-count
    v15_tone: V15Tone                                   #15
    v21_enhanced_dates: V21EnhancedDate[int]            #16
    v2_gcam: dict[str, float]                           #17
    v2_sharing_image: bytes                             #18
    v21_related_images: list[bytes]                     #19
    v21_social_image_embeds: list[bytes]                #20
    v21_social_video_embeds: list[bytes]                #21
    v21_quotations: list[list[bytes]]                   #22
    v21_all_names: list[tuple[bytes, int]]              #23
    v21_amounts: list[V21Amount]
    v21_translation_info: list[tuple[bytes, bytes]]
    v2_extras_xml: str
    

    def to_bson(self) -> bson.son.SON:
        # print (dataclasses.fields(self.__class__))
        row: bson.son.SON = bson.son.SON()
        for field in dataclasses.fields(self.__class__):
            value = getattr(self, field.name)
            if field.name in [
                'v1_counts',
                'v21_counts',
                'v21_amounts',
                'v2_enhanced_themes',
                'v1_locations',
                'v21_enhanced_dates',
                ]:
                value = [obj.serialize() for obj in value]
            elif field.name == 'v1_date':
                value = value.isoformat()
            elif field.name == 'v15_tone':
                value = value.serialize()
            row[field.name] = value
        row['__version__'] = 1
        return row

    @staticmethod
    def deserialize(bson_value:bson.son.SON) -> 'GKG':
        # print (f"{bson_value=}")
        version = bson_value['__version__']
        assert version == 1
        vl = [bson_value[field.name] for field in dataclasses.fields(GKG)]
        # print (f"{bson_value=}")
        gkg = GKG(
            vl[0],  # record id
            datetime.datetime.fromisoformat(vl[1]), # v1_date
            validate_as_SurceCollectionID(vl[2]),   # v2_source_collection_id
            vl[3],                                  # v2_source_common_name
            vl[4],                                  # document identifier
            [V1Count.deserialize(version, args)
             for args in vl[5]],                    # v1_counts
            [V21Count.deserialize(version, args)
             for args in vl[6]],                    # v21_counts
            vl[7],                                  # v1_themes
            [V2EnhancedTheme.deserialize(version, args)
             for args in vl[8]],                    # v2_enhanced_themes
            [V1Location.deserialize(version, args)
             for args in vl[9]],                    # v1_locations
            vl[10],                                 # v2_enhanced_locations
            vl[11],                                 # v1_persons
            vl[12],                                 # v2_enhanced_persons
            vl[13],                                 # v1_organizations
            vl[14],                                 # v2_enhanced_organizations
            V15Tone.deserialize(version, vl[15]),   # v15_tone
            [V21EnhancedDate.deserialize(version, args)
             for args in vl[16]],                   # v21_enhanced_dates
            vl[17],                                 # v2_gcam
            vl[18],                                 # v2_sharing_image
            vl[19],                                 # v2_related_image
            vl[20],                                 # v21_social_image_embdeds
            vl[21],                                 # v21_social_video_embdeds
            vl[22],                                 # v21_quotations
            vl[23],                                 # v21_all_names
            [V21Amount.deserialize(version, args)   # v21_amounts
             for args in vl[24]],
            vl[25],                                 # v21_translation_info
            vl[26],                                 # v2_extras_xml
            )
            
        return gkg


    def to_csv(self) -> str:
        # for c in self.v1_counts:
        #     print (f"{c.count_as_bytes=} {c.count=}")
        return b'\t'.join([
            self.gkg_record_id,
            bytes(self.v1_date.strftime('%Y%m%d%H%M%S'), 'ascii'),
            to_bytes(self.v2_source_collection_identifier),
            self.v2_source_common_name,
            self.v2_document_identifier,
            object_list_join_with_excessdelim(b';', b'#', self.v1_counts), # 5
            object_list_join_with_excessdelim(b';', b'#', self.v21_counts),
            double_join_with_excessdelim(b';', b',', self.v1_themes),
            object_list_join_with_excessdelim(b';', b',',
                                              self.v2_enhanced_themes),
            object_list_join(b';', b'#', self.v1_locations),
            double_join(b';', b'#', self.v2_enhanced_locations), # 10
            single_join(b';', self.v1_persons),
            double_join(b';', b'#', self.v2_enhanced_persons),
            single_join(b';', self.v1_organizations),
            double_join(b';', b',', self.v2_enhanced_organizations),
            single_join(b',', self.v15_tone.value_list()),       # 15
            object_list_join(b';', b'#', self.v21_enhanced_dates),
            double_join_dict(b',', b':', self.v2_gcam),
            self.v2_sharing_image,
            single_join(b';', self.v21_related_images),
            single_join_with_excessdelim(b';', self.v21_social_image_embeds),
            single_join_with_excessdelim(b';', self.v21_social_video_embeds),
            double_join(b'#', b'|', self.v21_quotations), # 22
            double_join(b';', b',', self.v21_all_names),
            object_list_join_with_excessdelim(b';', b',', self.v21_amounts),
            double_join(b';', b',', self.v21_translation_info),
            self.v2_extras_xml,
            ])
        
