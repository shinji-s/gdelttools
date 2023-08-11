import os
from typing import Literal, get_args

TagName = Literal[
    '_id', 'Day', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code',
    'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
    'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
    'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code',
    'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
    'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
    'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent',
    'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
    'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
    'Actor1Geo_Type', 'Actor1Geo_Fullname', 'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat',
    'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type',
    'Actor2Geo_Fullname', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code',
    'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
    'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_Fullname',
    'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code',
    'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID',
    'DATEADDED', 'SOURCEURL',
    ]

ValidTags:tuple[TagName,...] = get_args(TagName)


def find_event_column_index(tagname:TagName) -> int:
    with open(os.path.join(os.path.dirname(__file__), 'GDELT.ff')) as fp:
        lines = fp.readlines()
    return [
        x for x in enumerate(lines)
        if x[1].startswith(f'[{tagname}]')][0][0] // 2


def dump_event(line: bytes, skip_blanks) -> list[tuple[TagName, str]]:
    cols = line.split(b'\t')
    return [(tag, column_value) for i, tag in enumerate(ValidTags)
            if (column_value:=str(cols[i], 'utf-8')) or not skip_blanks]

if __name__ == '__main__':
    # print (find_event_column_index('GoldsteinScale'))
    skip_blank = True
    print (dump_event(b'''977166878	20200330	202003	2020	2020.2466											AFG	AFGHANISTAN	AFG								0	042	042	04	1	1.9	2	1	2	-6.44910644910645	0								4	Kabul, Kabol, Afghanistan	AF	AF13	3580	34.5167	69.1833	-3378435	4	Sussex, East Sussex, United Kingdom	UK	UKE2	40137	50.9167	-0.083333	-2609142	20210330024500	https://dissidentvoice.org/2021/03/will-drones-really-protect-us/
    ''', skip_blank ))
