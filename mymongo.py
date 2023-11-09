import functools
import os
import sys
import typing

import bson
import pymongo


"""
You can reconfigure the names of environment variables that are used to
get db connection info through by setting another environment variable
'MYMONGO_ENVVAR_MAP'.
'MONGO_DB_HOST', 'MONGO_DB_USERNAME', and 'MONGO_DB_PASSWORD' are probed
by default.
"""

envvar_name_mapping_str = os.getenv(
    'MYMONGO_ENVVAR_MAP',
    'host:MONGO_DB_HOST,username:MONGO_DB_USERNAME,password:MONGO_DB_PASSWORD')

envvar_name_mapping:dict[str,str] = functools.reduce(
    lambda acc, x: dict(acc, **dict([x.split(':')])),
    envvar_name_mapping_str.split(','),
    {})


def as_is(value:str):
    return value


def process_host(value:str) -> list[str]:
    return value.split(',')
    

def with_mongo(*args_ro, **kw_ro):
    def f(wrapee, *w_args, **w_kw):
        def g(*g_args, **g_kw):
            kw = kw_ro.copy()
            if 'document_class' not in kw:
                kw['document_class'] = bson.SON
            # Pull connection info from env-var if not specified in 'kw_'
            for kwarg_name, processor in [('host', process_host),
                                          ('username', as_is),
                                          ('password', as_is)]:
                if kwarg_name not in kw:
                    env_name = envvar_name_mapping.get(kwarg_name)
                    if env_name is not None:
                        if value := os.getenv(env_name):
                            kw[kwarg_name] = processor(value)
            conn = pymongo.MongoClient(*args_ro, **kw)
            try:
                wrapee(conn, *g_args, **g_kw)
            finally:
                conn.close()
        return g
    return f


if __name__ == '__main__':
    def touch_db(mongo_conn:pymongo.MongoClient):
        row:bson.son.SON = bson.son.SON(
            id=37474,
            name='A',
            value=3.336
            )
        mongo_conn.gdelt.testcoll.insert_one(row)
        fetched = mongo_conn.gdelt.eventscsv.find_one()
        assert isinstance(fetched, bson.SON)
        print (fetched['ActionGeo_Fullname'])
        print (fetched['ActionGeo_CountryCode'])
        print (list(fetched.keys()))
        mongo_conn.gdelt.testcoll.drop()
        print (mongo_conn.gdelt.list_collection_names())
    
    @with_mongo(host=['mongodb://root:xyz32jml@mongo:27017'])
    def do_it_0(mongo_conn:pymongo.MongoClient, msg):
        print (msg)
        touch_db(mongo_conn)


    @with_mongo(host=['mongo:27017'],
                username='root',
                password='xyz32jml')
    def do_it_1(mongo_conn:pymongo.MongoClient,
                arg1:str,
                arg2:str,
                kw:str|None=None) -> None:
        print (arg1, arg2, kw)
        touch_db(mongo_conn)

    @with_mongo()
    def do_it_2(mongo_conn:pymongo.MongoClient,
                arg1:str,
                arg2:str,
                kw:str|None=None) -> None:
        print (arg1, arg2, kw)
        touch_db(mongo_conn)


    do_it_0('Bonjour!')
    do_it_1('who', 'are', kw='you')
    do_it_2('who', 'are', kw='you')
