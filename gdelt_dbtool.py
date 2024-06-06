#! /usr/bin/env python3
import sys

from mymongo import with_mongo

@with_mongo()
def drop(conn, opts, args):
    for collection_name in args:
        if collection_name not in ('events', 'gkg'):
            raise Exception("You can only drop 'events' or 'gkg' table.")
        if not opts.yes:
            sys.stdout.write(f"Are you sure to drop {collection_name}?:")
            sys.stdout.flush()
            l = sys.stdin.readline()
            if l[0].upper() != 'Y':
                print ("Canceled deletion.")
                return
            sys.stdout.write(f"Dropping {opts.drop}: ")
            sys.stdout.flush()
        getattr(conn.gdelt, collection_name).drop()
        if not opts.yes:
            print ("Dropped.")

@with_mongo()
def index(conn, opts, args):
    for index_spec in args:
        collection_name, index_spec = index_spec.split(':', 1)
        print (f"Creating index '{index_spec}' on {collection_name}...")
        coll = getattr(conn.gdelt, collection_name)
        coll.create_index(index_spec.split(','))


@with_mongo()
def describe(conn, opts, args):
    for collection_name in args:
        coll = getattr(conn.gdelt, collection_name)
        x = coll.find_one({})
        for attr in x:
            print (attr)

@with_mongo()
def fetch(conn, opts, args):
    for collection_name in args:
        coll = getattr(conn.gdelt, collection_name)
        curs = coll.find()
        for i in range(opts.fetch):
            print (curs.next())

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option('', '--drop', action='store_true', default=False)
    parser.add_option('-u', '--index', action='store_true', default=False)
    parser.add_option('-d', '--describe', action='store_true', default=False)
    parser.add_option('-y', '--yes', action='store_true', default=False)
    parser.add_option('-n', '--fetch', type=int, default=0)
    all_actions = ['drop', 'index', 'describe', 'fetch']
    opts, args = parser.parse_args()

    specified_actions = [a for a in all_actions if getattr(opts, a)]
    if len(specified_actions) != 1:
        print (f"One of the actions '{all_actions}' must be specified once and only once")
        sys.exit(1)

    if opts.drop:
        drop(opts, args)
    elif opts.index:
        index(opts, args)
    elif opts.describe:
        describe(opts, args)
    elif 0 < opts.fetch:
        fetch(opts, args)
