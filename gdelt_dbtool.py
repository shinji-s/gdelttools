import sys

from mymongo import with_mongo

@with_mongo()
def drop(conn, opts, args):
    if opts.drop not in ('events', 'gkg'):
        raise Exception("You can only drop 'events' or 'gkg' table.")
    if not opts.yes:
        sys.stdout.write("Are you sure?:")
        sys.stdout.flush()
        l = sys.stdin.readline()
        if l[0].upper() != 'Y':
            print ("Canceled deletion.")
            return
        sys.stdout.write(f"Deleting {opts.drop}: ")
        sys.stdout.flush()
    getattr(conn.gdelt, opts.drop).drop()
    if not opts.yes:
        print ("Done")

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option('', '--drop', type=str, default=None)
    parser.add_option('-y', '--yes', action='store_true', default=False)
    opts, args = parser.parse_args()

    if opts.drop:
        drop(opts, args)
