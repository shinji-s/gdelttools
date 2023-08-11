import os
import sys
import zipfile

from meta import find_event_column_index

def check(gz_fname:str) -> None:
    with open(os.path.join(os.path.dirname(__file__), 'GDELT.ff')) as fp:
        lines = fp.readlines()
    GOLDSTEINSCALE_INDEX = find_event_column_index('GoldsteinScale')

    base_name = os.path.basename(gz_fname)[:-4] # -4 => -len('.zip')
    with zipfile.ZipFile(gz_fname, 'r') as archive:
        blob:bytes = archive.read(base_name)
        for line_index, line in enumerate(blob.split(b'\n')):
            if line == b'':
                continue
            cols = line.split(b'\t')
            try:
                gss = cols[GOLDSTEINSCALE_INDEX]
                if gss == b'':
                    print (f'Found empty Goldstein scale: {line_index}@{gz_fname}')
                else:
                    float(gss)
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                print (f'Offending line {line_index+1}: {str(line,'utf-8')}')
                raise
                
                
if __name__ == '__main__':
    check(sys.argv[1])
    # print (sys.argv[1])
