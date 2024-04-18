import re

head_re = re.compile(b'\n\\d{14}-\\d+\t\\d{14}\t\\d+\t')

def split_to_chunks(blob: bytes):
    pos = 0
    while 1:
        match = head_re.search(blob, pos)
        if match is None:
            pending_chunk = blob[pos:]
            if pending_chunk:
                yield pending_chunk
            return
        span_start, span_end = match.span()
        yield blob[pos:span_start]
        pos = span_start +1



if __name__ == '__main__':
    # with open('20150219174500.gkg.csv', 'rb') as f:
    with open('20160101000000.gkg.csv', 'rb') as f:
        blob = f.read()
        i = 0
        for l in split_to_chunks(blob):
            assert len(l.split(b'\t')) == 27
            assert l.count(b'\t')+1 == 27
            i += 1
            if i == 120:
                print (l)
            


    
