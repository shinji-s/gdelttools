import os
import sys
import zipfile

import meta

first_entry = True

def dump_event(line:bytes) -> None:
    global first_entry
    if first_entry:
        first_entry = False
    else:
        print ('-' * 79)
    output = meta.dump_event(line, skip_blanks=True)
    max_taglen = max([len(p[0]) for p in output])
    for tag, value in output:
        print (f'{tag:<{max_taglen}}: {value}')
    

def dump_events_in_lines(lines:list[bytes], line_indices:list[int]) -> None:
    enumerated_lines = list(enumerate(lines))
    if len(line_indices) == 0:
        # dump all lines if no line indices are specified.
        line_indices = list(range(len(lines)))
    else:
        line_indices.sort()
    # Reverse lists to avoid pop(0) which has O(n) complexity.
    enumerated_lines.reverse()
    line_indices.reverse()
    while len(enumerated_lines) and len(line_indices):
        if enumerated_lines[-1][0] == line_indices[-1]:
            dump_event(enumerated_lines[-1][1])
            line_indices.pop()
        enumerated_lines.pop()


def dump_events(gz_filepath:str, line_indices:list[int]) -> None:
    assert gz_filepath.endswith('.zip')
    base_name = os.path.basename(gz_filepath)[:-4] # -4 => -len('.zip')
    with zipfile.ZipFile(gz_filepath, 'r') as archive:
        lines = [x for x in archive.read(base_name).split(b'\n')]
        if lines[-1] == b'':
            lines.pop()
        dump_events_in_lines(lines, line_indices)
    

def main(src_spec) -> None:
    vec = src_spec.split(':')
    if len(vec) == 2:
        gz_filepath, line_indices_str = vec
        line_indices = [int(x) for x in line_indices_str.split(',')]
    else:
        gz_filepath = vec[0]
        line_indices = []
    dump_events(gz_filepath, line_indices)


if __name__ == '__main__':
    for spec in sys.argv[1:]:
        main(spec)
