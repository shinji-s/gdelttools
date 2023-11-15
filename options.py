import dataclasses
import re

def make_ymdhms_string(time_in_readable_format:str) -> str:
    pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})')
    m = pattern.match(time_in_readable_format)
    assert m, f"'{time_in_readable_format}' is unparsable."
    return ''.join([m.group(i) for i in range(1,7)])

class OptionBase:
    def __post_init__(self):
        for field in dataclasses.fields(self.__class__):
            if isinstance(getattr(self, field.name), field.type):
                continue
            raise ValueError(f"'{field.name}' should be of {field.type}")

@dataclasses.dataclass(frozen=True)
class GkgOptions(OptionBase):
    quiet: bool
    verbose: bool
    num_workers: int
    masterfile: str
    lower_limit_ymdhms: str
    upper_limit_ymdhms: str
    dry_run: bool
    no_store: bool


@dataclasses.dataclass(frozen=True)
class EventOptions(OptionBase):
    quiet: bool
    verbose: bool
    masterfile: str
    lower_limit_ymdhms: str
    upper_limit_ymdhms: str
    dry_run: bool
    no_store: bool
