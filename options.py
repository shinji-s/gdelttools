import dataclasses

@dataclasses.dataclass(frozen=True)
class Options:
    quiet: bool
    verbose: bool
    num_threads: int

    def __post_init__(self):
        for field in dataclasses.fields(self.__class__):
            if isinstance(getattr(self, field.name), field.type):
                continue
            raise ValueError(f"'{field.name}' should be of {field.type}")
