from ._abc import ABC


class ARGDefault[T](ABC):
    value: T

    def __init__(self, value: T):
        self.value = value

    def __bool__(self):
        return bool(self.value)

    def __repr__(self):
        return repr(self.value)


class ARGSBase(ABC):
    def __iter_fields__(self):
        for f in sorted(self.__repr_fields__):
            try:
                yield f, getattr(self, f)
            except AttributeError:
                continue

    def __repr__(self) -> str:
        attr = ", ".join(["%s=%r" % f for f in self.__iter_fields__()])
        return f"{self.__class__.__name__}({attr})"
