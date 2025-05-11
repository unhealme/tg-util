__all__ = (
    "ABC",
    "ABCMeta",
    "ARGSBase",
    "abstractmethod",
)

from abc import ABCMeta as _ABCMeta
from abc import abstractmethod

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class ABCMeta(_ABCMeta):
    __repr_fields__: tuple[str, ...]
    __slots__: tuple[str, ...]

    def __new__(
        cls,
        name: str,
        bases: tuple["type | ABCMeta", ...],
        namespace: dict[str, "Any"],
        /,
        **kwargs: "Any",
    ):
        if "__slots__" in namespace:
            raise TypeError("__slots__ shout not be defined")

        if "__annotations__" in namespace:
            namespace["__slots__"] = tuple(
                x for x in namespace["__annotations__"] if x not in namespace
            )
        else:
            namespace["__slots__"] = ()

        fields: list[str] = []
        for base in bases:
            try:
                fields.extend(base.__repr_fields__)
            except AttributeError:
                pass
        if fields:
            namespace["__repr_fields__"] = tuple(
                dict.fromkeys((*fields, *namespace["__slots__"]))
            )
        else:
            namespace["__repr_fields__"] = namespace["__slots__"]
        return super().__new__(cls, name, bases, namespace, **kwargs)


class ABC(metaclass=ABCMeta):
    pass


class ARGSBase(ABC):
    def __repr__(self) -> str:
        def iter_attr():
            for k in sorted(self.__repr_fields__):
                try:
                    yield k, getattr(self, k)
                except AttributeError:
                    continue

        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(["%s=%r" % attr for attr in iter_attr()]),
        )
