__all__ = (
    "ABC",
    "ABCMeta",
    "ARGSBase",
    "abstractmethod",
)

from abc import ABCMeta as _ABCMeta
from abc import abstractmethod
from itertools import chain

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class ABCMeta(_ABCMeta):
    __repr_fields__: tuple[str, ...]
    __slots__: tuple[str, ...]

    def __new__(
        mcls,
        name: str,
        bases: tuple[type, ...],
        namespace: "dict[str, Any]",
        /,
        **kwargs: "Any",
    ):
        if "__slots__" in namespace:
            err = "__slots__ should not be defined"
            raise TypeError(err)

        slots = ()
        if "__annotations__" in namespace:
            slots = tuple(x for x in namespace["__annotations__"] if x not in namespace)
        namespace["__slots__"] = slots

        fields: list[str] = [
            f for b in bases for f in getattr(b, "__repr_fields__", ())
        ]
        if fields:
            namespace["__repr_fields__"] = tuple(dict.fromkeys(chain(fields, slots)))
        else:
            namespace["__repr_fields__"] = slots
        return super().__new__(mcls, name, bases, namespace, **kwargs)


class ABC(metaclass=ABCMeta):
    pass


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
