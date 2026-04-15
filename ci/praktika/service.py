from abc import ABC, abstractmethod
from types import TracebackType
from typing import Optional, Type


class Service(ABC):
    @abstractmethod
    def start(self) -> bool:
        pass

    @abstractmethod
    def shutdown(self) -> bool:
        pass

    def __enter__(self) -> "Service":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shutdown()
