from abc import ABCMeta, abstractmethod
import asyncio


class AsyncApp(metaclass=ABCMeta):
    @abstractmethod
    async def main(self) -> None: ...

    def start(self) -> None:
        asyncio.run(self.main())
