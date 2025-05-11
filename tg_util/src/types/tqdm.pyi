from tqdm.asyncio import tqdm_asyncio

class tqdm[T](tqdm_asyncio[T]):
    async def __anext__(self) -> T: ...  # type: ignore
