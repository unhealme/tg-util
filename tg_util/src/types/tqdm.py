from tqdm.asyncio import tqdm_asyncio as _tqdm


class tqdm[T](_tqdm):
    def close(self):
        self.total = self.n
        return super().close()
