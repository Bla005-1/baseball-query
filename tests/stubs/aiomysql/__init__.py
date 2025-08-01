class DictCursor:
    pass

async def create_pool(**kwargs):
    class DummyPool:
        async def acquire(self):
            class Conn:
                async def cursor(self, *args, **kwargs):
                    class C:
                        async def execute(self, *args, **kwargs):
                            pass
                        async def fetchall(self):
                            return []
                        @property
                        def rowcount(self):
                            return 0
                    return C()
            return Conn()
        def close(self):
            pass
        async def wait_closed(self):
            pass
    return DummyPool()

