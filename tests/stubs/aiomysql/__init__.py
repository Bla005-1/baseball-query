class DictCursor:
    """Minimal stand-in for aiomysql.DictCursor."""
    pass

async def create_pool(**kwargs):
    class DummyPool:
        """Very small pool returning dummy connections."""

        async def acquire(self):
            class Conn:
                """Stubbed connection object with minimal cursor support."""

                async def cursor(self, *args, **kwargs):
                    class C:
                        """Dummy cursor that records nothing and returns empties."""

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

