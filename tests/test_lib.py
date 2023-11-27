from asyncio import TaskGroup
from tempfile import NamedTemporaryFile
from unittest import IsolatedAsyncioTestCase

from aiosqlite import Cursor
from wcpan.drive.sqlite._lib import read_write, read_only


class TransactionTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        file = self.enterContext(NamedTemporaryFile())
        self._dsn = file.name
        async with read_write(self._dsn) as query:
            await _prepare(query)

    async def testRead(self):
        async with read_only(self._dsn) as query:
            rv = await _inner_select(query, "alice")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

    async def testWrite(self):
        async with read_write(self._dsn) as query:
            await _inner_insert(query, 2, "bob")

        async with read_only(self._dsn) as query:
            rv = await _inner_select(query, "bob")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 2)

    async def testParallelReading(self):
        async with read_only(self._dsn) as q1, read_only(
            self._dsn
        ) as q2, TaskGroup() as tg:
            t1 = tg.create_task(_inner_select(q1, "alice"))
            t2 = tg.create_task(_inner_select(q2, "alice"))

        rv = t1.result()
        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)
        rv = t2.result()
        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

    async def testParallelReadingWrite(self):
        async with read_only(self._dsn) as rq, read_write(
            self._dsn
        ) as wq, TaskGroup() as tg:
            rt = tg.create_task(_inner_select(rq, "alice"))
            wt = tg.create_task(_inner_update(wq, 1, "bob"))

        rv = rt.result()
        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)
        rv = wt.result()
        self.assertIsNone(rv)

        async with read_only(self._dsn) as rq:
            rv = await _inner_select(rq, "bob")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

    async def testParallelWriting(self):
        with self.assertRaises(Exception):
            async with read_write(self._dsn, timeout=0.1) as q1, read_write(
                self._dsn, timeout=0.1
            ) as q2, TaskGroup() as tg:
                tg.create_task(_inner_update(q1, 1, "bob"))
                tg.create_task(_inner_update(q2, 1, "ccc"))

    async def testWriteWhileReading(self):
        async with read_only(self._dsn) as rq, read_write(self._dsn) as wq:
            rv = await _inner_select(rq, "alice")
            await _inner_update(wq, 1, "bob")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

        async with read_only(self._dsn) as rq:
            rv = await _inner_select(rq, "bob")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

    async def testReadWhileWriting(self):
        async with read_write(self._dsn) as wq, read_only(self._dsn) as rq:
            await _inner_update(wq, 1, "bob")
            rv = await _inner_select(rq, "alice")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)

        async with read_only(self._dsn) as rq:
            rv = await _inner_select(rq, "bob")

        self.assertIsNotNone(rv)
        assert rv
        self.assertEqual(rv, 1)


async def _prepare(query: Cursor):
    await query.execute(
        """
        CREATE TABLE student (
            id INTEGER NOT NULL,
            name VARCHAR(64),
            PRIMARY KEY (id)
        );
        """
    )
    await query.execute(
        """
        INSERT INTO student
        (id, name)
        VALUES
        (?, ?);
        """,
        (1, "alice"),
    )


async def _inner_select(query: Cursor, name: str) -> int | None:
    await query.execute(
        """
        SELECT id FROM student WHERE name=?;
        """,
        (name,),
    )
    rv = await query.fetchone()
    if rv is None:
        return None
    return rv["id"]


async def _inner_insert(query: Cursor, id: int, name: str) -> None:
    await query.execute(
        """
        INSERT INTO student
        (id, name)
        VALUES
        (?, ?);
        """,
        (id, name),
    )


async def _inner_update(query: Cursor, id: int, name: str) -> None:
    await query.execute(
        """
        UPDATE student
        SET name = ?
        WHERE id = ?
        ;
        """,
        (name, id),
    )
