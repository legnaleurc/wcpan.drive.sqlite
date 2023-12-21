from collections.abc import Callable
from concurrent.futures import Executor
from contextlib import contextmanager, closing
from functools import partial
from pathlib import PurePath
from sqlite3 import connect, Row
from typing import Pattern, cast, Concatenate
import re

from wcpan.drive.core.types import Node, ChangeAction
from wcpan.drive.core.lib import dispatch_change

from .exceptions import SqliteSnapshotError
from ._inner import (
    inner_delete_node_by_id,
    inner_get_metadata,
    inner_get_node_by_id,
    inner_insert_node,
    inner_set_metadata,
)


CURRENT_SCHEMA_VERSION = 4
KEY_ROOT_ID = "root_id"
KEY_CURSOR = "check_point"
SQL_CREATE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT NOT NULL,
        value TEXT,
        PRIMARY KEY (key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS nodes (
        id TEXT NOT NULL,
        name TEXT,
        trashed BOOLEAN,
        created INTEGER,
        modified INTEGER,
        PRIMARY KEY (id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_nodes_names ON nodes(name);",
    "CREATE INDEX IF NOT EXISTS ix_nodes_trashed ON nodes(trashed);",
    "CREATE INDEX IF NOT EXISTS ix_nodes_created ON nodes(created);",
    "CREATE INDEX IF NOT EXISTS ix_nodes_modified ON nodes(modified);",
    """
    CREATE TABLE IF NOT EXISTS files (
        id TEXT NOT NULL,
        mime_type TEXT,
        hash TEXT,
        size INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_files_mime_type ON files(mime_type);",
    """
    CREATE TABLE IF NOT EXISTS parentage (
        parent TEXT NOT NULL,
        child TEXT NOT NULL,
        PRIMARY KEY (parent, child),
        FOREIGN KEY (parent) REFERENCES nodes (id),
        FOREIGN KEY (child) REFERENCES nodes (id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_parentage_parent ON parentage(parent);",
    "CREATE INDEX IF NOT EXISTS ix_parentage_child ON parentage(child);",
    """
    CREATE TABLE IF NOT EXISTS images (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS videos (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        ms_duration INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS private (
        id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_private_id ON private(id);",
    "CREATE INDEX IF NOT EXISTS ix_private_key ON private(key);",
    f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION};",
]


type RegexpFunction = Callable[..., bool]


class OffMainProcess:
    def __init__(self, *, dsn: str, pool: Executor) -> None:
        self._dsn = dsn
        self._pool = pool

    async def __call__[
        **A, R
    ](
        self, fn: Callable[Concatenate[str, A], R], *args: A.args, **kwargs: A.kwargs
    ) -> R:
        from asyncio import get_running_loop
        from functools import partial

        bound = partial(fn, self._dsn, *args, **kwargs)
        loop = get_running_loop()
        return await loop.run_in_executor(self._pool, bound)


@contextmanager
def connect_(dsn: str, *, timeout: float | None, regexp: RegexpFunction | None):
    if timeout is None:
        timeout = 5.0
    with connect(dsn, timeout=timeout) as db:
        db.row_factory = Row
        # FIXME error in the real world
        # await db.execute("PRAGMA foreign_keys = 1;")
        if regexp:
            db.create_function("REGEXP", 2, regexp, deterministic=True)
        yield db


@contextmanager
def read_only(
    dsn: str, *, timeout: float | None = None, regexp: RegexpFunction | None = None
):
    with connect_(dsn, timeout=timeout, regexp=regexp) as db, closing(
        db.cursor()
    ) as cursor:
        yield cursor


@contextmanager
def read_write(dsn: str, *, timeout: float | None = None):
    with connect_(dsn, timeout=timeout, regexp=None) as db, closing(
        db.cursor()
    ) as cursor:
        try:
            yield cursor
            if db.in_transaction:
                db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise


def get_uploaded_size(dsn: str, begin: int, end: int) -> int:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT SUM(size) AS sum
            FROM files
                INNER JOIN nodes ON files.id = nodes.id
            where created >= ? AND created < ?
            ;""",
            (begin, end),
        )
        rv = query.fetchone()
        if not rv:
            return 0
        if rv["sum"] is None:
            return 0
        return rv["sum"]


def find_orphan_nodes(dsn: str) -> list[Node]:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT nodes.id AS id
            FROM parentage
                LEFT OUTER JOIN nodes ON parentage.child=nodes.id
            WHERE parentage.parent IS NULL
            ;"""
        )
        rv = query.fetchall()
        raw_query = (inner_get_node_by_id(query, _["id"]) for _ in rv)
        nodes = [_ for _ in raw_query if _]
    return nodes


def find_multiple_parents_nodes(dsn: str) -> list[Node]:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT child, COUNT(child) AS parent_count
            FROM parentage
            GROUP BY child
            HAVING parent_count > 1
            ;"""
        )
        rv = query.fetchall()
        raw_query = (inner_get_node_by_id(query, _["child"]) for _ in rv)
        nodes = [_ for _ in raw_query if _]
    return nodes


def initialize(dsn: str, /):
    with read_write(dsn) as query:
        # check the schema version
        query.execute("PRAGMA user_version;")
        rv = query.fetchone()
        if not rv:
            raise SqliteSnapshotError("no user_version")
        version = int(rv[0])

        if version != 0 and version != CURRENT_SCHEMA_VERSION:
            raise SqliteSnapshotError(
                "schema has been changed, please rebuild snapshot"
            )

        # initialize table
        for sql in SQL_CREATE_TABLES:
            query.execute(sql)


def get_node_by_path(dsn: str, path: PurePath, /) -> Node | None:
    # the first part is "/"
    parts = path.parts[1:]
    with read_only(dsn) as query:
        node_id = inner_get_metadata(query, "root_id")
        if not node_id:
            return None

        for part in parts:
            query.execute(
                """
                SELECT nodes.id AS id
                FROM parentage
                    INNER JOIN nodes ON parentage.child=nodes.id
                WHERE parentage.parent=? AND nodes.name=?
                ;""",
                (node_id, part),
            )
            rv = query.fetchone()
            if not rv:
                return None
            node_id = cast(str, rv["id"])

        node = inner_get_node_by_id(query, node_id)
    return node


def resolve_path_by_id(dsn: str, node_id: str, /) -> PurePath | None:
    parts: list[str] = []
    with read_only(dsn) as query:
        while True:
            query.execute(
                """
                SELECT name
                FROM nodes
                WHERE id=?
                ;""",
                (node_id,),
            )
            rv = query.fetchone()
            if not rv:
                return None

            name = rv["name"]

            query.execute(
                """
                SELECT parent
                FROM parentage
                WHERE child=?
                ;""",
                (node_id,),
            )
            rv = query.fetchone()
            if not rv:
                # reached root
                parts.insert(0, "/")
                break

            parts.insert(0, name)
            node_id = rv["parent"]

    path = PurePath(*parts)
    return path


def get_child_by_name(dsn: str, name: str, parent_id: str, /) -> Node | None:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT nodes.id AS id
            FROM nodes
                INNER JOIN parentage ON parentage.child=nodes.id
            WHERE parentage.parent=? AND nodes.name=?
            ;""",
            (parent_id, name),
        )
        rv = query.fetchone()

        if not rv:
            return None

        node = inner_get_node_by_id(query, rv["id"])
    return node


def get_children_by_id(dsn: str, node_id: str, /) -> list[Node]:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT child
            FROM parentage
            WHERE parent=?
            ;""",
            (node_id,),
        )
        rv = query.fetchall()
        raw_query = (inner_get_node_by_id(query, _["child"]) for _ in rv)
        nodes = [_ for _ in raw_query if _]
    return nodes


def get_trashed_nodes(dsn: str, /) -> list[Node]:
    with read_only(dsn) as query:
        query.execute(
            """
            SELECT id
            FROM nodes
            WHERE trashed=?
            ;""",
            (True,),
        )
        rv = query.fetchall()
        raw_query = (inner_get_node_by_id(query, _["id"]) for _ in rv)
        nodes = [_ for _ in raw_query if _]
    return nodes


def apply_changes(dsn: str, changes: list[ChangeAction], cursor: str, /) -> None:
    with read_write(dsn) as query:
        for change in changes:
            dispatch_change(
                change,
                on_remove=lambda _: inner_delete_node_by_id(query, _),
                on_update=lambda _: inner_insert_node(query, _),
            )
        inner_set_metadata(query, KEY_CURSOR, cursor)


def find_nodes_by_regex(dsn: str, pattern: str, /) -> list[Node]:
    fn = partial(sqlite3_regexp, pattern=re.compile(pattern, re.I))
    with read_only(dsn, regexp=fn) as query:
        query.execute("SELECT id FROM nodes WHERE name REGEXP ?;", ("_",))
        rv = query.fetchall()
        rv = (inner_get_node_by_id(query, _["id"]) for _ in rv)
        rv = [_ for _ in rv if _]
    return rv


def get_current_cursor(dsn: str, /) -> str | None:
    with read_only(dsn) as query:
        return inner_get_metadata(query, KEY_CURSOR)


def get_root(dsn: str, /) -> Node | None:
    with read_only(dsn) as query:
        root_id = inner_get_metadata(query, KEY_ROOT_ID)
        if not root_id:
            return None
        return inner_get_node_by_id(query, root_id)


def set_root(dsn: str, root: Node, /) -> None:
    with read_write(dsn) as query:
        inner_set_metadata(query, KEY_ROOT_ID, root.id)
        inner_insert_node(query, root)


def get_node_by_id(dsn: str, node_id: str, /) -> Node | None:
    with read_only(dsn) as query:
        return inner_get_node_by_id(query, node_id)


def sqlite3_regexp(_: str, cell: str | None, *, pattern: Pattern[str]) -> bool:
    if cell is None:
        # root node
        return False
    return pattern.search(cell) is not None
