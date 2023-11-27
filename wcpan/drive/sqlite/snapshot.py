from contextlib import asynccontextmanager
from functools import partial
from pathlib import PurePath
from typing import Pattern, TypeGuard, cast
import re

from aiosqlite import Cursor
from wcpan.drive.core.exceptions import DriveError, NodeNotFoundError
from wcpan.drive.core.types import (
    ChangeAction,
    Node,
    RemoveAction,
    SnapshotService,
    UpdateAction,
)

from ._lib import (
    CURRENT_SCHEMA_VERSION,
    inner_get_node_by_id,
    inner_insert_node,
    inner_set_metadata,
    KEY_CURSOR,
    KEY_ROOT_ID,
    read_only,
    read_write,
    SQL_CREATE_TABLES,
)


@asynccontextmanager
async def create_service(*, dsn: str):
    await _initialize(dsn)
    yield SqliteSnapshotService(dsn)


class SqliteSnapshotService(SnapshotService):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @property
    def api_version(self) -> int:
        return 4

    async def get_current_cursor(self) -> str:
        async with read_only(self._dsn) as query:
            cursor = await _inner_get_metadata(query, KEY_CURSOR)
        return "" if not cursor else cursor

    async def get_root(self) -> Node:
        async with read_only(self._dsn) as query:
            root_id = await _inner_get_metadata(query, KEY_ROOT_ID)
        if not root_id:
            raise NodeNotFoundError("root")
        root = await self.get_node_by_id(root_id)
        return root

    async def set_root(self, node: Node) -> None:
        async with read_write(self._dsn) as query:
            await inner_set_metadata(query, KEY_ROOT_ID, node.id)
            await inner_insert_node(query, node)

    async def get_node_by_id(self, node_id: str) -> Node:
        async with read_only(self._dsn) as query:
            node = await inner_get_node_by_id(query, node_id)
            if not node:
                raise NodeNotFoundError(node_id)
            return node

    async def get_node_by_path(self, path: PurePath) -> Node:
        if not path.is_absolute():
            raise ValueError("path must be an absolute path")
        node = await _get_node_by_path(self._dsn, path)
        if not node:
            raise NodeNotFoundError(str(path))
        return node

    async def resolve_path_by_id(self, node_id: str) -> PurePath:
        path = await _resolve_path_by_id(self._dsn, node_id)
        if not path:
            raise NodeNotFoundError(node_id)
        return path

    async def get_child_by_name(self, name: str, parent_id: str) -> Node:
        node = await _get_child_by_name(self._dsn, name, parent_id)
        if not node:
            raise NodeNotFoundError(name)
        return node

    async def get_children_by_id(self, parent_id: str) -> list[Node]:
        return await _get_children_by_id(self._dsn, parent_id)

    async def get_trashed_nodes(self) -> list[Node]:
        return await _get_trashed_nodes(self._dsn)

    async def apply_changes(
        self,
        changes: list[ChangeAction],
        cursor: str,
    ) -> None:
        return await _apply_changes(self._dsn, changes, cursor)

    async def find_nodes_by_regex(self, pattern: str) -> list[Node]:
        return await _find_nodes_by_regex(self._dsn, pattern)


async def _initialize(dsn: str):
    async with read_write(dsn) as query:
        # check the schema version
        await query.execute("PRAGMA user_version;")
        rv = await query.fetchone()
        if not rv:
            raise DriveError("no user_version")
        version = int(rv[0])

        if version != 0 and version != CURRENT_SCHEMA_VERSION:
            raise DriveError("schema has been changed, please rebuild snapshot")

        # initialize table
        for sql in SQL_CREATE_TABLES:
            await query.execute(sql)


async def _get_node_by_path(
    dsn: str,
    path: PurePath,
) -> Node | None:
    # the first part is "/"
    parts = path.parts[1:]
    async with read_only(dsn) as query:
        node_id = await _inner_get_metadata(query, "root_id")
        if not node_id:
            return None

        for part in parts:
            await query.execute(
                """
                SELECT nodes.id AS id
                FROM parentage
                    INNER JOIN nodes ON parentage.child=nodes.id
                WHERE parentage.parent=? AND nodes.name=?
                ;""",
                (node_id, part),
            )
            rv = await query.fetchone()
            if not rv:
                return None
            node_id = cast(str, rv["id"])

        node = await inner_get_node_by_id(query, node_id)
    return node


async def _resolve_path_by_id(dsn: str, node_id: str) -> PurePath | None:
    parts: list[str] = []
    async with read_only(dsn) as query:
        while True:
            await query.execute(
                """
                SELECT name
                FROM nodes
                WHERE id=?
                ;""",
                (node_id,),
            )
            rv = await query.fetchone()
            if not rv:
                return None

            name = rv["name"]

            await query.execute(
                """
                SELECT parent
                FROM parentage
                WHERE child=?
                ;""",
                (node_id,),
            )
            rv = await query.fetchone()
            if not rv:
                # reached root
                parts.insert(0, "/")
                break

            parts.insert(0, name)
            node_id = rv["parent"]

    path = PurePath(*parts)
    return path


async def _get_child_by_name(dsn: str, name: str, parent_id: str) -> Node | None:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT nodes.id AS id
            FROM nodes
                INNER JOIN parentage ON parentage.child=nodes.id
            WHERE parentage.parent=? AND nodes.name=?
            ;""",
            (parent_id, name),
        )
        rv = await query.fetchone()

        if not rv:
            return None

        node = await inner_get_node_by_id(query, rv["id"])
    return node


async def _get_children_by_id(dsn: str, node_id: str) -> list[Node]:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT child
            FROM parentage
            WHERE parent=?
            ;""",
            (node_id,),
        )
        rv = await query.fetchall()
        raw_query = (await inner_get_node_by_id(query, _["child"]) for _ in rv)
        nodes = [_ async for _ in raw_query if _]
    return nodes


async def _get_trashed_nodes(dsn: str) -> list[Node]:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT id
            FROM nodes
            WHERE trashed=?
            ;""",
            (True,),
        )
        rv = await query.fetchall()
        raw_query = (await inner_get_node_by_id(query, _["id"]) for _ in rv)
        nodes = [_ async for _ in raw_query if _]
    return nodes


def _is_remove(change: ChangeAction) -> TypeGuard[RemoveAction]:
    return change[0]


def _is_update(change: ChangeAction) -> TypeGuard[UpdateAction]:
    return not change[0]


async def _apply_changes(
    dsn: str,
    changes: list[ChangeAction],
    cursor: str,
) -> None:
    async with read_write(dsn) as query:
        for change in changes:
            if _is_remove(change):
                await _inner_delete_node_by_id(query, change[1])
                continue
            if _is_update(change):
                await inner_insert_node(query, change[1])
                continue
        await inner_set_metadata(query, KEY_CURSOR, cursor)


async def _find_nodes_by_regex(dsn: str, pattern: str) -> list[Node]:
    fn = partial(_sqlite3_regexp, re.compile(pattern, re.I))
    async with read_only(dsn, regexp=fn) as query:
        await query.execute("SELECT id FROM nodes WHERE name REGEXP ?;", (pattern,))
        rv = await query.fetchall()
        rv = (await inner_get_node_by_id(query, _["id"]) for _ in rv)
        rv = [_ async for _ in rv if _]
    return rv


async def _inner_get_metadata(query: Cursor, key: str) -> str | None:
    await query.execute("SELECT value FROM metadata WHERE key = ?;", (key,))
    rv = await query.fetchone()
    if not rv:
        return None
    return rv["value"]


async def _inner_delete_node_by_id(query: Cursor, node_id: str) -> None:
    # remove from private
    await query.execute(
        """
        DELETE FROM private
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from videos
    await query.execute(
        """
        DELETE FROM videos
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from images
    await query.execute(
        """
        DELETE FROM images
        WHERE id=?
        ;""",
        (node_id,),
    )

    # disconnect parents
    await query.execute(
        """
        DELETE FROM parentage
        WHERE child=? OR parent=?
        ;""",
        (node_id, node_id),
    )

    # remove from files
    await query.execute(
        """
        DELETE FROM files
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from nodes
    await query.execute(
        """
        DELETE FROM nodes
        WHERE id=?
        ;""",
        (node_id,),
    )


def _sqlite3_regexp(
    pattern: Pattern[str],
    _: str,
    cell: str | None,
) -> bool:
    if cell is None:
        # root node
        return False
    return pattern.search(cell) is not None
