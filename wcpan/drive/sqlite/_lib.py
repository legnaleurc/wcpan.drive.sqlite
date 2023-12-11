from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, UTC

from aiosqlite import connect, Row, Cursor
from wcpan.drive.core.types import Node


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


@asynccontextmanager
async def connect_(dsn: str, *, timeout: float | None, regexp: RegexpFunction | None):
    if timeout is None:
        timeout = 5.0
    async with connect(dsn, timeout=timeout) as db:
        db.row_factory = Row
        # FIXME error in real world
        # await db.execute("PRAGMA foreign_keys = 1;")
        if regexp:
            # FIXME type error from aiosqlite
            await db.create_function("REGEXP", 2, regexp)  # type: ignore
        yield db


@asynccontextmanager
async def read_only(
    dsn: str, *, timeout: float | None = None, regexp: RegexpFunction | None = None
):
    async with connect_(
        dsn, timeout=timeout, regexp=regexp
    ) as db, db.cursor() as cursor:
        yield cursor


@asynccontextmanager
async def read_write(dsn: str, *, timeout: float | None = None):
    async with connect_(dsn, timeout=timeout, regexp=None) as db, db.cursor() as cursor:
        try:
            yield cursor
            if db.in_transaction:
                await db.commit()
        except Exception:
            if db.in_transaction:
                await db.rollback()
            raise


async def inner_set_metadata(query: Cursor, key: str, value: str) -> None:
    await query.execute(
        """
        INSERT OR REPLACE INTO metadata
        VALUES (?, ?)
        ;""",
        (key, value),
    )


async def inner_get_node_by_id(
    query: Cursor,
    node_id: str,
) -> Node | None:
    await query.execute(
        """
        SELECT name, trashed, created, modified
        FROM nodes
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = await query.fetchone()
    if not rv:
        return None
    name = rv["name"]
    trashed = bool(rv["trashed"])
    ctime = rv["created"]
    mtime = rv["modified"]

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
        parent_id = None
    else:
        parent_id = rv["parent"]

    await query.execute(
        """
        SELECT mime_type, hash, size
        FROM files
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = await query.fetchone()
    is_folder = rv is None
    mime_type = "" if is_folder else rv["mime_type"]
    hash_ = "" if is_folder else rv["hash"]
    size = 0 if is_folder else rv["size"]

    await query.execute(
        """
        SELECT width, height
        FROM images
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = await query.fetchone()
    is_image = rv is not None
    width = rv["width"] if rv else 0
    height = rv["height"] if rv else 0

    await query.execute(
        """
        SELECT width, height, ms_duration
        FROM videos
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = await query.fetchone()
    is_video = rv is not None
    width = rv["width"] if rv else 0
    height = rv["height"] if rv else 0
    ms_duration = rv["ms_duration"] if rv else 0

    await query.execute(
        """
        SELECT key, value
        FROM private
        WHERE id=?;
        """,
        (node_id,),
    )
    rv = await query.fetchall()
    private = None if not rv else {_["key"]: _["value"] for _ in rv}

    return Node(
        id=node_id,
        parent_id=parent_id,
        name=name,
        ctime=datetime.fromtimestamp(ctime, UTC),
        mtime=datetime.fromtimestamp(mtime, UTC),
        is_directory=is_folder,
        is_trashed=trashed,
        is_image=is_image,
        is_video=is_video,
        mime_type=mime_type,
        size=size,
        hash=hash_,
        width=width,
        height=height,
        ms_duration=ms_duration,
        private=private,
    )


async def inner_insert_node(query: Cursor, node: Node) -> None:
    # add this node
    await query.execute(
        """
        INSERT OR REPLACE INTO nodes
        (id, name, trashed, created, modified)
        VALUES
        (?, ?, ?, ?, ?)
        ;""",
        (
            node.id,
            node.name,
            node.is_trashed,
            int(node.ctime.timestamp()),
            int(node.mtime.timestamp()),
        ),
    )

    # add file information
    if not node.is_directory:
        await query.execute(
            """
            INSERT OR REPLACE INTO files
            (id, mime_type, hash, size)
            VALUES
            (?, ?, ?, ?)
            ;""",
            (node.id, node.mime_type, node.hash, node.size),
        )

    # remove old parentage
    await query.execute(
        """
        DELETE FROM parentage
        WHERE child=?
        ;""",
        (node.id,),
    )
    # add parentage if there is any
    if node.parent_id:
        await query.execute(
            """
            INSERT INTO parentage
            (parent, child)
            VALUES
            (?, ?)
            ;""",
            (node.parent_id, node.id),
        )

    # add image information
    if node.is_image:
        await query.execute(
            """
            INSERT OR REPLACE INTO images
            (id, width, height)
            VALUES
            (?, ?, ?)
            ;""",
            (node.id, node.width, node.height),
        )

    # add video information
    if node.is_video:
        await query.execute(
            """
            INSERT OR REPLACE INTO videos
            (id, width, height, ms_duration)
            VALUES
            (?, ?, ?, ?)
            ;""",
            (node.id, node.width, node.height, node.ms_duration),
        )

    # remove old private
    await query.execute(
        """
        DELETE FROM private
        WHERE id=?
        ;""",
        (node.id,),
    )
    # add private information if any
    if node.private:
        for key, value in node.private.items():
            await query.execute(
                """
                INSERT INTO private
                (id, key, value)
                VALUES
                (?, ?, ?)
                ;""",
                (node.id, key, value),
            )
