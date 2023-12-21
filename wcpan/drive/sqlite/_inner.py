from datetime import datetime, UTC
from sqlite3 import Cursor

from wcpan.drive.core.types import Node


def inner_set_metadata(query: Cursor, key: str, value: str) -> None:
    query.execute(
        """
        INSERT OR REPLACE INTO metadata
        VALUES (?, ?)
        ;""",
        (key, value),
    )


def inner_get_node_by_id(
    query: Cursor,
    node_id: str,
) -> Node | None:
    query.execute(
        """
        SELECT name, trashed, created, modified
        FROM nodes
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = query.fetchone()
    if not rv:
        return None
    name = rv["name"]
    trashed = bool(rv["trashed"])
    ctime = rv["created"]
    mtime = rv["modified"]

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
        parent_id = None
    else:
        parent_id = rv["parent"]

    query.execute(
        """
        SELECT mime_type, hash, size
        FROM files
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = query.fetchone()
    is_folder = rv is None
    mime_type = "" if is_folder else rv["mime_type"]
    hash_ = "" if is_folder else rv["hash"]
    size = 0 if is_folder else rv["size"]

    width = 0
    height = 0
    ms_duration = 0

    query.execute(
        """
        SELECT width, height
        FROM images
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = query.fetchone()
    is_image = rv is not None
    if rv:
        width = rv["width"]
        height = rv["height"]

    query.execute(
        """
        SELECT width, height, ms_duration
        FROM videos
        WHERE id=?
        ;""",
        (node_id,),
    )
    rv = query.fetchone()
    is_video = rv is not None
    if rv:
        width = rv["width"]
        height = rv["height"]
        ms_duration = rv["ms_duration"]

    query.execute(
        """
        SELECT key, value
        FROM private
        WHERE id=?;
        """,
        (node_id,),
    )
    rv = query.fetchall()
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


def inner_insert_node(query: Cursor, node: Node) -> None:
    # add this node
    query.execute(
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
        query.execute(
            """
            INSERT OR REPLACE INTO files
            (id, mime_type, hash, size)
            VALUES
            (?, ?, ?, ?)
            ;""",
            (node.id, node.mime_type, node.hash, node.size),
        )

    # remove old parentage
    query.execute(
        """
        DELETE FROM parentage
        WHERE child=?
        ;""",
        (node.id,),
    )
    # add parentage if there is any
    if node.parent_id:
        query.execute(
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
        query.execute(
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
        query.execute(
            """
            INSERT OR REPLACE INTO videos
            (id, width, height, ms_duration)
            VALUES
            (?, ?, ?, ?)
            ;""",
            (node.id, node.width, node.height, node.ms_duration),
        )

    # remove old private
    query.execute(
        """
        DELETE FROM private
        WHERE id=?
        ;""",
        (node.id,),
    )
    # add private information if any
    if node.private:
        for key, value in node.private.items():
            query.execute(
                """
                INSERT INTO private
                (id, key, value)
                VALUES
                (?, ?, ?)
                ;""",
                (node.id, key, value),
            )


def inner_get_metadata(query: Cursor, key: str) -> str | None:
    query.execute("SELECT value FROM metadata WHERE key = ?;", (key,))
    rv = query.fetchone()
    if not rv:
        return None
    return rv["value"]


def inner_delete_node_by_id(query: Cursor, node_id: str) -> None:
    # remove from private
    query.execute(
        """
        DELETE FROM private
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from videos
    query.execute(
        """
        DELETE FROM videos
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from images
    query.execute(
        """
        DELETE FROM images
        WHERE id=?
        ;""",
        (node_id,),
    )

    # disconnect parents
    query.execute(
        """
        DELETE FROM parentage
        WHERE child=? OR parent=?
        ;""",
        (node_id, node_id),
    )

    # remove from files
    query.execute(
        """
        DELETE FROM files
        WHERE id=?
        ;""",
        (node_id,),
    )

    # remove from nodes
    query.execute(
        """
        DELETE FROM nodes
        WHERE id=?
        ;""",
        (node_id,),
    )
