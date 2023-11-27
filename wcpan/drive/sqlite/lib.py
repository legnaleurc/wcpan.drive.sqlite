from wcpan.drive.core.types import Node

from ._lib import read_only, inner_get_node_by_id


async def get_uploaded_size(dsn: str, begin: int, end: int) -> int:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT SUM(size) AS sum
            FROM files
                INNER JOIN nodes ON files.id = nodes.id
            where created >= ? AND created < ?
            ;""",
            (begin, end),
        )
        rv = await query.fetchone()
        if not rv:
            return 0
        if rv["sum"] is None:
            return 0
        return rv["sum"]


async def find_orphan_nodes(dsn: str) -> list[Node]:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT nodes.id AS id
            FROM parentage
                LEFT OUTER JOIN nodes ON parentage.child=nodes.id
            WHERE parentage.parent IS NULL
            ;"""
        )
        rv = await query.fetchall()
        raw_query = (await inner_get_node_by_id(query, _["id"]) for _ in rv)
        nodes = [_ async for _ in raw_query if _]
    return nodes


async def find_multiple_parents_nodes(dsn: str) -> list[Node]:
    async with read_only(dsn) as query:
        await query.execute(
            """
            SELECT child, COUNT(child) AS parent_count
            FROM parentage
            GROUP BY child
            HAVING parent_count > 1
            ;"""
        )
        rv = await query.fetchall()
        raw_query = (await inner_get_node_by_id(query, _["child"]) for _ in rv)
        nodes = [_ async for _ in raw_query if _]
    return nodes
