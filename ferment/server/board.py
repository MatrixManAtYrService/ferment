"""Serving a fermenter's board (ferment.board) to whoever is watching.

GET /board         the board now (`after`: only feed entries past that seq)
GET /board/stream  server-sent events: one `board` event right away, then one
                   per change. Each carries the whole board except the feed,
                   which holds only entries the stream hasn't sent yet, so a
                   client replaces its state and appends to its feed.

A browser's EventSource reconnects on its own; to pick the feed up where it
left off, a client passes the last `feed_seq` it saw as `after`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse

from ferment.board import Board, Snapshot

# How long the stream sits idle before sending a comment, so proxies and ssh
# tunnels see traffic and a gone client is noticed.
KEEPALIVE = 15.0


async def events(
    board: Board,
    after: int | None,
    disconnected: Callable[[], Awaitable[bool]],
    keepalive: float = KEEPALIVE,
) -> AsyncIterator[str]:
    version: int | None = None
    while not await disconnected():
        # Board.wait blocks a thread; worth one per watcher, of which a fermenter has few.
        if version is not None and await run_in_threadpool(board.wait, version, keepalive) == version:
            yield ": keepalive\n\n"
            continue
        snap = board.snapshot(after)
        version, after = snap.version, snap.feed_seq
        yield f"id: {snap.version}\nevent: board\ndata: {snap.model_dump_json()}\n\n"


def board_router(board: Board) -> APIRouter:
    router = APIRouter()

    @router.get("/board", response_model=Snapshot)
    def snapshot(after: int | None = None):
        return board.snapshot(after)

    @router.get("/board/stream")
    def stream(request: Request, after: int | None = None):
        return StreamingResponse(
            events(board, after, request.is_disconnected),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    return router
