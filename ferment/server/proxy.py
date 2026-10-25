"""Serving another HTTP server (on the workload VM, say) under a prefix of the
fermenter's, so attached clients reach it through the tunnel they have.

Bytes go both ways as they came (still encoded), streamed; hop-by-hop headers
don't. `token`, if given, supplies a bearer token for calls under `auth_paths`
that come without one, and is asked for a fresh one once on a 401.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

# Not passed through either way (they describe one hop, not the message).
HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "host", "content-length"}


def proxy_router(
    prefix: str,
    base: str,
    token: Callable[[httpx.AsyncClient], Awaitable[str]] | None = None,
    auth_paths: tuple[str, ...] = ("",),
    client: httpx.AsyncClient | None = None,
) -> APIRouter:
    """`prefix` here (e.g. /egress) is `base` there (e.g. http://host:44445)."""
    router = APIRouter()
    http = client or httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(30, read=300))
    cached: dict[str, str] = {}
    lock = asyncio.Lock()

    async def bearer(fresh: bool) -> str:
        async with lock:
            if fresh or "value" not in cached:
                cached["value"] = await token(http)
            return cached["value"]

    @router.api_route(prefix + "/{path:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request):
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP}
        # As the caller asked, or not at all: httpx would otherwise ask for gzip
        # on its behalf, and the bytes go back as they came.
        headers.setdefault("accept-encoding", request.headers.get("accept-encoding", "identity"))
        body = await request.body()
        add = token is not None and path.startswith(auth_paths) and "authorization" not in {k.lower() for k in headers}
        for attempt in range(2):
            if add:
                headers["authorization"] = f"Bearer {await bearer(fresh=attempt > 0)}"
            req = http.build_request(
                request.method, "/" + path, params=request.query_params, headers=headers, content=body
            )
            try:
                upstream = await http.send(req, stream=True)
            except httpx.HTTPError as e:
                return Response(f"{base} isn't answering: {e}\n", status_code=502)
            if upstream.status_code == 401 and add and attempt == 0:
                await upstream.aclose()
                continue  # expired: once more with a new one
            break
        out = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP}
        return StreamingResponse(
            _raw(upstream), status_code=upstream.status_code, headers=out, background=BackgroundTask(upstream.aclose)
        )

    return router


async def _raw(response: httpx.Response):
    """The body as it came (still encoded), streamed; or, if it was already read, at once."""
    if response.is_stream_consumed:
        yield response.content
        return
    async for chunk in response.aiter_raw():
        yield chunk
