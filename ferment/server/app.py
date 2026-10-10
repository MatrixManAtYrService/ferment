"""The generic part of a fermenter server: health, status, and the clock.

A concrete fermenter builds on `create_app` and adds its own routes.
"""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ferment.server.time_control import TimeController


class SetTime(BaseModel):
    at: dt.datetime


def create_app(controller: TimeController, title: str = "fermenter") -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        controller.resume()
        yield
        controller.stop()

    app = FastAPI(title=title, lifespan=lifespan)
    app.state.controller = controller

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.get("/status")
    def status():
        return controller.status()

    # Plain `def`: the restart blocks (stop, relaunch, wait for readiness), so
    # FastAPI runs it in a worker thread. The controller's lock serializes
    # concurrent requests from multiple attached clients.
    @app.post("/time")
    def set_time(req: SetTime):
        if req.at.tzinfo is None:
            raise HTTPException(422, "give a timezone, e.g. 2026-10-03T00:00:00Z")
        return controller.set_time(req.at)

    return app
