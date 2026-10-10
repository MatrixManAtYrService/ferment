"""Thin HTTP client for a fermenter server (same shape as eda's and ekg's)."""

from __future__ import annotations

import os
import sys

import httpx

# A clock change restarts the whole workload and waits for readiness.
DEFAULT_TIMEOUT = 600.0


def server_request(
    base_url: str,
    path: str,
    params: dict | None = None,
    json: dict | None = None,
    method: str | None = None,
) -> dict:
    """POST when a json body is given, GET otherwise; exit non-zero on failure."""
    url = f"{base_url}{path}"
    timeout = float(os.environ.get("FERMENT_TIMEOUT", DEFAULT_TIMEOUT))
    if method is None:
        method = "POST" if json is not None else "GET"
    try:
        response = httpx.request(method, url, params=params, json=json, timeout=timeout)
    except httpx.ConnectError:
        print(
            f"Error: cannot reach the fermenter at {base_url}.\n"
            "Is the tunnel up? (the attach command must stay running in another terminal)",
            file=sys.stderr,
        )
        sys.exit(1)
    except httpx.ReadTimeout:
        print(f"Error: request to {url} timed out after {timeout}s (FERMENT_TIMEOUT).", file=sys.stderr)
        sys.exit(3)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        print(f"Error {response.status_code}: {detail}", file=sys.stderr)
        sys.exit(2)
    return response.json()
