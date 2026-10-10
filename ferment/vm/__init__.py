"""VM providers. Pick one by name with `get_provider`."""

from __future__ import annotations

from ferment.vm.base import Provider, ProviderError, SshTarget, VMInfo, VMSpec


class NotImplementedProvider(Provider):
    """Placeholder for a provider ferment means to support but doesn't yet."""

    def __init__(self, name: str, **_options):
        self.name = name

    def _no(self, *_args, **_kwargs):
        raise NotImplementedError(f"the {self.name!r} VM provider is not implemented yet")

    create = list = delete = set_deadline = ssh_target = _no  # type: ignore[assignment]


def get_provider(name: str, **options) -> Provider:
    if name == "lima":
        from ferment.vm.lima import LimaProvider

        return LimaProvider(**options)
    if name in ("gcp", "aws"):
        return NotImplementedProvider(name)
    raise ProviderError(f"unknown VM provider {name!r} (known: lima, gcp, aws)")


__all__ = ["Provider", "ProviderError", "SshTarget", "VMInfo", "VMSpec", "get_provider"]
