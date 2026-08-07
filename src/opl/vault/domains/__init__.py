# src/opl/vault/domains/__init__.py
"""Every vault domain, discovered by scanning THIS DIRECTORY. Written once; wave 2
adds a domain without editing it.

THAT LAST SENTENCE IS THE WHOLE REASON THIS FILE IS THREE STATEMENTS LONG. The plan's
scope boundary stakes DV2's extensibility claim on wave 2 adding `hub_account`,
`hub_customer` and `link_payment` with a diff of "+N files, 0 modified" -- a claim the
git history either shows or does not, and which cannot be made retroactively. A list
of module names here would be the file that breaks it, so there is no list: `__path__`
is the package's own directory and `discover_domains` imports whatever is in it. Drop
`payments.py` beside `cnpj.py` and it is registered.

THE GUARDS RUN AT IMPORT, over every domain at once, which is why `build_registry` is
called here rather than lazily on first lookup. A registry with two domains claiming
one table name, or a satellite naming a hub nobody declares, breaks the import of every
module that reads the registry -- the same placement and the same argument as
`opl.bronze.registry`'s guard block: a CI test protects a merge, not the ad-hoc run of
a branch whose tests have not been run, and that is exactly how these jobs get launched
while a phase is in flight."""
from __future__ import annotations

from collections.abc import Mapping

from opl.vault.registry import Hub, Satellite, VaultTable, build_registry, discover_domains
from opl.vault.registry import parent_hub as _parent_hub
from opl.vault.registry import table_spec as _table_spec

DOMAINS = discover_domains(__path__, __name__)
REGISTRY: Mapping[str, VaultTable] = build_registry(DOMAINS)


def table_spec(name: str) -> VaultTable:
    """The registered spec for `name`, or refuse naming the alternatives."""
    return _table_spec(REGISTRY, name)


def parent_hub(satellite: Satellite) -> Hub:
    """The hub a registered satellite hangs off."""
    return _parent_hub(REGISTRY, satellite)
