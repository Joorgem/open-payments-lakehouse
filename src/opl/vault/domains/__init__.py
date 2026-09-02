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

from opl.vault.registry import (
    EffectivitySatellite,
    Hub,
    Link,
    Satellite,
    VaultTable,
    build_registry,
    discover_domains,
)
from opl.vault.registry import link_identity_columns as _link_identity_columns
from opl.vault.registry import linked_hubs as _linked_hubs
from opl.vault.registry import parent_hub as _parent_hub
from opl.vault.registry import parent_link as _parent_link
from opl.vault.registry import parent_of as _parent_of
from opl.vault.registry import table_spec as _table_spec

DOMAINS = discover_domains(__path__, __name__)
REGISTRY: Mapping[str, VaultTable] = build_registry(DOMAINS)


def table_spec(name: str) -> VaultTable:
    """The registered spec for `name`, or refuse naming the alternatives."""
    return _table_spec(REGISTRY, name)


def parent_of(satellite: Satellite) -> Hub | Link:
    """The hub OR LINK a registered satellite hangs off -- the resolution
    `load_satellite` needs, since a satellite's parent may be either since F2 wave 2.

    A SECOND RESOLVER BESIDE `parent_hub` AND NOT A WIDENING OF IT. The two answer
    different questions and both have callers: this one answers "what is this satellite
    keyed on", which is all a satellite loader needs; `parent_hub` answers "which hub's
    business key does this satellite's history hang on", which is what an SCD2 dimension
    and a PIT spine need and which a link parent has no answer to. Widening the one name
    would have handed `opl.gold.registry_guards` a `Link` where it reads
    `business_key_columns`, i.e. an `AttributeError` several frames from the declaration
    that caused it."""
    return _parent_of(REGISTRY, satellite)


def parent_hub(satellite: Satellite) -> Hub:
    """The HUB a registered satellite hangs off, refusing one parented on a link."""
    return _parent_hub(REGISTRY, satellite)


def linked_hubs(link: Link) -> tuple[Hub, ...]:
    """The hubs a registered link joins, in its declaration order -- which is the order
    its hash key is taken in, so it is the answer rather than a detail of it. One entry
    per END, so a self-referencing link yields the same hub twice."""
    return _linked_hubs(REGISTRY, link)


def parent_link(satellite: EffectivitySatellite) -> Link:
    """The link a registered effectivity satellite hangs off."""
    return _parent_link(REGISTRY, satellite)


def link_identity_columns(link: Link) -> tuple[str, ...]:
    """The source columns a registered link's own hash key is taken over, in hash
    order -- the link's grain, which is the grain its effectivity satellite's
    observation ledger must be keyed on."""
    return _link_identity_columns(REGISTRY, link)
