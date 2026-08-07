"""The vault registry MECHANISM -- and the one property the whole per-domain shape
exists for: a new domain is a new FILE, never an edit to an existing one.

WHY THAT PROPERTY IS TESTED AND NOT ASSERTED IN PROSE. The plan's scope boundary
stakes DV2's extensibility claim on wave 2 adding `hub_account`, `hub_customer` and
`link_payment` with a diff of "+N files, 0 modified". A registry holding its own
table list would have to be edited to register them, and the demonstration would be
false on the one file that matters -- and it cannot be demonstrated retroactively,
because the git history is the evidence. `test_a_new_domain_is_discovered_without_
editing_any_existing_file` builds a throwaway domain package in `tmp_path` and shows
it registering through the same entry point `opl.vault.domains` uses, with nothing
in `src/` touched.

DISCOVERY IS BY DIRECTORY SCAN, and the alternatives that would have failed the
claim are worth naming because each looks tidier: a list of module names in
`domains/__init__.py` (wave 2 edits `__init__.py`), an `import` per domain at the
foot of `registry.py` (wave 2 edits `registry.py`), a `[project.entry-points]` table
(wave 2 edits `pyproject.toml`). All three are "0 modified" only if you do not count
the file that does the counting."""
from __future__ import annotations

import sys

import pytest

from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.registry import (
    BusinessKeyColumn,
    Hub,
    Satellite,
    VaultDomain,
    build_registry,
    discover_domains,
    parent_hub,
)

_HUB = Hub(
    name="hub_thing",
    hash_key="hub_thing_hk",
    business_keys=(BusinessKeyColumn(name="thing_id", width=8),),
)
_SAT = Satellite(name="sat_thing_dados", parent="hub_thing", payload_columns=("colour",))


def _domain(*tables, name="probe") -> VaultDomain:
    return VaultDomain(name=name, tables=tables)


def test_a_new_domain_of_hubs_and_satellites_is_discovered_without_editing_any_file(
    tmp_path,
):
    """THE D5 PROOF, AND EXACTLY AS MUCH OF IT AS IS TRUE. A package that did not
    exist when the mechanism was written, dropped in as one file, registers its tables
    through it.

    WHAT THIS COVERS AND WHAT IT DOES NOT, stated because the claim in the plan is
    broader than what holds today and the Task 3 review was right to say so. It covers
    a domain made of HUBS AND SATELLITES -- which is `hub_account` and `hub_customer`,
    two of wave 2's three. It does NOT cover a domain introducing a new table KIND:
    `VaultTable = Hub | Satellite` and `VaultDomain` refuses anything else, so a
    `Link` spec and its guards land in `registry.py`. **Task 4 adds
    `link_empresa_estabelecimento` and is where the `Link` kind belongs** -- that is
    inside wave 1, which is fine, and it is why wave 2's `link_payment` will find the
    kind already there.

    The throwaway package mirrors `opl/vault/domains/` exactly: an `__init__.py` and
    one module exposing a module-level `DOMAIN`. Nothing registers by import SIDE
    EFFECT -- `DOMAIN` is a value that `discover_domains` reads -- which is what lets
    this test run without mutating the real registry, and what lets the whole-set
    guards below run over every domain at once instead of incrementally in whatever
    order the filesystem happened to yield."""
    package = tmp_path / "probe_domains"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "payments.py").write_text(
        "from opl.vault.registry import BusinessKeyColumn, Hub, Satellite, VaultDomain\n"
        "HUB = Hub(name='hub_account', hash_key='hub_account_hk',\n"
        "          business_keys=(BusinessKeyColumn(name='account_id'),))\n"
        "SAT = Satellite(name='sat_account_dados', parent='hub_account',\n"
        "                payload_columns=('status',))\n"
        "DOMAIN = VaultDomain(name='payments', tables=(HUB, SAT))\n",
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    try:
        discovered = discover_domains([str(package)], "probe_domains")
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("probe_domains.payments", None)
        sys.modules.pop("probe_domains", None)

    registry = build_registry(discovered)

    assert [domain.name for domain in discovered] == ["payments"]
    assert sorted(registry) == ["hub_account", "sat_account_dados"]


def test_the_cnpj_domain_registers_its_two_wave_one_tables():
    """The real package, through the real entry point. Pinned as literals for the
    reason the bronze registry pins its four table names: a rename is a re-keying of
    everything downstream and should cost a deliberate edit here."""
    assert sorted(domains.REGISTRY) == ["hub_empresa", "sat_empresa_dados"]
    assert domains.table_spec("hub_empresa").hash_key == "hub_empresa_hk"


def test_asking_for_a_table_that_is_not_registered_names_the_ones_that_are():
    with pytest.raises(ValueError, match="hub_empresa"):
        domains.table_spec("hub_conta")


def test_a_satellite_takes_its_hash_key_from_its_parent_hub():
    """The satellite spec carries NO hash-key field, and that is the point: a
    satellite whose hash key is spelled independently of its hub's is a satellite
    that can be pointed at nothing, silently, by a typo. Resolving it through the
    parent makes the two unable to disagree."""
    registry = build_registry([_domain(_HUB, _SAT)])

    assert parent_hub(registry, registry["sat_thing_dados"]).hash_key == "hub_thing_hk"
    assert parent_hub(registry, registry["sat_thing_dados"]) is registry["hub_thing"]


def test_the_cnpj_satellite_resolves_to_the_cnpj_hub():
    assert domains.parent_hub(domains.table_spec("sat_empresa_dados")).name == "hub_empresa"


def test_a_satellite_whose_parent_is_not_registered_is_refused():
    """Across domains as well as within one: `build_registry` sees every domain at
    once, so this refusal does not depend on the order the filesystem yielded the
    modules in."""
    with pytest.raises(ValueError, match="hub_thing"):
        build_registry([_domain(_SAT)])


def test_a_satellite_whose_parent_is_another_satellite_is_refused():
    """A satellite hangs off a hub or a link, never off another satellite. Without
    this the parent lookup would succeed and the satellite would key on a column its
    'parent' does not have."""
    other = Satellite(name="sat_other", parent="sat_thing_dados", payload_columns=("x",))

    with pytest.raises(ValueError, match="not a hub"):
        build_registry([_domain(_HUB, _SAT, other)])


def test_two_domains_claiming_the_same_table_name_are_refused():
    """The collision that only exists BECAUSE the registry is per-domain, and the
    reason the guards run over the whole set rather than per module: each domain
    file is individually valid and neither author sees the other's."""
    with pytest.raises(ValueError, match="hub_thing"):
        build_registry([_domain(_HUB, name="a"), _domain(_HUB, name="b")])


def test_two_domains_with_the_same_name_are_refused():
    with pytest.raises(ValueError, match="probe"):
        build_registry([_domain(_HUB), _domain(_SAT)])


def test_a_domain_with_no_tables_is_refused():
    """A module in the domains package that registers nothing is a module that was
    left half-written, not a domain."""
    with pytest.raises(ValueError, match="no tables"):
        VaultDomain(name="empty", tables=())


def test_a_hub_with_no_business_key_is_refused():
    with pytest.raises(ValueError, match="at least one"):
        Hub(name="hub_x", hash_key="hub_x_hk", business_keys=())


def test_a_bare_string_payload_is_refused():
    """`payload_columns="razao_social"` is a `Sequence[str]` structurally, so no type
    checker catches it and it iterates to twelve one-character column names."""
    with pytest.raises(TypeError, match="bare str"):
        Satellite(name="sat_x", parent="hub_x", payload_columns="razao_social")


def test_a_repeated_payload_column_is_refused():
    with pytest.raises(ValueError, match="more than once"):
        Satellite(name="sat_x", parent="hub_x", payload_columns=("a", "a"))


@pytest.mark.parametrize("reserved", [LOAD_DATE, RECORD_SOURCE, APPLIED_DATE, HASH_DIFF])
def test_a_payload_column_named_after_dv2_metadata_is_refused(reserved):
    """The satellite writes its own `load_date`, `record_source`, `applied_date` and
    `hash_diff`. A payload column of the same name would either collide on the write
    or, worse, be silently overwritten by the metadata -- so the source value would
    vanish and the column would still be there, full of plausible values."""
    with pytest.raises(ValueError, match=reserved):
        Satellite(name="sat_x", parent="hub_x", payload_columns=("a", reserved))


@pytest.mark.parametrize("reserved", [LOAD_DATE, RECORD_SOURCE])
def test_a_business_key_column_named_after_dv2_metadata_is_refused(reserved):
    with pytest.raises(ValueError, match=reserved):
        Hub(
            name="hub_x",
            hash_key="hub_x_hk",
            business_keys=(BusinessKeyColumn(name=reserved),),
        )


def test_a_hub_whose_hash_key_collides_with_a_business_key_column_is_refused():
    """`hash_key="cnpj_basico"` would write the digest over the business key it was
    derived from -- the hub would still have the right number of rows and the right
    column names, and the business key would be gone."""
    with pytest.raises(ValueError, match="cnpj_basico"):
        Hub(
            name="hub_x",
            hash_key="cnpj_basico",
            business_keys=(BusinessKeyColumn(name="cnpj_basico", width=8),),
        )


def test_a_satellite_payload_colliding_with_its_hubs_hash_key_is_refused():
    """Only visible once the parent is resolved, so it is a whole-set guard rather
    than a `__post_init__` one."""
    clash = Satellite(
        name="sat_clash", parent="hub_thing", payload_columns=("hub_thing_hk",)
    )

    with pytest.raises(ValueError, match="hub_thing_hk"):
        build_registry([_domain(_HUB, clash)])


def test_a_zero_width_business_key_is_refused():
    """`width=0` would pad to the empty string and collapse every key onto one."""
    with pytest.raises(ValueError, match="positive"):
        BusinessKeyColumn(name="x", width=0)


def test_the_specs_are_frozen():
    with pytest.raises(AttributeError):
        _HUB.name = "other"
