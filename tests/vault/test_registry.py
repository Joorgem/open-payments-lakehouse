"""The vault registry MECHANISM -- and the one property the whole per-domain shape
exists for: a new domain is a new FILE, never an edit to an existing one.

WHY THAT PROPERTY IS TESTED AND NOT ASSERTED IN PROSE. The plan's scope boundary
stakes DV2's extensibility claim on wave 2 adding `hub_account`, `hub_customer` and
`link_payment` with a diff of "+N files, 0 modified". A registry holding its own
table list would have to be edited to register them, and the demonstration would be
false on the one file that matters -- and it cannot be demonstrated retroactively,
because the git history is the evidence. `test_a_new_domain_of_hubs_satellites_and_
links_is_discovered_without_editing_any_file` builds a throwaway domain package in
`tmp_path` -- carrying wave 2's three tables by name -- and shows it registering
through the same entry point `opl.vault.domains` uses, with nothing in `src/`
touched.

DISCOVERY IS BY DIRECTORY SCAN, and the alternatives that would have failed the
claim are worth naming because each looks tidier: a list of module names in
`domains/__init__.py` (wave 2 edits `__init__.py`), an `import` per domain at the
foot of `registry.py` (wave 2 edits `registry.py`), a `[project.entry-points]` table
(wave 2 edits `pyproject.toml`). All three are "0 modified" only if you do not count
the file that does the counting."""
from __future__ import annotations

import sys
from contextlib import contextmanager

import pytest

from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.vault import domains
from opl.vault.columns import (
    APPLIED_DATE,
    CLOSED_BY,
    HASH_DIFF,
    IS_ACTIVE,
    LAST_OBSERVED_ON,
    LOAD_DATE,
    RECORD_SOURCE,
)
from opl.vault.domains.cnpj import COMPANY_PARTNER_GRAIN, REFERENCE_TABLES
from opl.vault.registry import (
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
    Link,
    LinkEnd,
    ReferenceTable,
    Satellite,
    VaultDomain,
    build_registry,
    discover_domains,
    link_identity_columns,
    linked_hubs,
    parent_hub,
)

_HUB = Hub(
    name="hub_thing",
    hash_key="hub_thing_hk",
    business_keys=(BusinessKeyColumn(name="thing_id", width=8),),
)
_SAT = Satellite(name="sat_thing_dados", parent="hub_thing", payload_columns=("colour",))
_OTHER_HUB = Hub(
    name="hub_other",
    hash_key="hub_other_hk",
    business_keys=(BusinessKeyColumn(name="other_id"),),
)
_LINK = Link(
    name="link_thing_other", hash_key="link_thing_other_hk", hubs=("hub_thing", "hub_other")
)


def _domain(*tables, name="probe") -> VaultDomain:
    return VaultDomain(name=name, tables=tables)


# The throwaway domain the D5 proof drops into `tmp_path`: WAVE 2'S THREE TABLES BY
# NAME, so the claim is exercised on the actual list the plan stakes it on.
_PROBE_DOMAIN_SOURCE = """\
from opl.vault.registry import BusinessKeyColumn, Hub, Link, Satellite, VaultDomain
HUB = Hub(name='hub_account', hash_key='hub_account_hk',
          business_keys=(BusinessKeyColumn(name='account_id'),))
CUSTOMER = Hub(name='hub_customer', hash_key='hub_customer_hk',
               business_keys=(BusinessKeyColumn(name='customer_id'),))
SAT = Satellite(name='sat_account_dados', parent='hub_account',
                payload_columns=('status',))
LINK = Link(name='link_payment', hash_key='link_payment_hk',
            hubs=('hub_account', 'hub_customer'))
DOMAIN = VaultDomain(name='payments', tables=(HUB, CUSTOMER, SAT, LINK))
"""


def _probe_package(tmp_path, name: str, modules: dict[str, str]):
    """A throwaway domains package in `tmp_path`, with `modules` as its module bodies.

    Shared by the D5 proof and by the four discovery refusals below, so all five go
    through the same shape a real `opl/vault/domains/` has: a package directory, an
    `__init__.py`, and one file per module."""
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    for module, body in modules.items():
        (package / f"{module}.py").write_text(body, encoding="utf-8")
    return package


@contextmanager
def _importable(tmp_path, package):
    """`package` importable by name for the duration, and gone from `sys.modules`
    afterwards.

    THE CLEANUP IS NOT HOUSEKEEPING. `discover_domains` IMPORTS what it finds, so a
    probe module left in `sys.modules` would be served from cache to the next test that
    happens to use the same package name -- and these tests deliberately reuse names to
    build the same package with different contents."""
    sys.path.insert(0, str(tmp_path))
    try:
        yield
    finally:
        sys.path.remove(str(tmp_path))
        for module in [
            name for name in sys.modules
            if name == package.name or name.startswith(f"{package.name}.")
        ]:
            sys.modules.pop(module, None)


def test_a_new_domain_of_hubs_satellites_and_links_is_discovered_without_editing_any_file(
    tmp_path,
):
    """THE D5 PROOF, AND EXACTLY AS MUCH OF IT AS IS TRUE. A package that did not
    exist when the mechanism was written, dropped in as one file, registers its tables
    through it.

    WHAT THIS COVERS AND WHAT IT DOES NOT. It covers a domain made of HUBS, SATELLITES
    AND LINKS, and the throwaway module below is wave 2's list BY NAME --
    `hub_account`, `hub_customer`, `link_payment` -- so the plan's claim is exercised
    kind for kind rather than by analogy. Task 3 could only claim two of the three,
    because `VaultTable` was `Hub | Satellite` then; Task 4 added the `Link` kind and
    its guards to `registry.py`, an edit INSIDE wave 1 that the plan always expected.
    It still does NOT cover a domain introducing a FOURTH kind: `VaultTable` and
    `VaultDomain.__post_init__` refuse anything else, so a new kind lands in
    `registry.py` exactly as `Link` did.

    The throwaway package mirrors `opl/vault/domains/` exactly. Nothing registers by
    import SIDE EFFECT -- `DOMAIN` is a value that `discover_domains` reads -- which is
    what lets this run without mutating the real registry, and what lets the whole-set
    guards run over every domain at once rather than in filesystem order."""
    package = _probe_package(tmp_path, "probe_domains", {"payments": _PROBE_DOMAIN_SOURCE})
    with _importable(tmp_path, package):
        discovered = discover_domains([str(package)], "probe_domains")

    registry = build_registry(discovered)

    assert [domain.name for domain in discovered] == ["payments"]
    assert sorted(registry) == [
        "hub_account", "hub_customer", "link_payment", "sat_account_dados"
    ]
    assert [hub.name for hub in linked_hubs(registry, registry["link_payment"])] == [
        "hub_account", "hub_customer"
    ]


# --------------------------------------------------------------------------- #
# The four ways discovery can go wrong. THE MECHANISM THE EXTENSIBILITY CLAIM RESTS
# ON, and every one of these was unasserted: each failure leaves wave 2's tables
# silently unregistered, surfacing later as `UnknownVaultTable` from a JOB, which
# points at the job rather than at the typo.
# --------------------------------------------------------------------------- #

def test_a_module_in_the_domains_package_binding_no_DOMAIN_is_refused(tmp_path):
    """"THE WHOLE DIFFERENCE BETWEEN DISCOVERY AND GUESSWORK", in `discover_domains`'
    own words, and until now nothing held it.

    A domain file whose constant is misspelled -- `DOMAINS`, `Domain`, or a module
    someone started and left half-written -- would otherwise be found, imported,
    contribute nothing, and leave every table it declares unregistered with the import
    succeeding. Skipping it is the tempting behaviour and it is the wrong one: the
    module is IN the domains package, so it has already declared its intent."""
    package = _probe_package(tmp_path, "probe_nodomain", {
        "payments": _PROBE_DOMAIN_SOURCE.replace("DOMAIN =", "DOMAINS ="),
    })

    with _importable(tmp_path, package):
        with pytest.raises(ValueError, match="binds no DOMAIN"):
            discover_domains([str(package)], "probe_nodomain")


def test_a_module_whose_DOMAIN_is_not_a_vault_domain_is_refused(tmp_path):
    """The near miss of the one above: the name is right and the VALUE is not a
    `VaultDomain`.

    A `TypeError` rather than a `ValueError`, and the distinction is the module's own:
    the name was found, so this is not a discovery failure but a shape one. Left
    unrefused it would reach `build_registry`, whose guards iterate `domain.tables` and
    would fail on whatever the object does or does not have -- an `AttributeError`
    naming neither the module nor the attribute it should have bound."""
    package = _probe_package(tmp_path, "probe_wrongtype", {
        "payments": "DOMAIN = 'payments'\n",
    })

    with _importable(tmp_path, package):
        with pytest.raises(TypeError, match="not a VaultDomain"):
            discover_domains([str(package)], "probe_wrongtype")


def test_a_domains_package_with_no_domain_module_at_all_is_refused(tmp_path):
    """An empty package is refused rather than yielding an empty tuple, because the
    consequence downstream is not empty: `build_registry(())` returns an empty mapping
    and EVERY vault job then refuses its own table name, pointing at the table.

    Reachable by a packaging mistake rather than by a typo -- a wheel built without the
    domain modules, a path handed to `discover_domains` that is not the package it was
    meant to be -- which is exactly the class of error that otherwise surfaces far from
    its cause."""
    package = _probe_package(tmp_path, "probe_empty", {})

    with _importable(tmp_path, package):
        with pytest.raises(ValueError, match="no domain module"):
            discover_domains([str(package)], "probe_empty")


def test_an_underscore_prefixed_module_is_skipped_and_does_not_have_to_be_a_domain(
    tmp_path,
):
    """The escape hatch that makes the refusal above liveable: a domains package can
    hold a shared helper without it having to pretend to be a domain.

    ASSERTED IN BOTH DIRECTIONS IN ONE TEST, because either half alone is satisfied by
    the wrong implementation. `_shared` binds no `DOMAIN`, so a discovery that did not
    skip it would RAISE -- that is the skip. And the result is exactly the one real
    domain, so a discovery that skipped it by reading it and discarding the result would
    still be wrong about what it found."""
    package = _probe_package(tmp_path, "probe_underscore", {
        "_shared": "HELPERS = ('not a domain',)\n",
        "payments": _PROBE_DOMAIN_SOURCE,
    })

    with _importable(tmp_path, package):
        discovered = discover_domains([str(package)], "probe_underscore")

    assert [domain.name for domain in discovered] == ["payments"]


def test_the_registered_tables_are_the_three_domains_tables():
    """The real package, through the real entry point. Pinned as literals for the
    reason the bronze registry pins its four table names: a rename is a re-keying of
    everything downstream and should cost a deliberate edit here.

    TWO DOMAINS SINCE F-DB, WHICH IS THE ONE THING THIS LIST NOW PROVES THAT IT DID NOT.
    The four `*_merchant*` entries are declared in `opl/vault/domains/merchant_domain.py`
    and nothing in `opl/vault/` names that module: discovery is a directory scan, so a
    second domain is registered by existing. That is the "+1 file, 0 modified" claim
    holding at the level it was made -- `domains/__init__.py` and `registry.py`'s
    discovery are untouched -- and this assertion is where a domain that stopped being
    discovered would show up as four missing names rather than as a job failing.

    AND THREE SINCE F2 WAVE 2, ON THE SAME MECHANISM AND WITH ONE FEWER TABLE TO SHOW IT.
    `link_payment` is `opl/vault/domains/payments_domain.py`'s whole contribution, and no
    file under `opl/vault/` names that module either -- so the claim is made a second time
    against a domain of ONE table, where a single missing name is the whole evidence.

    ADDING A NAME HERE IS THE DELIBERATE EDIT THIS TEST IS FOR, not a lock to route
    around: the paragraph above says so about a RENAME, and an addition is the same act.
    It is `sorted(REGISTRY)` rather than a subset check so that the other direction costs
    just as much -- a name here that no domain declares fails equally loudly."""
    assert sorted(domains.REGISTRY) == [
        "hub_empresa",
        "hub_estabelecimento",
        "hub_merchant",
        "link_company_partner",
        "link_empresa_estabelecimento",
        "link_merchant_empresa",
        "link_payment",
        "ref_cnae",
        "ref_motivo",
        "ref_municipio",
        "ref_natureza_juridica",
        "ref_pais",
        "ref_qualificacao",
        "sat_eff_company_partner",
        "sat_eff_merchant_empresa",
        "sat_empresa_dados",
        "sat_estabelecimento_dados",
        "sat_estabelecimento_endereco",
        "sat_merchant_dados",
    ]
    assert domains.table_spec("hub_empresa").hash_key == "hub_empresa_hk"
    assert domains.table_spec("hub_estabelecimento").business_key_columns == (
        "cnpj_basico", "cnpj_ordem", "cnpj_dv"
    )


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


# --------------------------------------------------------------------------- #
# The link kind, added in Task 4 with `link_empresa_estabelecimento`
# --------------------------------------------------------------------------- #

def test_a_link_resolves_to_its_hubs_in_declaration_order():
    """ORDER IS THE LINK'S IDENTITY, not a presentation detail: the link's own hash
    key is the standard applied to the participating hubs' business keys CONCATENATED
    IN THIS ORDER, so a resolver that returned them sorted, or in registry order,
    would re-key every link whose hubs happen not to be alphabetical."""
    registry = build_registry([_domain(_HUB, _OTHER_HUB, _LINK)])

    resolved = linked_hubs(registry, registry["link_thing_other"])

    assert [hub.name for hub in resolved] == ["hub_thing", "hub_other"]
    assert resolved[0] is registry["hub_thing"]


def test_a_link_naming_a_hub_no_domain_registers_is_refused():
    """The sibling of `test_a_satellite_whose_parent_is_not_registered_is_refused`,
    and it needs the whole set for the same reason: a link file and a hub file are
    two modules, and neither author sees the other's."""
    with pytest.raises(ValueError, match="hub_other"):
        build_registry([_domain(_HUB, _LINK)])


def test_a_link_naming_a_satellite_as_one_of_its_hubs_is_refused():
    """A satellite has no business key of its own, so the link's hash key could not
    be spelled -- but the lookup would SUCCEED, and the failure would arrive several
    layers away as a missing column."""
    bad = Link(name="link_x", hash_key="link_x_hk", hubs=("hub_thing", "sat_thing_dados"))

    with pytest.raises(ValueError, match="not a hub"):
        build_registry([_domain(_HUB, _SAT, bad)])


def test_a_descriptive_satellite_whose_parent_is_a_link_is_refused():
    """DV2 allows a DESCRIPTIVE satellite on a link and this vault still does not have
    one. The boundary stands after Task 5 added `EffectivitySatellite`, and the reason
    is unchanged: `parent_hub` returns a `Hub` and `load_satellite` takes one, so a
    link-parented `Satellite` would be a table nothing in this package can write. The
    effectivity satellite is a separate KIND precisely because it is not this."""
    on_link = Satellite(
        name="sat_on_link", parent="link_thing_other", payload_columns=("x",)
    )

    with pytest.raises(ValueError, match="not a hub"):
        build_registry([_domain(_HUB, _OTHER_HUB, _LINK, on_link)])


def test_a_link_with_fewer_than_two_identity_components_is_refused():
    """A link is a RELATIONSHIP and needs at least two things to relate. One hub and no
    dependent-child key is a hub with extra steps: its hash key would be the hub's own
    business key hashed a second time under another name.

    THE RULE COUNTS COMPONENTS AND NOT HUBS, which is the Task 5 widening. One hub plus
    a dependent-child key IS a relationship -- `link_company_partner` is exactly that --
    and the old "at least two hubs" would have refused it while refusing nothing this
    does not."""
    with pytest.raises(ValueError, match="needs at least two"):
        Link(name="link_x", hash_key="link_x_hk", hubs=("hub_thing",))


def test_a_link_with_one_hub_and_a_dependent_child_key_is_accepted():
    """The shape the rule above was widened for, asserted so the widening is not
    silently over-permissive: the identity is the hub's business key followed by the
    dependent-child key, in that order, which is the order the hash concatenates in."""
    link = Link(
        name="link_x", hash_key="link_x_hk", hubs=("hub_thing",),
        dependent_child_keys=(BusinessKeyColumn(name="line_no"),),
    )
    registry = build_registry([_domain(_HUB, _SAT, link)])

    assert link_identity_columns(registry, link) == ("thing_id", "line_no")


def test_a_link_naming_one_hub_twice_without_roles_is_refused():
    """A same-hub link is real DV2 and Task 5 built one. What is still refused is a
    same-hub link WITHOUT ROLES: both references would be written into one column
    called `hub_thing_hk` and one end of the relationship would silently be gone.

    The refusal moved from the spec to `build_registry` when roles arrived, because
    whether two ends collide depends on the HUB's hash-key name, which the spec does
    not have -- it names hubs, and the registry is what resolves them."""
    link = Link(
        name="link_x", hash_key="link_x_hk", hubs=("hub_thing", "hub_thing"),
        dependent_child_keys=(BusinessKeyColumn(name="line_no"),),
    )

    with pytest.raises(ValueError, match="twice"):
        build_registry([_domain(_HUB, _SAT, link)])


def test_a_self_referencing_link_with_roles_is_accepted_and_names_two_columns():
    """`link_company_partner` in miniature. Two ends on one hub, distinguished by role,
    so the references are two columns rather than one -- and the NON-IDENTIFYING end is
    absent from the identity, because its key is a function of the dependent-child key
    rather than a part of the link's own."""
    link = Link(
        name="link_x",
        hash_key="link_x_hk",
        hubs=(
            LinkEnd(hub="hub_thing", role="left"),
            LinkEnd(hub="hub_thing", role="right", identifying=False),
        ),
        dependent_child_keys=(BusinessKeyColumn(name="line_no"),),
    )
    registry = build_registry([_domain(_HUB, _SAT, link)])
    hubs = linked_hubs(registry, link)

    assert [end.reference_column(hub) for end, hub in zip(link.ends, hubs, strict=True)] == [
        "left_hub_thing_hk", "right_hub_thing_hk"
    ]
    assert link_identity_columns(registry, link) == ("thing_id", "line_no")
    assert [hub.name for hub in hubs] == ["hub_thing", "hub_thing"]


def test_a_link_whose_every_end_is_non_identifying_is_refused():
    """A link keyed on its dependent-child keys alone is anchored to no hub -- it is a
    hub wearing a link's name, and every reference it carries would be one it resolved
    rather than one it is keyed on."""
    with pytest.raises(ValueError, match="no identifying end"):
        Link(
            name="link_x", hash_key="link_x_hk",
            hubs=(LinkEnd(hub="hub_thing", role="left", identifying=False),),
            dependent_child_keys=(BusinessKeyColumn(name="line_no"),
                                  BusinessKeyColumn(name="part_no")),
        )


def test_a_dependent_child_key_colliding_with_a_reference_column_is_refused():
    """The quiet collision, at link grain. The loader writes one column per reference
    and one per dependent-child key; two of those sharing a name means one projection
    writes two values into one column, and the row count stays right."""
    link = Link(
        name="link_x", hash_key="link_x_hk", hubs=("hub_thing", "hub_other"),
        dependent_child_keys=(BusinessKeyColumn(name="hub_other_hk"),),
    )

    with pytest.raises(ValueError, match="twice"):
        build_registry([_domain(_HUB, _OTHER_HUB, link)])


def test_an_effectivity_satellite_on_a_hub_is_refused():
    """An effectivity satellite records when a RELATIONSHIP held, so it hangs off the
    table that asserts the relationship. Pointed at a hub it would key on a hash key
    the hub really has, which is what makes this worth refusing rather than leaving to
    fail: the table would build and would answer about the wrong thing."""
    on_hub = EffectivitySatellite(name="eff_x", parent="hub_thing", entry_column="opened")

    with pytest.raises(ValueError, match="not a link"):
        build_registry([_domain(_HUB, _SAT, on_hub)])


def test_an_effectivity_satellites_entry_column_may_not_be_one_the_loader_writes():
    """The window's OPEN is the source's own delivered value and the loader writes
    three columns of its own beside it. A collision would replace the delivered fact
    with something we inferred, keeping the column and losing the value -- which is the
    exact confusion the naming convention exists to prevent."""
    for column in (IS_ACTIVE, LAST_OBSERVED_ON, CLOSED_BY, LOAD_DATE):
        with pytest.raises(ValueError, match="loader writes that itself"):
            EffectivitySatellite(name="eff_x", parent="link_x", entry_column=column)


def test_an_effectivity_satellites_entry_column_may_not_be_a_key_of_its_link():
    """The other half, which needs the parent and so runs over the whole set: the open
    would be written into a key column, so the row would keep its shape and lose both
    values."""
    link = Link(
        name="link_x", hash_key="link_x_hk", hubs=("hub_thing", "hub_other"),
        dependent_child_keys=(BusinessKeyColumn(name="line_no"),),
    )
    on_key = EffectivitySatellite(name="eff_x", parent="link_x", entry_column="line_no")

    with pytest.raises(ValueError, match="dependent-child key of the parent link"):
        build_registry([_domain(_HUB, _OTHER_HUB, link, on_key)])


def test_the_cnpj_effectivity_satellite_resolves_to_the_partner_link():
    """The real package, through the real entry point -- and the grain the domain
    declares for it IS the link's own identity, which is the equality
    `opl.vault.effectivity` refuses a load on."""
    satellite = domains.table_spec("sat_eff_company_partner")
    link = domains.parent_link(satellite)

    assert link.name == "link_company_partner"
    assert domains.link_identity_columns(link) == (
        "cnpj_basico", "identificador_socio", "cpf_cnpj_socio"
    )
    assert tuple(COMPANY_PARTNER_GRAIN.key_columns) == domains.link_identity_columns(link)


def test_a_bare_string_hub_list_is_refused():
    """`hubs="hub_thing"` is a `Sequence[str]` structurally -- it would iterate to one
    "hub" per character, and the first refusal it met would be about a hub called
    `h`."""
    with pytest.raises(TypeError, match="bare str"):
        Link(name="link_x", hash_key="link_x_hk", hubs="hub_thing")


def test_a_link_whose_own_hash_key_is_one_of_its_hubs_hash_keys_is_refused():
    """The loader writes the link's digest and the hub reference into columns of the
    names below. Sharing one name means the projection writes two values into one
    column: the link keeps its right row count, and one of its two ends is silently
    the link's own digest, which joins to nothing."""
    bad = Link(name="link_x", hash_key="hub_thing_hk", hubs=("hub_thing", "hub_other"))

    with pytest.raises(ValueError, match="hub_thing_hk"):
        build_registry([_domain(_HUB, _OTHER_HUB, bad)])


def test_two_hubs_of_one_link_spelling_the_same_hash_key_column_are_refused():
    """The copy-paste shape: a second hub declared by pasting the first and renaming
    the table but not the hash key. Each hub is individually valid, and the link
    joining them would write both digests into one column."""
    twin = Hub(
        name="hub_twin",
        hash_key="hub_thing_hk",
        business_keys=(BusinessKeyColumn(name="twin_id"),),
    )
    bad = Link(name="link_x", hash_key="link_x_hk", hubs=("hub_thing", "hub_twin"))

    with pytest.raises(ValueError, match="hub_thing_hk"):
        build_registry([_domain(_HUB, twin, bad)])


def test_a_domain_declaring_something_that_is_not_a_vault_table_is_refused():
    """`VaultDomain` refuses anything outside `VaultTable`, which is what makes the
    D5 claim's "these kinds need nothing added here" checkable: the set of kinds a
    domain may declare is one union, and `isinstance` reads it rather than restating
    it."""
    with pytest.raises(TypeError, match="not a vault table"):
        VaultDomain(name="probe", tables=(BusinessKeyColumn(name="thing_id"),))


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


# --------------------------------------------------------------------------- #
# The reference-table kind, added in Task 6 for the six CNPJ lookup types
# --------------------------------------------------------------------------- #

def test_a_reference_table_registers_with_no_relationship_to_resolve():
    """The whole finding `opl.vault.specs` argues for `ReferenceTable`: a reference table
    names no other table, so it needs no whole-set guard -- only the per-table
    `__post_init__` below, which already ran before `build_registry` was called.
    A domain of a reference table ALONE builds clean, unlike a satellite or a link,
    which both refuse when built alone (see the tests above)."""
    ref = ReferenceTable(
        name="ref_thing", lookup_type="thing", natural_key="codigo", payload="descricao"
    )
    registry = build_registry([_domain(ref)])

    assert registry["ref_thing"] is ref


def test_a_reference_table_registers_alongside_a_hub_in_the_same_domain():
    """Nothing about `VaultDomain` or `build_registry` requires a domain's tables to
    be one kind -- the CNPJ domain itself mixes hubs, links, satellites and now six
    reference tables in one `DOMAIN.tables` tuple."""
    ref = ReferenceTable(
        name="ref_thing", lookup_type="thing", natural_key="codigo", payload="descricao"
    )
    registry = build_registry([_domain(_HUB, _SAT, ref)])

    assert sorted(registry) == ["hub_thing", "ref_thing", "sat_thing_dados"]


def test_a_reference_table_with_no_name_is_refused():
    with pytest.raises(ValueError, match="name"):
        ReferenceTable(name="", lookup_type="thing", natural_key="codigo", payload="descricao")


def test_a_reference_table_with_no_lookup_type_is_refused():
    with pytest.raises(ValueError, match="lookup_type"):
        ReferenceTable(name="ref_thing", lookup_type="", natural_key="codigo", payload="descricao")


@pytest.mark.parametrize("field", ["natural_key", "payload"])
def test_a_reference_table_needs_both_a_natural_key_and_a_payload_column(field):
    kwargs = {"name": "ref_thing", "lookup_type": "thing",
              "natural_key": "codigo", "payload": "descricao"}
    kwargs[field] = ""
    with pytest.raises(ValueError, match=field.replace("_", " ")):
        ReferenceTable(**kwargs)


def test_a_reference_table_whose_natural_key_equals_its_payload_is_refused():
    """The write would put the description where the key belongs, or the reverse --
    `codigo` and `descricao` naming the same column is not a table with one column,
    it is a spec that cannot say which value goes where."""
    with pytest.raises(ValueError, match="both"):
        ReferenceTable(
            name="ref_thing", lookup_type="thing", natural_key="codigo", payload="codigo"
        )


@pytest.mark.parametrize("reserved", [LOAD_DATE, RECORD_SOURCE])
def test_a_reference_tables_natural_key_may_not_be_dv2_metadata(reserved):
    """The loader writes `load_date` and `record_source` itself; a natural key or
    payload of the same name would be silently overwritten by the metadata on the
    write, keeping the column and losing the source's own value."""
    with pytest.raises(ValueError, match=reserved):
        ReferenceTable(
            name="ref_thing", lookup_type="thing", natural_key=reserved, payload="descricao"
        )


def test_two_domains_claiming_the_same_reference_table_name_are_refused():
    """The generic cross-domain collision guard already covers this kind -- nothing
    in `_collected_tables` special-cases which kind of `VaultTable` it is looking
    at, which is the property that let this kind need no new guard function."""
    ref_a = ReferenceTable(
        name="ref_thing", lookup_type="thing", natural_key="codigo", payload="descricao"
    )
    ref_b = ReferenceTable(
        name="ref_thing", lookup_type="other", natural_key="codigo", payload="descricao"
    )

    with pytest.raises(ValueError, match="ref_thing"):
        build_registry([_domain(ref_a, name="a"), _domain(ref_b, name="b")])


def test_the_cnpj_domain_models_all_six_reference_types_including_pais():
    """THE PAÍS DECISION. The task-6 brief's own prose lists five types and omits
    país; `bronze_cnpj_lookup` carries país as a sixth, identically shaped type, and
    `domains/cnpj.py` models it rather than leaving it out -- this pins that six, not
    five, are registered, and that each one's `lookup_type` is read from
    `LOOKUP_SUFFIX` rather than a hand-typed second spelling of the same six tags."""
    assert {ref.name: ref.lookup_type for ref in REFERENCE_TABLES} == {
        "ref_cnae": LOOKUP_SUFFIX["CNAE"],
        "ref_motivo": LOOKUP_SUFFIX["MOTI"],
        "ref_municipio": LOOKUP_SUFFIX["MUNIC"],
        "ref_natureza_juridica": LOOKUP_SUFFIX["NATJU"],
        "ref_pais": LOOKUP_SUFFIX["PAIS"],
        "ref_qualificacao": LOOKUP_SUFFIX["QUALS"],
    }
    for ref in REFERENCE_TABLES:
        assert (ref.natural_key, ref.payload) == ("codigo", "descricao")


# --------------------------------------------------------------------------- #
# The specs.py split (fix round 1) and the re-export contract it depends on
# --------------------------------------------------------------------------- #

def test_every_kind_specs_declares_is_re_exported_by_registry():
    """`registry.py`'s `__all__` is HAND-MAINTAINED -- unlike every whole-set guard
    in this file, which reads `VaultTable` rather than restating it, nothing derives
    the re-export list from `opl.vault.specs` automatically. Every caller in this
    repository writes `from opl.vault.registry import Hub` (and `Satellite`, `Link`,
    `LinkEnd`, `EffectivitySatellite`, `ReferenceTable`, `BusinessKeyColumn`,
    `VaultTable`) and none writes `from opl.vault.specs import ...` directly, so a
    future edit to `registry.py` that dropped one of these silently would break
    every one of those imports -- loudly, at collection time, but only for the
    files that happen to import the dropped name. This test is the one place that
    checks the WHOLE list against its source in a single assertion."""
    import opl.vault.registry as registry_module
    import opl.vault.specs as specs_module

    for name in (
        "BusinessKeyColumn", "EffectivitySatellite", "Hub", "Link", "LinkEnd",
        "ReferenceTable", "Satellite", "VaultTable",
    ):
        assert getattr(registry_module, name) is getattr(specs_module, name), name
