"""`link_payment` against real Spark: the vault's first transactional link, its first
DEPENDENT-CHILD KEY written by the generic loader, and its first self-referencing link
with two IDENTIFYING ends.

WHAT IS BEING SHOWN, AND WHAT IS NOT. Every number below is over a synthesised bronze
fixture on local Spark. Nothing here is a claim about a Databricks run: F2 wave 2 cannot
launch one (the workspace 403s on any NEW job), so this file is the whole of the
evidence that the mechanism fires, and it says so rather than borrowing a run's
authority.

THE THREE PROPERTIES THIS LINK HAS THAT NO SHIPPED LINK HAD, one fixture row per class,
meant to be read top to bottom:

  P_ONE        A pays B, transaction t-0001          the ordinary case
  P_TWO        A pays B AGAIN, transaction t-0002    THE DEPENDENT-CHILD KEY, alone
  P_REVERSED   B pays A, transaction t-0003          the ROLES, and that they differ
  P_ONE again  t-0001 redelivered in July            idempotence at link grain
  P_JULY       A pays C, transaction t-0004          an arrival in the second month

P_TWO IS THE ROW THAT ARGUES THE WHOLE TASK. It shares every hub reference with P_ONE --
the same payer, the same payee, both resolving to the same two `hub_empresa` digests --
so the ONLY thing that can tell the two payments apart is `transaction_id`. If the
loader does not hash it, the two collapse to one link row and the vault silently loses
half the payments between any pair that trades twice; if the loader hashes it and does
not WRITE it, the table's identity column describes a column it does not have. Both
failures are silent and both are refused here, by one assertion each.

P_REVERSED IS THE ROW THAT ARGUES THE ROLES. `payer_hub_empresa_hk` and
`payee_hub_empresa_hk` are two columns over ONE hub, which exists only because
`LinkEnd.role` prefixes the reference column; without a role both ends would be written
into `hub_empresa_hk` and one of the two counterparties would be gone with the row count
still right. The assertion is that P_REVERSED's payer digest IS P_ONE's payee digest,
which no single-role spelling can produce.

THE COUNTERPARTIES ARE EIGHT-CHARACTER ROOTS AND THE ENDS ARE DERIVED ANYWAY. Bronze
payments carries `payer_cnpj_basico` and `payee_cnpj_basico`, and `hub_empresa` is keyed
on `cnpj_basico` -- so neither end names its hub's business-key column, and both declare
a `LinkEnd.key_from`. That is `link_merchant_empresa`'s shape (F-DB), applied twice on
one hub for the first time; `fdb:1504` predicted this field's second consumer arrives
with wave 2 or not at all, and this is it.

`hub_empresa` IS LOADED FROM ITS OWN SOURCE, from a minimal empresas feed beside the
payments table. There is no version of this fixture where one table feeds both: bronze
payments has no `cnpj_basico` column at all, which is exactly why the ends are derived,
and `links.refuse_unloaded_hubs` refuses to write the link into a workspace where that
hub is missing or empty."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from opl.contracts import payments as payments_contract
from opl.vault import domains
from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.domains.cnpj import CNPJ_BASICO_WIDTH, HUB_EMPRESA
from opl.vault.hubs import load_hub
from opl.vault.links import (
    _refuse_a_link_this_loader_cannot_write,
    link_columns,
    load_link,
    non_identifying_ends,
    source_columns,
)
from opl.vault.specs import BusinessKeyColumn, KeyPrefix, Link, LinkEnd

from .conftest import (
    EMPRESA_ROOTS,
    JUL,
    JUN,
    LOADED_AT,
    PAYMENTS_SPEC,
    derived_table,
)

LINK = domains.table_spec("link_payment")
LINK_HUBS = domains.linked_hubs(LINK)
PARTNER_LINK = domains.table_spec("link_company_partner")


def hub_tables(names) -> dict[str, str]:
    """Where `link_payment`'s hub lives, keyed by hub name. ONE entry for a link with two
    ends: it is self-referencing, so both ends resolve to `hub_empresa` and a mapping
    keyed by hub name cannot disagree with itself about where that hub is."""
    return {HUB_EMPRESA.name: names.hub}


def load_payment_vault(spark, source, names, *, months=None):
    """`hub_empresa` then `link_payment`, in that order, over `months`.

    THE ORDER IS A PRECONDITION AND NOT A CONVENIENCE: `load_link` refuses a hub table
    that is missing or empty, because the link's references are COMPUTED rather than
    joined and would otherwise dangle on an insert-only table."""
    names.hub_result = load_hub(
        spark, HUB_EMPRESA, source_table=source.empresas, target_table=names.hub,
        load_date=LOADED_AT, months=months,
    )
    names.link_result = load_link(
        spark, LINK, hubs=LINK_HUBS, hub_tables=hub_tables(names),
        source_table=source.bronze, target_table=names.link, load_date=LOADED_AT,
        months=months,
    )
    return names


@pytest.fixture(scope="module")
def payments_loaded(spark, payments_source):
    """One load of both tables over both months, shared by every read-only assertion."""
    db = payments_source.db
    names = SimpleNamespace(hub=f"{db}.emp_shared", link=f"{db}.link_shared")
    return load_payment_vault(spark, payments_source, names)


# --------------------------------------------------------------------------- #
# The spec. No Spark: a declaration this file can read in milliseconds.
# --------------------------------------------------------------------------- #


def test_the_link_is_self_referencing_on_hub_empresa_under_two_roles():
    """Two ends, one hub, two roles -- and the roles are what make the table writable.

    WITHOUT A ROLE BOTH ENDS ARE WRITTEN INTO `hub_empresa_hk`, one column taking two
    values, and `registry._assert_every_link_joins_registered_hubs` refuses exactly that
    at import. So this assertion is not a restatement of the guard: the guard says the
    two reference columns must differ, and this says WHICH two they are, because the
    names are what every downstream join and every job-wiring lock spells."""
    assert LINK.hub_names == (HUB_EMPRESA.name, HUB_EMPRESA.name)
    assert [end.role for end in LINK.ends] == ["payer", "payee"]
    assert [end.reference_column(HUB_EMPRESA) for end in LINK.ends] == [
        f"payer_{HUB_EMPRESA.hash_key}", f"payee_{HUB_EMPRESA.hash_key}"
    ]
    assert LINK_HUBS == (HUB_EMPRESA, HUB_EMPRESA)


def test_both_ends_are_derived_and_identifying_from_the_contracts_counterparty_columns():
    """The pairing `link_merchant_empresa` introduced, used twice on one hub.

    DERIVED, because bronze payments has no `cnpj_basico`: it has `payer_cnpj_basico` and
    `payee_cnpj_basico`, so `link_candidates`' default -- read the hub's business key
    from the columns the hub is NAMED after -- finds nothing, and an undeclared end would
    be refused by the loader rather than guessed at.

    IDENTIFYING, because a payment IS the pair plus the transaction. An end marked
    `identifying=False` would drop its counterparty out of the digest, and every payment
    A ever made would then share a link key with every payment anyone made to A.

    THE WIDTH IS THE HUB'S OWN, cross-checked by `registry._refuse_a_derivation_that_does
    _not_fit` at import: a prefix of another length keys on a different-length root,
    produces a digest `load_hub` never wrote, and joins to nothing without failing."""
    assert [end.identifying for end in LINK.ends] == [True, True]
    assert [
        tuple(prefix.column for prefix in end.key_from or ()) for end in LINK.ends
    ] == [(column,) for column in payments_contract.COUNTERPARTY_COLUMNS]
    assert {
        prefix.width for end in LINK.ends for prefix in end.key_from or ()
    } == {CNPJ_BASICO_WIDTH}
    assert LINK.dependent_child_key_columns == (payments_contract.IDENTITY_COLUMN,)


def test_the_links_identity_is_the_two_counterparties_then_the_transaction_id():
    """The grain, in hash order, off the spec rather than restated.

    ORDER IS THE LINK'S IDENTITY: the components are flattened with no boundary marker
    (`loading.link_hash_key_expression`), so moving the transaction id ahead of a
    counterparty re-keys the whole table while every column stays correct."""
    assert domains.link_identity_columns(LINK) == (
        *payments_contract.COUNTERPARTY_COLUMNS, payments_contract.IDENTITY_COLUMN
    )


def test_the_loader_reads_the_two_counterparties_and_the_transaction_id_from_one_source():
    """`source_columns` is the list `refuse_non_string_columns` is handed, and since this
    task it carries the DEPENDENT-CHILD KEYS as well as the ends' own columns.

    THEY BELONG IN IT FOR THE SAME REASON THE ENDS' COLUMNS DO. A dependent-child key is
    hashed into the link's own digest by `link_hash_key_expression` and written into the
    table by `link_columns`; a source that does not carry it, or carries it typed, is a
    source this loader cannot read -- and left out of this list the failure arrives as an
    `AttributeError` inside a Spark task instead of as prose naming the column."""
    assert source_columns(LINK, LINK_HUBS) == [
        *payments_contract.COUNTERPARTY_COLUMNS, payments_contract.IDENTITY_COLUMN
    ]
    assert link_columns(LINK, LINK_HUBS) == [
        LINK.hash_key,
        f"payer_{HUB_EMPRESA.hash_key}",
        f"payee_{HUB_EMPRESA.hash_key}",
        payments_contract.IDENTITY_COLUMN,
    ]


def test_the_generic_loader_admits_a_dependent_child_key_and_still_refuses_a_derived_end():
    """THE NARROWING, IN ONE TEST, BECAUSE IT IS ONE DECISION WITH TWO HALVES.

    `_refuse_a_link_this_loader_cannot_write` refused EVERY link carrying a
    dependent-child key until this task. ADR 0011 recorded that as a deliberate deferral
    -- "a generic path with no consumer in the repository and no exercise against real
    data" -- and named the wave-2 task that has a table to point at it. `link_payment` is
    that table, so the first arm asserts the refusal is gone.

    WHAT MUST STILL BE REFUSED IS THE OTHER HALF OF THE ORIGINAL MESSAGE, and deleting
    the refusal outright would have taken it with it: an UNDECLARED DERIVED END.
    `link_company_partner`'s partner end reads no `key_from` and its `cnpj_basico` is the
    first eight characters of `cpf_cnpj_socio`, so `link_candidates` would compute it
    from `cnpj_basico` -- the COMPANY's column -- and every one of 27.99M relationships
    would read as a company partnered with itself, with the right row count and working
    joins. That link carries dependent-child keys too, which is precisely why the two
    conditions had to be separated rather than one dropped.

    THE CONDITION IS NOW `not end.identifying` ALONE, WHICH IS NARROWER PROSE AND A WIDER
    GUARD, and the paragraph above describes the first attempt rather than what runs. The
    T1 review broke the separated version by construction: declaring `key_from` on socios'
    non-identifying partner end satisfied "has a declared derivation" and the loader
    accepted the link. So the two conditions are not separated, they are **replaced** by
    the one that states this loader's actual mechanism -- it computes each end's reference
    from that end's own columns, independently, and an end whose reference is a function of
    the link's identity cannot be computed that way whatever it declares. See
    `test_a_non_identifying_end_stays_unwritable_here_even_with_a_declared_derivation`,
    which drives the reviewer's exact mutation."""
    _refuse_a_link_this_loader_cannot_write(LINK)

    with pytest.raises(ValueError, match="NON-IDENTIFYING end"):
        _refuse_a_link_this_loader_cannot_write(PARTNER_LINK)


# --------------------------------------------------------------------------- #
# The load, against real Spark
# --------------------------------------------------------------------------- #


def test_the_link_carries_the_transaction_id_beside_the_two_references(
    spark, payments_loaded
):
    """THE PROJECTION. The columns written are the link's hash key, one reference per
    roled end, the dependent-child key, and the two pieces of DV2 metadata.

    THE DEPENDENT-CHILD KEY IS THE ONE THIS TASK ADDS AND ITS ABSENCE IS SILENT.
    `link_hash_key_expression` has hashed dependent-child keys since Task 5, so a loader
    that hashes and does not write leaves a table whose identity column is a digest over
    a column the table does not have -- reconcilable by nobody, and failing nothing."""
    written = spark.read.table(payments_loaded.link).columns
    assert written == [
        LINK.hash_key,
        f"payer_{HUB_EMPRESA.hash_key}",
        f"payee_{HUB_EMPRESA.hash_key}",
        payments_contract.IDENTITY_COLUMN,
        LOAD_DATE,
        RECORD_SOURCE,
    ]


def test_two_payments_between_the_same_pair_are_two_link_rows(spark, payments_loaded):
    """P_ONE AND P_TWO, WHICH IS THE WHOLE TASK IN ONE ASSERTION.

    Both rows carry the same payer and the same payee, so both hub references are
    byte-identical between them: the only difference in the source is `transaction_id`.
    Two things have to be true at once for these to be two link rows, and each fails
    silently on its own --

      - the dependent-child key must be HASHED into the link's key, or the two payments
        share a digest and the anti-join keeps one of them;
      - it must be PROJECTED, or the earliest-`record_source` aggregate groups on
        (hash key, payer, payee) alone and folds the two into one row before the write.

    Either failure loses half the payments between any pair that trades twice, with the
    load reporting success. The assertion is deliberately over BOTH -- two rows, and two
    distinct hash keys -- because one of them alone is satisfied by the other's defect."""
    rows = (
        spark.read.table(payments_loaded.link)
        .filter(f"{payments_contract.IDENTITY_COLUMN} in ('t-0001', 't-0002')")
        .collect()
    )
    assert len(rows) == 2
    assert len({row[LINK.hash_key] for row in rows}) == 2
    assert len({row[f"payer_{HUB_EMPRESA.hash_key}"] for row in rows}) == 1
    assert len({row[f"payee_{HUB_EMPRESA.hash_key}"] for row in rows}) == 1


def test_the_two_roles_carry_two_different_companies_and_reversing_them_re_keys_the_row(
    spark, payments_loaded
):
    """P_ONE AND P_REVERSED. B pays A where A paid B, and that is a different fact.

    WITHOUT THE ROLES THERE IS ONE REFERENCE COLUMN and one of the two counterparties is
    simply gone -- the row count stays right and every join keeps working, which is why
    `LinkEnd.role` exists at all. The discriminating half is the CROSS assertion: the
    reversed payment's PAYER digest is the original's PAYEE digest, which no spelling
    that wrote both ends into one column could produce."""
    link = spark.read.table(payments_loaded.link)
    payer, payee = f"payer_{HUB_EMPRESA.hash_key}", f"payee_{HUB_EMPRESA.hash_key}"
    one = link.filter(f"{payments_contract.IDENTITY_COLUMN} = 't-0001'").collect()[0]
    reversed_ = link.filter(f"{payments_contract.IDENTITY_COLUMN} = 't-0003'").collect()[0]

    assert one[payer] != one[payee]
    assert reversed_[payer] == one[payee]
    assert reversed_[payee] == one[payer]
    assert reversed_[LINK.hash_key] != one[LINK.hash_key]


def test_every_reference_is_the_digest_load_hub_wrote_for_that_company(
    spark, payments_loaded
):
    """THE JOIN-SAFETY PROPERTY, AND THE ONE A DERIVED END CAN LOSE WITHOUT FAILING.

    Both ends read an eight-character root out of a column named after neither the hub
    nor its business key, pad it to the hub's declared width and hash it through
    `hash_key_over` -- the hub's OWN expression handed a different column. A second
    spelling anywhere on that path gives references that join to no hub row, silently, on
    a table that is insert-only.

    ASSERTED BY SET EQUALITY AGAINST THE HUB rather than by a join count, because a join
    of the wrong digests to an empty result and a join nobody performed report the same
    number: zero unmatched."""
    hub = spark.read.table(payments_loaded.hub)
    digests = {row[HUB_EMPRESA.hash_key] for row in hub.collect()}
    assert len(digests) == len(EMPRESA_ROOTS)

    link = spark.read.table(payments_loaded.link).collect()
    referenced = {row[f"payer_{HUB_EMPRESA.hash_key}"] for row in link} | {
        row[f"payee_{HUB_EMPRESA.hash_key}"] for row in link
    }
    assert referenced <= digests
    assert referenced == digests


def test_a_redelivered_payment_is_one_link_row_and_a_reload_appends_nothing(
    spark, payments_source, payments_target
):
    """TWO INSERT-ONLY PROPERTIES AT ONE GRAIN, and the fixture makes them different
    questions.

    THE REDELIVERY is in the DATA: `t-0001` appears in June's bronze and again in July's,
    byte-identical, which the contract calls "the SAME payment seen twice". One load over
    both months must produce ONE row for it -- that is the earliest-`record_source`
    aggregate collapsing the two observations, not the anti-join.

    THE RELOAD is in the LOADER: running the same window again must append zero, which is
    the anti-join on the link's own hash key. `already_present` is read off the target's
    row count before the second write, so it is what LANDED rather than what was
    planned."""
    load_payment_vault(spark, payments_source, payments_target)
    assert payments_target.link_result.appended == 4
    assert payments_target.link_result.already_present == 0

    ids = [
        row[payments_contract.IDENTITY_COLUMN]
        for row in spark.read.table(payments_target.link).collect()
    ]
    assert sorted(ids) == ["t-0001", "t-0002", "t-0003", "t-0004"]

    again = load_link(
        spark, LINK, hubs=LINK_HUBS, hub_tables=hub_tables(payments_target),
        source_table=payments_source.bronze, target_table=payments_target.link,
        load_date=LOADED_AT,
    )
    assert (again.appended, again.already_present) == (0, 4)


def test_a_window_narrows_the_link_to_the_payments_that_month_delivered(
    spark, payments_source, payments_target
):
    """The month window reaches the dependent-child key like every other column: June
    holds three payments and July holds two, one of which is a redelivery of June's.

    IT IS ASSERTED BECAUSE THE WINDOW IS WHERE A SILENT LOAD LIVES. `loading._validated
    _months` refuses an empty or malformed window precisely because "a vault table that
    gained no rows looks exactly like a vault table that had nothing to gain"; a window
    that selects a REAL month and the wrong rows looks the same way."""
    load_payment_vault(spark, payments_source, payments_target, months=[JUN])
    assert payments_target.link_result.appended == 3

    july = load_link(
        spark, LINK, hubs=LINK_HUBS, hub_tables=hub_tables(payments_target),
        source_table=payments_source.bronze, target_table=payments_target.link,
        load_date=LOADED_AT, months=[JUL],
    )
    # ONE, not two: July's other row is `t-0001` redelivered, whose link key June's load
    # already wrote, and the anti-join is what makes the second delivery free.
    assert july.appended == 1


def test_a_typed_transaction_id_is_refused_by_name_before_anything_is_written(
    spark, payments_source, payments_target
):
    """THE DEPENDENT-CHILD KEY IS PART OF THE HASH STANDARD'S INPUT, so it is subject to
    the standard's own precondition: STRING columns.

    THE DIVERGENCE THIS CATCHES IS NOT A CRASH ON EVERY PATH. `hash_key_column` would
    have Spark cast the value silently and hash the cast, so a `transaction_id` that
    arrived as a bigint would produce a full table of plausible digests that no re-load
    over a string column could ever reproduce. It is refused on the read path production
    uses -- a real Delta table with a real typed column -- rather than on a temp view.

    IT IS ALSO THE ASSERTION THAT PINS THE DEPENDENT-CHILD KEYS INTO `source_columns`.
    Left out of that list this load succeeds, which is the shape the refusal exists to
    prevent."""
    typed = derived_table(
        spark, payments_source.db, "typed",
        spark.read.table(payments_source.bronze).withColumn(
            payments_contract.IDENTITY_COLUMN,
            F.col(payments_contract.IDENTITY_COLUMN).substr(3, 4).cast("int"),
        ),
    )
    load_hub(
        spark, HUB_EMPRESA, source_table=payments_source.empresas,
        target_table=payments_target.hub, load_date=LOADED_AT,
    )
    with pytest.raises(TypeError, match=payments_contract.IDENTITY_COLUMN):
        load_link(
            spark, LINK, hubs=LINK_HUBS, hub_tables=hub_tables(payments_target),
            source_table=typed, target_table=payments_target.link, load_date=LOADED_AT,
        )
    assert not spark.catalog.tableExists(payments_target.link)


def test_the_link_is_refused_where_hub_empresa_has_not_been_loaded(
    spark, payments_source, payments_target
):
    """The preflight, at this link's own grain. Both ends reference a hub that
    `vault_payment_job` will not load -- `vault_empresa_job` does -- and no Databricks
    `depends_on` crosses a job boundary, so the ordering is an operator's to get right
    and the wrong order does not fail: every reference lands pointing at nothing and the
    run reports success."""
    with pytest.raises(ValueError, match="does not exist"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, hub_tables=hub_tables(payments_target),
            source_table=payments_source.bronze, target_table=payments_target.link,
            load_date=LOADED_AT,
        )
    assert not spark.catalog.tableExists(payments_target.link)


def test_the_domain_declares_the_link_and_its_satellite_and_reads_one_bronze_source():
    """The domain module is DATA, and this is the whole of what it registers.

    TWO TABLES SINCE T2, AND THIS TEST SAID ONE. The satellite was deferred with its own
    condition written out -- "`sat_link_payment` needs `load_satellite` to take a link and
    `registry._assert_every_satellite_hangs_off_a_hub` to change with it, one decision made
    by the task that owns both halves" -- and that task changed both. The ORDER is asserted
    because it is the order `discover_domains` yields and the order a reader meets: the
    relationship first, then what it carried.

    ONE SOURCE FOR BOTH, which is not a detail: the link and its satellite read the same
    bronze rows, which is what makes the satellite's hash key the link's own digest by
    construction rather than by a join.

    THE SOURCE IS THE BRONZE REGISTRY'S, not a literal: `opl.contracts.payments` pins the
    staging/bronze/quarantine triple and `opl.bronze.registry` lifts it, so a domain that
    respelled the Delta name would be a fifth place that string lives."""
    from opl.vault.domains import payments_domain

    assert payments_domain.DOMAIN.tables == (LINK, domains.table_spec("sat_link_payment"))
    assert payments_domain.PAYMENTS_SOURCE.endswith(PAYMENTS_SPEC.bronze)
    assert PAYMENTS_SPEC.bronze == payments_contract.BRONZE_TABLE


def test_a_non_identifying_end_stays_unwritable_here_even_with_a_declared_derivation():
    """THE T1 REVIEW'S FINDING, LOCKED. `link_company_partner` must stay unwritable by the
    generic loader WHATEVER anyone declares on its ends.

    The first spelling of the narrowed refusal was `key_from is None and not identifying`,
    and the reviewer broke it by construction: declare
    `key_from=(KeyPrefix("cpf_cnpj_socio", 8),)` on the partner end, leave
    `identifying=False` alone, and the generic loader ACCEPTED the link -- then would have
    written `partner_hub_empresa_hk = hash(substring(cpf_cnpj_socio, 1, 8))` for every row,
    dropping the three filters `opl.vault.partners` carries. `partners.py`'s own comment
    prices that at "27.2M relationships pointing at a company that does not exist, with
    nothing failing".

    So this drives the exact mutation the reviewer used. The condition is now
    `not end.identifying` alone, which is a statement about THIS LOADER's mechanism rather
    than about what the declaration happens to spell: an end whose reference is a function
    of the link's identity cannot be computed from that end's own columns, and no
    `key_from` changes that."""
    poisoned = Link(
        name="link_company_partner_poisoned",
        hash_key=PARTNER_LINK.hash_key,
        hubs=(
            LinkEnd(hub=HUB_EMPRESA.name, role="company"),
            LinkEnd(
                hub=HUB_EMPRESA.name,
                role="partner",
                identifying=False,
                key_from=(KeyPrefix(column="cpf_cnpj_socio", width=CNPJ_BASICO_WIDTH),),
            ),
        ),
        dependent_child_keys=PARTNER_LINK.dependent_child_keys,
    )

    assert non_identifying_ends(poisoned) == (poisoned.ends[1],)
    with pytest.raises(ValueError, match="NON-IDENTIFYING end"):
        _refuse_a_link_this_loader_cannot_write(poisoned)


def test_a_dependent_child_key_declaring_a_width_is_refused_before_anything_is_written():
    """THE T1 REVIEW'S SECOND FINDING, LOCKED. A widthed dependent-child key would be
    HASHED PADDED and WRITTEN RAW.

    Measured by the reviewer on a real session: with `width=12`,
    `link_hash_key_expression` composes `lpad(transaction_id, 12, '0')` into the digest
    while `link_columns` projects the bare column, so the link's identity column is a
    digest over a value the row does not carry and no re-load can reproduce it. Nothing
    fails -- the table is written, the joins work, and the key is unreproducible.

    `link_payment`'s own key declares NO width and that is correct: a `transaction_id` is
    a uuid the source delivers, not a fixed-width code, and `BusinessKeyColumn.width=None`
    means "take the value as it is". So this test drives a spec no domain declares, which
    is the only way to reach the refusal -- the alternative is a guard nobody has watched
    fail."""
    widthed = Link(
        name="link_payment_widthed",
        hash_key=LINK.hash_key,
        hubs=LINK.hubs,
        dependent_child_keys=(
            BusinessKeyColumn(name=payments_contract.IDENTITY_COLUMN, width=12),
        ),
    )

    with pytest.raises(ValueError, match="hashes the PADDED value"):
        _refuse_a_link_this_loader_cannot_write(widthed)

    # The real spec must stay writable: the refusal is about a width, not about having a
    # dependent-child key at all -- which is the arm F2 wave 2 exists to open.
    _refuse_a_link_this_loader_cannot_write(LINK)
