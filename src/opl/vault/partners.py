# src/opl/vault/partners.py
"""Load `link_company_partner`: one row per partnership relationship, ever,
insert-only -- and the derivation that makes a PJ partner resolve to `hub_empresa`
instead of to a `hub_socio` that would merge twenty-seven people onto every key.

WHY THIS LINK IS NOT LOADED BY `load_link`, stated first because everything else here
follows from it. `load_link` writes one hash-key reference per hub, computed from the
columns that hub's business key is NAMED after. `link_company_partner` has an end that
is not like that: the partner company's `cnpj_basico` is not a column of socios at all,
it is the first eight characters of `cpf_cnpj_socio`, and only when
`identificador_socio` says the partner is a company. That derivation is DOMAIN
knowledge -- the RFB's own layout -- and putting it into the generic loader would mean
either an expression in a spec or a mini-language for slicing columns. So the generic
loader stays generic and refuses this link (see `links.load_link`), and this module is
the one loader in the package that knows a source's own shape and says so.

WHAT IS NOT DUPLICATED HERE, because a second spelling of any of it re-keys the vault:
the hash standard (`opl.vault.hashing_spark`), the link's own hash key
(`loading.link_hash_key_expression`, the same call `load_link` makes), the hub
reference (`loading.hash_key_over`, which is `hash_key_expression` handed a different
column and therefore pads to the same widths in the same order), the earliest-month
`record_source` aggregate, and the month window. What this module adds is exactly two
expressions and a dedup rule.

THE PARTNER REFERENCE IS NULL WHERE THERE IS NO PARTNER COMPANY, AND THAT IS THE WHOLE
POINT OF THE CONDITION. `hash_key_column` never returns NULL -- a NULL component
encodes to the token `N` and hashes to a perfectly ordinary-looking digest -- so an
unconditional derivation would give all 27.2M PF partners and all 12,824 foreign ones a
`partner_hub_empresa_hk` that joins to no hub row, silently, and would give a PF
partner one derived from `substring('***355918**', 1, 8)` = `'***35591'`, which is a
digest of garbage that looks exactly like a digest of a company. A NULL reference says
"no partner company", which is the truth for both populations.

THE FOREIGN PARTNERS ARE ADMITTED, NOT EXCLUDED, and one company's foreign partners
collapse to ONE link row. 12,824 rows in 2026-07 carry `identificador_socio='3'` and
`cpf_cnpj_socio` NULL -- no business key at all -- so the link's key for them is
(company, '3', NULL), and a company with two foreign partners has one row rather than
two. That is a real coarsening and it is the lesser of the two errors: excluding them
would make the link's key universe differ from the observation ledger's at the same
grain, which is the universe the effectivity satellite gates its window closes on, and
would silently drop a whole class of real relationship. A surrogate key would be worse
still -- a row number is not stable across loads, so idempotence would go with it. See
ADR 0011, and `PartnerLinkLoadResult.collapsed_duplicates`, which counts the coarsening
rather than leaving it to be inferred.

THE DEDUP RULE, AND WHY THE LINK'S IS THE FREE HALF. The source is not unique on its own
business key: 27,990,592 rows over 27,986,263 distinct
(`cnpj_basico`, `identificador_socio`, `cpf_cnpj_socio`) in 2026-07 -- 4,329 collisions
(`01f19063-53c0-1f06-89f1-6aade0691af8`). A link row carries no payload, so collapsing
them loses nothing except the choice of `record_source`, and that is settled by
`earliest_record_source`'s `min` over (month, source) rather than by a `DISTINCT` whose
answer depends on partition order. The half that is NOT free is the effectivity
satellite's, which has to choose one `data_entrada_sociedade` -- see
`opl.vault.effectivity`."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import refuse_non_string_columns
from opl.vault.links import link_columns, refuse_mismatched_hubs, refuse_unloaded_hubs
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    earliest_record_source,
    hash_key_expression,
    hash_key_over,
    link_hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.registry import Hub, Link, identifying_hubs

# `identificador_socio`, per the RFB's layout. Measured over 2026-07's 27,990,592 rows
# (`01f19061-9328-1159-a4e8-63a8b433237e`): 717,650 PJ over 310,374 distinct CNPJs,
# 27,260,118 PF over 999,853 distinct masked CPFs, 12,824 foreign with the key NULL.
PARTNER_IS_A_COMPANY = "1"
PARTNER_IS_A_PERSON = "2"
PARTNER_IS_FOREIGN = "3"

# A CNPJ completo is fourteen characters and its first eight are the `cnpj_basico`
# `hub_empresa` keys on. `cpf_cnpj_socio` holds one for a PJ partner, the RFB's
# `***NNNNNN**` masked CPF (eleven characters) for a PF partner, and NULL for a foreign
# one. CROSS-CHECKED RATHER THAN TRUSTED: `_refuse_a_hub_this_slice_does_not_fit`
# asserts that BOTH ends' hubs really declare width 8, in the shape
# `opl.bronze.registry` uses for a literal it cannot import.
CNPJ_BASICO_WIDTH = 8
CNPJ_COMPLETO_WIDTH = 14

# The dependent-child keys this loader knows how to read, in the link's hash order.
PARTNER_KIND_COLUMN = "identificador_socio"
PARTNER_KEY_COLUMN = "cpf_cnpj_socio"


@dataclass(frozen=True)
class PartnerLinkLoadResult:
    """What one `link_company_partner` load did.

    `appended` and `already_present` are derived from the target's own row count before
    and after the append, so `appended` is what LANDED rather than what was planned."""

    table: str
    appended: int
    already_present: int
    # Source rows that shared a (link hash key, month) with another row and were folded
    # into it. NOT an error and NOT silent: 4,329 of 2026-07's 27,990,592 rows are
    # measured collisions on the link's own business key, and a company's several
    # foreign partners are indistinguishable by construction. Reported so the number is
    # in the operator's log next to what was written.
    collapsed_duplicates: int


def _refuse_a_hub_this_slice_does_not_fit(hub: Hub, role: str) -> None:
    """One end's hub must be keyed on `cnpj_basico` alone, at the width this module
    slices to.

    CHECKED FOR BOTH ENDS, AND THAT IS A CORRECTION. This validated the COMPANY end only
    while the message it raised argued about the PARTNER end's slice -- true here purely
    because `link_company_partner` is self-referencing, so both ends resolve to the same
    `Hub` object and checking either happened to check both. That coincidence is a
    property of one link, not of this function, and it is exactly the kind of thing a
    reader takes for a general guarantee. The two ends are checked by name instead.

    Both checks are real, and for different reasons: the COMPANY end is hashed by
    `hash_key_expression` from the source's own `cnpj_basico`, and the PARTNER end by
    `hash_key_over` from a substring of `cpf_cnpj_socio` -- so the partner hub's
    declared width is what `_padded` pads that substring to, and a hub declaring another
    width would take the digest of a differently-padded value from the one `load_hub`
    wrote."""
    if hub.business_key_columns != ("cnpj_basico",) or (
        hub.business_keys[0].width != CNPJ_BASICO_WIDTH
    ):
        raise ValueError(
            f"the {role} end resolves to hub {hub.name!r}, keyed on "
            f"{hub.business_key_columns} at width {hub.business_keys[0].width!r}. This "
            f"loader slices the partner's root as the first {CNPJ_BASICO_WIDTH} "
            f"characters of {PARTNER_KEY_COLUMN!r} and pads both ends to that hub's "
            "declared width, so a hub keyed or sized differently would take the digest "
            "of the wrong value -- and it would join to nothing without failing"
        )


def _refuse_a_link_this_derivation_does_not_fit(link: Link, hubs: Sequence[Hub]) -> None:
    """The link, its hubs and this module's knowledge of socios must describe one table.

    NAMED APART FROM `opl.vault.links._refuse_a_link_this_loader_cannot_write`, which it
    shared a name with while doing the OPPOSITE job: that one refuses a link with a
    dependent-child key or a non-identifying end, and this one refuses a link WITHOUT
    them. Two module-private functions may share a name; two functions this repository's
    own prose refers to unqualified may not.

    THE HAZARD IS THAT EVERY MISTAKE HERE PRODUCES A PLAUSIBLE TABLE. This loader reads
    two dependent-child keys by name and derives the partner root from one of them; a
    link declaring other keys, or a hub keyed on something else, would still load, still
    be idempotent, and still join -- to the wrong things. So everything that must agree
    is checked rather than assumed, including the width, which is a literal here and a
    spec value there."""
    refuse_mismatched_hubs(link, hubs)
    if [end.identifying for end in link.ends] != [True, False]:
        raise ValueError(
            f"link {link.name!r} declares ends "
            f"{[(end.role, end.identifying) for end in link.ends]} and this loader "
            "writes exactly two: an identifying COMPANY end read from the source's own "
            "`cnpj_basico`, then a non-identifying PARTNER end it derives. It indexes "
            "them positionally, so any other shape would hash and label the wrong one"
        )
    if link.dependent_child_key_columns != (PARTNER_KIND_COLUMN, PARTNER_KEY_COLUMN):
        raise ValueError(
            f"link {link.name!r} declares dependent-child keys "
            f"{link.dependent_child_key_columns} and this loader reads "
            f"{(PARTNER_KIND_COLUMN, PARTNER_KEY_COLUMN)}. It derives the partner "
            "company's root from the second of those by name, so another key list "
            "would leave every partner reference NULL without failing"
        )
    for hub, role in zip(hubs, ("company", "partner"), strict=True):
        _refuse_a_hub_this_slice_does_not_fit(hub, role)


def partner_company_root() -> Column:
    """The partner company's `cnpj_basico`, or NULL where the partner is not a company.

    BOTH CONDITIONS, AND NEITHER IS REDUNDANT. `identificador_socio = '1'` is the
    source's own delivered statement that the partner is a legal person, and it is the
    one to lead on -- inferring it from the value's length would be our inference where
    the RFB gives us a fact. The length test is the guard beside it: a row that claims
    to be a PJ and carries eleven characters is malformed, and `substring` would happily
    return its first eight, which is a digest of a company that does not exist. Together
    they say "a fourteen-character CNPJ that the source itself calls a company"."""
    return F.when(
        (F.col(PARTNER_KIND_COLUMN) == F.lit(PARTNER_IS_A_COMPANY))
        & (F.length(F.col(PARTNER_KEY_COLUMN)) == F.lit(CNPJ_COMPLETO_WIDTH)),
        F.substring(F.col(PARTNER_KEY_COLUMN), 1, CNPJ_BASICO_WIDTH),
    )


def partner_link_candidates(
    spark: SparkSession,
    link: Link,
    hubs: Sequence[Hub],
    *,
    source_table: str,
    months: Sequence[str] | None,
) -> DataFrame:
    """One row per relationship in the window: the link's hash key, the company and
    partner references, the two dependent-child keys, and the earliest month's
    `record_source`.

    THE PARTNER REFERENCE IS BUILT BY `hash_key_over`, WHICH IS THE HUB'S OWN
    EXPRESSION handed a different column -- same padding, same order, same standard. A
    second spelling would produce a reference that joins to nothing and reports success
    doing it, which is the quietest wrong answer this layer can give."""
    source = read_snapshot_window(spark, source_table, months)
    company, partner = hubs
    identity = identifying_hubs(link, hubs)
    refuse_non_string_columns(
        source, [*company.business_key_columns, *link.dependent_child_key_columns]
    )
    root = partner_company_root()
    keyed = source.select(
        link_hash_key_expression(link, identity).alias(link.hash_key),
        hash_key_expression(company).alias(link.ends[0].reference_column(company)),
        # THE `when` AROUND THE DIGEST IS THE POINT, not the one inside `root`.
        # `hash_key_column` encodes NULL to the token `N` and hashes it, so without this
        # every PF and foreign partner would carry ONE shared, perfectly ordinary-looking
        # digest that joins to no hub row -- 27.2M relationships pointing at a company
        # that does not exist, with nothing failing.
        F.when(root.isNotNull(), hash_key_over(partner, [root])).alias(
            link.ends[1].reference_column(partner)
        ),
        *(F.col(name) for name in link.dependent_child_key_columns),
        F.col(SNAPSHOT_MONTH_COLUMN),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return earliest_record_source(keyed, link_columns(link, hubs))


def _collapsed_duplicates(
    spark: SparkSession, link: Link, hubs: Sequence[Hub], source_table: str,
    months: Sequence[str] | None,
) -> int:
    """Source rows in the window, minus distinct (link key, month) pairs in it.

    A SECOND PASS, DELIBERATELY, AND IT IS WHAT KEEPS THE DEDUP FROM BEING A SILENT
    `DISTINCT`. The number is small and measured -- 4,329 in 2026-07 -- and the whole
    argument for folding duplicates rests on them being few and payload-free, so the
    load that folds them should be the load that says how many."""
    source = read_snapshot_window(spark, source_table, months)
    keyed = source.select(
        link_hash_key_expression(link, identifying_hubs(link, hubs)).alias(link.hash_key),
        F.col(SNAPSHOT_MONTH_COLUMN),
    )
    return keyed.count() - keyed.distinct().count()


def load_partner_link(
    spark: SparkSession,
    link: Link,
    *,
    hubs: Sequence[Hub],
    hub_tables: Mapping[str, str],
    source_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str] | None = None,
) -> PartnerLinkLoadResult:
    """Append every relationship of `source_table` that `target_table` does not already
    hold, stamped with `load_date`.

    Idempotent by anti-join on the link's own hash key, like `load_link`: a re-run finds
    every key present and appends nothing. `load_date` has no default for `load_hub`'s
    reason -- a loader that stamps its own clock cannot be asserted against.

    THE HUB PREFLIGHT IS SHARED WITH `load_link` AND MATTERS MORE HERE, because this is
    the loader whose job does not load the hub it references: `vault_partner_job` loads
    the link and `vault_empresa_job` loads `hub_empresa`, both ends of it. It runs ahead
    of `_collapsed_duplicates`, which is a second full scan of the window, so a
    wrong-order run is refused before it pays for anything."""
    _refuse_a_link_this_derivation_does_not_fit(link, hubs)
    refuse_unloaded_hubs(spark, link, hubs, hub_tables)
    before = rows_in(spark, target_table)
    collapsed = _collapsed_duplicates(spark, link, hubs, source_table, months)
    candidates = partner_link_candidates(
        spark, link, hubs, source_table=source_table, months=months
    )
    if before:
        candidates = candidates.join(
            spark.read.table(target_table).select(link.hash_key),
            on=link.hash_key,
            how="left_anti",
        )
    (
        candidates.select(
            *link_columns(link, hubs),
            F.lit(load_date).alias(LOAD_DATE),
            F.col(RECORD_SOURCE),
        )
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return PartnerLinkLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
        collapsed_duplicates=collapsed,
    )
