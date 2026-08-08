# src/opl/vault/columns.py
"""The DV2 metadata column names every hub, link and satellite in this vault carries.

SEVEN NAMES AND THE TWO FROZEN SETS OVER THEM, IN ONE PLACE, AND NOTHING ELSE IN THIS
MODULE -- four DV2 metadata names, and three effectivity-window names that arrived with
Task 5. (This paragraph read "FOUR NAMES ... the two loaders" until Task 7's correction
pass; it was written when both were true and was not revisited when the kinds and the
loaders multiplied.) They are spelled by the spec guards (which refuse a payload column
that would collide with one), by the SIX loaders (which write them) -- `load_hub`,
`load_satellite`, `load_link`, `load_partner_link`, `load_effectivity_satellite`,
`load_reference_table` -- and by every test that reads a vault table: three groups that
would otherwise each carry their own literal. A payload column silently
overwritten by a metadata column of the same name is not a crash; it is a column full
of plausible values with the source's own value gone, which is why the collision is
refused rather than resolved.

PURE: NOTHING HERE IMPORTS ANYTHING, so this module and `opl.vault.specs` -- whose
per-table `__post_init__` guards are the only thing in this package that reads
`METADATA_COLUMNS` -- both import where pyspark is not installed, and a spec can be
declared and refused without a session. (Those guards lived in `opl.vault.registry`,
which this paragraph named, until Task 6's fix round moved the five kind specs into
`opl.vault.specs`; `registry.py` now reaches them through that import and does not
read the frozen set itself. `opl.bronze.masking` has an unrelated `METADATA_COLUMNS`
of its own, which is why the sentence says "in this package".)

THAT DOES NOT MAKE THE REGISTRY AS A WHOLE SPARK-FREE, and an earlier version of this
paragraph claimed it did. `opl.vault.domains.__init__` runs the whole-set guards at
import, and it imports `domains/cnpj.py`, which declares an `ObservationGrain` and so
pulls in `opl.vault.observation` and pyspark behind it. `import opl.vault.domains`
therefore fails in the extraction environment, where pyspark is deliberately an
optional extra (ADR 0004). Nothing needs it to succeed there -- the extraction scripts
never touch the vault -- but the claim is stated correctly rather than borrowed from
`opl.bronze.registry`, which really does hold it and is tested for it.

WHY `applied_date` IS THE ONE WORTH READING TWICE. DV2 gives a satellite exactly one
timestamp, `load_date`, and everything downstream reconstructs history from it -- which
is correct only when the load and the fact are the same event. Ours are not: the RFB
publishes a monthly snapshot dated in its own filename (2026-06-13, 2026-07-11 -- not
month-end), and we load it whenever we get to it. Collapsing the two would make the
vault's history a history of OUR pipeline's schedule. `applied_date` carries the
source's date and `load_date` carries ours, and the pair is what lets a reader ask
"what was true in June?" rather than "what had we loaded by June?".
"""
from __future__ import annotations

# LDTS. When WE loaded the row. Technical, ours, and always injected by the caller --
# see `opl.vault.hubs.load_hub` for why a loader that stamps its own clock cannot be
# tested.
LOAD_DATE = "load_date"

# RSRC. Where the row's data came from, carried through from bronze's `_record_source`
# rather than re-derived, so there is one spelling of it in the lakehouse.
RECORD_SOURCE = "record_source"

# The satellite's business timeline: when the fact was true AT THE SOURCE, taken from
# `_snapshot_ref_date`. Hubs do not carry it -- a hub row asserts that a key exists,
# which is not a statement about a point in time.
APPLIED_DATE = "applied_date"

# The satellite's change detector: the business-key hash standard applied to the
# payload columns instead of to a business key. A satellite row is written only when
# this differs from the previous applied_date's value for the same hash key.
HASH_DIFF = "hash_diff"

# Refused as a business-key or payload column name by the registry's guards. A frozen
# set rather than a list so no caller can extend it in place.
METADATA_COLUMNS = frozenset({LOAD_DATE, RECORD_SOURCE, APPLIED_DATE, HASH_DIFF})

# The effectivity satellite's three columns, and THE ONLY PLACE IN THIS VAULT WHERE A
# DERIVED CLAIM IS WRITTEN DOWN. They are named apart from `METADATA_COLUMNS` because
# only one table kind carries them, and they are named in OUR vocabulary on purpose:
# the window's open keeps the source's own column name (`data_entrada_sociedade`,
# delivered on 100% of rows), and these three are ours. A reader who cannot tell a
# delivered fact from an inferred one is the failure ADR 0011 exists to prevent, and
# the naming is the cheapest part of preventing it.

# Whether the relationship was effective as at this row's `applied_date`. DataVault4dbt's
# own column name for the pattern this table implements (research §3b).
IS_ACTIVE = "is_active"

# DERIVED. On a closing row, the `applied_date` of the last month we OBSERVED the
# relationship -- not the date it ended, which the source never tells us. NULL while
# active.
LAST_OBSERVED_ON = "last_observed_on"

# DERIVED. The `ObservationState` that authorised the close, so the gate is a property
# of the DATA and not only of the loader: a window closed because our own DQ gate
# quarantined the row would say so here instead of being indistinguishable from a
# departure. NULL while active.
CLOSED_BY = "closed_by"

EFFECTIVITY_COLUMNS = frozenset({IS_ACTIVE, LAST_OBSERVED_ON, CLOSED_BY})
