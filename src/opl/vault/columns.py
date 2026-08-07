# src/opl/vault/columns.py
"""The DV2 metadata column names every hub, link and satellite in this vault carries.

FOUR NAMES, IN ONE PLACE, AND NOTHING ELSE IN THIS MODULE. They are spelled by the
registry's guards (which refuse a payload column that would collide with one), by the
two loaders (which write them), and by every test that reads a vault table -- three
groups that would otherwise each carry their own literal. A payload column silently
overwritten by a metadata column of the same name is not a crash; it is a column full
of plausible values with the source's own value gone, which is why the collision is
refused rather than resolved.

PURE: NOTHING HERE IMPORTS ANYTHING, so this module and `opl.vault.registry` -- whose
per-table `__post_init__` guards are the only thing that reads `METADATA_COLUMNS` --
both import where pyspark is not installed, and a spec can be declared and refused
without a session.

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
