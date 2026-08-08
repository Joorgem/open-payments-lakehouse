# src/opl/vault/registry_reference.py
"""`ReferenceTable`: DV2's reference-table kind -- a natural key and a payload, no
hub and no hash key. Split out of `registry.py` because that file has no room left
to give it (789 of 800 lines before this task); see its module docstring for the
paragraph this module is the other half of.

WHAT A REFERENCE TABLE ROW ASSERTS, for the reason `opl.vault.hubs` states it for a
hub: "this code means this description", nothing about when or whether it changed.
CNAE, município, natureza jurídica, motivo, qualificação de sócio and país (see
`opl/vault/domains/cnpj.py` for why the sixth is modelled alongside the brief's
five) are RFB code lists -- `codigo` -> `descricao` -- closed and versioned by the
RFB itself, not evolved by anything this vault observes.

NO HUB, BECAUSE THE NATURAL KEY IS ALREADY THE IDENTITY. A hub exists to give a
business key a STABLE SURROGATE that a link or a satellite can reference without
repeating a wide or composite key. `codigo` is neither: one short column, already
unique within its own type (Task 6's whole trap is that it is NOT unique across
types -- see `opl.vault.reference`), and nothing in this vault joins to it through a
digest. Hashing it would add a column carrying no information the natural key does
not, for a join nothing here performs.

NO HASH KEY, FOR THE SAME REASON AND STATED AS ITS OWN DECISION, because `Hub`,
`Link` and `EffectivitySatellite` all make one a FIELD: a hash key exists to give a
business key a fixed-width, collision-resistant join column, and a reference
table's own natural key already IS that column for anything that will join to it.
Adding one here would be a second, unused spelling of `codigo`.

NO WHOLE-SET GUARD IN `build_registry`, AND THAT IS A FINDING RATHER THAN AN
OMISSION. Every guard in `registry.py` exists because a satellite, a link or an
effectivity satellite names ANOTHER TABLE BY STRING, and the whole set has to be
seen at once to catch a name nobody registers. `ReferenceTable` names no other
table -- no `parent`, no `hubs`, nothing to resolve -- so there is nothing for a
whole-set guard to check that `__post_init__` below has not already refused.
`VaultDomain.__post_init__` and the three `isinstance` guards in `registry.py` all
read `VaultTable` rather than restating the union they refuse or admit, so this
kind needs no new guard function there at all -- only the import and the added word
in the union, which is where `registry.py`'s side of this decision lives.

`natural_key` AND `payload` ARE `str`, NOT `Sequence[str]`, AND THAT IS NOT A
SHORTCUT. `opl.contracts.cnpj_schemas.TABLES['lookup']` is `['codigo', 'descricao']`
-- two columns, fixed, because that is the whole shape of an RFB lookup CSV row --
so a `Sequence[str]` here would model a generality this contract cannot produce, the
same argument `opl.vault.hashing`'s empty-`components` refusal makes about a
zero-length business key: once a caller has consumed the answer, the wrong shape
looks like a modelling decision instead of a bug. Widening it is a deliberate edit
the day a reference source with a wider row arrives, not a defensive default now.

`lookup_type` NAMES WHICH SLICE OF `bronze_cnpj_lookup` THIS TABLE READS, and it is
validated as a non-empty string here and nowhere stronger: this module does not
import `opl.bronze.lookup_routing`, so the registry MECHANISM stays bronze-agnostic
the way `opl.vault.registry` already is. `opl/vault/domains/cnpj.py` sets it FROM
`opl.bronze.lookup_routing.LOOKUP_SUFFIX` rather than retyping the six strings, and
`opl.vault.reference` is where the type actually routes rows -- by calling
`lookup_type_from_filename`, not a second spelling of it -- and where the
motivo/qualificação collision this table exists to prevent is actually closed."""
from __future__ import annotations

from dataclasses import dataclass

from opl.vault.columns import METADATA_COLUMNS


@dataclass(frozen=True, kw_only=True)
class ReferenceTable:
    """A DV2 reference table: a natural key, a payload column, and the bronze
    `lookup_type` that routes rows to it. See the module docstring for why it
    carries neither a hash key nor a whole-set guard."""

    name: str
    lookup_type: str
    natural_key: str
    payload: str

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("a reference table needs a name")
        if not self.lookup_type or not self.lookup_type.strip():
            raise ValueError(f"reference table {self.name!r} needs a lookup_type")
        for role, column in (("natural key", self.natural_key), ("payload", self.payload)):
            if not isinstance(column, str) or not column.strip():
                raise ValueError(
                    f"reference table {self.name!r} needs a {role} column name, got "
                    f"{column!r}"
                )
        if self.natural_key == self.payload:
            raise ValueError(
                f"reference table {self.name!r} names {self.natural_key!r} as both "
                "its natural key and its payload column -- the write would put the "
                "description where the key belongs, or the key where the "
                "description belongs, depending which is projected last"
            )
        reserved = METADATA_COLUMNS & {self.natural_key, self.payload}
        if reserved:
            raise ValueError(
                f"reference table {self.name!r} names {sorted(reserved)} as its "
                "natural key or payload, and the loader writes those itself "
                f"({', '.join(sorted(METADATA_COLUMNS))}). The source's own value "
                "would be silently overwritten by the metadata on the write"
            )
