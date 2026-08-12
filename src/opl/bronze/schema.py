"""Spark StructType built from a versioned column contract. Bronze is all-string
on purpose: preserves leading zeros, future alphanumeric CNPJ keys, decimal-comma
amounts and AAAAMMDD dates verbatim; silver casts.

ALL-STRING IS ALSO WHAT MAKES THE PAYMENT STREAM'S DRIFT MEASURABLE, which is a
different argument reaching the same schema. The generator emits every contract
value as a non-empty string (`opl.generator.events.record_of` is typed
`dict[str, str]`), so a NULL in the payments bronze table has exactly one cause:
the JSON did not carry the field. See `opl.contracts.payments`.

READS THE CATALOGUE, NOT `cnpj_schemas`, since F1b Task 3. The function is keyed on
a CONTRACT and payments is now one; asking `cnpj_schemas` would have made this the
place that decides a generated source does not exist."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from opl.contracts.catalogue import columns_for


def struct_for(contract: str) -> StructType:
    """The all-string read schema for `contract`. KeyError if unknown.

    THE DRIFT COLUMN IS NOT IN IT, and that absence is load-bearing rather than
    incidental: `opl.contracts.payments.COLUMNS` deliberately excludes
    `DRIFT_COLUMN`, so a drifted record's extra key matches no declared field and
    Auto Loader routes it to `_rescued_data` -- which fires `dq.evaluate`'s
    highest-precedence rule. A read schema that declared the column would ABSORB the
    drift instead, silently, leaving the gate green over a defect it exists to
    catch. `payments._assert_no_drifting_column_is_declared_by_v1` refuses that edit
    at import; this docstring is where the consequence lands."""
    return StructType([StructField(c, StringType(), True) for c in columns_for(contract)])
