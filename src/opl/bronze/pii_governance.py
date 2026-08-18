"""Who may READ the socios tables, and what the catalog says those columns HOLD.

The mask in `opl.bronze.masking` decides what a reader who is already through the
door SEES. This module decides who is at the door at all, and it records the
classification that makes the whole control legible to someone who never reads this
repository. Same shape as `masking`: it builds SQL strings and nothing else -- no
session, no `spark.sql`, no catalog -- so every statement below is pinned by a unit
test on a machine with no Databricks.

WHY THIS IS NOT IN THE BUNDLE, WHICH IS WHERE A REVIEWER WILL LOOK FIRST. A
Databricks Asset Bundle has no `tables` resource, and `grants` is a field on Catalog,
Schema, Volume, RegisteredModel, ExternalLocation and VectorSearchIndex -- never on a
table. Declaring the schema instead fails three separate ways here: this repo's only
target is `mode: development`, which rewrites `name: default` to `dev_<prefix>_default`
and would deploy green while governing a NEW, EMPTY schema; a production target keeps
the name and then collides with the existing `default`, owned by
`_workspace_admins_workspace_<id>`; and `resources.<securable>.grants` is
AUTHORITATIVE, so declaring it would revoke `_workspace_users_workspace_<id>`'s
`USE SCHEMA / CREATE TABLE / CREATE FUNCTION / CREATE VOLUME / CREATE MODEL /
CREATE MATERIALIZED VIEW` on the schema every pipeline in this project writes into.
`bundle deployment bind` would adopt the schema and put 55.8M rows of personal data
inside `bundle destroy`'s blast radius. So: imperative, idempotent, in a job. ADR 0008.

THE SHAPE OF THE MODEL IS NOT WHAT "RBAC" USUALLY MEANS, AND SAYING SO IS PART OF THE
DELIVERABLE. A workspace-local group WORKS in a mask predicate and is REFUSED as a
grant principal -- `GRANT SELECT ... TO opl_pii_readers` returns
`PRINCIPAL_DOES_NOT_EXIST` (SQLSTATE 42704), measured. The only account group that
resolves in this workspace is `account users`, i.e. everyone, which cannot function as
a control. So the shipped model is necessarily TWO objects that cannot be one: the
workspace-local group `opl_pii_readers` in the mask predicate, and `SELECT` granted
PER PRINCIPAL here. A reviewer expecting a single `GRANT SELECT ... TO
opl_pii_readers` is looking for a statement this platform refuses.

BOTH HALVES MUST HOLD FOR A NAME TO BE READ, AND EITHER DRIFT FAILS CLOSED. A
principal in the group without `SELECT` cannot open the table at all; a principal with
`SELECT` who is not in the group reads `***`. That is defence in depth by accident of
the platform rather than by design, and it is worth keeping for the accident.

AND THE TWO HALVES CLOSE AT DIFFERENT SPEEDS, WHICH DECIDES WHICH ONE IS THE CONTROL.
Measured on the opl-free warehouse with a fresh OAuth token on every read: `REVOKE`
takes effect on the VERY NEXT STATEMENT, while a group-membership change is not
visible to `is_member` for MINUTES in BOTH directions. So the grant is the half that
can be closed in a hurry, and the group is the half that cannot. See ADR 0008.

THE ROSTER IS A CONSTANT IN REVIEWED CODE, not a job parameter and not read back out
of the group. A parameter default cannot validate anything, and reading the group
would make the grants a function of workspace state nobody reviewed -- while ALSO
being impossible to state as SQL, since the group is not a grant principal. It is
empty today, which is a decision: the real socios tables stay fail-closed, and the
permissive branch of THIS project's mask stays unexercised. ADR 0008 says so plainly
rather than letting an empty tuple imply it was overlooked.

THE REVOKE HALF IS THE POINT, not a symmetry. ADR 0008's own weakest paragraph says
what guards these tables today is that no `GRANT SELECT` was ever issued -- "an
absence, not a control. One GRANT reverses it, nothing in this repository would
notice, and no test asserts the empty result." This module is what notices: the plan
is computed against the roster, so a principal that acquired `SELECT` out of band
loses it on the next run of the task.

THE TAG VOCABULARY IS THE ACCOUNT'S, NOT ONE THIS PROJECT INVENTED, and the call that
establishes that is recorded because an earlier claim about it named no endpoint:
`GET /api/2.1/tag-policies` returns **70** governed tag policies on this account,
`class.name`, `class.br_cpf` and `class.br_cnpj` among them. (`/api/2.0/tag-policies`
answers that it is deprecated and names the 2.1 path; `/api/2.0/account/tag-policies`
is `Not Found`; `SHOW TAG POLICIES` is a parse error.) Their allowed-value list is
EMPTY, which does not mean unusable -- it means the empty string is the only assignable
value, measured: `SET TAGS ('class.name' = 'personal_name')` is refused
(`INVALID_PARAMETER_VALUE`, "not an allowed value ... Allowed values: []") and
`SET TAGS ('class.name' = '')` succeeds and reads back from
`information_schema.column_tags`. A dot is otherwise a RESERVED CHARACTER in a tag key
(`Tag key contains reserved characters (., =, >, <, %, &, ?, \\)`), so a bespoke `opl.*`
namespace is unavailable and the governed keys are the only dotted ones that exist.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.bronze.masking import MASKED_COLUMNS
from opl.contracts.cnpj_schemas import TABLES

# The workspace-local group whose members the mask admits, restated here rather than
# imported so that a reader of THIS module sees which group the grants are the other
# half of. `test_the_two_halves_name_the_same_group` holds it equal to masking's.
PII_READER_GROUP = "opl_pii_readers"

# WHO HOLDS `SELECT` ON THE SOCIOS TABLES. Empty, deliberately: see the module
# docstring. Entries are grant principals -- a service principal's APPLICATION ID
# (the UUID), a user's email -- and never a workspace-local group, which the platform
# refuses. Anything here is granted; anything found holding SELECT and NOT here is
# revoked.
PII_READERS: tuple[str, ...] = ()

# ALL THREE SOCIOS TABLES, WHICH IS ONE MORE THAN THE MASK COVERS, and the difference
# is the whole reason this module exists. `masking` excludes STAGING because a mask
# there would make `promote_batch` read `***` and append it into bronze. A GRANT does
# nothing of the kind: it changes who may open the table and changes no value any
# reader gets. Staging is where the exposure actually is -- it holds the names IN THE
# CLEAR and nothing drains it -- so excluding it here would be excluding the table the
# control is most needed on, for a reason that belongs to a different mechanism.
GOVERNED_ROLES: tuple[str, ...] = ("bronze", "quarantine", "staging")

# The privilege this module is authoritative over. Exactly one, named rather than
# `ALL PRIVILEGES`: a revoke loop that swept every action type would fight whatever
# the platform or a future bundle granted, and the privilege that reveals a personal
# name is this one.
GOVERNED_PRIVILEGE = "SELECT"

# The governed tag keys this project uses, from the account's own 70. The value is the
# empty string because that is the only value these policies admit.
TAG_NAME = "class.name"
TAG_BR_CPF = "class.br_cpf"
TAG_BR_CNPJ = "class.br_cnpj"
GOVERNED_TAG_VALUE = ""

# WHAT EACH COLUMN HOLDS, in the account's vocabulary, keyed by CONTRACT for the same
# reason `MASKED_COLUMNS` is: the classification follows the data, so a second table
# ingesting socios inherits it.
#
# `cpf_cnpj_socio` carries BOTH keys and that is not hedging -- `identificador_socio`
# decides which one a row holds, so a single key would be false for the other kind of
# partner. It is classified and deliberately NOT masked: the Receita already masks it
# at source (`***DDDDDD**`, six middle digits, irreversible), which is ADR 0008's
# central context claim -- and a tag is where that claim becomes visible to someone
# reading the catalog instead of the ADR.
#
# `cnpj_basico` is deliberately absent. It is a CNPJ and tagging it would be true, but
# it identifies the COMPANY; this control is about natural persons, and a
# classification that drifts into "every identifier we could name" stops meaning
# anything.
CLASSIFIED_COLUMNS: dict[str, dict[str, tuple[str, ...]]] = {
    "socios": {
        "nome_socio_razao_social": (TAG_NAME,),
        "nome_do_representante": (TAG_NAME,),
        "cpf_cnpj_socio": (TAG_BR_CPF, TAG_BR_CNPJ),
    },
}


class UngrantablePrincipal(ValueError):
    """A principal this module refuses to interpolate into a GRANT."""


@dataclass(frozen=True)
class GrantPlan:
    """What one table's `SELECT` grants must become. Immutable, and derived from the
    observation rather than from what was issued -- a plan computed from the
    statements this run intends to send could not express "somebody else granted
    SELECT out of band", which is the case the revoke half exists for."""

    grant: tuple[str, ...]
    revoke: tuple[str, ...]


def _quoted(principal: str) -> str:
    """A backtick-quoted grant principal, refusing one that could break out.

    The roster is code and not user input today, and that is exactly when this is
    cheap to write: the day a principal arrives from a job parameter or a catalog
    read, the escape is already here rather than being the thing nobody added."""
    if not principal or any(character in principal for character in "`\\;\n\r"):
        raise UngrantablePrincipal(
            f"{principal!r} is not a principal this module will put in a GRANT: it is "
            "empty or carries a character that would end the quoted identifier"
        )
    return f"`{principal}`"


def show_grants_sql(table: str) -> str:
    """What the catalog currently says about `table`.

    Columns, measured: `Principal | ActionType | ObjectType | ObjectKey`. Only grants
    ON THE TABLE come back -- `USE CATALOG` and `USE SCHEMA`, which a reader also
    needs, are on other securables and are deliberately out of this module's reach.
    Traversal without SELECT reveals nothing but a name."""
    return f"SHOW GRANTS ON TABLE {table}"


def grant_select_ddl(table: str, principal: str) -> str:
    """Takes effect on the NEXT statement -- measured, not assumed, which is why the
    two-principal proof uses GRANT/REVOKE and never a membership flip."""
    return f"GRANT {GOVERNED_PRIVILEGE} ON TABLE {table} TO {_quoted(principal)}"


def revoke_select_ddl(table: str, principal: str) -> str:
    """The half ADR 0008 was missing. Measured: the revoked principal's very next
    statement fails `INSUFFICIENT_PERMISSIONS`, SQLSTATE 42501, while it is still a
    member of the group -- so this is the control that can be closed in a hurry."""
    return f"REVOKE {GOVERNED_PRIVILEGE} ON TABLE {table} FROM {_quoted(principal)}"


def set_column_tag_ddl(table: str, column: str, tag_key: str) -> str:
    """Classify one column with one governed tag key.

    Idempotent: re-applying an identical tag succeeds (measured). The value is always
    `GOVERNED_TAG_VALUE`, which is the empty string, because a governed policy with an
    empty allowed-value list admits nothing else -- a non-empty value is refused with
    `INVALID_PARAMETER_VALUE`."""
    return (
        f"ALTER TABLE {table} ALTER COLUMN `{column}` "
        f"SET TAGS ('{tag_key}' = '{GOVERNED_TAG_VALUE}')"
    )


def classified_columns(contract: str) -> tuple[tuple[str, str], ...]:
    """(column, tag key) for `contract`, in declaration order, one pair per key."""
    return tuple(
        (column, tag_key)
        for column, tag_keys in CLASSIFIED_COLUMNS.get(contract, {}).items()
        for tag_key in tag_keys
    )


def governed_contracts() -> tuple[str, ...]:
    """The contracts this module governs: exactly the ones that declare a mask.

    Derived from `MASKED_COLUMNS` rather than listed again, so a contract that gains
    a mask gains its grants and its tags in the same edit. Sorted so the statement
    order a test pins does not depend on dict insertion."""
    return tuple(sorted(MASKED_COLUMNS))


def plan_grants(observed: tuple[str, ...], roster: tuple[str, ...] = PII_READERS) -> GrantPlan:
    """The difference between what the catalog says and what the roster declares.

    Sorted in both directions so the statements a run issues are a function of the
    two sets and not of the order `SHOW GRANTS` happened to return. Revoking is not
    conditional on the roster being non-empty: an empty roster means NOBODY may hold
    SELECT on these tables, which is this project's current state and is a stronger
    statement than issuing nothing."""
    held = set(observed)
    declared = set(roster)
    return GrantPlan(
        grant=tuple(sorted(declared - held)),
        revoke=tuple(sorted(held - declared)),
    )


def selecting_principals(rows: object) -> tuple[str, ...]:
    """The principals holding `SELECT`, out of `SHOW GRANTS`'s (principal, action)
    pairs. Takes pairs rather than Spark `Row`s so this module stays importable where
    pyspark is not -- the task is what knows the column names."""
    return tuple(
        str(principal)
        for principal, action in rows  # type: ignore[attr-defined]
        if str(action).upper() == GOVERNED_PRIVILEGE
    )


def _assert_every_classified_column_exists() -> None:
    """At IMPORT, for the same reason the registry's mask/CHECK guard is: a tag on a
    column that does not exist fails at RUN time, inside the job, after the grants
    have already been issued -- and this module is imported by the task that issues
    them, so the failure moves to CI and to any import at all."""
    for contract, columns in CLASSIFIED_COLUMNS.items():
        unknown = sorted(set(columns) - set(TABLES[contract]))
        if unknown:
            raise ValueError(
                f"{contract} classifies {unknown}, which are not its columns "
                f"({', '.join(TABLES[contract])})"
            )


_assert_every_classified_column_exists()
