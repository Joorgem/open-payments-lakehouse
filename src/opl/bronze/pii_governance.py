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
is computed against the roster, so a principal that acquired a table-level `SELECT` out
of band loses it on the next run of the task -- and one that acquired a read this
module cannot take away is reported and fails the run, per the next paragraph.

AND THE LOOK IS WIDER THAN THE REVOKE, WHICH IS THE ONE ASYMMETRY IN THIS MODULE.
`GRANT ALL PRIVILEGES ON TABLE ... TO <p>` makes `p` a reader, and `SHOW GRANTS` reports
that as `ALL PRIVILEGES` -- never as `SELECT`. A lens matching the literal string
`SELECT` was therefore blind to "just give them everything", which is the single most
likely shape of the out-of-band grant the revoke half exists to catch. AND THE OBVIOUS
REMEDY IS WRONG: measured 2026-08-18 on a throwaway table, `REVOKE SELECT ON TABLE ...
FROM <p>` against that grant SUCCEEDS and leaves `ALL PRIVILEGES` standing. The same is
true of a `SELECT` INHERITED FROM THE SCHEMA, which `SHOW GRANTS ON TABLE` does return
(as `ObjectType = SCHEMA`) and which that REVOKE also succeeds against while removing
nothing. So this module LOOKS wide, REVOKES only the one shape a `REVOKE SELECT ON
TABLE` was measured to remove -- a direct, table-level `SELECT` -- and hands the rest
back as an escalation the task PRINTS and then FAILS on. Reporting is not the fallback
here: a read this control cannot close is precisely the condition the revoke half was
written to notice.

AND THE WIDE HALF IS A DENY-LIST OF THE HARMLESS, NOT AN ALLOW-LIST OF THE HARMFUL,
WHICH IS THE SECOND REPAIR AND THE ONE THAT STOPS THIS BUG RECURRING. The first repair
widened a list of read-conferring actions from one entry to two, and a widened list is
still a list: every action not on it was dropped BEFORE anything classified it, so it
produced no revoke, no escalation, no raise and a green run. `MANAGE` was in that gap,
and it is the worst possible occupant -- Databricks documents it as the privilege to
manage privileges and transfer ownership, i.e. one statement away from granting itself
`SELECT`, and documents that `ALL PRIVILEGES` does NOT include it. Measured 2026-08-19
on a throwaway: `GRANT MANAGE ON TABLE` comes back from `SHOW GRANTS ON TABLE` as
`MANAGE | TABLE`; `REVOKE SELECT ON TABLE` leaves it standing; and so does
`REVOKE ALL PRIVILEGES ON TABLE`, which is the remediation the old escalation would
have printed. So the polarity is inverted: an action is dropped only if it is in
`HARMLESS_ACTIONS`, and everything else -- including a privilege the platform has not
invented yet -- lands in the loud branch.

WHAT THIS STATEMENT CANNOT SEE, recorded because two of the three are real reads.
(1) A DESCENDANT OBJECT. `SHOW GRANTS ON TABLE` reaches every privilege that applies to
this table wherever it was granted, ancestors included -- and reaches nothing granted on
an object that READS this table. Measured: `GRANT SELECT ON VIEW v` where `v` reads `t`
does not appear on `SHOW GRANTS ON TABLE t`. `create_dataops_views`, a task in the same
job as the governance task, creates two views over the socios tables; they project
`COUNT(*)`, `_batch_id` and `_source_file` and no name column, which
`tests/dataops/test_views.py` now holds. (2) OWNERSHIP, which confers a permanent read
and which `ALTER TABLE ... OWNER TO` moves -- something a `MANAGE` holder can issue. It
is bounded by the mask, which this repo measured applies to the owner as well; the grant
half of it is not bounded by anything here. Not closed, and deliberately not probed by
the task: its statement list is a paste-lock this repo treats as a proof, and an owner
read whose expected value nobody has reviewed would be a new way to fail a run without
being a control. (3) A principal reading through a `MANAGE` it has not yet spent, which
is exactly why (see above) `MANAGE` now raises instead of being dropped.

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

# The privilege this module ISSUES and REVOKES. Exactly one, named rather than
# `ALL PRIVILEGES`: a revoke loop that swept every action type would fight whatever
# the platform or a future bundle granted, and the privilege that reveals a personal
# name is this one.
GOVERNED_PRIVILEGE = "SELECT"

# WHAT IS DROPPED WITHOUT A VERDICT, and it is the ONLY thing that is -- see the
# docstring's polarity paragraph. Everything else observed on a governed table is
# revoked or escalated, so a privilege this project has never heard of is loud rather
# than silent.
#
# THE SET IS MEASURED IN BOTH HALVES. Which actions can appear at all: on 2026-08-19,
# `GRANT <p> ON TABLE <throwaway>` was issued for thirteen privileges against this
# metastore (privilege version 1.0), and exactly six were applicable -- SELECT, MODIFY,
# APPLY TAG, MANAGE, READ METADATA and ALL PRIVILEGES. BROWSE, USE SCHEMA, CREATE TABLE,
# EXECUTE, REFRESH and MODIFY CLEAN ROOM were all refused with
# `PRIVILEGE_NOT_APPLICABLE_TO_ENTITY`, which is why they are not listed here: an entry
# for a privilege that cannot reach this statement is an entry nothing can check.
# Whether each confers a read is the documentation's: MODIFY is "insert, update, and
# delete data" and needs SELECT alongside it to read rows; APPLY TAG is "add and edit
# tags"; READ METADATA is metadata "without the ability to ... read its data".
#
# `ALL PRIVILEGES` IS NOT HERE AND NEITHER IS `MANAGE`, which is the whole point.
# `SHOW GRANTS` spells the first with a space and `information_schema.table_privileges`
# with an underscore (`ALL_PRIVILEGES`), and neither catalog expands it into a `SELECT`
# row of its own; `_normalised_action` folds the underscore and the space so one
# constant matches whichever catalog is being read, in either direction.
HARMLESS_ACTIONS: frozenset[str] = frozenset({"MODIFY", "APPLY TAG", "READ METADATA"})

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


class UngovernedRead(RuntimeError):
    """A read of a governed table that this module cannot close, so it is RAISED.

    Not a warning and not a log line: the escalation exists because `REVOKE SELECT ON
    TABLE` was measured to SUCCEED and change nothing against `ALL PRIVILEGES` and
    against a schema-inherited `SELECT`. A task that printed and carried on would
    report SUCCESS over a reader of 55.8M personal names it had just failed to remove
    -- the same shape as the absence this whole module was written to replace."""


@dataclass(frozen=True)
class ObservedGrant:
    """One `SHOW GRANTS` row this control has to settle -- revoke it or escalate it.

    Not "one row that confers a read": whether it does is exactly what this module was
    twice wrong about, so the type carries what the catalog said and the verdict is
    taken from `revocable` and `HARMLESS_ACTIONS` rather than assumed by arriving here.

    Four fields, not two, and that is the earlier repair: the first reader kept
    `(Principal, ActionType)` and discarded `ObjectType`/`ObjectKey`, so an inherited
    `SELECT` was indistinguishable from a table one and got a REVOKE that succeeded
    while changing nothing -- printed as `REVOKED`, a claim with no evidence."""

    principal: str
    action: str
    object_type: str
    object_key: str

    @property
    def revocable(self) -> bool:
        """Whether `REVOKE SELECT ON TABLE` removes this row. Measured three ways on a
        throwaway, 2026-08-18: a direct table-level `SELECT` disappears from
        `SHOW GRANTS` after it, while `ALL PRIVILEGES` on the table and a `SELECT`
        inherited from the schema both SURVIVE it -- and the statement SUCCEEDS in
        every case, which is why "it did not raise" is not evidence of a revoke."""
        return self.action == GOVERNED_PRIVILEGE and self.object_type == "TABLE"


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

    Columns, measured: `Principal | ActionType | ObjectType | ObjectKey`. ALL FOUR are
    read, and an earlier version of this module took the first two and threw the rest
    away -- which made a grant ON THE TABLE indistinguishable from one INHERITED FROM
    THE SCHEMA. Both come back here: measured 2026-08-18, `GRANT SELECT ON SCHEMA s TO
    p` appears in `SHOW GRANTS ON TABLE s.t` as `p | SELECT | SCHEMA | s`. What does
    NOT come back is a privilege a table cannot carry: `workspace.default` grants
    `USE SCHEMA` and five `CREATE *` to `_workspace_users_workspace_<id>`, and
    `SHOW GRANTS ON TABLE workspace.default.bronze_cnpj_socios` returns zero rows.

    SO THE REACH IS UPWARDS ONLY: every privilege that applies to THIS TABLE, wherever
    among its ANCESTORS it was granted. Not "every privilege a reader needs", and --
    the correction -- not every object through which this table can be read. A
    DESCENDANT is outside it: measured, `GRANT SELECT ON VIEW v` where `v` reads `t`
    does not appear on `SHOW GRANTS ON TABLE t`. See the module docstring for what that
    costs here today, which is nothing, and for why."""
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


def plan_grants(observed: tuple[str, ...], roster: tuple[str, ...] | None = None) -> GrantPlan:
    """The difference between what the catalog says and what the roster declares.

    Sorted in both directions so the statements a run issues are a function of the
    two sets and not of the order `SHOW GRANTS` happened to return. Revoking is not
    conditional on the roster being non-empty: an empty roster means NOBODY may hold
    SELECT on these tables, which is this project's current state and is a stronger
    statement than issuing nothing.

    `roster=None` means the module's reviewed roster, READ AT CALL TIME rather than
    bound as a default at import. That is what lets a test substitute a non-empty
    roster and exercise the ordering the safety argument rests on -- with `PII_READERS`
    as a default argument, every test in this repository ran against an empty grant
    list and "every REVOKE precedes every GRANT" could not fail. `roster=()` still
    means the empty roster, and is not the same thing as `None`.

    `set(observed)` is LOAD-BEARING rather than stylistic. `SHOW GRANTS` returns one
    row per (principal, privilege, securable), so a principal holding both a table
    grant and a schema grant comes back TWICE -- measured 2026-08-18: one principal,
    three rows. Under the four-column reader those rows are now told apart and only
    the table-level one reaches this function, so that particular duplicate no longer
    arrives; the set stays because nothing here should depend on the catalog never
    repeating a principal, and because the difference is set arithmetic anyway."""
    held = set(observed)
    declared = set(PII_READERS if roster is None else roster)
    return GrantPlan(
        grant=tuple(sorted(declared - held)),
        revoke=tuple(sorted(held - declared)),
    )


def _normalised_action(action: object) -> str:
    """One spelling for a privilege the two catalogs spell differently. Measured:
    `SHOW GRANTS` returns `ALL PRIVILEGES`, `information_schema.table_privileges`
    returns `ALL_PRIVILEGES`, for the same grant."""
    return " ".join(str(action).upper().replace("_", " ").split())


def grants_to_settle(rows: object) -> tuple[ObservedGrant, ...]:
    """Every observed grant this control must settle -- the WIDE half, by EXCLUSION.

    A filter on "is it one of the reads we thought of" is what made this module blind
    to `MANAGE` twice; the question asked here is "is it one of the few we measured to
    confer nothing", and everything else is revoked or escalated.

    Takes 4-tuples rather than Spark `Row`s so this module stays importable where
    pyspark is not; the task is what knows the column names. Order is the catalog's,
    which is what a reported escalation should quote."""
    return tuple(
        ObservedGrant(
            principal=str(principal),
            action=_normalised_action(action),
            object_type=str(object_type).upper(),
            object_key=str(object_key),
        )
        for principal, action, object_type, object_key in rows  # type: ignore[attr-defined]
        if _normalised_action(action) not in HARMLESS_ACTIONS
    )


def revocable_principals(grants: tuple[ObservedGrant, ...]) -> tuple[str, ...]:
    """The principals whose read this task can actually withdraw -- the NARROW half,
    and the only input `plan_grants` is allowed to have."""
    return tuple(grant.principal for grant in grants if grant.revocable)


def unrevocable_grants(grants: tuple[ObservedGrant, ...]) -> tuple[ObservedGrant, ...]:
    """Everything this task cannot withdraw: a read it cannot close (`ALL PRIVILEGES`,
    a schema-inherited `SELECT`), a privilege that can grant itself one (`MANAGE`), and
    any action this project has not classified. Never silently dropped: the task prints
    each one and then fails on it -- see `UngovernedRead`."""
    return tuple(grant for grant in grants if not grant.revocable)


def unrevocable_escalation(table: str, grant: ObservedGrant) -> str:
    """What an operator has to be told, including the statement that does close it.

    The remediation ECHOES THE OBSERVED ACTION rather than naming one blanket revoke,
    and that is measured rather than tidy: on a throwaway holding all six table-
    applicable privileges, `REVOKE ALL PRIVILEGES ON TABLE` removed ALL PRIVILEGES,
    MODIFY and APPLY TAG and LEFT `MANAGE` and `READ METADATA` standing, which the
    documentation states as `ALL PRIVILEGES` not including them. `REVOKE MANAGE ON
    TABLE` removed MANAGE, and `REVOKE SELECT ON SCHEMA s` removed the inherited one --
    each verified by re-reading `SHOW GRANTS` after."""
    return (
        f"{grant.principal} can read {table}, or grant itself the read, through "
        f"{grant.action} on {grant.object_type} {grant.object_key} -- which this task "
        f"CANNOT withdraw: a REVOKE {GOVERNED_PRIVILEGE} ON TABLE removes only a direct "
        f"table-level {GOVERNED_PRIVILEGE} (measured), so against this it would succeed "
        f"and remove nothing. Close it with: REVOKE {grant.action} ON "
        f"{grant.object_type} {grant.object_key} FROM `{grant.principal}`"
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
