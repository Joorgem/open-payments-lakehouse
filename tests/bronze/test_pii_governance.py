# tests/bronze/test_pii_governance.py
"""The OTHER half of the socios column mask: who may open the tables, and what the
catalog says their columns hold.

WHAT CAN BE TESTED HERE AND WHAT CANNOT, the same split `test_masking.py` draws. No
statement in this module reaches Unity Catalog -- `GRANT`, `SHOW GRANTS` and
`SET TAGS` exist only on Databricks -- so what is pinned locally is the SQL that will
be issued and the pure decision that chooses it. The two facts that can only be
measured against a workspace are recorded in ADR 0008 with the statement ids: a
workspace-local group is REFUSED as a grant principal, and the governed tag policies
admit only the empty value.

THE DECISION FUNCTION IS TESTED SEPARATELY FROM THE SQL, deliberately. `plan_grants`
is where "revoke anything the roster does not name" lives, and it is the half ADR 0008
records as missing -- "an absence, not a control. One GRANT reverses it, nothing in
this repository would notice". A test that only compared strings would pin the shape
of a REVOKE without ever asking whether one gets issued.
"""
from __future__ import annotations

import pytest

from opl.bronze import masking, pii_governance
from opl.bronze.pii_governance import (
    CLASSIFIED_COLUMNS,
    GOVERNED_PRIVILEGE,
    GOVERNED_ROLES,
    GOVERNED_TAG_VALUE,
    HARMLESS_ACTIONS,
    PII_READER_GROUP,
    PII_READERS,
    TAG_BR_CNPJ,
    TAG_BR_CPF,
    TAG_NAME,
    UngrantablePrincipal,
    classified_columns,
    governed_contracts,
    grant_select_ddl,
    grants_to_settle,
    plan_grants,
    revocable_principals,
    revoke_select_ddl,
    set_column_tag_ddl,
    show_grants_sql,
    unrevocable_escalation,
    unrevocable_grants,
)
from opl.contracts.cnpj_schemas import TABLES

# INVENTED, and that is the point: it has the SHAPE the platform requires of a grant
# principal for a service principal -- an applicationId, i.e. a UUID -- and names
# nothing in any workspace. The earlier fixture was the real applicationId of F4's
# probe principal, which is deleted but was still a live-workspace identifier sitting
# in three test files with nothing saying so.
_SP = "5f2c8a10-0d4e-4b77-9a31-6c0e7d21b845"
_TABLE = "workspace.default.bronze_cnpj_socios"
_SCHEMA = "workspace.default"


# --------------------------------------------------------------------------
# The roster, and the two halves it is one of
# --------------------------------------------------------------------------


def test_the_roster_is_empty_and_that_is_a_decision():
    """THE HONEST STATUS OF THIS CONTROL, pinned so it cannot drift silently.

    An empty roster means nobody holds `SELECT` on the socios tables, so the real
    mask's PERMISSIVE branch stays unexercised -- which ADR 0008 states plainly rather
    than letting the absence imply it was overlooked. Adding a principal here is a
    decision to let a real identity read 55.8M real personal names, and this test
    going red is what makes that decision arrive at a reviewer instead of in a diff
    nobody read closely."""
    assert PII_READERS == (), (
        "a principal was added to the PII reader roster. That is a real decision and "
        "may be the right one -- update ADR 0008's 'the permissive branch remains "
        "unexercised by choice' paragraph in the same commit, or this repository now "
        "says something false about who reads personal names."
    )


def test_the_two_halves_name_the_same_group():
    """The mask predicate and the grants are two objects because the platform refuses
    to let them be one -- a workspace-local group works in a mask predicate and is
    refused as a grant principal. Two objects can disagree; this is what stops the
    group name from doing so."""
    assert PII_READER_GROUP == masking.PII_READER_GROUP


def test_every_contract_that_is_masked_is_also_governed(monkeypatch):
    """Grants and tags are derived from `MASKED_COLUMNS` rather than listed again, so
    a contract that gains a mask gains its access control in the same edit. If these
    ever came apart, the new masked table would be the one nothing revokes on.

    THE DERIVATION IS WHAT IS TESTED, AND THE FIRST VERSION OF THIS TEST DID NOT TEST IT.
    It read `governed_contracts() == tuple(sorted(masking.MASKED_COLUMNS))` -- the
    expected side spelled the function body character for character over the same
    imported dict, so both sides moved together for any edit. Measured by F4's closing
    code review: replacing the body with a frozen `return ("socios",)` left fifty-five
    tests green. The value was pinned; the derivation was not.

    THE ASYMMETRY IS WHY IT MATTERED. The MASK side of a second masked contract is
    genuinely locked -- `tests/test_governance_job_wiring.py` derives the job's
    `ensure_masked_table` tasks from `MASKED_COLUMNS` and holds them equal in both
    directions -- so CI would go red until the YAML named the new table. The
    GRANT/REVOKE/TAG side would not: `governed_tables()` reads this function, so a new
    masked table would get no `SHOW GRANTS`, no revoke and no tags, while
    `apply_pii_governance` printed the OLD count and exited 0. That is the half ADR 0008
    calls the actual control."""
    # Today's answer, as an independent literal rather than as the expression under test.
    assert governed_contracts() == ("socios",)
    # And the derivation itself: a contract added to the mask map must appear here
    # without this module being edited. A frozen literal fails this and passes the line
    # above, which is exactly the mutation that went undetected.
    monkeypatch.setitem(masking.MASKED_COLUMNS, "zz_probe", ("some_name_column",))
    assert governed_contracts() == ("socios", "zz_probe")


def test_staging_is_governed_here_although_the_mask_refuses_it():
    """THE ONE PLACE THIS MODULE AND `masking` DELIBERATELY DISAGREE.

    `ensure_masked_table` never names staging: a MASK there would make `promote_batch`
    read `***` and append it into bronze, and would stop the DQ rule that rejects a
    missing name. A GRANT does neither -- it changes who may open the table and
    changes no value any reader gets. Staging holds the names IN THE CLEAR and nothing
    drains it, so it is the table this control is most needed on."""
    assert "staging" in GOVERNED_ROLES
    assert set(GOVERNED_ROLES) == {"bronze", "quarantine", "staging"}


# --------------------------------------------------------------------------
# The plan: what actually gets issued
# --------------------------------------------------------------------------


def test_a_principal_holding_select_that_the_roster_does_not_name_is_revoked():
    """ADR 0008's own weakest paragraph, closed. What guards these tables today is
    that no `GRANT SELECT` was ever issued -- "an absence, not a control. One GRANT
    reverses it, nothing in this repository would notice." This is what notices."""
    plan = plan_grants((_SP,), roster=())
    assert plan.revoke == (_SP,)
    assert plan.grant == ()


def test_an_empty_roster_still_revokes_rather_than_doing_nothing():
    """The failure this guards against is a plan that treats "nobody is declared" as
    "there is nothing to do". An empty roster is a statement -- NOBODY may hold SELECT
    on these tables -- and it is this project's current state."""
    plan = plan_grants(("someone@example.com", _SP), roster=())
    assert plan.revoke == (_SP, "someone@example.com"), "sorted, both revoked"


def test_a_principal_that_already_holds_select_is_not_granted_again():
    """Idempotence, and it is computed from the CATALOG rather than from what this run
    intends -- which is the only way the revoke half can see a grant issued out of
    band."""
    plan = plan_grants((_SP,), roster=(_SP,))
    assert plan == plan_grants((_SP,), roster=(_SP,))
    assert plan.grant == () and plan.revoke == ()


def test_a_declared_principal_that_holds_nothing_is_granted():
    plan = plan_grants((), roster=(_SP,))
    assert plan.grant == (_SP,) and plan.revoke == ()


def _rows(*rows: tuple[str, str, str, str]) -> list[tuple[str, str, str, str]]:
    """`SHOW GRANTS ON TABLE`'s four columns, in its own order:
    `Principal | ActionType | ObjectType | ObjectKey`."""
    return list(rows)


def test_only_a_privilege_measured_to_confer_nothing_is_dropped_without_a_verdict():
    """THE POLARITY, stated as the only actions that leave no trace.

    `SHOW GRANTS` returns every action type on the table and this module revokes exactly
    one, so something has to be dropped. What is dropped is a MEASURED set of three:
    2026-08-19, thirteen privileges were granted against a throwaway in this metastore
    and six were applicable at all -- SELECT, MODIFY, APPLY TAG, MANAGE, READ METADATA,
    ALL PRIVILEGES -- and of those, the documentation gives MODIFY ("insert, update, and
    delete data", SELECT still required to read rows), APPLY TAG and READ METADATA
    ("without the ability to ... read its data") as conferring no read."""
    assert HARMLESS_ACTIONS == {"MODIFY", "APPLY TAG", "READ METADATA"}
    grants = grants_to_settle(
        _rows(
            ("a@x", "SELECT", "TABLE", _TABLE),
            ("b@x", "MODIFY", "TABLE", _TABLE),
            ("c@x", "APPLY_TAG", "TABLE", _TABLE),
            ("e@x", "READ METADATA", "TABLE", _TABLE),
            ("d@x", "select", "table", _TABLE),
        )
    )
    assert [grant.principal for grant in grants] == ["a@x", "d@x"]
    assert revocable_principals(grants) == ("a@x", "d@x")
    assert GOVERNED_PRIVILEGE == "SELECT"


def test_manage_is_a_read_this_control_cannot_close_and_must_not_drop():
    """THE SAME BUG'S SECOND APPEARANCE, and the reason the lens is now an exclusion.

    The first repair widened a list of read-conferring actions from one entry to two.
    A widened list is still a list: `MANAGE` was not on it, so it was dropped before
    anything classified it -- no revoke, no escalation, no raise, green run. It is the
    worst occupant that gap could have had. Databricks documents `MANAGE` as managing
    privileges and transferring ownership, i.e. one statement from granting itself
    SELECT, and documents that `ALL PRIVILEGES` does not include it -- measured
    2026-08-19 on a throwaway holding all six applicable privileges: `REVOKE ALL
    PRIVILEGES ON TABLE` removed ALL PRIVILEGES, MODIFY and APPLY TAG and LEFT MANAGE
    (and READ METADATA) standing; `REVOKE MANAGE ON TABLE` removed it."""
    grants = grants_to_settle(_rows((_SP, "MANAGE", "TABLE", _TABLE)))
    assert [grant.action for grant in grants] == ["MANAGE"], "it must not be dropped"
    assert revocable_principals(grants) == (), "and REVOKE SELECT would not remove it"
    assert unrevocable_grants(grants) == grants, "so the run has to fail on it"
    assert f"REVOKE MANAGE ON TABLE {_TABLE} FROM `{_SP}`" in (
        unrevocable_escalation(_TABLE, grants[0])
    ), "the remediation must echo the observed action, never ALL PRIVILEGES"


def test_a_privilege_this_project_has_never_heard_of_is_loud_rather_than_silent():
    """THE PROPERTY THE INVERSION BUYS, and the only one that stops a third instance.

    Unity Catalog gains privileges; this repository does not learn about them on the day
    they ship. Under the old filter the next one landed in the silent branch by default.
    Under this one it lands in the branch that prints a line and fails the run, and the
    cost of being wrong is a governance run an operator has to look at rather than a
    reader nobody sees."""
    grants = grants_to_settle(_rows((_SP, "READ EVERYTHING SOMEDAY", "TABLE", _TABLE)))
    assert unrevocable_grants(grants) == grants
    assert "READ EVERYTHING SOMEDAY" in unrevocable_escalation(_TABLE, grants[0])


def test_all_privileges_is_a_read_and_the_old_lens_could_not_see_it():
    """THE HOLE THIS LENS WAS WIDENED FOR, and it is the likeliest shape of the
    out-of-band grant the revoke half exists to catch: "just give them everything".

    Measured 2026-08-18 on a throwaway: `GRANT ALL PRIVILEGES ON TABLE t TO p` is
    reported by `SHOW GRANTS` as `p | ALL PRIVILEGES | TABLE | t` -- never as SELECT --
    so a lens matching the literal string `SELECT` put the principal in NEITHER list
    and issued nothing at all."""
    grants = grants_to_settle(_rows((_SP, "ALL PRIVILEGES", "TABLE", _TABLE)))
    assert [grant.principal for grant in grants] == [_SP], "the wide half must see it"
    assert revocable_principals(grants) == (), "and the narrow half must not touch it"
    assert unrevocable_grants(grants) == grants


def test_both_spellings_of_all_privileges_are_the_same_privilege():
    """`SHOW GRANTS` says `ALL PRIVILEGES`, `information_schema.table_privileges` says
    `ALL_PRIVILEGES`, for the same grant -- both measured on the same throwaway. One
    normalisation matches either. It runs in both directions: the test above hands in
    `APPLY_TAG` and the harmless set spells it `APPLY TAG`, which is how this metastore
    returns it, so a grant that confers nothing is not escalated over a separator."""
    for spelling in ("ALL PRIVILEGES", "ALL_PRIVILEGES", "all privileges"):
        grants = grants_to_settle(_rows((_SP, spelling, "TABLE", _TABLE)))
        assert [grant.action for grant in grants] == ["ALL PRIVILEGES"], spelling


def test_a_select_inherited_from_the_schema_is_seen_and_is_never_revoked():
    """THE SECOND MEASURED HOLE, and it was a silent no-op rather than an omission.

    `GRANT SELECT ON SCHEMA s TO p` IS returned by `SHOW GRANTS ON TABLE s.t`, as
    `p | SELECT | SCHEMA | s` -- and `REVOKE SELECT ON TABLE s.t FROM p` against it
    SUCCEEDS and leaves the row in place (both measured). The old reader discarded
    `ObjectType`, so it revoked forever and printed `REVOKED` every time."""
    grants = grants_to_settle(_rows((_SP, "SELECT", "SCHEMA", _SCHEMA)))
    assert [grant.object_type for grant in grants] == ["SCHEMA"]
    assert revocable_principals(grants) == ()
    assert plan_grants(revocable_principals(grants), roster=()).revoke == ()


def test_a_direct_table_select_is_the_one_shape_that_gets_revoked():
    """The narrow half, stated positively: exactly the row a `REVOKE SELECT ON TABLE`
    was measured to remove -- it disappeared from `SHOW GRANTS` afterwards while the
    principal's `ALL PRIVILEGES` and schema-inherited rows survived the same
    statement."""
    grants = grants_to_settle(
        _rows(
            (_SP, "ALL PRIVILEGES", "TABLE", _TABLE),
            (_SP, "SELECT", "TABLE", _TABLE),
            (_SP, "SELECT", "SCHEMA", _SCHEMA),
        )
    )
    assert revocable_principals(grants) == (_SP,), "one of the three, not three"
    assert len(unrevocable_grants(grants)) == 2


def test_the_escalation_names_the_statement_that_actually_closes_it():
    """A report an operator cannot act on is a log line. Both remediations are
    measured: `REVOKE ALL PRIVILEGES ON TABLE` removed the table-level one and
    `REVOKE SELECT ON SCHEMA` removed the inherited one, each verified by re-reading
    `SHOW GRANTS` afterwards."""
    grant = grants_to_settle(_rows((_SP, "ALL PRIVILEGES", "TABLE", _TABLE)))[0]
    message = unrevocable_escalation(_TABLE, grant)
    assert f"REVOKE ALL PRIVILEGES ON TABLE {_TABLE} FROM `{_SP}`" in message
    inherited = grants_to_settle(_rows((_SP, "SELECT", "SCHEMA", _SCHEMA)))[0]
    assert f"REVOKE SELECT ON SCHEMA {_SCHEMA} FROM `{_SP}`" in (
        unrevocable_escalation(_TABLE, inherited)
    )


def test_the_roster_is_resolved_at_call_time_and_an_empty_tuple_is_not_none(monkeypatch):
    """WHY `roster=None` RATHER THAN `roster=PII_READERS`, and it is not a style
    choice. A default argument is bound at import, so with the roster empty by decision
    NO test in this repository could ever run this task with a non-empty one -- which
    is how `test_every_revoke_precedes_every_grant_and_the_tags_come_last` came to
    assert an ordering it could not observe. `roster=()` still means the empty roster
    and must not fall back to the module's."""
    monkeypatch.setattr(pii_governance, "PII_READERS", ("zz-declared@example.com",))
    assert plan_grants(()).grant == ("zz-declared@example.com",)
    assert plan_grants((), roster=()).grant == ()


# --------------------------------------------------------------------------
# The generated SQL
# --------------------------------------------------------------------------


def test_the_grant_and_revoke_name_the_table_and_backtick_the_principal():
    """A service principal's grant principal is its APPLICATION ID -- a UUID, whose
    hyphens make it an identifier a bare spelling would not survive."""
    assert grant_select_ddl(_TABLE, _SP) == f"GRANT SELECT ON TABLE {_TABLE} TO `{_SP}`"
    assert revoke_select_ddl(_TABLE, _SP) == (
        f"REVOKE SELECT ON TABLE {_TABLE} FROM `{_SP}`"
    )
    assert show_grants_sql(_TABLE) == f"SHOW GRANTS ON TABLE {_TABLE}"


@pytest.mark.parametrize("principal", ["", "a`b", "a\\b", "a;DROP", "a\nb"])
def test_a_principal_that_could_break_out_of_its_quoting_is_refused(principal):
    """The roster is code today, which is exactly when this is cheap to write: the day
    a principal arrives from a job parameter or a catalog read, the escape is already
    here rather than being the thing nobody added."""
    with pytest.raises(UngrantablePrincipal):
        grant_select_ddl(_TABLE, principal)
    with pytest.raises(UngrantablePrincipal):
        revoke_select_ddl(_TABLE, principal)


def test_the_tag_value_is_empty_because_the_governed_policies_admit_nothing_else():
    """MEASURED, not chosen. `GET /api/2.1/tag-policies` returns 70 governed policies
    on this account, `class.name` among them, and their allowed-value list is empty:
    `SET TAGS ('class.name' = 'personal_name')` is refused with INVALID_PARAMETER_VALUE
    ("not an allowed value ... Allowed values: []") and `= ''` succeeds and reads back
    from `information_schema.column_tags`. A non-empty value here would be a statement
    the workspace rejects at run time, inside the job."""
    assert GOVERNED_TAG_VALUE == ""
    assert set_column_tag_ddl(_TABLE, "nome_do_representante", TAG_NAME) == (
        f"ALTER TABLE {_TABLE} ALTER COLUMN `nome_do_representante` "
        "SET TAGS ('class.name' = '')"
    )


def test_the_tag_keys_are_the_accounts_own_and_not_a_bespoke_namespace():
    """A dot is a RESERVED CHARACTER in a tag key -- `opl.pii` is refused with "Tag key
    contains reserved characters (., =, >, <, %, &, ?, \\)" -- so a bespoke `opl.*`
    namespace is unavailable and the dotted governed keys are the only dotted keys that
    exist. Using the account's vocabulary is therefore not a preference; inventing one
    with the same shape is impossible."""
    for tag_key in (TAG_NAME, TAG_BR_CPF, TAG_BR_CNPJ):
        assert tag_key.startswith("class.")
    used = {key for keys in CLASSIFIED_COLUMNS["socios"].values() for key in keys}
    assert used == {TAG_NAME, TAG_BR_CPF, TAG_BR_CNPJ}


# --------------------------------------------------------------------------
# The classification
# --------------------------------------------------------------------------


def test_both_masked_columns_are_classified_as_a_person_name():
    """The classification must not be narrower than the mask: a column hidden from
    every reader and described to the catalog as nothing is a control no governance
    review can find."""
    for column in masking.MASKED_COLUMNS["socios"]:
        assert CLASSIFIED_COLUMNS["socios"][column] == (TAG_NAME,)


def test_the_source_masked_identifier_is_classified_and_deliberately_not_masked():
    """`cpf_cnpj_socio` carries BOTH keys because `identificador_socio` decides which
    one a row holds. It is classified and NOT masked, which is ADR 0008's central
    context claim -- the Receita already masks it at source, six middle digits,
    irreversible -- made visible to someone reading the catalog instead of the ADR."""
    assert CLASSIFIED_COLUMNS["socios"]["cpf_cnpj_socio"] == (TAG_BR_CPF, TAG_BR_CNPJ)
    assert "cpf_cnpj_socio" not in masking.MASKED_COLUMNS["socios"]


def test_the_company_identifier_is_deliberately_not_classified():
    """`cnpj_basico` is a CNPJ and tagging it would be true. It identifies the COMPANY;
    this control is about natural persons, and a classification that drifts into every
    identifier anyone could name stops meaning anything."""
    assert "cnpj_basico" in TABLES["socios"]
    assert "cnpj_basico" not in CLASSIFIED_COLUMNS["socios"]


def test_every_classified_column_exists_in_its_contract():
    """A tag on a column that does not exist fails at RUN time, inside the job, after
    the grants have already been issued. The module asserts this at IMPORT; this is
    the same claim from outside, so deleting the import-time guard is visible."""
    for contract, columns in CLASSIFIED_COLUMNS.items():
        for column in columns:
            assert column in TABLES[contract]


def test_a_column_with_two_keys_yields_one_pair_per_key_in_order():
    """`classified_columns` is what the task loops over, so a flattening that lost the
    second key would silently drop half of a classification."""
    assert classified_columns("socios") == (
        ("nome_socio_razao_social", TAG_NAME),
        ("nome_do_representante", TAG_NAME),
        ("cpf_cnpj_socio", TAG_BR_CPF),
        ("cpf_cnpj_socio", TAG_BR_CNPJ),
    )
    assert classified_columns("empresas") == ()
