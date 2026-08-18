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

from opl.bronze import masking
from opl.bronze.pii_governance import (
    CLASSIFIED_COLUMNS,
    GOVERNED_PRIVILEGE,
    GOVERNED_ROLES,
    GOVERNED_TAG_VALUE,
    PII_READER_GROUP,
    PII_READERS,
    TAG_BR_CNPJ,
    TAG_BR_CPF,
    TAG_NAME,
    UngrantablePrincipal,
    classified_columns,
    governed_contracts,
    grant_select_ddl,
    plan_grants,
    revoke_select_ddl,
    selecting_principals,
    set_column_tag_ddl,
    show_grants_sql,
)
from opl.contracts.cnpj_schemas import TABLES

_SP = "d0e35b43-be45-4466-b4b7-6eec2d3a1fc8"
_TABLE = "workspace.default.bronze_cnpj_socios"


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


def test_every_contract_that_is_masked_is_also_governed():
    """Grants and tags are derived from `MASKED_COLUMNS` rather than listed again, so
    a contract that gains a mask gains its access control in the same edit. If these
    ever came apart, the new masked table would be the one nothing revokes on."""
    assert governed_contracts() == tuple(sorted(masking.MASKED_COLUMNS))


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


def test_only_select_is_read_out_of_show_grants():
    """`SHOW GRANTS` returns every action type on the table. This module is
    authoritative over exactly one privilege, named rather than `ALL PRIVILEGES`: a
    revoke loop that swept every action would fight whatever the platform granted, and
    the privilege that reveals a personal name is this one."""
    rows = [("a@x", "SELECT"), ("b@x", "MODIFY"), ("c@x", "APPLY_TAG"), ("d@x", "select")]
    assert selecting_principals(rows) == ("a@x", "d@x")
    assert GOVERNED_PRIVILEGE == "SELECT"


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
