# tests/bronze/test_reader_formats.py
"""WHICH FORMAT EACH CONTRACT IS PARSED AS, and that the options match it.

NEITHER `source_format` NOR `read_options` HAD A SINGLE TEST ANYWHERE IN `tests/` BEFORE
F-API TASK 2 -- verified by search across the whole suite. `csv_read_options` was covered
twice over (cp1252 and multiLine), `jsonl_read_options` appeared only inside another
test's docstring, and the two functions that DECIDE which of them a stream gets were
unexercised. That is the wrong half to leave uncovered, because getting the option set
wrong is loud and getting the DISPATCH wrong is silent:

  * Spark IGNORES CSV options on a JSON read. Nothing warns, nothing fails, and the
    stream parses with Spark's own defaults instead of the declared ones.
  * CSV over JSON Lines parses each line as ONE field. The whole JSON object lands in the
    first declared column and every other column is PERMISSIVE-padded NULL -- so the DQ
    gate reports `null_or_empty_<the second column>` for every row, and a triager starts
    from a blank column when the fault is the format dispatch, one layer up, for the
    entire table.

So the pins below are PER CONTRACT and TOTAL over `CONTRACT_COLUMNS`: parametrized off
the catalogue rather than off a list here, because a list here would be the thing that
goes stale exactly when a new contract is added -- which is the case that matters.

The expectation itself is stated as a small explicit map. It is the one place in these
tests where a second spelling is correct: the point is to compare the module's answer
against a stated intention, and deriving the intention from the module would be a test
that agrees with whatever the module currently does.

Nothing here starts Spark: `source_format` and `read_options` are pure dicts."""
from __future__ import annotations

import pytest

from opl.bronze.reader import (
    CSV_FORMAT,
    JSON_FORMAT,
    csv_read_options,
    jsonl_read_options,
    read_options,
    source_format,
)
from opl.contracts.catalogue import CONTRACT_COLUMNS

# WHAT EACH CONTRACT'S BYTES ACTUALLY ARE, stated independently of the module under test.
_EXPECTED = {
    # The Receita's headerless semicolon-CSV layouts. `simples` is one of them and has no
    # bronze table -- `cnpj_schemas.TABLES` declares the RFB's layouts, the registry
    # decides which are ingested, and the two are deliberately not the same set
    # (`test_an_unregistered_contract_is_refused_and_says_what_to_do`). It is here
    # because this dispatch is keyed on the CONTRACT, so it answers for every contract
    # that exists rather than for every table that is registered.
    "lookup": CSV_FORMAT,
    "estabelecimentos": CSV_FORMAT,
    "empresas": CSV_FORMAT,
    "socios": CSV_FORMAT,
    "simples": CSV_FORMAT,
    # The generated payment stream: one JSON object per line, so a mid-stream key that
    # the contract does not declare reaches `_rescued_data` instead of misaligning a
    # positional reader.
    "payments": JSON_FORMAT,
    # The PTAX record: one JSON object per quote, built from a validated response.
    "ptax": JSON_FORMAT,
}


def test_the_expectation_is_stated_for_every_contract_that_exists():
    """Guard the guard. Parametrizing off `CONTRACT_COLUMNS` while comparing against a
    map declared here means the map has to be total too -- otherwise a new contract
    would arrive with no stated intention and the per-contract test below would fail
    with a KeyError from the TEST rather than a verdict about the module."""
    assert set(_EXPECTED) == set(CONTRACT_COLUMNS)


@pytest.mark.parametrize("contract", sorted(CONTRACT_COLUMNS))
def test_every_contract_is_parsed_as_the_format_its_bytes_are_written_in(contract):
    assert source_format(contract) == _EXPECTED[contract]


@pytest.mark.parametrize("contract", sorted(CONTRACT_COLUMNS))
def test_the_options_a_contract_gets_are_the_ones_its_format_declares(contract):
    """`read_options` asks `source_format` rather than re-testing the contract, so this
    is what says the two cannot come apart. A JSON contract handed the CSV option set
    would be silent -- Spark discards options that do not apply to the format."""
    expected = jsonl_read_options() if _EXPECTED[contract] == JSON_FORMAT else csv_read_options()
    assert read_options(contract) == expected


def test_a_contract_nothing_declares_raises_rather_than_getting_csv():
    """THE DEFAULT THAT WAS THERE UNTIL F-API TASK 2, refused.

    `JSON_FORMAT if contract == PAYMENTS_CONTRACT else CSV_FORMAT` answered every
    contract, including ones added after it was written, and every wrong answer was
    semicolon CSV. A KeyError here is the fix: the same refusal `catalogue.columns_for`
    and `rules.rules_for` make for an unknown key, and loud enough that nobody reads a
    quarantine full of NULLs instead."""
    with pytest.raises(KeyError):
        source_format("a-contract-nobody-declares")
    with pytest.raises(KeyError):
        read_options("a-contract-nobody-declares")


def test_the_json_options_are_the_three_that_make_the_drift_verdict_possible():
    """Pinned because each of the three is load-bearing and each is silent if dropped.

    `multiLine=false` IS the format: with it true, Spark reads a whole file as one JSON
    document and a many-line stream parses as a single malformed record. `encoding=UTF-8`
    is the reading half of what the landing writer states explicitly -- left unset, Spark
    detects, and a detector that guessed wrong turns a mis-encoded byte into a silently
    different STRING rather than the U+FFFD the gate refuses. `mode=PERMISSIVE` is what
    makes `rescuedDataColumn` the reporting channel rather than FAILFAST or a NULL."""
    assert jsonl_read_options() == {
        "multiLine": "false",
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
    }


def test_a_format_with_no_option_set_is_refused_at_import_rather_than_given_csvs(
    monkeypatch,
):
    """THE RESIDUAL `else` THE FIRST FIX LEFT BEHIND, closed one question later.

    `_SOURCE_FORMATS` became total over the CATALOGUE in F-API Task 2, so every contract
    declares a format. `read_options` was still `jsonl if source_format(...) == JSON else
    csv`, which is total over nothing: a THIRD format -- parquet for a future source, avro,
    text -- fell into the `else` and got the RFB's semicolon-CSV dialect. Same default
    wearing a conditional as the dispatch that was fixed, and just as silent, because Spark
    discards options that do not apply to the format it was given.

    Both halves of the totality are exercised: a declared format with no option set, and an
    option set no declared format names. The first is the edit somebody will make (a new
    source, a new format, the options forgotten); the second is what keeps the guard from
    being satisfiable by adding entries nobody reads."""
    from opl.bronze import reader

    monkeypatch.setitem(reader._SOURCE_FORMATS, "future", "parquet")
    with pytest.raises(ValueError, match="parquet"):
        reader._assert_every_declared_format_has_options()

    monkeypatch.undo()
    monkeypatch.setitem(reader._FORMAT_OPTIONS, "avro", dict)
    with pytest.raises(ValueError, match="avro"):
        reader._assert_every_declared_format_has_options()


def test_every_option_set_is_a_fresh_dict_no_caller_can_mutate_for_the_next():
    """`_FORMAT_OPTIONS` holds the FACTORIES rather than their results, and this is what
    says so. A module-level dict of option dicts would be one object per format shared by
    every stream in a job, so an ingest that popped or overwrote a key would change how the
    next table is parsed -- silently, in the direction Spark does not report."""
    first, second = read_options("ptax"), read_options("ptax")
    assert first == second and first is not second
    first["multiLine"] = "true"
    assert read_options("ptax")["multiLine"] == "false"


def test_no_json_contract_is_handed_the_csv_dialect():
    """The failure stated as the thing that would be seen, not as the setting.

    A JSON contract given `sep=";"` and `encoding=cp1252` is not an error anywhere in
    Spark -- the options are discarded. What reaches a reader is a table whose columns
    are NULL, and this asserts the discarded-option path is not one this repo can enter
    by editing one of the two functions and not the other."""
    for contract, fmt in _EXPECTED.items():
        if fmt != JSON_FORMAT:
            continue
        assert "sep" not in read_options(contract)
        assert read_options(contract)["encoding"] == "UTF-8"
