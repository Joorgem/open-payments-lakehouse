# tests/test_bundle_resource_allowlist.py
"""WHAT THIS BUNDLE MAY DECLARE AT ALL. Not a schedule question, which is why it is here.

SPLIT OUT OF `tests/test_bundle_targets_and_schedules.py` BY F8'S SECOND CORRECTION PASS,
and the seam is the subject rather than the line count. That module answers two questions
-- which jobs declare a cadence, and who writes `pause_status` -- and this allowlist
answers neither: it is about which RESOURCE COLLECTIONS may appear anywhere in the bundle,
and it would read the same if no job in this repository had a schedule. It arrived in that
file because the phase that wrote it was a scheduling phase. The count forced the split (the
additions below carried that module to 833 against a strictly-under-800 cap) and the axis
chose where it fell.

WHAT IT COVERS, STATED WITH ITS EDGES, because documents elsewhere in this repository rest
a safety argument on it and their readers arrive here. WHICH documents is not counted in
this file: a count of the sites carrying that argument has already been written short once
in this phase, so it is derived instead --

    git grep -ln test_bundle_resource_allowlist -- docs databricks

`bundle_docs()` parses what a suffix sweep finds under `databricks/` (`tests/job_yaml.py`
carries which suffixes those are, what is skipped, and what the sweep picks up that the
bundle does not read as source), and every
resource collection in each is held to the allowlist at the paths `_SWEPT_PATHS` below
carries, in the CLI's own spelling. THOSE PATHS ARE NOT COUNTED HERE EITHER, and they are
not asserted complete either: a test derives the set from `databricks bundle schema`, which
needs the CLI and therefore runs on a developer box and skips in CI. Its docstring says so.

THE TARGET PATH IS NOT A REFINEMENT. It is where a securable would land under the
PRODUCTION target -- the one [ADR 0018] Decision 6's grounds 2 and 3 are about -- and until
this module existed the sweep read only the top level while every document the grep above
names said no securable could enter the bundle without a test going red. Measured, not
inferred: a scratch bundle declaring `targets.prodx.resources.schemas` validates `exit=0`
under CLI v1.8.0 and renders resource kinds `['jobs', 'schemas']`.

WHAT IT IS KNOWN NOT TO REACH -- each measured on the same scratch bundle rather than
assumed, and not offered as a complete list of what nobody has thought of:

  * a resource declared in a file the bundle `include`s from OUTSIDE `databricks/`.
    `include: ../outside/*.yml` validates `exit=0` and renders the resource, and no file
    this sweep reads would mention it. What holds the line there is that adding such an
    entry means editing `databricks.yml`, which IS one of the documents swept;
  * any grant issued outside the bundle at all. `apply_pii_governance` issues them
    imperatively at run time, which is Decision 6's own ruling and not something a sweep
    over YAML could observe.

[ADR 0018]: docs/adr/0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md
"""
from __future__ import annotations

import json
import shutil
import subprocess

import pytest
import yaml
from job_yaml import (
    BUNDLE,
    BUNDLE_DOC_SUFFIXES,
    CLI_OUTPUT_DIR,
    REPO,
    bundle_docs,
    bundle_files,
)

# THE ONLY TWO RESOURCE COLLECTIONS THIS BUNDLE MAY DECLARE, AS AN ALLOWLIST RATHER THAN A
# LIST OF SECURABLES TO REFUSE. ADR 0018 Decision 6 rejected declarative governance partly
# on grounds that can fire ONLY over a securable. Enumerating the securables would be a
# third copy of a list whose existing copies already disagreed -- six object types in ADR
# 0018 against four in the documents quoting it. An allowlist needs no such list: anything
# that is not a job or a dashboard stops here, securable or not, and a new collection
# becomes a decision somebody has to type out.
#
# AND "A SECURABLE REFUSAL" IS NOT WHAT THIS IS, which is the correction the documents named
# by this file's grep now carry. The allowlist is WIDER than the securables: it refuses
# `secret_scopes` and `sql_warehouses` too, neither of which carries `grants`, and this
# workspace holds real state of both kinds -- one secret scope, and the warehouse
# `databricks.yml` resolves by name. Declaring either would be a legitimate act this lock
# makes somebody argue for, not a hazard it exists to stop.
_DECLARABLE = ("jobs", "dashboards")
# THE PLACES A BUNDLE DOCUMENT CAN DECLARE A RESOURCE COLLECTION, IN THE CLI SCHEMA'S OWN
# SPELLING AND AS THE ONE COPY THIS MODULE HAS. `_resource_collections` walks the document
# from these strings and `test_the_swept_paths_are_every_path_the_cli_schema_types_config_
# resources` derives the same set out of `databricks bundle schema`, so a place the sweep
# does not walk is a place that arm names rather than a place nobody notices. That is the
# repair for how this got here: the sweep read the top level only, and the documents citing
# it said otherwise, because both were written from a count.
#
# `environments` IS THE DEPRECATED SPELLING OF `targets` -- the schema `$ref`s the same
# `config.Target` type for both, which is why it carries `resources` at all -- AND IT IS NOT
# REACHABLE BESIDE `targets`. Measured: a bundle declaring both is refused outright,
# *"both 'environments' and 'targets' are specified; only 'targets' should be used"*. So
# this repository's bundle, which declares `targets`, could not carry one today; what makes
# the path reachable is a future edit converting to the deprecated spelling, and sweeping it
# costs one entry in this tuple.
_SWEPT_PATHS = (
    "$root.resources",
    "$root.targets.<name>.resources",
    "$root.environments.<name>.resources",
)

# The segment that stands for "every key of this mapping" in a schema path.
_ANY_NAME = "<name>"


def _reached(node, steps: tuple[str, ...], where: str = "") -> list[tuple[str, dict]]:
    """Every (dotted location, mapping) that `steps` reaches inside `node`.

    `<name>` matches every key, which is how ONE schema path covers a document declaring
    several targets and how the location reported back carries the target's real name."""
    if not isinstance(node, dict):
        return []
    if not steps:
        return [(where, node)]
    head, rest = steps[0], steps[1:]
    keys = sorted(node) if head == _ANY_NAME else ([head] if head in node else [])
    return [
        reached
        for key in keys
        for reached in _reached(node[key] or {}, rest, f"{where}.{key}" if where else key)
    ]


def _resource_collections(doc) -> list[tuple[str, str]]:
    """(where, kind) for every place a bundle document can declare a resource collection."""
    return sorted(
        (where, kind)
        for path in _SWEPT_PATHS
        for where, collections in _reached(doc or {}, tuple(path.split(".")[1:]))
        for kind in collections
    )


def _resource_faults(docs: dict[str, object]) -> list[str]:
    """Every resource collection in every swept bundle file, against the allowlist."""
    return [
        f"{name}: declares {where}.{kind}, which is neither of {_DECLARABLE}"
        for name, doc in docs.items()
        for where, kind in _resource_collections(doc)
        if kind not in _DECLARABLE
    ]


def test_the_bundle_declares_only_jobs_and_dashboards():
    """THE REFUSAL ADR 0018 DECISION 6 IS QUOTED AS RESTING ON.

    Every document this file's grep names says that what keeps Decision 6's second and
    third grounds hypothetical is MECHANICAL -- that they can fire only over a securable
    and the bundle declares none. Until F8 they said so and nothing enforced it; until
    F8's second correction pass the enforcement read the top level alone, and until this
    one it did not walk the deprecated spelling of a target. See this file's docstring for
    what it still does not reach."""
    assert not _resource_faults(bundle_docs())


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. A lock the sweep cannot reach is a lock nobody notices is gone -- the
# top-level arm passed for months over a sweep blind to a place a securable could be
# declared -- so each arm below mutates AT a swept path and names that path in its assertion.
# --------------------------------------------------------------------------------


def _a_resource_file() -> tuple[str, dict]:
    """The first swept document declaring a top-level resource collection, and its name.

    DERIVED, and not the classification another module keeps: this arm is about what a
    document may declare, so the document it mutates is chosen by declaring something."""
    docs = bundle_docs()
    found = sorted(name for name, doc in docs.items() if (doc or {}).get("resources"))
    assert found, "no bundle file under databricks/ declares a resource collection"
    return found[0], docs[found[0]]


def _the_production_target(document: dict) -> str:
    """The target declaring `mode: production`, read out of the bundle rather than named.

    A renamed `prod` is then punished by `_target_faults` in
    `tests/test_bundle_targets_and_schedules.py`, which is the lock that owns the question,
    instead of silently turning the arm below into a no-op."""
    found = [
        name
        for name, body in (document.get("targets") or {}).items()
        if (body or {}).get("mode") == "production"
    ]
    assert len(found) == 1, f"{found} declare mode: production; this arm expects exactly one"
    return found[0]


def test_the_lock_goes_red_when_a_securable_is_declared_at_the_top_level():
    """A schema declared where the jobs are -- the exact shape ADR 0018 Decision 6 refuses,
    and the one whose `grants` would be AUTHORITATIVE over a schema this project does not
    own. The collection it replaces is read out of the document, not named here."""
    name, document = _a_resource_file()
    assert not _resource_faults({name: document})
    declared = next(iter(document["resources"]))
    document["resources"] = {"schemas": document["resources"].pop(declared)}
    faults = _resource_faults({name: document})
    assert any("resources.schemas" in fault for fault in faults), faults


def test_the_lock_goes_red_when_a_securable_is_declared_under_the_production_target():
    """THE PLACE IT MATTERS MOST, AND THE ONE THE SWEEP MISSED LONGEST.

    Before F8's second correction pass a schema could sit under the production target --
    exactly where ADR 0018 Decision 6's grounds 2 and 3 fire -- with every test green,
    while every document this file's grep names said no securable could enter without one
    going red. The target is read out of the committed bundle by its MODE rather than
    named, so this arm follows a rename."""
    document = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    assert not _resource_faults({"databricks.yml": document})
    target = _the_production_target(document)
    document["targets"][target]["resources"] = {"schemas": {"governed": {"name": "default"}}}
    faults = _resource_faults({"databricks.yml": document})
    assert any(f"targets.{target}.resources.schemas" in fault for fault in faults), faults


def test_the_lock_goes_red_when_a_securable_is_declared_under_a_deprecated_environment():
    """THE THIRD PATH, AND THE ARM BUILDS THE STATE THAT IS ACTUALLY REACHABLE.

    `environments` cannot sit beside `targets` -- the CLI refuses the pair -- so this arm
    does what the conversion to the deprecated spelling would do: the whole `targets`
    mapping becomes `environments`, and the securable is planted in the production one. An
    earlier draft moved that one target and DELETED the rest, which is not a conversion. The
    target is still read out of the committed bundle by its mode, so the arm follows a
    rename."""
    document = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    assert not _resource_faults({"databricks.yml": document})
    name = _the_production_target(document)
    declared = set(document["targets"])
    document["environments"] = document.pop("targets")
    assert set(document["environments"]) == declared, "the conversion dropped a target"
    document["environments"][name]["resources"] = {"schemas": {"governed": {"name": "default"}}}
    faults = _resource_faults({"databricks.yml": document})
    assert any(f"environments.{name}.resources.schemas" in fault for fault in faults), faults


# --------------------------------------------------------------------------------
# WHERE THE SWEPT PATHS COME FROM. Derived from the CLI's own schema, on a developer
# box -- see the arm's docstring for why that is not a CI lock and is said so plainly.
# --------------------------------------------------------------------------------

# The schema type a resource collection has. Matched on the tail of a `$ref`, because the
# CLI spells it as a path into `$defs` (`.../bundle/config.Resources`).
_RESOURCES_TYPE = "config.Resources"


def _bundle_schema() -> dict:
    """`databricks bundle schema`, parsed, or a skip naming the one reason for skipping.

    CREDENTIAL-FREE AND BUNDLE-FREE, measured: with `DATABRICKS_HOST`/`DATABRICKS_TOKEN`
    unset and `DATABRICKS_CONFIG_FILE` pointed at a path that does not exist, the output is
    byte-identical to the authenticated run, and it was taken from a directory holding no
    `databricks.yml`. THE ONLY SKIP IS THE CLI BEING ABSENT: a CLI that is present and exits
    non-zero fails here, because what this derives is the CLI's own answer and a skip would
    report green over not having asked it. `encoding` is named because the schema carries
    non-ASCII and Windows would otherwise decode it in the ANSI codepage and raise.

    NO `MSYS_NO_PATHCONV` IN THE CHILD ENVIRONMENT: it was copied in from `CLAUDE.md`, whose
    rule is about `databricks api /...` typed at a Git Bash prompt. MSYS rewrites arguments
    when an MSYS SHELL launches a native binary; a CPython child is not one, so the variable
    did nothing here and no argument below starts with a slash."""
    cli = shutil.which("databricks")
    if cli is None:
        pytest.skip("no `databricks` CLI on PATH; this derivation is a developer-box arm")
    done = subprocess.run(
        [cli, "bundle", "schema"],
        cwd=REPO, capture_output=True, text=True, encoding="utf-8",
    )
    assert not done.returncode, f"`bundle schema` exited {done.returncode}: {done.stderr[:300]}"
    return json.loads(done.stdout)


def _resolved(schema: dict, ref: str) -> dict:
    node = schema
    for part in ref.removeprefix("#/").split("/"):
        node = node[part]
    return node


def _typed_resources(schema: dict, node, path: str) -> list[str]:
    """The paths under `node` THIS WALK REACHES whose type is `config.Resources`, in the
    schema's spelling.

    WHICH CONSTRUCTS IT FOLLOWS IS IN THE CODE and no claim is made about JSON Schema at
    large. `anyOf`/`allOf` were added because the first draft followed `oneOf` alone while
    the live schema carries an `anyOf` too -- no `config.Resources` sits under it today, so
    the narrow walk agreed by luck, and agreeing by luck is the failure this arm exists to
    catch. `items` is followed with a distinct `[]` in the path rather than the parent's
    spelling: a collection nested in an ARRAY is somewhere `_resource_collections` does not
    walk, so it must surface as a path `_SWEPT_PATHS` lacks, not as a duplicate of one it
    has.

    EVERY `$ref` IS FOLLOWED AND NOTHING IS MEMOISED, which the first draft of this walk
    guarded against with a visited-set on the ground that the schema is cyclic. It is not:
    measured on CLI v1.8.0, the unguarded walk terminates and returns the same paths as the
    guarded one, so the guard was a claim nothing here checked -- and a visited-set is the
    dangerous half of the pair, because a path it prunes is a path this derivation reports
    as absent. A schema that DID close a cycle would blow the stack here, loudly, rather
    than quietly returning fewer places than the CLI accepts."""
    if not isinstance(node, dict):
        return []
    ref = node.get("$ref")
    if ref is not None:
        if ref.endswith(_RESOURCES_TYPE):
            return [path]
        return _typed_resources(schema, _resolved(schema, ref), path)
    found = [
        found
        for combinator in ("oneOf", "anyOf", "allOf")
        for branch in node.get(combinator) or ()
        for found in _typed_resources(schema, branch, path)
    ]
    found += [
        found_here
        for key, child in (node.get("properties") or {}).items()
        for found_here in _typed_resources(schema, child, f"{path}.{key}")
    ]
    if isinstance(node.get("additionalProperties"), dict):
        found += _typed_resources(schema, node["additionalProperties"], f"{path}.{_ANY_NAME}")
    if isinstance(node.get("items"), dict):
        found += _typed_resources(schema, node["items"], f"{path}[]")
    return found


def test_the_swept_paths_are_every_path_the_cli_schema_types_config_resources():
    """THE SET IS DERIVED FROM THE CLI, NOT COUNTED IN A DOCSTRING.

    NOT A CI LOCK, AND THAT IS SAID RATHER THAN GLOSSED. CI installs no Databricks CLI --
    `git grep -in databricks -- .github/` returns nothing -- so this arm SKIPS on every CI
    run and is a derivation that happens on a developer box. What stands in CI is
    `_SWEPT_PATHS` itself and the arms that exercise each entry of it.

    WHAT IT DERIVES IS A TYPING, NOT A GUARANTEE ABOUT EVERY WAY A RESOURCE CAN ARRIVE. The
    schema also carries `$root.python` and `$root.experimental.python`, typed otherwise;
    nobody in this repository has exercised them and this module claims nothing about
    them."""
    schema = _bundle_schema()
    derived = sorted(set(_typed_resources(schema, schema, "$root")))
    assert derived == sorted(_SWEPT_PATHS), (
        f"`databricks bundle schema` types {derived} as {_RESOURCES_TYPE} and this module "
        f"sweeps {sorted(_SWEPT_PATHS)}. A path the CLI accepts and the sweep does not walk "
        "is a place a securable can be declared with every test green."
    )


# A `$ref` the walk stops at, spelled the way the CLI spells one: a path into `$defs` whose
# tail is the type. The schemas below need no `$defs` entry for it to land in, because a
# matching `$ref` is where `_typed_resources` returns instead of descending.
_A_RESOURCES_REF = {"$ref": f"#/$defs/bundle/{_RESOURCES_TYPE}"}

# Each case reaches a `config.Resources` through ONE construct `_typed_resources` follows,
# with the path the walk should report for it. `items` carries a `[]` because a collection
# nested in an ARRAY is somewhere `_resource_collections` does not walk: it has to surface as
# a path `_SWEPT_PATHS` lacks rather than as a duplicate of one it has.
_CONSTRUCT_CASES = (
    ("oneOf", {"properties": {"resources": {"oneOf": [_A_RESOURCES_REF]}}}, "$root.resources"),
    ("anyOf", {"properties": {"resources": {"anyOf": [_A_RESOURCES_REF]}}}, "$root.resources"),
    ("allOf", {"properties": {"resources": {"allOf": [_A_RESOURCES_REF]}}}, "$root.resources"),
    (
        "additionalProperties",
        {"properties": {"targets": {"additionalProperties": _A_RESOURCES_REF}}},
        f"$root.targets.{_ANY_NAME}",
    ),
    ("items", {"properties": {"stack": {"items": _A_RESOURCES_REF}}}, "$root.stack[]"),
    (
        "$ref",
        {"$defs": {"x": _A_RESOURCES_REF}, "properties": {"resources": {"$ref": "#/$defs/x"}}},
        "$root.resources",
    ),
)


@pytest.mark.parametrize(("construct", "schema", "expected"), _CONSTRUCT_CASES)
def test_the_walk_follows_the_construct_each_case_names_to_a_resources_type(
    construct: str, schema: dict, expected: str
):
    """THE HALF OF THIS DERIVATION THAT RUNS IN CI, and the reason it is written synthetically.

    The arm above needs the CLI, so it skips wherever CI runs; and in the live schema no
    `config.Resources` sits under the constructs `c685724` added, so narrowing the walk back
    to `oneOf` alone left this module green even on a box that HAS the CLI. A schema built in
    the test needs neither CLI nor bundle, so dropping a construct `_CONSTRUCT_CASES` names
    turns that case red WITH THE CLI ABSENT as well as present -- both watched.

    NO CLAIM IS MADE ABOUT JSON SCHEMA. What the walk follows is in its own docstring, and
    what the derived set is and is not is in this module's."""
    assert _typed_resources(schema, schema, "$root") == [expected], construct


# --------------------------------------------------------------------------------
# WHAT THE SWEEP READS OFF DISK. The suffix set and the CLI's own output directory both
# live in `tests/job_yaml.py`; the arms below are what make those constants load-bearing
# rather than decorative.
# --------------------------------------------------------------------------------


def test_the_sweep_reads_a_document_under_every_suffix_a_bundle_document_may_carry(tmp_path):
    """One file per entry of `BUNDLE_DOC_SUFFIXES`, so a suffix added to that tuple without
    the walk following it fails here rather than silently widening nothing. JSON is in the
    tuple because it deploys: `include: resources/*.json` validates and renders the job
    declared in it. The body is JSON, which `yaml.safe_load` parses under any of these
    suffixes, so one literal serves them all."""
    for suffix in BUNDLE_DOC_SUFFIXES:
        (tmp_path / f"probe{suffix}").write_text('{"resources": {}}', encoding="utf-8")
    read = [path.name for path in bundle_files(tmp_path)]
    assert read == sorted(f"probe{suffix}" for suffix in BUNDLE_DOC_SUFFIXES), read


def test_the_sweep_does_not_read_the_clis_own_record_of_what_it_deployed(tmp_path):
    """`.databricks/bundle/<target>/resources.json` is the CLI's output, not bundle source,
    and it carries `pause_status` -- rendering that key is the CLI's job. Reading it makes
    the `pause_status` absence sweeps RED on any box that has deployed and GREEN in CI,
    which has no `.databricks/`. Excluded by directory NAME, at any depth."""
    deployed = tmp_path / CLI_OUTPUT_DIR / "bundle" / "free"
    deployed.mkdir(parents=True)
    (deployed / "resources.json").write_text(
        '{"resources": {"jobs": {"j": {"schedule": {"pause_status": "UNPAUSED"}}}}}',
        encoding="utf-8",
    )
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: probe\n", encoding="utf-8")
    assert [path.name for path in bundle_files(tmp_path)] == ["databricks.yml"]


def test_the_exclusion_reads_the_path_below_the_swept_root_and_not_the_whole_drive(tmp_path):
    """AN ANCESTOR OF THE CHECKOUT NAMED `.databricks` MUST NOT EMPTY THE SWEEP.

    The first version matched `path.parts`, which carries the drive and the directories
    above the repository as well, so a clone under such a directory made this helper return
    NOTHING -- and a sweep that read no files reports the same green as a clean tree."""
    root = tmp_path / CLI_OUTPUT_DIR / "checkout"
    root.mkdir(parents=True)
    (root / "databricks.yml").write_text("bundle:\n  name: probe\n", encoding="utf-8")
    assert [path.name for path in bundle_files(root)] == ["databricks.yml"]
