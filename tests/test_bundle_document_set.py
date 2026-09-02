# tests/test_bundle_document_set.py
r"""WHICH FILES ARE BUNDLE DOCUMENTS, AND WHETHER THE HELPERS CAN DRIFT NARROWER THAN THE
BUNDLE.

`tests/job_yaml.py` answers "which files does a sweep read" twice, and until this module
existed NEITHER answer was re-derived from anything. `BUNDLE_DOC_SUFFIXES` names the suffixes
a bundle document may carry; `resource_files()` reads ONE directory level of
`databricks/resources/` and its docstring says it may be WIDER than the `include:` globs the
bundle declares, *"which is the direction to be wrong in"*. Both were true when they were
written. Both were held by nothing.

WHAT A MEMBERSHIP LOCK CAN HONESTLY BE, because that is the design question here and getting
it wrong would be this phase's own defect wearing a green. A tuple's MEMBERS cannot be derived
from anywhere -- they ARE the declaration -- so no arm below claims the members are right. Each
claims one narrower thing, and the docstring on each says which:

  * the tuple is not empty, and every arm that walks it asserts that BEFORE walking it. A
    lock whose non-vacuity rides on a list being non-empty stops guaranteeing anything the
    day the list empties, which this suite has already shipped once;
  * every member is SPELLABLE as a suffix -- one that `PurePath.suffix` can never return is
    dead weight wearing the look of a decision;
  * the two sweeps read EXACTLY this tuple. Removing any one member stops BOTH of them
    reading a file that carries it, which a sweep holding its own hardcoded copy would not do,
    and which the existing "one file per entry" arms cannot see because they only ever add;
  * every member is a suffix THE CLI ITSELF reads as a bundle document, and a suffix the tuple
    does not carry is one it refuses. That is the only direction in which "are the members
    right" is answerable at all. It needs the CLI, so it is a developer-box arm that SKIPS in
    CI and says so;
  * every suffix ON DISK under the bundle root that the CLI reads as a bundle document is a
    member. That is the direction that catches a member being DELETED, which none of the
    arms above can see: they all walk whatever the tuple happens to hold.

WHAT IS NOT CLAIMED, AND MUST NOT BE READ INTO THE GREEN: that the tuple is COMPLETE. Nothing
here enumerates the suffixes the CLI accepts -- it asks about the ones declared, about the
ones this tree carries, and about one that is neither -- so a suffix the CLI accepts that no
file here uses and no `include:` names could be missing, or could be deleted from the tuple,
with every arm here green. `.yaml` is in exactly that position today: no file under
`databricks/` carries it. That gap is narrowed by that helper being allowed to be wider
than the bundle, and by the `include:` arms below. It is not closed.

THE `include:` DIRECTION IS THE OTHER HALF OF THE SAME SENTENCE. `resource_files()` is
over-strict today: three suffixes read against one `include:` pattern. If `include:` ever grows
`resources/**/*.yml`, or a second pattern outside `resources/`, the helper silently becomes
NARROWER than the bundle, its docstring's "over-strict" flips, and every classification sweep
that reads it under-reads with all tests green. The patterns are DERIVED from the bundle here
rather than written down, for the reason this phase keeps finding: a written-down set of sites
is what it has published short.

A PATTERN WHOSE SUFFIX IS NOT A LITERAL IS A FAULT, NOT AN APPROXIMATION.
`resources/*.y*ml` reaches two members of the tuple and equals none of them, and the helper
filters on `path.suffix in BUNDLE_DOC_SUFFIXES` -- an exact comparison. Refusing such a
pattern is the over-strict direction, which is the one this module is written to keep.
"""
from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import bundle_cli
import job_yaml
import pytest
import yaml
from job_yaml import BUNDLE, RESOURCES

# THE DIRECTORY `resource_files()` READS, SPELLED THE WAY AN `include:` PATTERN SPELLS IT --
# relative to the bundle root, forward-slashed. Derived from the two paths `job_yaml` already
# declares rather than typed, so a move of either is a red here instead of a silent pass.
_RESOURCES_DIR = RESOURCES.relative_to(BUNDLE.parent).as_posix()

# The glob component that makes a pattern recurse. It has to be refused by NAME as well as by
# the parent comparison below, because `resources/**` has parent `resources` and would sail
# through that comparison alone. THAT IT DESCENDS IS MEASURED, not assumed: on CLI v1.8.0
# `resources/**/*.yml` resolves a file one directory BELOW `resources/` and renders the job
# declared in it -- a file `resource_files()` never returns -- while `resources/**` resolves the
# subdirectory itself and is refused for not being a bundle document. Neither is the one level
# the helper reads.
_RECURSIVE = "**"

# The key the bundle declares its resource files under, in the CLI's own spelling.
_INCLUDE = "include"

# A SUFFIX THE TUPLE DOES NOT CARRY, for the negative half of the CLI derivation. Asserted
# absent from the tuple by the arm that uses it, so adding it to `BUNDLE_DOC_SUFFIXES` turns
# that arm red rather than turning it into a test of nothing.
_NOT_A_BUNDLE_SUFFIX = ".txt"

# The scratch bundle's name, its resource file's stem, and the job that resource declares.
# The body is JSON, which is also YAML, so ONE literal serves every suffix under test -- the
# trick `test_bundle_resource_allowlist.py`'s suffix arm already uses.
_PROBE_STEM = "probe"
_PROBE_JOB_KEY = "probe_job"
_PROBE_JOB = json.dumps({"resources": {"jobs": {_PROBE_JOB_KEY: {"name": _PROBE_STEM}}}})

_EMPTY_TUPLE = (
    "`job_yaml.BUNDLE_DOC_SUFFIXES` is empty, so every arm that walks it would report a "
    "green it could not have earned. A sweep with no suffixes reads no files at all"
)


def _include_patterns(text: str) -> list[str]:
    """The `include:` patterns a bundle document declares, refused if it declares none.

    AN EMPTY LIST IS A FAULT RATHER THAN AN EMPTY SWEEP. `not _confinement_faults([])` is
    green, and so is `{suffix for pattern in []}` being empty -- a bundle whose `include:`
    vanished would leave both arms below reporting exactly what a healthy bundle reports.
    That is the shape [ADR 0018] names: a check reporting the expected value because there
    was nothing to look at.

    [ADR 0018]: docs/adr/0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md"""
    declared = yaml.safe_load(text).get(_INCLUDE)
    assert isinstance(declared, list) and declared, (
        f"this bundle declares `{_INCLUDE}: {declared!r}`. The arms that read it hold a "
        "property of every pattern, and every pattern of none is a green nobody earned"
    )
    return declared


def _confinement_fault(pattern: str) -> str | None:
    """What is wrong with one `include:` pattern, in the words the reader needs to fix it.

    THE PROPERTY IS THE RESOURCE SWEEP'S OWN SHAPE: one directory level, that directory. A
    pattern reaching deeper, higher, or elsewhere selects a file the bundle deploys and the
    helper never returns -- and the classification sweeps that read the helper would then be
    total over a set smaller than the bundle's, with nothing going red."""
    path = PurePosixPath(pattern)
    if _RECURSIVE in path.parts:
        return f"{pattern!r} recurses; `resource_files()` reads one directory level and stops"
    if path.is_absolute() or str(path.parent) != _RESOURCES_DIR:
        return (
            f"{pattern!r} selects files outside {_RESOURCES_DIR}/, which `resource_files()` "
            "does not read. Widen the helper first, or the sweeps that read it go on being "
            "total over less than the bundle deploys"
        )
    return None


def _confinement_faults(patterns: list[str]) -> list[str]:
    """Every `include:` pattern that would put the resource sweep behind the bundle."""
    return [fault for pattern in patterns for fault in [_confinement_fault(pattern)] if fault]


def _names_read(root: Path) -> set[str]:
    """Every file name `job_yaml`'s two sweeps return for `root`, as one set.

    BOTH SWEEPS, because both filter on the same tuple and only one of them recurses. An arm
    that watched `bundle_files` alone would leave `resource_files` free to hold a copy."""
    return {path.name for path in job_yaml.bundle_files(root)} | {
        path.name for path in job_yaml.resource_files(root)
    }


def _rendered_jobs(root: Path, suffix: str) -> dict:
    """The jobs the CLI renders for a scratch bundle whose only resource file carries `suffix`.

    THE RENDERED DOCUMENT, NEVER THE EXIT CODE. `tests/bundle_cli.py` takes the workspace
    away, so the exit code is non-zero whatever the bundle says -- and measured on CLI v1.8.0
    the rendered JSON still arrives on stdout, carrying the resolved `include:` and the job
    the included file declared. A suffix the CLI refuses renders no job at all."""
    resources = root / _RESOURCES_DIR
    resources.mkdir(parents=True, exist_ok=True)
    (root / BUNDLE.name).write_text(
        f"bundle:\n  name: {_PROBE_STEM}\n{_INCLUDE}:\n  - {_RESOURCES_DIR}/*{suffix}\n",
        encoding="utf-8",
    )
    (resources / f"{_PROBE_STEM}{suffix}").write_text(_PROBE_JOB, encoding="utf-8")
    done = bundle_cli.validate(root, "-o", "json")
    assert done.stdout.strip(), (
        f"`bundle validate -o json` wrote nothing to stdout (exit {done.returncode}); this "
        f"arm reads what the CLI RENDERED: {done.stderr.strip()[:300]}"
    )
    return json.loads(done.stdout).get("resources", {}).get("jobs", {})


def test_every_declared_suffix_is_spellable_and_declared_once():
    """THE FLOOR, AND THE TWO WAYS A MEMBER CAN BE DEAD WITHOUT LOOKING DEAD.

    `PurePath.suffix` returns either the empty string or a string beginning with a dot, and
    the sweeps compare against it with `in`. So a member spelled `yml` can never equal any
    suffix any file has -- it is not a narrower rule, it is no rule -- and a member spelled
    twice is a declaration that reads like two decisions and behaves like one.

    WHAT THIS DOES NOT ESTABLISH: that a member is a suffix anything accepts. `.zzz` passes
    here. The CLI arm below is the one that asks that question."""
    declared = job_yaml.BUNDLE_DOC_SUFFIXES
    assert declared, _EMPTY_TUPLE
    unspellable = [s for s in declared if not s.startswith(".") or s == "." or s != s.strip()]
    assert not unspellable, (
        f"{unspellable} can never equal a `PurePath.suffix`, so no file can ever match them"
    )
    assert len(set(declared)) == len(declared), f"a suffix is declared twice: {declared}"


def test_removing_any_declared_suffix_narrows_both_sweeps_by_exactly_that_suffix(
    tmp_path, monkeypatch
):
    """THE MEMBERSHIP ARM: the sweeps read this tuple and nothing else.

    The two "one file per entry" arms already in this suite only ever ADD a suffix, so a
    sweep that had quietly grown its own hardcoded set -- wider than the tuple, or equal to
    it by coincidence -- passes both of them. This one takes a member away and requires the
    sweep to lose exactly the file that carried it.

    WHAT THIS DOES NOT ESTABLISH: that the member belongs in the tuple. Any suffix put here
    would pass, because the sweeps filter on whatever the tuple holds. What it refuses is a
    sweep that has stopped asking."""
    declared = job_yaml.BUNDLE_DOC_SUFFIXES
    assert declared, _EMPTY_TUPLE
    for suffix in declared:
        (tmp_path / f"{_PROBE_STEM}{suffix}").write_text(_PROBE_JOB, encoding="utf-8")
    everything = {f"{_PROBE_STEM}{suffix}" for suffix in declared}
    assert _names_read(tmp_path) == everything
    for suffix in declared:
        monkeypatch.setattr(
            job_yaml, "BUNDLE_DOC_SUFFIXES", tuple(s for s in declared if s != suffix)
        )
        assert _names_read(tmp_path) == everything - {f"{_PROBE_STEM}{suffix}"}, suffix
    monkeypatch.setattr(job_yaml, "BUNDLE_DOC_SUFFIXES", declared)
    assert _names_read(tmp_path) == everything


def test_every_declared_suffix_is_one_the_cli_reads_as_a_bundle_document(tmp_path):
    """THE ONLY DIRECTION IN WHICH "ARE THE MEMBERS RIGHT" HAS AN ANSWER, and it is the CLI's.

    `job_yaml`'s comment says the set is the CLI's own and cites a scratch bundle for the
    `.json` member. That measurement was taken by hand, published as prose, and exercised by
    nothing; this is it, kept. One scratch bundle per member, each in its own directory so a
    leftover probe cannot be read by the next pattern.

    A DEVELOPER-BOX ARM. No CI job that runs this suite installs the Databricks CLI, so this
    SKIPS on every CI run -- `tests/bundle_cli.py` carries the skip and its one reason. What
    stands in CI is the tuple itself and the arms above."""
    declared = job_yaml.BUNDLE_DOC_SUFFIXES
    assert declared, _EMPTY_TUPLE
    for suffix in declared:
        rendered = _rendered_jobs(tmp_path / suffix.replace(".", "_"), suffix)
        assert _PROBE_JOB_KEY in rendered, (suffix, sorted(rendered))


def test_a_suffix_the_tuple_does_not_carry_is_one_the_cli_refuses(tmp_path):
    """THE OTHER HALF OF THE PROBE, without which the arm above is a test of nothing.

    An arm that only ever watches the CLI accept has not shown the CLI discriminates. With
    the workspace scrubbed the CLI still refuses this one by name -- *"must be YAML or JSON
    files"* -- and renders no job, so the two halves differ in the answer and not in the
    environment. The suffix is asserted ABSENT from the tuple first, so adding it there turns
    this arm red rather than quietly making it vacuous.

    IT DOES NOT SHOW THE TUPLE IS COMPLETE: one refused suffix is not every refused suffix."""
    assert _NOT_A_BUNDLE_SUFFIX not in job_yaml.BUNDLE_DOC_SUFFIXES, (
        f"{_NOT_A_BUNDLE_SUFFIX} is declared now, so this arm no longer probes a non-member"
    )
    rendered = _rendered_jobs(tmp_path, _NOT_A_BUNDLE_SUFFIX)
    assert _PROBE_JOB_KEY not in rendered, sorted(rendered)


def _suffixes_under(root: Path) -> list[str]:
    """Every distinct suffix a file at any depth of `root` carries, the CLI's own output
    directory dropped for `job_yaml.CLI_OUTPUT_DIR`'s own reason.

    THE FILESYSTEM AND NOT `git ls-files`, for the reason `CLAUDE.md` records four false
    greens for: an untracked resource file is still a file `include:` picks up and
    `bundle deploy` sends, and git is blind to exactly those."""
    return sorted(
        {
            path.suffix
            for path in root.rglob("*")
            if path.is_file()
            and path.suffix
            and job_yaml.CLI_OUTPUT_DIR not in path.relative_to(root).parts
        }
    )


def test_every_suffix_on_disk_the_cli_reads_as_a_bundle_document_is_declared(tmp_path):
    """THE DIRECTION THAT CATCHES A MEMBER BEING DELETED, which no arm above can.

    Every arm above walks whatever the tuple holds, so shrinking the tuple shrinks the arm
    with it and nothing goes red. This one starts from the FILES instead: for each suffix
    this tree actually carries that the tuple does not, it asks the CLI whether that suffix
    is one it reads as a bundle document. A `yes` is a file the bundle deploys and every
    sweep here is blind to.

    A DEVELOPER-BOX ARM, and only for the suffixes that are NOT already declared -- the
    short-circuit below is what keeps it to a couple of CLI runs rather than one per suffix
    in the tree.

    WHAT IT DOES NOT REACH: a suffix nothing here carries. `.yaml` is that case today, so
    deleting it from the tuple stays green here and everywhere else in this module."""
    present = _suffixes_under(BUNDLE.parent)
    assert present, f"no file under {BUNDLE.parent.name}/ carries a suffix; nothing was read"
    missing = [
        suffix
        for suffix in present
        if suffix not in job_yaml.BUNDLE_DOC_SUFFIXES
        and _PROBE_JOB_KEY in _rendered_jobs(tmp_path / suffix.replace(".", "_"), suffix)
    ]
    assert not missing, (
        f"{missing} sit on files under {BUNDLE.parent.name}/ and the CLI reads them as bundle "
        "documents, but `job_yaml.BUNDLE_DOC_SUFFIXES` does not carry them"
    )


def test_every_include_pattern_is_confined_to_one_level_of_the_directory_the_helper_reads():
    """THE LOCK ON THE `include:` DIRECTION, derived from the bundle rather than written here.

    `resource_files()` is over-strict today and its docstring says that is the direction to
    be wrong in. This is what keeps that sentence true: a pattern that recurses, or that
    reaches outside `resources/`, makes the helper NARROWER than the bundle, and every sweep
    that reads it then claims a totality over less than gets deployed -- with nothing red."""
    assert not _confinement_faults(_include_patterns(BUNDLE.read_text(encoding="utf-8")))


def test_the_confinement_arm_reddens_on_each_way_include_can_outgrow_the_helper():
    """THE PLANTED POSITIVE, MADE PERMANENT, and the must-stay-green side beside it.

    An arm that reddens on everything is as useless as one that reddens on nothing, so the
    second group is the half that keeps this from being a wider net: a pattern naming one
    file, and one naming every file, both sit inside the directory the helper reads."""
    for pattern in (
        f"{_RESOURCES_DIR}/{_RECURSIVE}/*.yml",
        f"{_RESOURCES_DIR}/{_RECURSIVE}",
        f"{_RESOURCES_DIR}/nested/*.yml",
        f"/{_RESOURCES_DIR}/*.yml",
        f"../outside/{_RESOURCES_DIR}/*.yml",
        "elsewhere/*.yml",
        "*.yml",
    ):
        assert _confinement_faults([pattern]), pattern
    for pattern in (
        f"{_RESOURCES_DIR}/*.yml",
        f"{_RESOURCES_DIR}/*",
        f"{_RESOURCES_DIR}/{_PROBE_STEM}.yml",
    ):
        assert not _confinement_faults([pattern]), pattern


def test_a_bundle_that_declares_no_include_pattern_is_refused_rather_than_swept_empty():
    """THE FLOOR UNDER THE TWO ARMS THAT READ `include:`, as a check and not a sentence.

    Both of them are shaped `every pattern satisfies P`, which an empty list satisfies. The
    guarantee would expire exactly when the bundle lost the declaration those arms exist to
    read."""
    for text in (
        f"bundle:\n  name: {_PROBE_STEM}\n",
        f"bundle:\n  name: {_PROBE_STEM}\n{_INCLUDE}: []\n",
        f"bundle:\n  name: {_PROBE_STEM}\n{_INCLUDE}: {_RESOURCES_DIR}/*.yml\n",
    ):
        with pytest.raises(AssertionError):
            _include_patterns(text)


def test_every_suffix_the_include_patterns_name_is_one_the_tuple_carries():
    """WHERE THE TWO HALVES OF THIS MODULE MEET.

    Confinement puts the bundle's patterns inside the directory that helper reads;
    this puts their SUFFIXES inside the tuple it filters by. Together they are what makes
    "the helper is over-strict" a derived statement rather than a remembered one.

    A pattern naming no suffix at all (`resources/*`) constrains nothing here and is dropped
    -- it is wider than any member, which is the safe direction. A pattern whose suffix is
    not a literal (`*.y*ml`) is NOT dropped: it equals no member, the helper compares exactly,
    and nobody has shown the helper covers it."""
    patterns = _include_patterns(BUNDLE.read_text(encoding="utf-8"))
    named = sorted({PurePosixPath(pattern).suffix for pattern in patterns} - {""})
    assert named, f"no pattern in {patterns} names a suffix; this arm has nothing to hold"
    unknown = [suffix for suffix in named if suffix not in job_yaml.BUNDLE_DOC_SUFFIXES]
    assert not unknown, (
        f"`{_INCLUDE}:` deploys {unknown}, which `job_yaml.BUNDLE_DOC_SUFFIXES` does not "
        "carry, so every sweep that reads that tuple is blind to files the bundle deploys"
    )
