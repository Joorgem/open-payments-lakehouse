# tests/test_bundle_glob_allowlist.py
r"""WHERE A BUNDLE-DOCUMENT GLOB MAY BE SPELLED UNDER `tests/`, AND IT IS ONE MODULE.

THIS LOCK CATCHES NOTHING TODAY, AND SAYING SO PLAINLY IS PART OF IT. Every pair in
`_ALLOWED` below is a deliberate exception carrying its own reason on its own line; not one
of them is a defect this arm is waiting to report. What it refuses is the NEXT one -- a
sweep that claims totality over `databricks/resources/` while reading one suffix of the
set, written by somebody who copied a neighbour and did not know there was a shared
helper. It guards nothing today and everything tomorrow.

WHY THAT IS WORTH AN ARM AT ALL. A group of sweeps was widened to read
`job_yaml.resource_files()` for exactly that defect -- they said "every file" and read
`*.yml` -- and nothing then asserted that they still do. Measured before this module
existed: substituting every one of them back to its own `*.yml` glob, with a `.yaml`
resource file planted under `databricks/resources/`, left every arm that reads them
green. That is the false green they were widened to remove, reproduced. Which sweeps read
the shared helper is derived rather than written down here, because a written-down set of
sites is what this phase has published short. It excludes THIS module, which names the
helper in prose without reading it -- an exclusion measured rather than tidied in: without
it the command returns its own paragraphs, the line below among them, and a derivation that
returns the prose describing it has stopped being a derivation.

    git grep -n resource_files -- tests/ ':!tests/test_bundle_glob_allowlist.py'

The probe that reddened them was deleted with the commit that used it. This module is what
stands in its place, and it is deliberately a DIFFERENT shape: locking each sweep
individually needs each one to accept a `root` it can be pointed at, and several do not,
while one arm over the SPELLING needs nothing of them and reaches the sweep nobody has
written yet.

IT PARSES THE SOURCE. IT DOES NOT GREP IT, and the difference is not stylistic here. The
AST does not see comments at all, so `tests/job_yaml.py`'s own published derivation -- a
glob pattern living inside a `#` comment -- is invisible to this walk by construction
rather than by an exclusion somebody has to maintain. String literals are the same story
in the other direction, and this module had to be written around it: the probe sources
below are BUILT by `_probe` rather than written out, because a source-level grep cannot
tell a glob CALL from a glob QUOTED IN A TEST, and the greps published over this tree
would have grown sites that no sweep performs. That a grep needs that kind of care around
it is the argument for not being one.

KEYED BY (MODULE, PATTERN), NEVER BY LINE NUMBER. A line number rots on the next edit
above it, and a rotted citation still reads like a citation -- this phase has already
published several that no longer resolve. The pair follows a move within a file and stops
at a move between files, which is the direction that should stop.

EXACT IN BOTH DIRECTIONS, which is what keeps an allowlist from becoming a place to put
things -- the argument `tests/test_size_caps.py` already makes for its cap list. An entry
naming a pair that is no longer spelled on disk FAILS, so the list shrinks as sites are
fixed and cannot rot behind a rename. What it does not count is OCCURRENCES: a module that
spells an allowlisted pattern twice is still one entry, because the decision being recorded
is the spelling, not how often it appears.

A PATTERN THIS MODULE CANNOT READ IS A FAULT, NOT A SKIP. A module-level string constant
is resolved and then classified on its value, so the live case (`registry*.py`, in
`tests/bronze/test_registry_guard_wiring.py`) stays green on its merits rather than on
this lock being unable to look. Anything else -- a pattern assembled at run time, a name
bound twice -- is refused, so "a module-level constant is the only indirection a glob
under `tests/` takes" is not a claim in a docstring here: it is what the arm below
enforces. A pattern nothing can read would otherwise be a green nobody earned, which is
the shape [ADR 0018] names: a check reporting the expected value because it could not
look.

WHAT IT IS KNOWN NOT TO REACH, each derived rather than assumed, and not offered as a list
of everything nobody has thought of:

  * a sweep that does not glob -- the `os` walkers, `glob.iglob`, `fnmatch`, or an
    `iterdir()` filtered on `Path.suffix`. None is CALLED under `tests/` today, and the
    command below exits 1. It matches on the opening parenthesis on purpose, because the
    paragraph you are reading names those functions and a grep cannot tell a call from a
    mention -- which is this module's whole argument, arriving uninvited in its own
    docstring. `glob.glob` is NOT in this bullet and measuring is why: the walk matches an
    attribute call by its attribute name, so a call spelled through the module is caught
    like any other `glob` -- `iglob` alone is the name the tuple does not carry. Naming it
    here without its parenthesis is not squeamishness: the command below matches on that
    parenthesis, and a mention carrying one would make this paragraph its own only hit:

    git grep -nE '\bos\.(walk|scandir|listdir)\(|\bglob\.i?glob\(|\bfnmatch\.' -- tests/
  * a glob naming no suffix at all (`rglob("*")`, which `tests/job_yaml.py` itself uses).
    That is WIDER than a bundle document, not narrower, so it is not the defect here, and
    `_selects_a_bundle_document` holds that line deliberately rather than by accident;
  * anything outside `tests/`. `scripts/`, `src/` and the workflows spell their own paths
    and no arm here reads them.

[ADR 0018]: docs/adr/0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md
"""
from __future__ import annotations

import ast
from pathlib import Path, PurePosixPath

from job_yaml import BUNDLE_DOC_SUFFIXES, REPO

_TESTS = REPO / "tests"

# THE ONE MODULE THAT MAY SPELL A BUNDLE-DOCUMENT SUFFIX FREELY, because it is where the
# suffix set lives and where every sweep is supposed to get its file list. Its path is
# relative to the swept root, like every key below. It spells no such glob today --
# `bundle_files` recurses on `rglob("*")` and `resource_files` uses `iterdir()` -- so this
# exemption is not load-bearing yet; it is what makes the rule "outside `job_yaml`" instead
# of "nowhere", and `test_the_home_module_is_where_a_bundle_document_glob_may_be_spelled`
# is what keeps it from being decoration.
_HOME = "job_yaml.py"

# The two calls that take a glob pattern. `Path.glob`/`Path.rglob` are attribute calls; a
# bare name is matched too, so `from glob import glob` cannot spell one past this walk.
_SWEEP_NAMES = ("glob", "rglob")

# `Path.glob(pattern, *, case_sensitive=...)` accepts its pattern by keyword, so a call
# with no positional argument is not automatically a call with no pattern.
_PATTERN_KEYWORD = "pattern"

# THE NAMES `_selects_a_bundle_document` ASKS A PATTERN ABOUT. Only the suffixes are
# load-bearing; the stem is arbitrary and shared so that the two probes differ in nothing
# else. `_NOT_A_BUNDLE_DOCUMENT` carries a suffix outside the tuple on purpose: a pattern
# reaching it as well is not making a claim about bundle documents at all.
_PROBE_STEM = "resource"
_NOT_A_BUNDLE_DOCUMENT = f"{_PROBE_STEM}.py"

# A FLOOR AND NOT A COUNT, for the reason `tests/test_size_caps.py` gives for the floors on
# its own measurements: an exact count is a claim about the repository that goes stale on
# the next file added, which is the species this tree keeps catching. Both arms that read
# the real tree assert an EMPTINESS, and an emptiness is also what a walk that read nothing
# reports -- so each reads this floor first. The reverse arm is the one that published the
# guarantee in prose: it said it fails when the walk reads nothing, while `set(_ALLOWED) -
# anything` is empty once the list is, so the guarantee rode on entries this module exists
# to remove and expired exactly when the module succeeded. Measured: with `_ALLOWED` empty
# and the walk returning nothing, both arms were green.
_FLOOR_ON_THE_WALK = 100

_UNEARNED_GREEN = (
    f"the walk read {{read}} modules under {_TESTS.name}/, which is fewer than a tree any "
    "arm here can claim an emptiness over. This green is one the walk could not have earned"
)

# EVERY BUNDLE-DOCUMENT GLOB SPELLED OUTSIDE `job_yaml` TODAY, EACH WITH ITS OWN REASON.
# Keys are (module path relative to `tests/`, the glob pattern), and a new entry is a
# decision somebody has to type out -- the argument `_DECLARABLE` already makes in
# `tests/test_bundle_resource_allowlist.py`. NOT EVERY ENTRY HERE IS AN EXCEPTION ON ITS
# MERITS: one of them IS a sweep claiming totality over a directory it reads one suffix of,
# and its reason says so. An allowlist reason may record a deferral -- the fix moves with a
# file this module does not own -- but calling a deferred defect "not a defect" is how a
# list like this becomes the place to put things. Which entry it is, its own line says;
# counting them here would be a claim that rots on the next one added.
_ALLOWED: dict[tuple[str, str], str] = {
    ("test_gold_job_wiring.py", "gold_*.yml"):
        "a naming-convention closure: it selects a convention, not a directory's totality",
    ("test_vault_job_wiring.py", "vault_*.yml"):
        "the same closure over the vault convention, and quoted in committed job YAMLs",
    ("triage_agent/test_blast_radius_lock.py", "vault_merchant*.yml"):
        "asserts a rename() took effect on one known file inside a copytree, not a sweep",
    ("test_readme_counts.py", "*.yml"):
        "THE DEFECT, DEFERRED AND NOT EXCUSED: it derives the bundle's job and task counts "
        "from one suffix while its own docstring says `the bundle's jobs`. The same glob is "
        "a fragment README.md publishes and another arm there asserts is present, so the "
        "two halves move together and are fixed with the README, not from here",
    ("test_revision_stamp.py", "databricks.yml"):
        "names ONE file; whether to widen it to the other bundle-root spellings is open",
}


def _string_bindings(node: ast.stmt) -> list[tuple[str, str]]:
    """The `NAME = "literal"` pairs one statement binds, annotated or not."""
    if isinstance(node, ast.Assign):
        targets, value = node.targets, node.value
    elif isinstance(node, ast.AnnAssign) and node.value is not None:
        targets, value = [node.target], node.value
    else:
        return []
    if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
        return []
    return [(t.id, value.value) for t in targets if isinstance(t, ast.Name)]


def _module_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level names bound ONCE to a string literal.

    A name bound twice is dropped rather than resolved to whichever assignment came last:
    this walk does not know which one reaches the glob, and guessing would be the one
    outcome worse than refusing. Dropped means UNREADABLE, which `_glob_faults` reports.

    An AUGMENTED assignment drops the name whatever it adds, literal or not, because
    `_G = '*.y'` followed by `_G += 'ml'` is a name bound twice wearing a spelling that
    reads like one binding -- and resolving it to the first half would classify a glob of
    every bundle document as a glob of nothing."""
    bound: dict[str, str] = {}
    rebound: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            rebound.add(node.target.id)
        for name, value in _string_bindings(node):
            if name in bound:
                rebound.add(name)
            bound[name] = value
    return {name: value for name, value in bound.items() if name not in rebound}


def _called_name(func: ast.expr) -> str | None:
    """The name a call spells, whether as an attribute or as a bare name."""
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _sweep_calls(tree: ast.Module) -> list[ast.Call]:
    """Every `glob`/`rglob` call at any depth of the module."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _called_name(node.func) in _SWEEP_NAMES
    ]


def _pattern_of(call: ast.Call, constants: dict[str, str]) -> str | None:
    """The literal pattern a glob call asks for, or `None` when it cannot be read.

    Positional first, then the keyword the signature accepts, then a module-level constant
    -- the one indirection any glob under `tests/` takes today."""
    argument = call.args[0] if call.args else next(
        (kw.value for kw in call.keywords if kw.arg == _PATTERN_KEYWORD), None
    )
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return argument.value
    if isinstance(argument, ast.Name):
        return constants.get(argument.id)
    return None


def _selects_a_bundle_document(pattern: str) -> bool:
    """Whether `pattern` reaches the files a bundle document may be named.

    AN EXACT SUFFIX MATCH ALONE IS NOT THE QUESTION, and measuring it is how this arm found
    out: `*.y*ml`, `*yml` and `*.[yj]ml` all reach bundle documents, and NONE of them has a
    bundle suffix, because a metacharacter spelled inside or before the suffix walks
    straight past `PurePosixPath.suffix`. `*.y*ml` is a plausible hand-spelling of "every
    bundle document" that reaches the `y` suffixes and not `.json` -- which is not an edge
    case but the defect itself, a totality claim over a subset, wearing the one spelling
    the classification could not see. So the pattern is also MATCHED against a name per
    entry of the suffix tuple, and reaching any of them is enough.

    The last direction is what keeps that from swallowing the opposite case: a pattern that
    reaches `_NOT_A_BUNDLE_DOCUMENT` too is WIDER than a bundle document rather than a
    totality claim over one -- `rglob("*")` is the live example, and the docstring above
    already argues it is not the defect here. Comparison is case-folded in both directions,
    so a `*.YML` spelling is not a way past this."""
    lowered = pattern.lower()
    if PurePosixPath(pattern).suffix.lower() in BUNDLE_DOC_SUFFIXES:
        return True
    if not PurePosixPath(lowered).name:
        return False
    if PurePosixPath(_NOT_A_BUNDLE_DOCUMENT).match(lowered):
        return False
    return any(
        PurePosixPath(f"{_PROBE_STEM}{suffix}").match(lowered)
        for suffix in BUNDLE_DOC_SUFFIXES
    )


def _bundle_globs(module: str, source: str) -> list[tuple[int, str | None]]:
    """Every glob call in `source` this lock has something to say about.

    Two kinds, and the second is why the first can be believed: a pattern that selects the
    files a bundle document may be named, and a pattern that could not be read at all
    (`None`)."""
    tree = ast.parse(source, filename=module)
    constants = _module_constants(tree)
    return [
        (call.lineno, pattern)
        for call in _sweep_calls(tree)
        for pattern in [_pattern_of(call, constants)]
        if pattern is None or _selects_a_bundle_document(pattern)
    ]


def _fault(module: str, line: int, pattern: str | None) -> str:
    """What is wrong with one glob call, in the words the reader needs to fix it."""
    if pattern is None:
        return (
            f"{module}:{line} globs a pattern this arm cannot read. Spell it as a literal "
            "or a module-level constant; a pattern nothing can read is a green nobody earned"
        )
    return (
        f"{module}:{line} globs {pattern!r}, a bundle-document glob spelled outside "
        f"{_HOME}. Read the file list from `job_yaml`, or add the pair to `_ALLOWED` with "
        "the reason it is not a sweep claiming a directory it does not read"
    )


def _glob_faults(sources: dict[str, str]) -> list[str]:
    """Every bundle-document glob in `sources` that is neither `job_yaml`'s nor allowlisted."""
    return [
        _fault(module, line, pattern)
        for module, source in sorted(sources.items())
        if module != _HOME
        for line, pattern in _bundle_globs(module, source)
        if pattern is None or (module, pattern) not in _ALLOWED
    ]


def _spelled_pairs(sources: dict[str, str]) -> set[tuple[str, str]]:
    """The (module, pattern) pairs actually spelled in `sources`, for the reverse check."""
    return {
        (module, pattern)
        for module, source in sources.items()
        if module != _HOME
        for _, pattern in _bundle_globs(module, source)
        if pattern is not None
    }


def _module_sources(root: Path = _TESTS) -> dict[str, str]:
    """Every Python module at any depth under `root`, keyed by its path relative to it.

    THE FILESYSTEM AND NOT `git ls-files`, for the reason `CLAUDE.md` records false greens
    for: an untracked module under `tests/` is a module pytest collects and runs,
    and git is blind to exactly those -- so a probe planted to check this lock would leave
    it green, which is the opposite of what a probe is for. `utf-8-sig` because a Windows
    editor has put a BOM on a file in this repository before, and `ast.parse` would raise
    a SyntaxError from a lock that never mentions encodings."""
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8-sig")
        for path in sorted(root.rglob("*.py"))
    }


def _probe(pattern: str, through_a_constant: bool = False) -> str:
    """A module that globs `pattern`, directly or through a module-level constant.

    BUILT RATHER THAN QUOTED, and the reason is measured rather than tidy: the derivations
    published over this tree -- `tests/job_yaml.py`'s among them -- match a DOUBLE-quoted
    literal, and `repr` spells a single-quoted one, so no probe here lands in a command
    shipped by another module. Writing the probes out by hand would have added phantom
    sites to exactly the kind of published set this module exists to keep honest."""
    if through_a_constant:
        return f"import pathlib\n_G = {pattern!r}\npathlib.Path('.').glob(_G)\n"
    return f"import pathlib\npathlib.Path('.').glob({pattern!r})\n"


def test_no_test_module_spells_a_bundle_document_glob_of_its_own():
    """THE LOCK, total over `tests/` and green over an allowlist that is all of today.

    Nothing here is a defect today; see this module's docstring. What this refuses is a new
    sweep spelling `*.yml` where it means "every bundle document", which is how the widened
    sweeps got their false green in the first place.

    THE FLOOR IS READ FIRST because this arm's entire content is `not [...]`, and a walk
    that read no modules reports the same empty list a clean tree does."""
    sources = _module_sources()
    assert len(sources) >= _FLOOR_ON_THE_WALK, _UNEARNED_GREEN.format(read=len(sources))
    assert not _glob_faults(sources)


def test_every_allowlisted_pair_is_still_spelled_on_disk():
    """THE REVERSE DIRECTION, and it is what stops the list becoming a place to put things.

    An entry whose module was renamed, or whose glob was moved into `job_yaml` where it
    belongs, fails here -- so the list shrinks as sites are fixed instead of quietly
    re-permitting the spelling for whoever adds it back.

    IT ALSO FAILS IF THE WALK READ NOTHING, and that used to be a sentence rather than a
    check. `set(_ALLOWED) - anything` is empty once the list is empty, so the guarantee
    rode on entries this module exists to remove: with `_ALLOWED` shrunk to nothing, a walk
    returning nothing was green here. The floor is what the sentence now rests on."""
    sources = _module_sources()
    assert len(sources) >= _FLOOR_ON_THE_WALK, _UNEARNED_GREEN.format(read=len(sources))
    stale = sorted(set(_ALLOWED) - _spelled_pairs(sources))
    assert not stale, (
        f"these pairs are allowlisted and no longer spelled under {_TESTS.name}/: {stale}. "
        "Delete the entry; an allowlist that outlives its site permits a spelling nobody "
        "has argued for."
    )


def test_the_lock_goes_red_on_a_glob_of_every_suffix_a_bundle_document_may_carry():
    """THE PLANTED POSITIVE, MADE PERMANENT, one per entry of `BUNDLE_DOC_SUFFIXES`.

    The widened sweeps rested on a probe that was deleted with the commit that ran it, and
    a run-record integer whose exerciser is gone is one nobody can check. This is that
    probe, kept."""
    for suffix in BUNDLE_DOC_SUFFIXES:
        pattern = f"*{suffix}"
        faults = _glob_faults({"test_probe.py": _probe(pattern)})
        assert any(repr(pattern) in fault for fault in faults), (pattern, faults)


def test_the_allowlist_is_keyed_by_the_pair_and_not_by_the_pattern_alone():
    """THE OTHER HALF OF THE PROBE: an allowlisted pair stays green, and the SAME pattern
    stays red in a module that has not argued for it.

    A lock that is only watched going red is half-measured -- an arm that reddens on
    everything is as useless as one that reddens on nothing."""
    for module, pattern in _ALLOWED:
        assert not _glob_faults({module: _probe(pattern)}), (module, pattern)
        assert _glob_faults({"test_elsewhere.py": _probe(pattern)}), (module, pattern)


def test_the_home_module_is_where_a_bundle_document_glob_may_be_spelled():
    """`job_yaml` owns the suffix set, so it owns the spelling; every other module borrows
    its file list. It spells no such glob today, which is why this arm plants one rather
    than reading the exemption off disk and calling that a check."""
    source = _probe("*.yaml")
    assert not _glob_faults({_HOME: source})
    assert _glob_faults({"test_elsewhere.py": source})


def test_a_pattern_the_arm_cannot_read_is_refused_rather_than_skipped():
    """A GLOB WHOSE PATTERN IS ASSEMBLED AT RUN TIME IS THE HOLE THIS CLOSES.

    Skipping it would report the expected value because the arm could not look. Refusing it
    costs whoever writes one a literal or a module-level constant, and buys back the
    totality the arm claims."""
    assembled = "import pathlib\npathlib.Path('.').glob(_prefix + '.yml')\n"
    assert _glob_faults({"test_probe.py": assembled})


def test_a_module_level_constant_is_resolved_rather_than_waved_through():
    """THE ONE INDIRECTION LIVE UNDER `tests/` TODAY, classified on its value.

    `tests/bronze/test_registry_guard_wiring.py` globs `registry*.py` through a named
    constant, and refusing that would be this arm punishing a spelling for being readable.
    So the constant is resolved -- and the same indirection carrying a bundle-document
    suffix is caught, which is the half that matters."""
    assert not _glob_faults({"test_probe.py": _probe("registry*.py", through_a_constant=True)})
    assert _glob_faults({"test_probe.py": _probe("*.yml", through_a_constant=True)})


def test_a_name_bound_twice_is_refused_rather_than_resolved_to_the_last_one():
    """Two assignments and this walk cannot say which reaches the call, so it says so."""
    twice = "import pathlib\n_G = 'a.py'\n_G = '*.yml'\npathlib.Path('.').glob(_G)\n"
    assert _glob_faults({"test_probe.py": twice})


def test_a_name_built_up_by_augmented_assignment_is_refused_too():
    """A NAME BOUND TWICE WEARING A SPELLING THAT READS LIKE ONE BINDING.

    `_G = '*.y'` then `_G += 'ml'` globs every bundle document, and resolving it to the
    first half classifies it on the suffix `.y` -- a green over a totality claim, which is
    the exact shape this module refuses. Both were measured green before this arm: the
    second because a non-literal augment left the first binding standing untouched."""
    for augment in ("_G += 'ml'", "_G += _SUFFIX"):
        source = f"import pathlib\n_G = '*.y'\n{augment}\npathlib.Path('.').glob(_G)\n"
        assert _glob_faults({"test_probe.py": source}), augment


def test_a_metacharacter_inside_the_suffix_does_not_walk_past_the_classification():
    """`*.y*ml` REACHES BUNDLE DOCUMENTS AND HAS NO BUNDLE SUFFIX.

    Classification was an exact suffix match, so a wildcard spelled inside or before the
    suffix walked past it -- and every pattern in the first group was measured GREEN before
    this arm existed. A sweep spelled that way is a hand-spelling of "every bundle
    document" that reads the `y` suffixes and not `.json`: a totality claim over a subset,
    which is the defect this module exists to refuse rather than an edge case near it.

    THE SECOND GROUP IS WHY THE FIRST IS NOT JUST A WIDER NET. A glob naming no suffix at
    all reaches more than bundle documents rather than claiming to be all of them, and this
    module's docstring says that is not what it refuses. An arm that only reddens is as
    unmeasured as one that only greens."""
    for pattern in ("*.y*ml", "*.y?ml", "*.[yj]ml", "*.yaml*", "*yml", "*.y[am]*l"):
        assert _glob_faults({"test_probe.py": _probe(pattern)}), pattern
    for pattern in ("*", "**/*", "*.py", "registry*.py", "*.md", "*.whl"):
        assert not _glob_faults({"test_probe.py": _probe(pattern)}), pattern


def test_a_pattern_naming_no_file_at_all_is_classified_rather_than_raising():
    """`PurePosixPath('').match(...)` RAISES, and a lock that raises has not measured.

    An empty pattern and a bare `.` have no final component, so neither names a file and
    neither selects a bundle document. Without the guard the classifier reaches the match
    and a ValueError comes out where a verdict should -- a red nobody can act on, which is
    the same failure as a green nobody earned. The rest are neighbours that must stay
    green: they name a file, they just do not name a bundle document."""
    for pattern in ("", ".", "..", "a/", "[", "  "):
        assert not _glob_faults({"test_probe.py": _probe(pattern)}), pattern


def test_the_pattern_is_read_when_the_call_passes_it_by_keyword():
    """`Path.glob` takes its pattern by keyword too, so a call with no positional argument
    is not a call with no pattern. Reading only `args[0]` would skip this one silently."""
    keyword = f"import pathlib\npathlib.Path('.').glob({_PATTERN_KEYWORD}='*.yml')\n"
    assert _glob_faults({"test_probe.py": keyword})


def test_the_sweep_reads_modules_off_disk_at_any_depth_below_its_root(tmp_path):
    """WHAT MAKES THE ARMS ABOVE MORE THAN A UNIT TEST OF THEIR OWN STRINGS.

    Everything else here feeds `_glob_faults` a dict built in the test. This plants a real
    file in a real subdirectory and shows the walk finds it, keyed by a path that carries
    the subdirectory -- which is the shape one allowlist key already has."""
    nested = tmp_path / "triage_agent"
    nested.mkdir()
    (nested / "test_probe.py").write_text(_probe("*.yml"), encoding="utf-8")
    faults = _glob_faults(_module_sources(tmp_path))
    assert any("triage_agent/test_probe.py" in fault for fault in faults), faults
