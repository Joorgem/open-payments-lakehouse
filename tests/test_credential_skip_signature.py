# tests/test_credential_skip_signature.py
"""WHEN THE RENDERING HALF IS ALLOWED TO SKIP, PUT TO THE CLI THAT WRITES THE TEXT.

`tests/test_bundle_targets_and_schedules.py` renders this bundle under both targets, which
needs credentials, so it skips where there are none -- and it skips only on a RECOGNISED
signature, because turning every non-zero exit into a skip once swallowed a probe: a bundle
that had stopped validating read exactly like a box with no token. The signatures live in a
tuple there, `_RECOGNISED_CREDENTIAL_FAILURES`, and its own comment records where each one was
measured.

NOTHING RE-DERIVED THAT TUPLE. Measured in this phase's first review: emptying it to `()` left
every arm green, on this box and in CI. It is load-bearing in exactly one state -- a box that
HAS the CLI and CANNOT authenticate -- and neither a developer box nor CI is in that state, so
the members sat unexercised while reading like a decision. Emptied, the rendering arms stop
skipping and start FAILING wherever credentials are absent, which is a red nobody could act
on; widened by one careless member, they start skipping over the failure the tuple exists to
let through.

WHAT A MEMBERSHIP LOCK CAN HONESTLY BE HERE, because the members cannot be derived from
anywhere -- they ARE the declaration:

  * the tuple is not empty, and every arm below says so before walking it;
  * every member is spelled the way the recogniser COMPARES -- lowercased and non-blank. A
    member carrying a capital can never match, because the haystack is `stderr.lower()`, and
    the text the tuple's own comment records for the second member starts with one. Such a
    member is not a narrower rule, it is no rule, and it looks exactly like a decision;
  * the credential-absent state THE CLI ACTUALLY PRODUCES is recognised. That is the one
    member this repository can put to the authority that writes it, and it is what makes the
    tuple non-empty in a way an `assert` over a length never could;
  * a real BUNDLE error is NOT recognised. Without this half the arm above is satisfied by a
    member so wide it swallows everything, which is precisely the failure the tuple's own
    comment says it was written after.

WHAT IS NOT ESTABLISHED, AND MUST NOT BE READ INTO THE GREEN:

  * the *invalid access token* member. Producing it needs a host and a rejected PAT, so it
    needs a network call and a credential this suite must not fabricate. It stays a recorded
    measurement in the tuple's own comment, exercised by nothing, and the spelling arm below
    is all that holds it;
  * that the tuple is COMPLETE. A credential state the CLI spells some third way would still
    turn a skip into a red -- which is the direction the tuple's docstring already chooses;
  * the CONSUMER. What is derived here is the membership, not the one line in the declaring
    module that lowercases stderr and asks `any(...)`. `_matched` below MIRRORS that line
    and says so; a change there that stopped lowercasing would leave these arms green.

IT READS THE TUPLE OUT OF THE SOURCE RATHER THAN IMPORTING IT. A test module importing
another test module gives the suite a collection-order dependency it does not otherwise have
-- the reason `tests/job_yaml.py` and `tests/adr_files.py` both exist -- and the declaring
module is at 796 lines against a strictly-under-800 cap, so the arm could not have gone there
either. Parsing rather than grepping is `tests/test_bundle_glob_allowlist.py`'s argument,
arriving again: a grep cannot tell a declaration from a mention, and this file mentions the
name in its own prose.

THE CLI'S CREDENTIAL-ABSENT TEXT CARRIES LOCAL PATHS -- the config file it looked for and the
directory the binary lives in, which on this box contains the operator's user name. It is fine
in a terminal and must not be pasted into a committed artefact, which is why the arms below
report the MEMBER that matched rather than the text it matched in.
"""
from __future__ import annotations

import ast
from pathlib import Path

import bundle_cli
import pytest

# WHERE THE TUPLE IS DECLARED, and the name it is declared under. Both are read from disk
# below; a rename of either turns the reader red rather than turning it into a test of
# nothing, which is the whole reason it refuses instead of skipping.
_DECLARING_MODULE = "test_bundle_targets_and_schedules.py"
_TUPLE_NAME = "_RECOGNISED_CREDENTIAL_FAILURES"

# The scratch bundle these probes are taken over: the smallest document the CLI accepts as
# one. Nothing about this repository's bundle is involved, deliberately -- what is being
# derived is what the CLI SAYS, not what this bundle renders to.
_SCRATCH_BUNDLE = "databricks.yml"
_SCRATCH_TEXT = "bundle:\n  name: probe\n"

# A target the scratch bundle does not declare. Asking for it is a BUNDLE error, refused
# before the CLI reaches authentication -- measured on v1.8.0 with the environment scrubbed,
# stderr carrying `no such target` and no credential text at all.
_NO_SUCH_TARGET = "no_such_target_for_this_probe"

_EMPTY_TUPLE = (
    f"`{_TUPLE_NAME}` is empty. Every arm here would then hold a property of no members, and "
    "the rendering arms it guards would fail wherever credentials are absent"
)


def _declared(source: str, name: str) -> tuple[str, ...]:
    """The tuple `name` is bound to at module level in `source`.

    REFUSED RATHER THAN SKIPPED when it cannot be read -- bound twice, bound to something
    that is not a literal, or gone. A reader that shrugged would report the expected value
    because it could not look, which is the shape [ADR 0018] names and the shape this whole
    module exists to remove from a tuple.

    [ADR 0018]: docs/adr/0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md"""
    bound = [
        node.value
        for node in ast.parse(source, filename=_DECLARING_MODULE).body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    ]
    assert len(bound) == 1, (
        f"{_DECLARING_MODULE} binds {name} {len(bound)} times at module level; this reader "
        "resolves it to one declaration and would otherwise be guessing which one is live"
    )
    try:
        literal = ast.literal_eval(bound[0])
    except ValueError as unreadable:
        raise AssertionError(
            f"{_DECLARING_MODULE} binds {name} to something this reader cannot evaluate. "
            "Spell it as a literal tuple; a declaration nothing can read is a green nobody "
            "earned"
        ) from unreadable
    assert isinstance(literal, tuple), f"{name} is a {type(literal).__name__}, not a tuple"
    return literal


def _signatures() -> tuple[str, ...]:
    """The declared credential-failure signatures, read off disk.

    `utf-8-sig` because a Windows editor has put a BOM on a file in this repository before,
    and `ast.parse` would raise a SyntaxError from a lock that never mentions encodings."""
    source = (Path(__file__).resolve().parent / _DECLARING_MODULE).read_text(encoding="utf-8-sig")
    return _declared(source, _TUPLE_NAME)


def _matched(stderr: str) -> list[str]:
    """Which signatures the declaring module's skip would recognise in `stderr`.

    A MIRROR OF ONE LINE IN ANOTHER MODULE, said plainly because it cannot be imported: that
    line lowercases stderr and asks `any(...)`. This returns WHICH members matched instead of
    whether any did, so the arms below can name one without printing the CLI's text."""
    lowered = stderr.lower()
    return [signature for signature in _signatures() if signature in lowered]


def _scratch_bundle(root: Path) -> Path:
    """The smallest document the CLI accepts as a bundle, written at `root`."""
    root.mkdir(parents=True, exist_ok=True)
    (root / _SCRATCH_BUNDLE).write_text(_SCRATCH_TEXT, encoding="utf-8")
    return root


def test_every_signature_is_spelled_the_way_the_recogniser_compares():
    """THE FLOOR AND THE ONE WAY A SIGNATURE CAN BE DEAD WITHOUT LOOKING DEAD.

    The comparison is against `stderr.lower()`, so a member carrying an upper-case letter can
    never match anything -- and the text the tuple's own comment records for the second member
    begins with a capital, which is exactly how that mistake gets made. A blank member is the
    opposite
    failure and is worse: `"" in anything` is true, so one turns every non-zero exit into a
    skip and the rendering arms stop being able to fail at all.

    THIS RUNS IN CI. The two arms below need the CLI and do not."""
    declared = _signatures()
    assert declared, _EMPTY_TUPLE
    unmatchable = [s for s in declared if s != s.lower() or not s.strip()]
    assert not unmatchable, (
        f"{unmatchable} cannot do the job the tuple is for: the recogniser compares against "
        "`stderr.lower()`, so a member with a capital never matches and a blank one always does"
    )
    assert len(set(declared)) == len(declared), f"a signature is declared twice: {declared}"


def test_the_credential_absent_state_the_cli_produces_is_recognised(tmp_path):
    """THE MEMBERSHIP ARM: the tuple is put to the authority that writes the text.

    `tests/bundle_cli.py` takes the workspace away by dropping every `DATABRICKS_` variable
    and pointing the config file at a path that does not exist, so this is the state the
    rendering arms exist to skip on, produced on purpose rather than waited for. An emptied
    tuple fails here; so does one that has lost the member the CLI actually writes.

    A DEVELOPER-BOX ARM. No CI job that runs this suite installs the CLI, so it SKIPS on
    every CI run and says so. And a box that resolves a credential ANYWAY -- some auth this
    scrub does not reach -- cannot produce the state, so it skips too, naming that rather
    than reporting a green over a probe that never happened.

    WHAT IT DOES NOT ESTABLISH: the other member, which needs a rejected token and a network
    call. See this module's docstring."""
    done = bundle_cli.validate(_scratch_bundle(tmp_path))
    if not done.returncode:
        pytest.skip(
            "the CLI authenticated with every DATABRICKS_ variable removed and the config "
            "file absent, so the credential-absent state cannot be produced on this box"
        )
    assert done.stderr.strip(), f"the CLI exited {done.returncode} and said nothing on stderr"
    matched = _matched(done.stderr)
    assert matched, (
        f"none of {_signatures()} appears in the credential-absent stderr this CLI writes, so "
        "the rendering arms would FAIL rather than skip wherever credentials are absent. Run "
        "`databricks bundle validate` with no credentials and re-declare the tuple from what "
        "it says"
    )


def test_a_bundle_error_is_not_recognised_as_a_credential_state(tmp_path):
    """THE OTHER HALF, without which the arm above is satisfied by a member that matches
    everything.

    The tuple's own comment states the property this holds: *an exit carrying neither string
    is RED whatever caused it*. Asking a bundle for a target it does not declare is refused
    before the CLI reaches authentication, so its stderr is a bundle error and nothing else --
    and a member wide enough to swallow it is a member that would swallow the probe the tuple
    was written after.

    A DEVELOPER-BOX ARM, for the reason the arm above gives."""
    done = bundle_cli.validate(_scratch_bundle(tmp_path), "-t", _NO_SUCH_TARGET)
    assert done.returncode, "the CLI accepted a target the scratch bundle does not declare"
    assert done.stderr.strip(), "the CLI refused the target and said nothing on stderr"
    matched = _matched(done.stderr)
    assert not matched, (
        f"{matched} matches the stderr of a BUNDLE error, so the rendering arms would skip "
        "over a bundle that had stopped validating and read it as a box with no token -- "
        "which is the probe that was swallowed once already"
    )


def test_a_declaration_this_reader_cannot_resolve_is_refused_rather_than_skipped():
    """THE READER'S OWN FAILURE ARM: every way the tuple can stop being readable.

    A rename, a second binding, a value assembled at run time, and a value that is not a
    tuple. Each would otherwise leave the arms above holding a property of nothing, and the
    green would look exactly like today's."""
    for source in (
        "_SOMETHING_ELSE = ('a',)\n",
        f"{_TUPLE_NAME} = ('a',)\n{_TUPLE_NAME} = ('b',)\n",
        f"{_TUPLE_NAME} = tuple(_parts)\n",
        f"{_TUPLE_NAME} = ['a']\n",
    ):
        with pytest.raises(AssertionError):
            _declared(source, _TUPLE_NAME)


def test_the_reader_resolves_the_declaration_that_is_actually_on_disk():
    """AND THE GREEN SIDE OF IT, because an arm that only reddens has not been measured.

    The live tuple is read from the declaring module, not built here: this is what turns
    `_declared`'s refusals above from a unit test of their own strings into a claim about
    this repository."""
    declared = _signatures()
    assert declared, _EMPTY_TUPLE
    assert all(isinstance(signature, str) for signature in declared), declared
