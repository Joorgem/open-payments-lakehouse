"""The business-key hash standard, pinned. Everything downstream of Task 1 -- every
hub, link and satellite this phase and the next add -- keys on `hash_key` returning
the SAME digest for the SAME business key forever, so this file's job is narrower
than "the function works": it is to make an accidental change to the standard itself
(the delimiter, the NULL handling, the case/trim rule) impossible to land quietly.

WHY THE EXPECTED DIGESTS ARE LITERALS, NOT COMPUTED HERE. A test that builds its
expectation by calling `hash_key` (or by re-running `_encode_component`) passes
through any change to either -- the encoding could invert its own logic and this
file would still be green, because both sides of the assertion moved together. Every
`hash_key(...) ==` below compares against a hex string typed into this file, computed
once, independently, with a bare `hashlib.sha256` over a hand-built encoded string
that mirrors the documented format in `opl.vault.hashing`'s module docstring. A
change to the delimiter, the tag characters, or the trim/case rule turns these red
without touching this file, which is the point.

WHAT THIS FILE DOES NOT TEST: the WIRING of a satellite's business key to
`hash_key` -- there is no satellite yet. Task 1 is the standard alone."""
from __future__ import annotations

import subprocess
import sys
import textwrap
import unicodedata

import pytest

from opl.vault.hashing import (
    EMPTY_TOKEN,
    NULL_TOKEN,
    WHITESPACE_TOKEN,
    hash_key,
    zero_pad_cnpj,
)


def test_a_pinned_digest_for_two_ordinary_components():
    """The literal every future refactor of `hash_key` has to keep producing.

    Input: ["ABC", "123"]. Per the documented format, each string component
    normalises to `S<len(normalised)>:<normalised>` and components join on `||`,
    giving the encoded string `S3:ABC||S3:123` -- hashed independently to produce
    the literal below (`python3 -c "import hashlib;
    print(hashlib.sha256(b'S3:ABC||S3:123').hexdigest())"`)."""
    assert hash_key(["ABC", "123"]) == (
        "cee7673e28fd1b569ccfac50fd30cc2d7cf5dace5996c0e15a3d95d4ce7444fd"
    )


def test_a_pinned_digest_for_a_single_null_component():
    """`hash_key([None])` encodes to the bare `NULL_TOKEN` ("N"), hashed alone."""
    assert hash_key([None]) == (
        "8ce86a6ae65d3692e7305e2c58ac62eebd97d3d943e093f577da25c36988246b"
    )
    assert NULL_TOKEN == "N"


def test_null_empty_string_and_whitespace_are_three_distinct_digests():
    """THE PROPERTY THE BRIEF NAMES DIRECTLY, asserted pairwise so no two of the
    three can share a hiding place.

    These are not equivalent inputs merely dressed up as different literals: a
    12,824-row real column in bronze carries `None` for a foreign partner with no
    CPF/CNPJ; an empty string and a whitespace-only string are the defensive edge
    cases a careless implementation collapses into either NULL or each other. A
    NAIVE `trim`-then-hash would make `""` and `"   "` collide, since both trim to
    the same empty content -- and that collision is exactly what `_encode_component`
    exists to prevent for THIS one case, without weakening trim for real content
    (see `test_leading_trailing_whitespace_collides_with_the_trimmed_value` below,
    which is the case where collapsing to the same digest is correct).

    THE NULL TOKEN IS UNFORGEABLE BY TYPE, NOT BY LUCK OF SPELLING: `_encode_component`
    selects the `N` branch on `component is None`, a Python type check, never on
    string equality against a sentinel. No string a source column could ever hold
    -- not `"N"` itself -- can select that branch, because the branch is chosen
    before any string comparison happens. A magic-string sentinel (`"^^"`, `"\\x00"`)
    would only be forgeable-in-practice; this is forgeable-in-no-practice."""
    null_digest = hash_key([None])
    empty_digest = hash_key([""])
    whitespace_digest = hash_key(["   "])

    assert null_digest != empty_digest
    assert null_digest != whitespace_digest
    assert empty_digest != whitespace_digest

    assert empty_digest == (
        "a9f51566bd6705f7ea6ad54bb9deb449f795582d6529a0e22207b8981233ec58"
    )
    assert whitespace_digest == (
        "fcb5f40df9be6bae66c1d77a6c15968866a9e6cbd7314ca432b019d17392f6f4"
    )
    assert EMPTY_TOKEN == "E"
    assert WHITESPACE_TOKEN == "W"


def test_leading_trailing_whitespace_collides_with_the_trimmed_value():
    """The case where COLLIDING is the correct behaviour, and the counterpart to
    the test above: trim exists precisely so that a business key read with
    incidental padding (a fixed-width extract, an operator's stray space) hashes
    identically to the same key read clean. `"  abc  "` and `"ABC"` both normalise
    to `S3:ABC` -- one component, real content, no whitespace-only branch."""
    assert hash_key(["  abc  "]) == hash_key(["ABC"])
    assert hash_key(["  abc  "]) == (
        "e49c1f2c83b6089f28eca39d890c959ec2e6811368d7371caad5b98074fd53dc"
    )


def test_case_is_normalised_independently_of_trim():
    """Lower, upper and mixed case, all trimmed the same amount, must agree."""
    assert hash_key(["abc"]) == hash_key(["ABC"])
    assert hash_key(["AbC"]) == hash_key(["ABC"])


def test_a_component_containing_the_delimiter_does_not_collide():
    """THE DELIMITER-COLLISION EDGE CASE, decided by length-prefixing rather than
    escaping or refusal -- see `opl.vault.hashing`'s module docstring for the
    reasoning and the two rejected alternatives.

    `["a||b", "c"]` and `["a", "b||c"]` both join, naively, to the literal string
    `a||b||c` -- indistinguishable by a hash over the joined text alone. Each
    component here is length-prefixed BEFORE the join, so the first list encodes
    to `S4:A||B||S1:C` (component one is 4 characters, "A||B") and the second to
    `S1:A||S4:B||C` (component two is 4 characters, "B||C") -- different strings,
    different digests, pinned below so the property survives a future refactor of
    the join itself."""
    first = hash_key(["a||b", "c"])
    second = hash_key(["a", "b||c"])
    assert first != second
    assert first == "1b7639968c10107344f0c5158348d5239c04325e7b7f657011ce4e031424b2b8"
    assert second == "f6ff5027ae8ba6d38bdf412e44d5aef1f7e120972183b6a451c7bb6ed38b3e56"


def test_a_masked_cpf_does_not_collide_with_a_shorter_mask():
    """THE REAL BUSINESS KEY named in the brief: the RFB masks a partner's CPF to
    `***NNNNNN**` (11 characters). `***000000**` and `***0**` are both plausible
    masked values and must not collide merely because both begin and end with
    asterisks -- length-prefixing makes the difference in asterisk-run length
    part of the encoded string itself, not something a fixed-width hash over raw
    concatenation could lose."""
    full = hash_key(["***000000**"])
    short = hash_key(["***0**"])
    assert full != short
    assert full == "7464e2e8a349240d3c0c259fbdf1412513b483c6746bed5f6924a9235bb90103"
    assert short == "f7f35ca410f3aa2ab7796ae837419a55a125c3c82ce531ec8eb236319853fec8"


def test_a_pinned_digest_for_an_accented_component():
    """UTF-8, NOT ASCII, IS THE ENCODING THIS MODULE HASHES WITH -- the module
    docstring names "RFB accented names" as the reason, and until this test no
    accented input was ever hashed by this suite. `"JOSÉ"` normalises to
    `S4:JOSÉ` (4 characters: J, O, S, É), independently hashed to the literal
    below."""
    assert hash_key(["JOSÉ"]) == (
        "a58625e7c1552c96910382123d9f6fa8315a5e600c2b148fa2a0822461cd86c9"
    )


def test_hash_key_applies_no_unicode_normalisation_nfc_and_nfd_split():
    """A DOCUMENTED GAP, NOT A SILENT ONE: `"JOSÉ"` spelled NFC (`É` as one code
    point) and the same name spelled NFD (`E` plus a combining acute accent) look
    identical and are the same business key semantically, but `_encode_component`
    applies no `unicodedata.normalize` step -- so the two spellings encode to
    different strings and `hash_key` returns two DIFFERENT digests for what a
    human reader would call one name. This is the opposite failure from the
    ss/ß case-folding collapse in the module docstring: a SPLIT, not a merge.
    Every business key this module hashes today is ASCII, so this is not
    load-bearing yet -- the module states this as a stance, not a promise, and
    this test pins that the two forms currently disagree."""
    nfc = unicodedata.normalize("NFC", "JOSÉ")
    nfd = unicodedata.normalize("NFD", "JOSÉ")
    assert nfc != nfd
    assert hash_key([nfc]) != hash_key([nfd])
    assert hash_key([nfc]) == (
        "a58625e7c1552c96910382123d9f6fa8315a5e600c2b148fa2a0822461cd86c9"
    )


def test_hash_key_refuses_a_bare_str_instead_of_a_one_element_list():
    """THE IMPORTANT GAP AN ADVERSARIAL REVIEW FOUND: `str` satisfies
    `Sequence[str]` structurally, so `hash_key("AB")` type-checks and RUNS --
    but iterating a `str` walks its characters, so it silently hashes the
    two-component list `["A", "B"]`, not the one-component key `"AB"`. Before
    this refusal existed, `hash_key("AB") == hash_key(["A", "B"])`, and a hub
    loader that wrote `hash_key(row.cnpj_basico)` instead of
    `hash_key([row.cnpj_basico])` would key every two-character component as
    two single-character ones with no error anywhere. This is the BEFORE/AFTER
    proof: the assertion below is exactly what would have failed to raise
    before the `isinstance(components, str)` check was added."""
    with pytest.raises(TypeError, match="bare str"):
        hash_key("AB")


def test_hash_key_refuses_an_empty_component_list():
    """THE GAP A REVIEW FOUND: `hash_key([])` joins to the empty string, which
    hashes to `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
    -- the well-known SHA-256 of `""`, a perfectly plausible-looking 64
    characters. An empty component list is not a business key with a boring
    value; it is a caller that built its column list wrong, and every such
    caller would otherwise silently collide on this one digest. Refused rather
    than defined, because there is no reading of "a business key with no
    components" that a hub or satellite loader should ever produce on purpose."""
    with pytest.raises(ValueError, match="zero components"):
        hash_key([])


def test_zero_padding_is_a_no_op_on_an_already_correct_width():
    """FACT FROM THE BRIEF: real bronze already carries `cnpj_basico` at exactly 8
    characters in all 72.3M + 69.1M measured rows, so padding is DEFENSIVE, not
    corrective -- the interesting behaviour is that it changes nothing when the
    input is already the right width, not that it can fix a short one."""
    assert zero_pad_cnpj("12345678", width=8) == "12345678"


def test_zero_padding_pads_a_short_value_on_the_left_with_zeros():
    assert zero_pad_cnpj("123", width=8) == "00000123"


def test_zero_padding_never_converts_through_a_number():
    """ALPHANUMERIC CNPJs TAKE EFFECT 2026-07-31: a future `cnpj_basico` may
    contain letters, and `int(value)` would raise on one today -- proving this
    passes is proof no numeric path is taken, not merely that letters "happen" to
    survive."""
    assert zero_pad_cnpj("1A34B678", width=8) == "1A34B678"
    assert zero_pad_cnpj("A1", width=8) == "000000A1"


def test_zero_padding_diverges_from_zfill_only_on_a_leading_sign():
    """THE ACTUAL, TESTED difference between `rjust` (chosen) and `zfill`
    (rejected): `zfill` special-cases a leading `+`/`-` and moves it ahead of the
    padding; `rjust` does not. No CNPJ carries a sign character, so this is the
    one input shape where the two functions would ever disagree -- and this test
    is what gives the `rjust`-not-`zfill` decision actual pressure, rather than
    leaving it an assertion no test could fail on."""
    assert "-1".zfill(8) == "-0000001"
    assert zero_pad_cnpj("-1", width=8) == "000000-1"
    assert zero_pad_cnpj("-1", width=8) != "-1".zfill(8)


def test_zero_padding_refuses_a_non_positive_width():
    """A `width` of zero or less is not a valid fixed-width column -- a caller
    bug, per this module's stance everywhere else that a caller bug is refused
    loudly rather than producing a boring-looking result. Without this check,
    `zero_pad_cnpj("", width=0)` would silently return `""`."""
    with pytest.raises(ValueError, match="positive integer"):
        zero_pad_cnpj("", width=0)
    with pytest.raises(ValueError, match="positive integer"):
        zero_pad_cnpj("", width=-1)


def test_zero_padding_refuses_rather_than_truncates_an_overlong_value():
    """THE FAILURE THIS MODULE MUST NEVER COMMIT: a value longer than its
    component's fixed width is refused, not silently truncated. A silent
    truncation would map two DIFFERENT real `cnpj_basico` values -- one 9
    characters, one its first-8-characters prefix -- onto the SAME padded string
    and therefore the same hash key, colliding two different companies onto one
    business key. That is the worst failure this module can produce, so it is
    refused loudly rather than repaired quietly."""
    with pytest.raises(ValueError, match="9 characters.*width 8"):
        zero_pad_cnpj("123456789", width=8)


def test_the_module_is_pure_and_spark_free():
    """THE PROPERTY, NOT ONE SPELLING OF ITS CAUSE. Modelled directly on
    `tests/bronze/test_registry_guards.py::test_the_registry_still_imports_where_
    pyspark_is_not_installed`: a subprocess with a meta-path blocker that raises
    `ImportError` for `pyspark` or any `pyspark.*` submodule, so this catches
    pyspark arriving through ANY future import this module gains, not just a
    literal `import pyspark` grepped for today.

    WHY IT MATTERS HERE SPECIFICALLY: this hashing standard is meant to be called
    from hub/link/satellite loaders, and the brief for THIS task requires it be
    usable "without a session" -- but the concrete, load-bearing reason (see
    `opl.bronze.registry`'s equivalent test) is that extraction scripts run
    off-Databricks with no Spark installed at all, and pyspark is deliberately the
    optional `spark` extra rather than a core dependency. In-process would prove
    nothing: pyspark is installed in THIS test environment and already sitting in
    `sys.modules` by the time any test runs, so only a fresh subprocess with the
    import path actually blocked demonstrates the property."""
    blocked = textwrap.dedent(
        """
        import sys

        class _NoPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(
                        "pyspark is not installed in the extraction environment"
                    )
                return None

        sys.meta_path.insert(0, _NoPyspark())
        from opl.vault.hashing import hash_key, zero_pad_cnpj
        assert len(hash_key(["12345678", None, ""])) == 64
        assert zero_pad_cnpj("1", width=8) == "00000001"
        assert "pyspark" not in sys.modules
        print("ok")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", blocked],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
