"""The Data Vault 2.0 business-key hash standard (master design spec, section 4.2):
SHA-256 over `trim`, `upper-case`, an explicit NULL token, and a fixed `||`
delimiter between components. Everything downstream of this task -- every hub, link
and satellite this phase and the next add -- keys on `hash_key` returning the same
digest for the same business key forever, so a change to the recipe below is a
re-keying of the whole vault and must never happen by accident.

PURE AND SPARK-FREE, LIKE `opl.bronze.registry`'S REGISTRY GUARDS, AND FOR THE SAME
REASON: `hashlib` is the standard library and nothing here imports `opl.config` or
anything that could drag pyspark in behind it. The extraction scripts run
off-Databricks with no Spark installed at all -- pyspark is deliberately the
optional `spark` extra, not a core dependency, because installing it into
serverless breaks the wheel install -- and a hash standard usable only inside a
Spark session could not be unit-tested without one either.
`tests/vault/test_hashing.py::test_the_module_is_pure_and_spark_free` holds this
down as a property, in a subprocess with pyspark import-blocked, the same way the
registry's equivalent test does.

THE ENCODING FORMAT, stated once here because every other decision in this module
follows from it. Each component of a business key encodes to one of four forms
before the components join on `||`:

    N                    -- the component is Python `None`
    E                    -- the component is the empty string `""`
    W                    -- the component is non-empty but entirely whitespace
    S<len(norm)>:<norm>  -- anything else, `norm` = `component.strip().upper()`

WHY FOUR BRANCHES AND NOT ONE "trim-then-hash": trim's whole job is to make an
incidentally-padded business key ("  ABC  ") hash identically to the same key read
clean ("ABC"), and that collapsing is correct there -- both really are the same
key. But blindly stripping every component the same way collapses `""` and `"   "`
onto the SAME empty string too, and those are not the same key: bronze carries
12,824 real rows where `cpf_cnpj_socio` is NULL, and an empty string or a
whitespace-only string is a distinct, defensive edge case from NULL and from each
other. `N`, `E` and `W` are disjoint single-character branches so none of the three
can ever produce the same encoded segment, while `S<len>:<norm>` still lets real
content collapse under trim exactly as intended -- the two requirements ("collapse
incidental padding" and "never collapse NULL/empty/whitespace into each other")
looked like one rule and are actually two, and splitting the degenerate cases out
is what lets both hold at once.

THE NULL TOKEN IS UNFORGEABLE BY TYPE, NOT BY THE OBSCURITY OF ITS SPELLING. `N` is
selected by `component is None`, a Python type check evaluated before any string
comparison runs -- so no string a source column could ever hold, INCLUDING the
literal text `"N"`, can select that branch; `"N"` as a real business key encodes to
`S1:N` instead. A magic-string sentinel (`"^^"`, a NUL byte) would only be
forgeable-in-practice, and the brief asks for a token no legitimate source value
COULD forge, not merely one that happens not to appear in the RFB's data today.

THE DELIMITER-COLLISION DECISION, AND THE TWO ALTERNATIVES REJECTED. `["a||b",
"c"]` and `["a", "b||c"]` both join, on the delimiter alone, to the literal text
`a||b||c` -- indistinguishable by any hash over the joined string. Three ways to
close it:

  1. ESCAPE `||` within a component (e.g. spell it `|\\|` and `|` as `\\|`).
     Rejected: an escape scheme needs an escape character too, which then needs
     its own escaping, and a partially-wrong escaping bug is exactly the kind of
     defect that is silent until two keys happen to collide -- worse, it is
     REVERSIBLE-LOOKING while being wrong, which is the specific shape of bug this
     module exists to make impossible rather than merely unlikely.
  2. REFUSE a component containing `||`. Rejected: it turns an encoding decision
     into a runtime failure on live data, and there is no argument that no future
     business key hashed through this module will ever legitimately contain two
     adjacent pipes -- refusing trades a collision risk for an availability risk,
     for a case the format can instead make simply not arise.
  3. LENGTH-PREFIX each component with the exact character count of its own
     normalised content: `S<len(norm)>:<norm>`. CHOSEN. Decoding is
     unambiguous by construction -- read the tag, then (for `S`) read digits up
     to `:`, then read exactly that many characters as the component, then expect
     `||` or end-of-string -- so two different component LISTS can never produce
     the same encoded string, GIVEN THE SAME NORMALISED CONTENT for each component
     (case-folding itself is a deliberate many-to-one collapse -- see below -- so
     this is a claim about the format's join, not about `str.upper()`).
     `["a||b", "c"]` encodes to `S4:A||B||S1:C` (component one is the 4-character
     string `A||B`) and `["a", "b||c"]` to `S1:A||S4:B||C` -- different strings on
     sight, and provably so for any input, not merely the ones tested.

The length prefix is taken from the NORMALISED (trimmed, upper-cased) string, not
the raw one -- taking it from the raw string would let two components with
different padding but the same trimmed content encode to different byte counts
mid-string, which breaks the same unambiguous-decoding argument this format relies
on for its collision-freedom. That constraint is also why `N`/`E`/`W` carry no
length prefix of their own: each is already a complete, one-character segment with
nothing following it to misalign.

CASE-FOLDING IS A DELIBERATE MANY-TO-ONE COLLAPSE, NOT AN INJECTIVE MAP, AND THE
FORMAT'S COLLISION-FREEDOM CLAIM ABOVE IS SCOPED AROUND THAT. `str.upper()` is not
one-to-one: `"ss".upper() == "SS" == "ß".upper()` and `"fi".upper() == "FI" ==
"ﬁ".upper()` (U+FB01 LATIN SMALL LIGATURE FI), so `hash_key(["ss"]) ==
hash_key(["\xdf"])`. This is the SAME intended collapse as `hash_key(["abc"]) ==
hash_key(["ABC"])` -- two spellings of what the business rule treats as one key --
not a gap in the length-prefix argument, which is about component LISTS never
colliding for a FIXED normalisation, not about `upper()` being injective.

NO UNICODE NORMAL FORM IS APPLIED, AND THAT IS A STATED GAP, NOT A SILENT ONE.
`"JOSÉ"` spelled NFC (`É` as one code point) and the same name spelled NFD (`E`
plus a combining acute accent) are visually identical and semantically the same
business key, but `_encode_component` does no `unicodedata.normalize` step, so
they encode to different strings and `hash_key` returns two different digests for
them -- a SPLIT, the opposite failure from the ss/ß merge above. Every business key
this module hashes today is ASCII, so this is not load-bearing yet, but the module
takes no normal-form stance and any caller depending on NFC/NFD equivalence would
be wrong to assume one.

CNPJ IS HANDLED SEPARATELY, BY `zero_pad_cnpj`, NOT BY THIS ENCODING. Zero-padding
a business-key COMPONENT before it is hashed is a decision about that component's
canonical form, not about how components join -- so it is a preparation step a
caller applies to a `cnpj_basico`/`cnpj_ordem`/`cnpj_dv` value before passing the
result to `hash_key`, exactly as it would apply any other column-specific cleanup."""
from __future__ import annotations

import hashlib
from collections.abc import Sequence

# The fixed delimiter the spec names. Never derived, never configurable: a
# per-caller delimiter would make the SAME business key hash differently
# depending on who called `hash_key`, which defeats the point of a standard.
DELIMITER = "||"

# The four encoding tags, named rather than inlined as bare string literals at
# each call site, so the module docstring's "N / E / W / S<len>:<norm>" table has
# one place that is provably the truth: `test_a_pinned_digest_for_a_single_null_
# component` asserts `NULL_TOKEN == "N"` against the same constant this module
# hashes with, so the docstring and the behaviour cannot drift apart silently.
NULL_TOKEN = "N"
EMPTY_TOKEN = "E"
WHITESPACE_TOKEN = "W"
_STRING_TAG = "S"


def _encode_component(component: str | None) -> str:
    """One business-key component, encoded per the four-branch format above.

    Branch order is significant for readability, not for correctness -- `is
    None`, then `== ""`, then "strips to empty", then everything else -- each
    check only has to be exact against ITS OWN branch, because the four are
    disjoint by construction (see the module docstring's collision argument)."""
    if component is None:
        return NULL_TOKEN
    if component == "":
        return EMPTY_TOKEN
    normalised = component.strip().upper()
    if normalised == "":
        return WHITESPACE_TOKEN
    return f"{_STRING_TAG}{len(normalised)}:{normalised}"


def hash_key(components: Sequence[str | None]) -> str:
    """The business-key hash: SHA-256 over the `||`-joined encoding of
    `components`, each normalised and NULL-safe per this module's format.

    Returns the lower-case hex digest -- 64 characters, the form every hub/link
    key comparison in this vault will compare against. Encoded as UTF-8 before
    hashing: `hashlib.sha256` takes bytes, and UTF-8 is the one encoding that
    round-trips every character this module's callers pass through it, RFB
    accented names included.

    REFUSES A BARE `str`, ON PURPOSE. `str` satisfies `Sequence[str]` structurally,
    so a type checker will not catch `hash_key("AB")` where the caller meant
    `hash_key(["AB"])` -- and the two are NOT the same call: iterating a `str`
    walks its characters, so `hash_key("AB")` hashes the two-COMPONENT list
    `["A", "B"]`, not the one-component key `"AB"`, and returns a plausible
    64-character digest for a completely different business key. Found by
    adversarial review, the same review that found the empty-`components` gap
    below -- and the same argument applies with more force here: cheap to refuse
    now, and a re-keying of the whole vault once a loader has keyed on the wrong
    digest.

    REFUSES AN EMPTY `components`, ON PURPOSE. A business key with zero
    components is not a business key -- it is a caller that built its column
    list wrong, and without this check the mistake is invisible: `hash_key([])`
    would otherwise return `hashlib.sha256(b"").hexdigest()`, the well-known
    digest of the empty string, a perfectly ordinary-looking 64 characters that
    every such caller would silently share. Found by review before any hub or
    satellite called this, which is the only time to close it for free -- once a
    loader exists, an empty column list starts looking like "this hub has no
    business key" rather than "this call is a bug"."""
    if isinstance(components, str):
        raise TypeError(
            f"hash_key() received a bare str {components!r} -- a str is a "
            "Sequence[str] structurally, so this call would hash its individual "
            "characters as separate components; pass a one-element list instead, "
            f"e.g. hash_key([{components!r}])"
        )
    if not components:
        raise ValueError(
            "hash_key() received zero components -- a business key must have at "
            "least one, or every caller that passes an empty list would silently "
            f"share the single digest {hashlib.sha256(b'').hexdigest()!r}"
        )
    encoded = DELIMITER.join(_encode_component(component) for component in components)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def zero_pad_cnpj(value: str, *, width: int) -> str:
    """Left-pad `value` to `width` with `"0"`, refusing rather than truncating an
    overlong value. Never numeric: no `int()` conversion, anywhere.

    DEFENSIVE, NOT CORRECTIVE, per the brief's own measurement: `cnpj_basico` is
    already exactly 8/8 characters across 72.3M establishment rows and 69.1M
    company rows, so the interesting behaviours are that padding is a NO-OP on an
    already-correct value, and that a too-long value is REFUSED rather than
    silently truncated -- a truncation would map two different real CNPJs onto
    the same padded string and collide two different companies onto one hash
    key, the worst failure this module can produce.

    `rjust`, not `zfill`: THE ONLY DIFFERENCE BETWEEN THE TWO is a leading sign
    character -- `zfill` special-cases a leading `+`/`-` and moves it ahead of the
    padding (`"-1".zfill(8) == "-0000001"`), while `rjust` pads on the left with
    no such special case (`"-1".rjust(8, "0") == "000000-1"`). No CNPJ, digit-only
    or alphanumeric, carries a sign character, so no input this function is
    called on can tell the two apart today -- `rjust` is the DEFENSIVE preference
    (no numeric-flavoured behaviour to carry at all), not a fix for a case that
    can currently occur, and is not proven by a test that only exercises
    sign-free input.

    REFUSES A NON-POSITIVE `width`, ON PURPOSE: a caller-supplied `width` of zero
    or less is not a valid fixed-width column, so `zero_pad_cnpj("", width=0)`
    silently returning `""` would hide the same class of caller bug the overlong-
    value refusal above exists to catch."""
    if width <= 0:
        raise ValueError(f"width must be a positive integer, got {width!r}")
    if len(value) > width:
        raise ValueError(
            f"{value!r} is {len(value)} characters, over its width {width} -- "
            "refusing to truncate: a truncated value could collide with another "
            "CNPJ's true prefix and merge two companies onto one business key"
        )
    return value.rjust(width, "0")
