# tests/test_iac_terraform.py
"""WHAT THE TERRAFORM UNDER `iac/` MAY DECLARE, AND WHAT ITS CI CHECK MAY RUN.

`iac/` exists because a Databricks Asset Bundle cannot declare a group, and the group the
socios mask predicate names has therefore always been a hand-made workspace object that
nothing under version control described. `scripts/rebuild_pii_reader_sp.py` says so from
the other side: it refuses with "no workspace group named <name>; create it first".

THE ALLOW-LIST IS THE SAME SHAPE AND THE SAME ARGUMENT AS `_DECLARABLE` IN
`tests/test_bundle_resource_allowlist.py`, and it is here for the same reason that one is
there. A Terraform layer over a workspace the bundle already deploys into is a second
writer over one set of objects, and the way that arrives is one resource type at a time,
each individually reasonable. An allow-list makes the next one a decision somebody types
out. `iac/README.md` carries which types are refused and why -- Unity Catalog grants in
particular, whose refusal rests on the provider's own documented semantics rather than on
a recollection of them.

WHY THE CI JOB IS LOCKED HERE TOO, which is the arm this module would be worth least
without. Measured in `iac/` with `DATABRICKS_HOST` and `DATABRICKS_TOKEN` unset and
`DATABRICKS_CONFIG_FILE` pointed at a path that does not exist, `terraform plan` over the
committed defaults exits 0 and reports `Plan: 1 to add`. Nothing in the configuration
reads while the member list is empty, so the plan neither authenticates nor looks: it
prints that line whether or not the object is already there, and nothing in the run could
tell those apart. Name one member and the same command exits 1 on `cannot
configure default credentials`. A `plan` step in CI would therefore either fail for want
of a secret or pass without meaning, and which one it did would depend on the shape the
configuration happened to be in. `fmt -check` and `validate` read nothing, so they mean
the same thing on a runner as on the box that wrote them.

WHAT THIS MODULE DOES NOT DO: run Terraform. The CI job that executes these arms installs
none -- only the `terraform` job does, and that job runs no pytest -- so shelling out to
one would make every arm here SKIP in exactly the place they are needed. The parsing is
textual and its limits are stated at `_declared_blocks`.
"""
from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[1]
_WORKFLOW = _REPO / ".github" / "workflows" / "ci.yml"
_IAC = "iac"
_README = _REPO / _IAC / "README.md"

# THE ONLY THING `iac/` MAY DECLARE, AS (block kind, type) PAIRS. `data` is in the same
# tuple as `resource` deliberately: a data source is not a resource and creates nothing,
# but it is still a way for this directory to reach into the workspace, and leaving it
# out of the allow-list would leave half the configuration unwatched by a lock whose
# docstring claims to cover it.
_DECLARABLE = (
    ("resource", "databricks_group"),
    ("resource", "databricks_group_member"),
    ("data", "databricks_user"),
)

# `resource "type" "name" {` at the start of a line, which is what `terraform fmt`
# produces and what `terraform fmt -check` in CI holds the files to.
_BLOCK = re.compile(r'^(resource|data)\s+"([A-Za-z0-9_]+)"\s+"[A-Za-z0-9_]+"\s*\{', re.M)

# The identity constant on the Python side of the same name.
_GROUP_CONSTANT = "PII_READER_GROUP"

# The two ways a workflow step spells "the repository root". A SET OF SPELLINGS OF ONE
# DIRECTORY, not a claim about a population: what the arm below needs is that the Format
# step is not confined to `iac/`, and the root is the only directory guaranteed to be an
# ancestor of everything `git ls-files "*.tf"` can return.
_WORKFLOW_ROOT = (".", "${{ github.workspace }}")

# Literals that must never reach a committed file in this repository, as (name, pattern).
# NAMED SHAPES RATHER THAN NAMED VALUES: a test that grepped for this operator's user
# name would have to carry this operator's user name, which is the thing it exists to
# keep out of the tree.
_FORBIDDEN = (
    ("a Databricks personal access token", re.compile(r"dapi[0-9a-f]{8,}")),
    ("a workspace URL", re.compile(
        r"https?://[A-Za-z0-9.-]*\.(cloud\.databricks\.com|azuredatabricks\.net"
        r"|gcp\.databricks\.com)")),
    ("an organization id", re.compile(r"\bo=\d{6,}\b")),
    ("an email address, which is a workspace user name",
     re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}")),
    ("a home directory path, which carries an OS user name",
     re.compile(r"([A-Za-z]:[\\/]Users[\\/]|/home/|/Users/)[A-Za-z0-9._-]+")),
)


def _tracked(*patterns: str) -> tuple[str, ...]:
    """Every tracked path matching `patterns`, as git spells it.

    `check=True` so a git that is absent or angry FAILS rather than handing back an
    empty listing that every sweep below would then pass over -- which is the shape
    `tests/test_size_caps.py` guards from the other side and the reason its last
    assertion exists."""
    listed = subprocess.run(
        ["git", "ls-files", *patterns],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return tuple(sorted(path for path in listed.stdout.split("\n") if path.strip()))


def _terraform_files() -> tuple[str, ...]:
    """Tracked Terraform sources, wherever they are, not just under `iac/`.

    A `.tf` outside this directory would be a second Terraform nobody decided on, so the
    sweep is by suffix over the whole tree rather than by directory."""
    return _tracked("*.tf", "*.tf.json")


def _declared_blocks() -> list[tuple[str, str, str]]:
    """(path, block kind, type) for every `resource`/`data` block in a tracked `.tf`.

    TEXTUAL, AND HERE IS WHAT THAT DOES NOT REACH: a block written in `.tf.json` (the
    JSON syntax Terraform also accepts, swept above but not parsed by this regex), one
    produced by a module this configuration called, and one indented rather than
    starting its own line. `terraform fmt -check` unindents top-level blocks, so the
    third is held closed by a different check rather than by this one -- and that check
    has to sweep as widely as this function does, or the two together read nothing at all
    over a `.tf` outside `iac/`. That is why CI runs the Format step from the repository
    root while the rest of its job runs in `iac/`, and why the arm below locks it there."""
    return [
        (path, kind, kind_type)
        for path in _terraform_files()
        for kind, kind_type in _BLOCK.findall((_REPO / path).read_text(encoding="utf-8"))
    ]


def test_the_terraform_only_declares_the_identity_types_the_allow_list_names():
    """A third kind of object arriving in `iac/` is a decision, not a diff.

    The failure this refuses is not a bad resource type; it is Terraform quietly growing
    into the objects the bundle already deploys, which is a second engine over one set of
    objects and is what `iac/README.md` argues against at length."""
    declared = _declared_blocks()
    faults = sorted(
        f"{path}: {kind} {kind_type!r}"
        for path, kind, kind_type in declared
        if (kind, kind_type) not in _DECLARABLE
    )
    assert not faults, (
        f"{faults} are declared under Terraform and are not on the allow-list "
        f"{_DECLARABLE}. Adding one means arguing for it in `iac/README.md` first: the "
        "decision that put identity here declined Unity Catalog grants on the provider's "
        "own documented semantics, and the reversal condition is recorded there."
    )


def test_the_sweep_is_reading_the_terraform_and_not_an_empty_listing():
    """GUARD THE GUARD, because the assertion above passes over zero files.

    A pathspec that stopped matching, a directory renamed, or a `.tf` that was never
    `git add`-ed would each leave the allow-list reporting green over nothing. Presence
    of the declared identity pair is the floor -- an exact count here would be a claim
    about the configuration that goes stale on its next line."""
    declared = _declared_blocks()
    pairs = {(kind, kind_type) for _, kind, kind_type in declared}
    assert declared, (
        "no `resource` or `data` block was found in any tracked Terraform file. `git "
        "ls-files` is blind to a file that has never been added -- `git add -N` it "
        "before trusting this."
    )
    assert ("resource", "databricks_group") in pairs, pairs
    assert ("resource", "databricks_group_member") in pairs, pairs


def _tf_default(variable: str) -> str:
    """The literal `default` of one `variable` block in `iac/variables.tf`."""
    text = (_REPO / _IAC / "variables.tf").read_text(encoding="utf-8")
    start = text.index(f'variable "{variable}"')
    rest = text[start:]
    end = rest.find('\nvariable "')
    block = rest if end < 0 else rest[:end]
    found = re.search(r'^\s*default\s*=\s*"([^"]*)"', block, re.M)
    assert found, f"`{variable}` in iac/variables.tf declares no string default"
    return found.group(1)


def _python_group_literals() -> dict[str, str]:
    """Every module-level `PII_READER_GROUP = "..."` in tracked Python, path -> value.

    DERIVED RATHER THAN NAMED. The Python side spells this constant in more than one
    module, and a test that named the modules would be a written-down set of sites of
    exactly the kind this repository keeps publishing short."""
    found: dict[str, str] = {}
    for path in _tracked("*.py"):
        source = (_REPO / path).read_text(encoding="utf-8")
        if _GROUP_CONSTANT not in source:
            continue
        for node in ast.parse(source, filename=path).body:
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
                continue
            if any(getattr(t, "id", None) == _GROUP_CONSTANT for t in node.targets):
                found[path] = node.value.value
    return found


def test_the_group_terraform_declares_is_the_principal_the_mask_predicate_names():
    """The IaC and the SQL predicate are two spellings of one workspace object.

    `MASK_PREDICATE` is built as `is_member('<PII_READER_GROUP>')`, so a group declared
    here under a different name is a group nothing reads -- the mask would fail closed,
    which is safe, and the declaration would be decoration, which is worse than absent
    because it looks like coverage."""
    declared = _tf_default("pii_readers_group_name")
    python = _python_group_literals()
    assert python, (
        f"no module-level `{_GROUP_CONSTANT}` was found in tracked Python; the "
        "comparison below would hold vacuously"
    )
    disagreeing = sorted(f"{path}={value!r}" for path, value in python.items() if value != declared)
    assert not disagreeing, (
        f"iac/variables.tf declares the group as {declared!r} and {disagreeing} disagree. "
        "Both sides name one workspace group; move them together or the mask predicate "
        "reads a group Terraform does not declare."
    )


def test_no_committed_terraform_artefact_carries_a_host_token_or_user_name():
    """The leak site, swept as the files rather than as the suffixes.

    EVERYTHING TRACKED UNDER `iac/` is in scope, not only `.tf`: the README, the example
    tfvars and the provider lock are as committed as the configuration is, and a host
    pasted into a README is as public as one pasted into HCL. A real `.tfvars` is
    git-ignored and therefore outside this sweep by construction, which is the intended
    division -- the operator's own file may hold what this tree may not."""
    swept = sorted(set(_tracked(_IAC)) | set(_terraform_files()))
    assert "iac/identity.tf" in swept, swept
    faults = sorted(
        f"{path}: {name} -- {found.group(0)[:24]!r}"
        for path in swept
        for name, pattern in _FORBIDDEN
        for found in [pattern.search((_REPO / path).read_text(encoding="utf-8"))]
        if found
    )
    assert not faults, (
        f"{faults}. This repository is public. Publish the SHAPE -- `<user>@<domain>`, "
        "`<workspace-host>`, `<group-id>` -- and keep the literal in the git-ignored "
        "`.env` or `terraform.tfvars` the README points at."
    )


def _readme_ci_commands() -> tuple[str, ...]:
    """The commands the README's "What CI runs" fenced block prints, one per line.

    A trailing `#` comment is an annotation and is cut; no command in that block contains
    a `#` for any other reason, and one that did would have to be matched differently."""
    text = _README.read_text(encoding="utf-8")
    start = text.index("## What CI runs")
    fence = text.index("```bash", start) + len("```bash")
    body = text[fence:text.index("```", fence)]
    return tuple(
        stripped
        for line in body.strip().split("\n")
        for stripped in [line.split("#")[0].strip()]
        if stripped
    )


def test_the_readme_block_that_says_what_ci_runs_is_what_ci_runs():
    """A README THAT PRINTS A COMMAND CI DOES NOT RUN IS READ AS THE MEASURED THING.

    It printed `terraform init -backend=false` while the job ran that plus `-input=false`,
    and the drift survived because nothing compared the two. The comparison is by whole
    line and not by substring, deliberately: the wrong text WAS a substring of the right
    one, so a substring check would have reported green over exactly this defect.

    ONE DIRECTION, AND WHICH ONE IS PART OF THE CLAIM. `missing` is quantified over
    `commands` alone, so what is held is EVERY PRINTED COMMAND IS RUN and not its
    converse: a step added to the `terraform` job and never written into the README
    leaves this green, by construction rather than for want of a case. Measured -- both
    sides are the same three lines today, and a fourth `run` line on the job side alone
    leaves `missing` empty.

    THAT IS THE DIRECTION THE DEFECT ARRIVED IN, and the converse is not free: `runs` is
    every `run` line of every step, so demanding the README print all of them would make
    the block a transcript of the shell rather than the shortcut it is written as. The
    limit is named here rather than closed, because closing it is a decision about what
    belongs in that block and it is owed an argument, not an assertion."""
    commands = _readme_ci_commands()
    assert commands, "`iac/README.md` prints no commands under `## What CI runs`"
    runs = {
        line.strip()
        for step in _terraform_job()["steps"]
        for line in step.get("run", "").strip().split("\n")
        if line.strip()
    }
    missing = sorted(c for c in commands if c not in runs)
    assert not missing, (
        f"{missing} are printed in `iac/README.md` under `## What CI runs` and are run by "
        f"no step of the `terraform` job, which runs {sorted(runs)}. Move them together: "
        "the block is a shortcut for the workflow and is read as a measurement of it."
    )


def _terraform_job() -> dict:
    spec = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    job = spec["jobs"].get("terraform")
    assert job, "`.github/workflows/ci.yml` declares no `terraform` job"
    return job


def _strings(node) -> list[str]:
    """Every string anywhere inside a parsed YAML node, KEYS INCLUDED.

    THE KEYS WERE MISSING AND THAT MADE THE ARM BELOW BLIND TO THE COMMONEST SPELLING
    OF THE THING IT REFUSES. A credential reaches a job as `DATABRICKS_TOKEN: <value>`,
    where the name that matters is the KEY; collecting only `node.values()` left
    `env: {DATABRICKS_TOKEN: not-a-real-token}` planted in the `terraform` job with
    this module still reporting `7 passed`. Measured by that plant, in F8 T5, by the
    commit that added the one job in this file entitled to hold that credential."""
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [str(key) for key in node] + [
            s for value in node.values() for s in _strings(value)
        ]
    if isinstance(node, list):
        return [s for value in node for s in _strings(value)]
    return []


def test_the_terraform_ci_job_validates_credential_free_and_never_plans():
    """BOTH HALVES, because either alone reports green over the failure it exists to stop.

    Deleting the job satisfies "no plan step" perfectly. Adding a plan step satisfies
    "validate runs" perfectly. The measurement in this module's header is why the second
    half is not pedantry: a credential-free `plan` over this configuration exits 0 and
    reports `1 to add` without having looked at anything."""
    job = _terraform_job()
    runs = " ; ".join(step.get("run", "") for step in job["steps"])
    for wanted in ("terraform fmt -check", "terraform validate"):
        assert wanted in runs, f"the `terraform` job does not run `{wanted}`: {runs!r}"
    for refused in ("terraform plan", "terraform apply", "terraform destroy"):
        assert refused not in runs, (
            f"the `terraform` job runs `{refused}`. A plan needs credentials only when "
            "the configuration reads something, so in CI it either fails for want of a "
            "secret or passes without meaning. See iac/README.md."
        )
    reaching = sorted(s for s in _strings(job) if "secrets." in s or "DATABRICKS_" in s)
    assert not reaching, (
        f"{reaching}: the `terraform` job names a credential. It is credential-free by "
        "design, and a gated deploy belongs in a job of its own."
    )


def test_the_format_step_sweeps_the_whole_tree_and_not_only_the_directory_it_runs_beside():
    """THE BACKSTOP `_declared_blocks` NAMES MUST REACH AS FAR AS `_declared_blocks` DOES.

    That regex cannot read an indented `resource` block and says so; what holds the case
    closed is `terraform fmt -check`, which unindents. But the sweep is tree-wide, so a
    `fmt -check` confined to `iac/` leaves a `.tf` elsewhere unread by BOTH of them.
    Measured on an indented block in a `.tf` outside `iac/`: `fmt -check -recursive` exits
    0 run from `iac/` and 3 run from the root. The job's own default is
    `working-directory: iac`, so the Format step carries an override that reads as
    redundant and is not -- this arm is what makes deleting it a red build."""
    job = _terraform_job()
    formatting = [s for s in job["steps"] if "terraform fmt" in s.get("run", "")]
    assert formatting, "the `terraform` job runs no `terraform fmt` step"
    for step in formatting:
        assert "-recursive" in step["run"], (
            f"`{step['run']}` is not recursive, so it reads one directory. "
            "`_declared_blocks` sweeps the tree."
        )
        assert step.get("working-directory") in _WORKFLOW_ROOT, (
            f"the `terraform fmt` step runs in {step.get('working-directory')!r}. It has "
            "to run from the repository root: `_declared_blocks` sweeps `*.tf` tree-wide "
            "and names `fmt -check` as what holds an indented block closed, and a check "
            f"confined to a subdirectory does not cover that sweep. {_WORKFLOW_ROOT} are "
            "the spellings of the root this arm accepts."
        )
