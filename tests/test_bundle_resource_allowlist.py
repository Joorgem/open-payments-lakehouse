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

WHAT IT COVERS, STATED WITH ITS EDGES, because four documents rest a safety argument on it
and a reader of those four arrives here. `bundle_docs()` parses every `*.yml` and `*.yaml`
file under `databricks/`, and every resource collection in each is held to the allowlist
in BOTH places a bundle document can declare one:

  * `resources.<kind>` at the top level;
  * `targets.<name>.resources.<kind>`.

THE SECOND IS NOT A REFINEMENT. It is where a securable would land under the PRODUCTION
target -- the one [ADR 0018] Decision 6's grounds 2 and 3 are about -- and until this
module existed the sweep read only the top level while `databricks/databricks.yml`, ADR
0008, ADR 0018 and ADR 0021 all said no securable could enter the bundle without a test
going red. Measured, not inferred: a scratch bundle declaring
`targets.prodx.resources.schemas` validates `exit=0` under CLI v1.8.0 and renders resource
kinds `['jobs', 'schemas']`.

TWO THINGS IT DOES NOT COVER, both measured on the same scratch bundle rather than
assumed:

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

import yaml
from job_yaml import BUNDLE, bundle_docs

# THE ONLY TWO RESOURCE COLLECTIONS THIS BUNDLE MAY DECLARE, AS AN ALLOWLIST RATHER THAN A
# LIST OF SECURABLES TO REFUSE. ADR 0018 Decision 6 rejected declarative governance partly
# on grounds that can fire ONLY over a securable. Enumerating the securables would be a
# third copy of a list whose existing copies already disagreed -- six object types in ADR
# 0018 against four in the documents quoting it. An allowlist needs no such list: anything
# that is not a job or a dashboard stops here, securable or not, and a new collection
# becomes a decision somebody has to type out.
#
# AND "A SECURABLE REFUSAL" IS NOT WHAT THIS IS, which is the correction the four documents
# citing it now carry. The allowlist is WIDER than the securables: it refuses `secret_scopes`
# and `sql_warehouses` too, neither of which carries `grants`, and this workspace holds real
# state of both kinds -- one secret scope, and the warehouse `databricks.yml` resolves by
# name. Declaring either would be a legitimate act this lock makes somebody argue for, not a
# hazard it exists to stop.
_DECLARABLE = ("jobs", "dashboards")


def _resource_collections(doc) -> list[tuple[str, str]]:
    """(where, kind) for EVERY place a bundle document can declare a resource collection."""
    doc = doc or {}
    found = [("resources", kind) for kind in sorted(doc.get("resources") or {})]
    for target, body in sorted((doc.get("targets") or {}).items()):
        found += [
            (f"targets.{target}.resources", kind)
            for kind in sorted((body or {}).get("resources") or {})
        ]
    return found


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

    `databricks.yml`, ADR 0008, ADR 0018 and ADR 0021 all say that what keeps Decision 6's
    second and third grounds hypothetical is MECHANICAL -- that they can fire only over a
    securable and the bundle declares none. Until F8 all four said so and nothing enforced
    it; until this module's split the enforcement read half the places a securable can be
    declared. See this file's docstring for what it still does not reach."""
    assert not _resource_faults(bundle_docs())


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. One per place a collection can be declared, because the top-level
# arm passed for months over a sweep that could not see the other place.
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
    """THE PLACE THE SWEEP COULD NOT SEE UNTIL NOW, AND THE PLACE IT MATTERS MOST.

    Before this arm a schema could sit under the production target -- exactly where ADR
    0018 Decision 6's grounds 2 and 3 fire -- with every test green, while four documents
    said no securable could enter without one going red. The target is read out of the
    committed bundle by its MODE rather than named, so this arm follows a rename."""
    document = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    assert not _resource_faults({"databricks.yml": document})
    target = _the_production_target(document)
    document["targets"][target]["resources"] = {"schemas": {"governed": {"name": "default"}}}
    faults = _resource_faults({"databricks.yml": document})
    assert any(f"targets.{target}.resources.schemas" in fault for fault in faults), faults
