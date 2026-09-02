# iac/identity.tf -- the group the mask predicate names, declared instead of assumed.
#
# WHAT WAS MISSING, IN THE REPOSITORY'S OWN WORDS. `scripts/rebuild_pii_reader_sp.py`
# refuses with "no workspace group named <name>; create it first" -- the script that
# rebuilds the PII-reader apparatus cannot create the group it needs, and no bundle
# resource, no job task and no script in this repository declared that the group exists.
# It was created by hand and every artefact downstream of it (the mask predicate, ADR
# 0008's fail-closed argument, `assert_mask_predicate`) reads a workspace object nothing
# under version control describes. This file is that description.
#
# WHY TERRAFORM AND NOT THE BUNDLE. A Databricks Asset Bundle has no group resource --
# the CLI's own schema types the collections a bundle may declare, and identity is not
# among them (`tests/test_bundle_resource_allowlist.py` is the lock that keeps this
# bundle to jobs and dashboards, and its `_SWEPT_PATHS` arm derives the places from
# `databricks bundle schema`). Terraform adds exactly two categories over the bundle,
# identity and Unity Catalog privileges, and README.md says why the second one is
# declined rather than merely absent.

resource "databricks_group" "pii_readers" {
  display_name = var.pii_readers_group_name

  # THE GROUP IS A SAFETY PRECONDITION, SO REPLACING IT IS NOT AN ORDINARY CHANGE. If
  # the group disappears, `is_member('opl_pii_readers')` returns false for everyone and
  # the mask fails CLOSED -- that direction is safe. What is not safe is the window: a
  # replace destroys and recreates, and a recreated group is empty of whatever grants
  # and memberships the old one carried, which `apply_pii_governance` would then settle
  # against a state nobody reviewed. UNEXERCISED: nothing here has been applied, so this
  # has never refused anything.
  lifecycle {
    prevent_destroy = true
  }
}

# A READ, NOT A DECLARATION. `databricks_group_member` binds ids, and a person is a name;
# this data source is how a name becomes an id without anyone pasting workspace state
# into a committed file. It expands to nothing while `pii_reader_user_names` is empty,
# which is the state today -- and it is also why `terraform plan` reaches for credentials
# as soon as the list is not empty. That is the correct behaviour and it is the reason CI
# runs `fmt -check` and `validate` and never `plan`: a plan that needs no credentials is
# a plan that read nothing, and it reports "1 to add" whether or not the object already
# exists in the workspace.
data "databricks_user" "pii_reader" {
  for_each  = toset(var.pii_reader_user_names)
  user_name = each.value
}

# ONE RESOURCE PER MEMBERSHIP EDGE, which is what makes this safe to adopt over a group
# somebody else also writes to. `databricks_group_member` manages the pair it names and
# is not authoritative over the group's roster: a member added imperatively -- by
# `scripts/rebuild_pii_reader_sp.py --group`, say -- is not one this configuration would
# remove, because Terraform has no resource for it. That is the opposite of the grant
# resources README.md declines, and the difference is why identity is here and grants
# are not.
resource "databricks_group_member" "pii_reader" {
  for_each  = data.databricks_user.pii_reader
  group_id  = databricks_group.pii_readers.id
  member_id = each.value.id
}
