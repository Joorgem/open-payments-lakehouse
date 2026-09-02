# iac/versions.tf -- the two pins, and what each of them is a pin ON.
#
# THE FLOOR IS WHAT THIS HAS ACTUALLY RUN UNDER rather than a version history read off
# a changelog. `terraform version` on the box that wrote this reports v1.15.8, and the
# CI job pins the same string with `setup-terraform`, so the check's local behaviour and
# its CI behaviour are one thing instead of two that happen to agree today. That
# agreement is the point: the first credential check written for this directory passed
# locally for a reason that does not exist on a runner (the provider reads
# `~/.databrickscfg` when nobody passes it credentials), and a check that is green in
# one place and red in the other for an undeclared reason teaches the next author the
# wrong thing.
terraform {
  required_version = ">= 1.15.8"

  required_providers {
    databricks = {
      source = "databricks/databricks"
      # EXACT, WITH A LOCK FILE BESIDE IT. `.terraform.lock.hcl` is committed and
      # carries hashes for BOTH platforms this configuration is initialised on --
      # windows_amd64 (the dev box) and linux_amd64 (the GitHub runner). A lock
      # written on one platform alone makes `terraform init` fail on the other with
      # a message about the lock rather than about this configuration, which is a CI
      # red that says nothing. Regenerate both together:
      #
      #     terraform providers lock \
      #       -platform=linux_amd64 -platform=windows_amd64
      version = "1.130.0"
    }
  }
}

# NO ARGUMENTS, AND THAT IS THE DECISION RATHER THAN AN OMISSION. Every way of naming a
# workspace here -- a host, a profile name, a token -- is either a literal this public
# repository must not carry or a value that differs per operator. The provider's default
# credential chain reads the environment and `~/.databrickscfg`, so the operator chooses
# the workspace outside the configuration:
#
#     DATABRICKS_CONFIG_PROFILE=<profile> terraform plan
#
# See README.md for the gated path. Nothing in this directory has ever been applied.
provider "databricks" {}
