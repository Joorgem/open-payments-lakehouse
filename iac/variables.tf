# iac/variables.tf -- the two inputs, and why the second one is empty.

variable "pii_readers_group_name" {
  description = <<-EOT
    The workspace-local group the socios mask predicate names. It is a variable so this
    directory can be pointed at a throwaway group in a proof, NOT because the name is
    free: `is_member('<name>')` is compiled into the mask function, so changing it here
    without changing the predicate leaves a declared group nobody reads. The Python
    spelling of the same string is `PII_READER_GROUP`; derive its sites rather than
    trusting a count here --

        git grep -n 'PII_READER_GROUP' -- src

    `tests/test_iac_terraform.py` holds this default and the Python constant equal.
  EOT
  type        = string
  default     = "opl_pii_readers"
}

variable "pii_reader_user_names" {
  description = <<-EOT
    Workspace user names (email form) to declare as members of that group.

    EMPTY IS THE STATE, NOT A PLACEHOLDER. `docs/adr/0008-pii-masking-socios.md` records
    the group as existing and holding nobody, and `scripts/rebuild_pii_reader_sp.py`
    says why in its own words: membership "is what lets a real identity read 55.8M real
    personal names, and it is a decision with an owner". So this list is empty here and
    the member resource expands to no instances at all. Filling it is that decision
    being taken, by the person who owns it, in a `.tfvars` file git-ignores.

    A SERVICE PRINCIPAL IS NOT REACHABLE THROUGH THIS LIST. `data "databricks_user"`
    resolves user names only; an SP member would need a different data source, and the
    SP path in this repository (`scripts/rebuild_pii_reader_sp.py`) is imperative and
    stays that way.
  EOT
  type        = list(string)
  default     = []
}
