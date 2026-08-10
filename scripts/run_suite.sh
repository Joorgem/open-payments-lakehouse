#!/usr/bin/env bash
# scripts/run_suite.sh -- run the whole test suite as a reconciled partition, and
# print ONE summary a run-evidence document can quote.
#
# WHY THIS EXISTS. The suite is ~1,460 s of work and the agent shell kills a single
# command at 600 s, so "the suite passes" had become four hand-stitched pytest
# invocations pasted into a document. Four numbers with four timestamps invite exactly
# the "which run was that from?" ambiguity the evidence format exists to remove: a
# reader cannot tell a partition from an overlapping sample, and cannot tell a chunk
# that was quietly dropped from one that was never in the list.
#
# WHAT IT ADDS OVER RUNNING THE FOUR BY HAND -- the reconciliation, and it is in the
# OUTPUT rather than in prose around it:
#   1. The chunks' collected node ids are compared, as SETS, against the whole suite's.
#      Equal sets prove the partition is total AND non-overlapping. A sum alone does
#      not: two chunks that overlap by three tests while a third chunk silently loses
#      three still sums correctly.
#   2. The chunk pass counts are summed and compared against the suite's own
#      --collect-only selected count. Disagreement exits non-zero.
#   3. A chunk that outgrows the 600 s cap fails LOUDLY, naming itself, rather than
#      being discovered as a mysterious kill with no output. The partition is a
#      property of today's runtimes and it will need re-splitting; this is how that
#      becomes visible on the day it happens instead of three tasks later.
#
# EXIT CODES, because a green exit here is meant to be quotable:
#   0  every chunk ran, every chunk passed, and the reconciliation closed.
#   1  a test failed, a chunk exceeded the cap, the reconciliation did NOT close, or an
#      argument was refused.
#   2  NOTHING IS CLAIMED ABOUT THE SUITE. Two runs reach it: a SUBSET of chunks was
#      requested, and `--collect-only`, which reconciles the partition and runs no
#      test. Deliberately non-zero in both cases so neither can be pasted as evidence
#      that the suite passes. (A `--collect-only` run whose partition is BROKEN exits 1
#      instead, because that is a finding rather than an absence of one.)
#
# USAGE
#   scripts/run_suite.sh              # all chunks; the command a document quotes
#   scripts/run_suite.sh 2 4          # only those chunks, always exit 2
#   scripts/run_suite.sh --collect-only   # reconcile the partition, run no tests
#
# Local Spark needs JAVA_HOME and HADOOP_HOME on PATH (see CLAUDE.md). This script
# does not set them: a shell that cannot run pytest should say so as pytest, not be
# silently repaired here.
set -uo pipefail

# RUN FROM THE REPOSITORY ROOT, whatever directory the operator invoked this from. The
# chunk arguments below are repo-relative pytest paths and so is the log root, so the
# two only agree if the working directory is pinned rather than assumed.
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

# THE LOG ROOT, REPO-RELATIVE AND GIT-IGNORED, AND THAT IS A PRIVACY DECISION RATHER
# THAN A TIDINESS ONE. This script's output is pasted verbatim into run-evidence
# documents that are published, and those documents promise to redact the operator's OS
# username. `${TMPDIR:-/tmp}` -- what this used to use -- is
# `C:\Users\<operator>\AppData\Local\Temp` on Windows, so the `logs:` line below would
# have carried that username into the published paste. The path printed is relative for
# the same reason: there is no absolute path in this output to leak.
LOG_ROOT=".run-suite-logs"

CAP_SECONDS="${SUITE_CHUNK_CAP:-600}"
WARN_SECONDS="${SUITE_CHUNK_WARN:-480}"

# THE PARTITION: "<name>|<pytest path arguments>". Split by measured runtime, not by
# meaning -- the only requirement the reconciliation enforces is that the four together
# collect exactly what the bare suite collects.
#
# EVERY VAULT CHUNK NAMES ITS FILES, SO A NEW FILE UNDER tests/vault IS IN NO CHUNK.
# Chunk 1 ignores that directory and the other three are explicit lists, so a vault test
# module added without an edit here is collected by the bare suite, found in no chunk,
# and `reconcile_partition` exits non-zero -- which is the reconciliation working, and is
# also a surprise if you were told the new tests "land in the non-vault chunk". That is
# true of a test added under `tests/` and false of one added under `tests/vault/`.
# F2 wave 1's workspace run added `test_effectivity_window.py` and put it in
# `vault-ledger-registry` rather than in `vault-estab-socios`: the socios chunk has
# ranged 454-1,197 s across runs against a 600 s cap and is the one under a standing
# split warning, and this file reads the same socios fixture the ledger chunk's
# `test_observation.py` already exercises the ledger through.
#
# `vault-estab-socios` WAS ONE CHUNK AND IS NOW TWO, BECAUSE IT ACTUALLY BLEW THE CAP.
# Measured 2026-08-09: **632 s, over the 600 s cap**, `!! OVER THE 600s CAP` fired, all 54
# tests passing. The split condition everyone expected was "the next task that ADDS an
# estabelecimentos or socios test must split this first" -- and that is not what happened.
# F2 wave 1's workspace run added no test here at all (its new file went to
# `vault-ledger-registry`); the chunk crossed the line on CONTENTION alone, on unchanged
# code, having measured 454 s earlier the same day. So the trigger was never test count.
# Recorded runtimes for this one chunk, same code: 519, 555, 454, 632, 1,197 s. Anyone
# tempted to merge these two files back together should read that list first, and note
# that the 92%-of-cap figure the handoff carried was one sample of it.
#
# SPLIT BY FILE, ONE EACH, because the two are exactly even where it is measurable: 27
# tests each, 721 and 791 lines. The runtime is dominated by Spark fixture setup rather
# than by test count, so neither half is 316 s; each pays the setup again. That is the
# cost of the split and it is worth it -- a chunk over the cap makes `run_suite.sh` exit 1
# and the suite unquotable as evidence, which is a worse failure than a slower total.
#
# `test_satellite_diagnostics.py` IS IN `vault-cnpj-hashing`, AND THAT IS A CHOICE ABOUT
# WHICH CHUNK CAN AFFORD IT rather than about subject alone. It loads `sat_empresa_dados`
# over a small empresas fixture, which is `test_cnpj_vault.py`'s table and this chunk's
# subject; and the two chunks under the standing split warning are `vault-estab` and
# `vault-socios`, whose one file each was split off precisely because that pair crossed
# the cap. Adding a sixth file to `vault-ledger-registry` (five files, including the two
# at 800 lines) would load the other candidate. This chunk's third member is a fixture
# the file builds itself, so what it costs is one more Spark module setup.
CHUNKS=(
  "non-vault|--ignore=tests/vault"
  "vault-cnpj-hashing|tests/vault/test_cnpj_vault.py tests/vault/test_hashing.py tests/vault/test_hashing_spark.py tests/vault/test_satellite_diagnostics.py"
  "vault-estab|tests/vault/test_estabelecimento_vault.py"
  "vault-socios|tests/vault/test_socios_vault.py"
  "vault-ledger-registry|tests/vault/test_loading.py tests/vault/test_observation.py tests/vault/test_registry.py tests/vault/test_reference_vault.py tests/vault/test_effectivity_window.py"
)

chunk_name() { printf '%s' "${CHUNKS[$1]%%|*}"; }
chunk_args() { printf '%s' "${CHUNKS[$1]#*|}"; }

log_dir() {
  # One directory per invocation, under `LOG_ROOT` above, so every number this script
  # prints has a file behind it and a reader can find it from the repository root.
  # `main` creates it; `mkdir -p` there makes this parent too.
  printf '%s/%s' "$LOG_ROOT" "$(date +%Y%m%d-%H%M%S)-$$"
}

collect_ids() {
  # Node ids of the tests pytest would SELECT for these arguments, one per line, sorted.
  # Counting these beats parsing the summary line: deselected tests are already gone,
  # and a set is what the partition check needs anyway.
  local out="$1"; shift
  uv run pytest --collect-only -q "$@" >"$out.raw" 2>&1
  grep -a '::' "$out.raw" | sed 's/[[:space:]]*$//' | sort >"$out"
}

deselected_count() {
  # From "922/928 tests collected (6 deselected) in 4.20s". Zero when pytest prints no
  # such clause, which is the shape when nothing is deselected.
  local n
  n="$(grep -aoE '\(([0-9]+) deselected\)' "$1.raw" | grep -aoE '[0-9]+' | tail -n 1)"
  printf '%s' "${n:-0}"
}

summary_of() {
  # The pytest summary line, recovered by MATCHING it rather than by tailing the file:
  # Spark's JVM teardown prints process-termination lines to stderr AFTER the summary,
  # and a `tail -3` in this repo's history displaced it and destroyed a run's evidence.
  grep -aE '[0-9]+ (passed|failed|error)' "$1" | grep -aE 'in [0-9.]+s' | tail -n 1
}

count_in() {
  # "$2 passed" / "$2 failed" out of a summary line; 0 when the word is absent.
  local n
  n="$(printf '%s' "$1" | grep -aoE "[0-9]+ $2" | grep -aoE '^[0-9]+' | tail -n 1)"
  printf '%s' "${n:-0}"
}

reconcile_partition() {
  # SET equality between the union of the chunks' node ids and the bare suite's.
  # Returns 0 when they match; prints the discrepancy either way.
  local dir="$1" suite_ids="$2" i
  : >"$dir/union.raw"
  for i in "${!CHUNKS[@]}"; do cat "$dir/collect-$i" >>"$dir/union.raw"; done
  sort "$dir/union.raw" >"$dir/union.sorted"
  sort -u "$dir/union.sorted" >"$dir/union"
  local dupes missing extra
  dupes=$(( $(wc -l <"$dir/union.sorted") - $(wc -l <"$dir/union") ))
  missing="$(comm -23 "$suite_ids" "$dir/union" | wc -l)"
  extra="$(comm -13 "$suite_ids" "$dir/union" | wc -l)"
  printf '  partition vs suite      %s in no chunk, %s in no suite run, %s in two chunks\n' \
    "$missing" "$extra" "$dupes"
  [ "$missing" -eq 0 ] && [ "$extra" -eq 0 ] && [ "$dupes" -eq 0 ]
}

collect_everything() {
  # Every chunk's ids plus the bare suite's. Cheap (import-only), and it is what makes
  # a --collect-only invocation of this script useful on its own.
  local dir="$1" i
  printf 'COLLECTING (no tests run yet)\n'
  for i in "${!CHUNKS[@]}"; do
    # shellcheck disable=SC2046  # word splitting is the point: chunk args are a list
    collect_ids "$dir/collect-$i" $(chunk_args "$i")
    printf '  %-22s %6s selected\n' "$(chunk_name "$i")" "$(wc -l <"$dir/collect-$i")"
  done
  collect_ids "$dir/collect-suite"
  printf '  %-22s %6s selected, %s deselected\n\n' \
    "(bare suite)" "$(wc -l <"$dir/collect-suite")" "$(deselected_count "$dir/collect-suite")"
}

run_chunk() {
  # One chunk, timed, with its own log. Echoes "<passed> <failed> <elapsed> <rc>".
  local dir="$1" i="$2" started ended elapsed rc line
  started="$(date +%s)"
  # shellcheck disable=SC2046
  uv run pytest $(chunk_args "$i") -q >"$dir/run-$i.log" 2>&1
  rc="$?"
  ended="$(date +%s)"
  elapsed=$(( ended - started ))
  line="$(summary_of "$dir/run-$i.log")"
  printf '%s %s %s %s\n' \
    "$(count_in "$line" passed)" "$(count_in "$line" failed)" "$elapsed" "$rc"
}

chunk_index() {
  # `$1` as a 0-based index into CHUNKS, or REFUSE BY NAME. Both halves are fixes for
  # a shape that used to pass: `[0-9]*` is a glob and not a full match, so `3abc`
  # arrived as chunk 3 with the `abc` silently dropped; and a number past the end
  # reached `${CHUNKS[$1]}` and died under `set -u` with bash's own unbound-variable
  # message, naming neither the argument nor the range it had to be in.
  case "$1" in
    ''|*[!0-9]*)
      printf 'unknown argument %s -- expected a chunk number or --collect-only\n' \
        "$1" >&2
      return 1 ;;
  esac
  if [ "$1" -lt 1 ] || [ "$1" -gt "${#CHUNKS[@]}" ]; then
    printf 'there is no chunk %s -- the partition has %s, numbered 1 to %s\n' \
      "$1" "${#CHUNKS[@]}" "${#CHUNKS[@]}" >&2
    return 1
  fi
  printf '%s' "$(( $1 - 1 ))"
}

main() {
  local dir requested=() only_collect=0 arg index
  for arg in "$@"; do
    case "$arg" in
      --collect-only) only_collect=1 ;;
      *) index="$(chunk_index "$arg")" || return 1; requested+=( "$index" ) ;;
    esac
  done
  dir="$(log_dir)"; mkdir -p "$dir"
  printf '\nopl test suite, run as a reconciled partition of %s chunks\n' "${#CHUNKS[@]}"
  printf 'logs: %s\n\n' "$dir"
  collect_everything "$dir"

  local partition_ok=0
  printf 'PARTITION\n'
  reconcile_partition "$dir" "$dir/collect-suite" || partition_ok=1
  printf '\n'
  if [ "$only_collect" -eq 1 ]; then
    printf 'VERDICT: COLLECT-ONLY -- partition %s, no tests run\n\n' \
      "$( [ "$partition_ok" -eq 0 ] && echo reconciled || echo BROKEN )"
    return $(( partition_ok == 0 ? 2 : 1 ))
  fi
  [ "${#requested[@]}" -eq 0 ] && requested=( "${!CHUNKS[@]}" )
  report "$dir" "$partition_ok" "${requested[@]}"
}

report() {
  # Run the requested chunks, then print the arithmetic and decide the exit code.
  local dir="$1" partition_ok="$2"; shift 2
  local requested=( "$@" ) i out passed failed elapsed rc
  local total=0 bad="$partition_ok" over=0
  printf 'RUNNING\n'
  for i in "${requested[@]}"; do
    out="$(run_chunk "$dir" "$i")"
    read -r passed failed elapsed rc <<<"$out"
    total=$(( total + passed ))
    [ "$rc" -ne 0 ] && bad=1
    local flag=''
    if [ "$elapsed" -ge "$CAP_SECONDS" ]; then
      flag='  !! OVER THE 600s CAP -- SPLIT THIS CHUNK'; over=1; bad=1
    elif [ "$elapsed" -ge "$WARN_SECONDS" ]; then
      flag='  !  within 2 min of the cap'
    fi
    printf '  %-22s %5s passed  %3s failed  %5ss  rc=%s%s\n' \
      "$(chunk_name "$i")" "$passed" "$failed" "$elapsed" "$rc" "$flag"
  done
  verdict "$dir" "$total" "$bad" "$over" "${#requested[@]}"
}

verdict() {
  # THE RECONCILIATION, printed as arithmetic so a reader checks it rather than trusts
  # it. `expected` is the suite's own --collect-only count -- a second, independent
  # derivation of the same number, which is the whole point of comparing them.
  local dir="$1" total="$2" bad="$3" over="$4" ran="$5"
  local expected deselected collected
  expected="$(wc -l <"$dir/collect-suite")"
  deselected="$(deselected_count "$dir/collect-suite")"
  collected=$(( expected + deselected ))
  printf '\nRECONCILIATION\n'
  printf '  chunk passes summed     %6s\n' "$total"
  printf '  --collect-only selected %6s   (%s collected, %s deselected)\n' \
    "$expected" "$collected" "$deselected"
  printf '  logs                    %s\n\n' "$dir"
  if [ "$ran" -ne "${#CHUNKS[@]}" ]; then
    printf 'VERDICT: PARTIAL -- %s of %s chunks. This run is NOT evidence that the\n' \
      "$ran" "${#CHUNKS[@]}"
    printf '  suite passes, and exits non-zero so it cannot be quoted as though it were.\n\n'
    return 2
  fi
  if [ "$over" -ne 0 ]; then
    printf 'VERDICT: FAILED -- a chunk outgrew the %ss cap. Re-split the partition in\n' "$CAP_SECONDS"
    printf '  CHUNKS above; the totals below are still valid.\n\n'; return 1
  fi
  if [ "$bad" -ne 0 ] || [ "$total" -ne "$expected" ]; then
    printf 'VERDICT: FAILED -- %s\n\n' \
      "$( [ "$total" -ne "$expected" ] && echo "$total passed != $expected selected" \
          || echo 'a chunk reported failures or a broken partition' )"
    return 1
  fi
  printf 'VERDICT: RECONCILED -- %s passed, %s selected, agreed by two derivations\n\n' \
    "$total" "$expected"
}

# THE INVOCATION IS WRAPPED IN BRACES AND ENDS IN AN EXPLICIT `exit`, AND BOTH HALVES
# ARE LOAD-BEARING. Bash reads a script INCREMENTALLY, by byte offset, while it runs it.
# A ~1,470 s run therefore holds this file open for twenty-five minutes, and anything
# that rewrites it in that window -- an editor, a `git checkout`, an agent's own edit --
# leaves bash resuming at an offset that now lands mid-token in different text. What
# comes out is fragments executed as commands AFTER the verdict has printed, at line
# numbers past the end of the file that was started, with the exit status already
# decided. Observed once, on the first end-to-end run of this script:
#
#   VERDICT: RECONCILED -- 922 passed, 922 selected, agreed by two derivations
#   scripts/run_suite.sh: line 231: $'\n    fi\n    printf ': command not found
#   scripts/run_suite.sh: line 241: $1: unbound variable
#   $ echo $?
#   0
#
# A tool whose whole purpose is to be quotable as evidence must not print a green
# verdict and then emit errors, least of all while returning 0. `{ ...; }` is a compound
# command, so bash PARSES `main "$@"; exit $?` in full before executing any of it, and
# the `exit` then terminates the shell without another read. A bare `main "$@"` followed
# by `exit $?` on its own line does NOT fix this -- the `exit` is read after `main`
# returns, from the offset that has already moved. Both were tested against a file
# rewritten mid-run; only this form survives.
{ main "$@"; exit $?; }
