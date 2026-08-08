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
#   1  a test failed, a chunk exceeded the cap, or the reconciliation did NOT close.
#   2  a SUBSET of chunks was requested. Nothing is claimed about the suite. This is
#      deliberately non-zero so a partial run can never be pasted as evidence that the
#      suite passes.
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

CAP_SECONDS="${SUITE_CHUNK_CAP:-600}"
WARN_SECONDS="${SUITE_CHUNK_WARN:-480}"

# THE PARTITION: "<name>|<pytest path arguments>". Split by measured runtime, not by
# meaning -- the only requirement the reconciliation enforces is that the four together
# collect exactly what the bare suite collects.
CHUNKS=(
  "non-vault|--ignore=tests/vault"
  "vault-cnpj-hashing|tests/vault/test_cnpj_vault.py tests/vault/test_hashing.py tests/vault/test_hashing_spark.py"
  "vault-estab-socios|tests/vault/test_estabelecimento_vault.py tests/vault/test_socios_vault.py"
  "vault-ledger-registry|tests/vault/test_loading.py tests/vault/test_observation.py tests/vault/test_registry.py tests/vault/test_reference_vault.py"
)

chunk_name() { printf '%s' "${CHUNKS[$1]%%|*}"; }
chunk_args() { printf '%s' "${CHUNKS[$1]#*|}"; }

log_dir() {
  # One directory per invocation, printed at the end, so every number below has a file
  # behind it. Under the repo's ignored scratch root rather than /tmp: on Windows the
  # two are not the same place and a reader has to be able to find it.
  local root="${TMPDIR:-/tmp}/opl-run-suite"
  mkdir -p "$root"
  printf '%s/%s' "$root" "$(date +%Y%m%d-%H%M%S)-$$"
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

main() {
  local dir requested=() only_collect=0 arg
  for arg in "$@"; do
    case "$arg" in
      --collect-only) only_collect=1 ;;
      [0-9]*) requested+=( "$(( arg - 1 ))" ) ;;
      *) printf 'unknown argument %s\n' "$arg" >&2; return 1 ;;
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

main "$@"
