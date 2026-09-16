#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$project_root"
mkdir -p target/llvm-cov/html
cargo llvm-cov report --json --output-path coverage.json
cargo llvm-cov report --lcov --output-path lcov.info
cargo llvm-cov report --cobertura --output-path target/llvm-cov/cobertura.xml
cargo llvm-cov report --text --output-path coverage.txt
cargo llvm-cov report --html --output-dir target/llvm-cov
jq -e '.data | type == "array" and length > 0' coverage.json >/dev/null
jq '
  reduce .data[] as $item (
    {functions:{covered:0,count:0},lines:{covered:0,count:0},regions:{covered:0,count:0}};
    .functions.covered += ($item.totals.functions.covered // 0)
    | .functions.count += ($item.totals.functions.count // 0)
    | .lines.covered += ($item.totals.lines.covered // 0)
    | .lines.count += ($item.totals.lines.count // 0)
    | .regions.covered += ($item.totals.regions.covered // 0)
    | .regions.count += ($item.totals.regions.count // 0)
  )
' coverage.json > ci-summary.json
line_percent=$(jq -r 'if .lines.count == 0 then 0 else ((.lines.covered * 10000 / .lines.count | round) / 100) end' ci-summary.json)
line_whole=${line_percent%.*}
if [ "$line_whole" -ge 90 ]; then color=4c1
elif [ "$line_whole" -ge 80 ]; then color=97ca00
elif [ "$line_whole" -ge 70 ]; then color=dfb317
elif [ "$line_whole" -ge 60 ]; then color=fe7d37
else color=e05d44
fi
jq -n --arg message "${line_percent}%" --arg color "$color" \
    '{schemaVersion:1,label:"coverage",message:$message,color:$color}' > coverage-badge.json
{
    echo '## Code coverage'
    echo
    echo '| Metric | Covered | Total | Coverage |'
    echo '| --- | ---: | ---: | ---: |'
    jq -r '
      def pct($m): if $m.count == 0 then "n/a" else "\(((($m.covered * 10000 / $m.count) | round) / 100))%" end;
      ["functions", "lines", "regions"][] as $key
      | .[$key] as $m
      | "| \($key) | \($m.covered) | \($m.count) | \(pct($m)) |"
    ' ci-summary.json
    echo
    echo '- HTML report: artifact `coverage-reports`, path `target/llvm-cov/html/index.html`.'
    echo '- LCOV report: artifact `coverage-reports`, path `lcov.info`.'
    echo '- Shields endpoint: artifact `coverage-reports`, path `coverage-badge.json`.'
    echo
    echo '<details><summary>Text summary</summary>'
    echo
    echo '```text'
    cat coverage.txt
    echo '```'
    echo
    echo '</details>'
} > coverage-summary.md
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat coverage-summary.md >> "$GITHUB_STEP_SUMMARY"
fi
