#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const START_MARKER = "<!-- BENCHMARK-SNAPSHOT:START -->";
const END_MARKER = "<!-- BENCHMARK-SNAPSHOT:END -->";

function main() {
  const resultsPath = path.resolve(process.argv[2] || "docs/benchmarks/results.json");
  const readmePath = path.resolve(process.argv[3] || "README.md");

  const report = JSON.parse(fs.readFileSync(resultsPath, "utf8"));
  const readme = fs.readFileSync(readmePath, "utf8");

  const start = readme.indexOf(START_MARKER);
  if (start === -1) {
    throw new Error(`README benchmark start marker not found in ${readmePath}`);
  }

  const end = readme.indexOf(END_MARKER, start);
  if (end === -1) {
    throw new Error(`README benchmark end marker not found in ${readmePath}`);
  }

  const replacement = `${START_MARKER}\n${renderReadmeSnapshot(report)}\n${END_MARKER}`;
  const updated = `${readme.slice(0, start)}${replacement}${readme.slice(end + END_MARKER.length)}`;
  fs.writeFileSync(readmePath, updated);
}

function renderReadmeSnapshot(report) {
  let markdown = "";

  markdown += `Current benchmark snapshot (\`make bench\`, ${report.config.entries} \`Row\` values, ${report.config.key_size}-byte keys). Each backend is benchmarked in its own child process. The workload writes the full row, reads it back with full \`get!\`-equivalent retrieval into a Rust struct, reads only \`rewards\` through \`get!\`, updates \`rewards\` to 0 through either full deserialize/mutate/re-encode or direct CBOR rewrite, then deletes the entry. The table below shows the fixed-width \`row_static\` path-targeted variants, resident memory before the backend run, peak observed resident memory during the run, resident memory after the backend run, and the persisted raw store size after the baseline seeded workload.\n\n`;
  markdown += "| Backend | Insert | Full get! | Partial get! | Full update! | Partial update! | Delete | RSS before / peak / after | Disk |\n";
  markdown += "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |\n";

  for (const result of report.results) {
    markdown += `| ${result.backend} | ${renderOps(result.insert.ops_per_sec)} | ${renderOps(result.full_read.ops_per_sec)} | ${renderOps(result.partial_get_static.ops_per_sec)} | ${renderOps(result.full_update_static.ops_per_sec)} | ${renderOps(result.partial_update_static.ops_per_sec)} | ${renderOps(result.delete.ops_per_sec)} | ${renderMibShort(result.memory.rss_before_bytes)} / ${renderMibShort(result.memory.rss_peak_bytes)} / ${renderMibShort(result.memory.rss_after_bytes)} | ${renderDiskUsage(result.on_disk_bytes)} |\n`;
  }

  markdown += "\n### Backend Comparison Charts\n\n";
  markdown += renderReadmeChart("Insert", report.results, (result) => result.insert.ops_per_sec);
  markdown += "\n";
  markdown += renderReadmeChart("Full get!", report.results, (result) => result.full_read.ops_per_sec);
  markdown += "\n";
  markdown += renderReadmeChart("Partial get!", report.results, (result) => result.partial_get_static.ops_per_sec);
  markdown += "\n";
  markdown += renderReadmeChart("Full update!", report.results, (result) => result.full_update_static.ops_per_sec);
  markdown += "\n";
  markdown += renderReadmeChart("Partial update!", report.results, (result) => result.partial_update_static.ops_per_sec);
  markdown += "\n";
  markdown += renderReadmeChart("Delete", report.results, (result) => result.delete.ops_per_sec);

  return markdown.trimEnd();
}

function renderReadmeChart(title, results, metric) {
  const chartResults = results.filter((result) => result.backend !== "memory");
  const maxValue = chartResults.reduce((current, result) => Math.max(current, metric(result)), 0);
  const width = 28;

  let markdown = `#### ${title}\n\n` + "```text\n";
  for (const result of chartResults) {
    const value = metric(result);
    const filled = maxValue > 0 ? Math.min(width, Math.round((value / maxValue) * width)) : 0;
    const empty = Math.max(0, width - filled);
    markdown += `${result.backend.padEnd(10, " ")} | ${"#".repeat(filled)}${" ".repeat(empty)} ${renderOps(value)}\n`;
  }
  markdown += "```\n";
  return markdown;
}

function renderMibShort(bytes) {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function renderDiskUsage(bytes) {
  if (bytes == null) {
    return "n/a";
  }
  if (bytes >= 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
  }
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  }
  if (bytes >= 1024) {
    return `${Math.round(bytes / 1024)} KiB`;
  }
  return `${bytes} B`;
}

function renderOps(opsPerSec) {
  if (opsPerSec >= 1_000_000) {
    return `${(opsPerSec / 1_000_000).toFixed(1)}M ops/s`;
  }
  if (opsPerSec >= 1_000) {
    return `${Math.round(opsPerSec / 1_000)}k ops/s`;
  }
  return `${Math.round(opsPerSec)} ops/s`;
}

main();