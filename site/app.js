import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm";
import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";

const DB_FILE = "plates.db";
const DB_URL = "./data/plates.db";
const CSV_FILE = "plates_merged.csv";
const CSV_URL = "./data/plates_merged.csv";

const statusPill = document.querySelector("#status-pill");
const snapshotPill = document.querySelector("#snapshot-pill");
const metricsRoot = document.querySelector("#metrics");
const queryInput = document.querySelector("#query-input");
const queryMeta = document.querySelector("#query-meta");
const queryError = document.querySelector("#query-error");
const queryResults = document.querySelector("#query-results");
const runButton = document.querySelector("#run-query");
const maxOfferChartRoot = document.querySelector("#chart-max-offer");
const topPlatesChartRoot = document.querySelector("#chart-top-plates");

let db;
let conn;
let tableRef = "snapshots";

function setStatus(text, isLoading = false) {
  statusPill.textContent = text;
  statusPill.classList.toggle("loading", isLoading);
}

function renderTable(columns, rows) {
  queryResults.innerHTML = "";
  if (!rows.length) {
    queryResults.innerHTML = `<p class="muted">Query completed. No rows returned.</p>`;
    return;
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  for (const col of columns) {
    const th = document.createElement("th");
    th.textContent = col;
    trHead.appendChild(th);
  }
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const col of columns) {
      const td = document.createElement("td");
      td.textContent = formatValue(row[col]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  queryResults.appendChild(table);
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return String(value);
}

function tableToRows(arrowTable) {
  return arrowTable.toArray().map((row) => {
    if (typeof row.toJSON === "function") {
      return row.toJSON();
    }
    return row;
  });
}

async function runQuery(sql) {
  const startedAt = performance.now();
  const result = await conn.query(sql);
  const elapsed = performance.now() - startedAt;
  const columns = result.schema.fields.map((field) => field.name);
  const rows = tableToRows(result);
  return { columns, rows, elapsed };
}

async function loadData() {
  try {
    await db.registerFileURL(DB_FILE, DB_URL, duckdb.DuckDBDataProtocol.HTTP, true);
    await conn.query(`ATTACH '${DB_FILE}' AS plates_data (READ_ONLY);`);
    await conn.query(`SELECT COUNT(*) FROM plates_data.snapshots;`);
    tableRef = "plates_data.snapshots";
    setStatus("DuckDB file attached", false);
    return;
  } catch (error) {
    await db.registerFileURL(CSV_FILE, CSV_URL, duckdb.DuckDBDataProtocol.HTTP, true);
    await conn.query(
      `CREATE OR REPLACE TABLE snapshots AS SELECT * FROM read_csv_auto('${CSV_FILE}', HEADER = true);`
    );
    tableRef = "snapshots";
    setStatus("DB attach failed, using merged CSV fallback", false);
  }
}

function setDefaultQuery() {
  queryInput.value = [
    `SELECT`,
    `  scraped_date,`,
    `  listing_type,`,
    `  COUNT(*) AS rows_count,`,
    `  MAX(COALESCE(current_offer, starting_price)) AS max_visible_price`,
    `FROM ${tableRef}`,
    `GROUP BY scraped_date, listing_type`,
    `ORDER BY scraped_date DESC, listing_type`,
    `LIMIT 120;`,
  ].join("\n");
}

async function renderMetrics() {
  const { rows } = await runQuery(
    `
      SELECT
        COUNT(*) AS rows_count,
        COUNT(DISTINCT plate_number) AS unique_plates,
        COUNT(DISTINCT scraped_at) AS snapshots_count,
        MAX(scraped_at) AS latest_snapshot
      FROM ${tableRef};
    `
  );

  const item = rows[0] || {};
  const cards = [
    { label: "Rows", value: item.rows_count ?? "--" },
    { label: "Unique Plates", value: item.unique_plates ?? "--" },
    { label: "Snapshots", value: item.snapshots_count ?? "--" },
    { label: "Latest Snapshot", value: item.latest_snapshot ?? "--" },
  ];

  metricsRoot.innerHTML = cards
    .map(
      (card) => `
        <article class="card">
          <p class="label">${card.label}</p>
          <p class="value">${formatValue(card.value)}</p>
        </article>
      `
    )
    .join("");

  snapshotPill.textContent = `Snapshot: ${formatValue(item.latest_snapshot ?? "--")}`;
}

function renderMaxOfferChart(rows) {
  if (!rows.length) {
    maxOfferChartRoot.innerHTML = `<p class="muted">No chart data yet.</p>`;
    return;
  }
  const chart = Plot.plot({
    marginLeft: 56,
    height: 300,
    style: { background: "transparent", color: "#f4f6ff", fontFamily: "IBM Plex Mono, monospace" },
    x: { label: "Date" },
    y: { label: "CHF", grid: true },
    marks: [
      Plot.line(rows, { x: "scraped_date", y: "max_offer", stroke: "#64d9ff", strokeWidth: 2.5 }),
      Plot.dot(rows, { x: "scraped_date", y: "max_offer", fill: "#50ffaf", r: 3 }),
    ],
  });
  maxOfferChartRoot.replaceChildren(chart);
}

function renderTopPlatesChart(rows) {
  if (!rows.length) {
    topPlatesChartRoot.innerHTML = `<p class="muted">No chart data yet.</p>`;
    return;
  }
  const chart = Plot.plot({
    marginLeft: 70,
    height: 300,
    style: { background: "transparent", color: "#f4f6ff", fontFamily: "IBM Plex Mono, monospace" },
    x: { label: "CHF", grid: true },
    y: { label: "Plate" },
    marks: [Plot.barX(rows, { x: "price", y: "plate_number", fill: "#50ffaf", sort: { y: "-x" } })],
  });
  topPlatesChartRoot.replaceChildren(chart);
}

async function renderCharts() {
  const { rows: dailyRows } = await runQuery(
    `
      SELECT
        scraped_date,
        MAX(COALESCE(current_offer, starting_price)) AS max_offer
      FROM ${tableRef}
      GROUP BY scraped_date
      ORDER BY scraped_date;
    `
  );

  const { rows: topRows } = await runQuery(
    `
      WITH latest AS (
        SELECT MAX(scraped_at) AS max_snapshot FROM ${tableRef}
      )
      SELECT
        plate_number,
        COALESCE(current_offer, starting_price) AS price
      FROM ${tableRef}
      WHERE scraped_at = (SELECT max_snapshot FROM latest)
      ORDER BY price DESC NULLS LAST
      LIMIT 15;
    `
  );

  renderMaxOfferChart(dailyRows);
  renderTopPlatesChart(topRows);
}

async function executeSqlFromEditor() {
  queryError.hidden = true;
  queryMeta.textContent = "Running query...";

  try {
    const { columns, rows, elapsed } = await runQuery(queryInput.value);
    renderTable(columns, rows);
    queryMeta.textContent = `${rows.length} row(s) in ${elapsed.toFixed(1)}ms`;
  } catch (error) {
    queryError.hidden = false;
    queryError.textContent = error.message;
    queryMeta.textContent = "Query failed.";
  }
}

async function boot() {
  setStatus("Loading DuckDB WASM...", true);
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
  );
  const worker = new Worker(workerUrl);
  db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();

  setStatus("Loading data files...", true);
  await loadData();
  setDefaultQuery();
  await renderMetrics();
  await renderCharts();
  await executeSqlFromEditor();
}

runButton.addEventListener("click", executeSqlFromEditor);
queryInput.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    executeSqlFromEditor();
  }
});

boot().catch((error) => {
  setStatus("Initialization failed", false);
  queryError.hidden = false;
  queryError.textContent = error.stack || error.message;
});
