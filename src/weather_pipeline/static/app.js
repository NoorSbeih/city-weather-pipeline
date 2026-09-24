/* Minimal dashboard: city picker -> timeseries chart + table. */

const citySelect = document.getElementById("city");
const metaEl = document.getElementById("meta");
const rowsEl = document.getElementById("rows");
let chart;

async function fetchJson(path) {
  const res = await fetch(path);
  if (!res.ok) {
    throw new Error(`${res.status} ${await res.text()}`);
  }
  return res.json();
}

function fmt(n, digits = 1) {
  if (n === null || n === undefined) return "—";
  return Number(n).toFixed(digits);
}

function renderTable(series) {
  const recent = [...series].reverse().slice(0, 14);
  rowsEl.innerHTML = recent
    .map(
      (r) => `<tr>
        <td>${r.date}</td>
        <td class="num">${fmt(r.temperature_mean_c)}</td>
        <td class="num">${fmt(r.temperature_mean_7d_avg_c)}</td>
        <td class="num">${fmt(r.temperature_anomaly_30d_c)}</td>
        <td class="num">${fmt(r.precipitation_mm)}</td>
        <td class="num">${fmt(r.wind_speed_max_kmh)}</td>
      </tr>`
    )
    .join("");
}

function renderChart(series) {
  const labels = series.map((r) => r.date);
  const mean = series.map((r) => r.temperature_mean_c);
  const avg7 = series.map((r) => r.temperature_mean_7d_avg_c);
  const ctx = document.getElementById("tempChart");

  if (chart) chart.destroy();
  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Mean °C",
          data: mean,
          borderColor: "#3d9b8f",
          backgroundColor: "transparent",
          tension: 0.2,
          pointRadius: 0,
          borderWidth: 2,
        },
        {
          label: "7d avg °C",
          data: avg7,
          borderColor: "#c4a35a",
          backgroundColor: "transparent",
          tension: 0.2,
          pointRadius: 0,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { color: "#8b9aab" } },
      },
      scales: {
        x: {
          ticks: { color: "#8b9aab", maxTicksLimit: 8 },
          grid: { color: "#2a3542" },
        },
        y: {
          ticks: { color: "#8b9aab" },
          grid: { color: "#2a3542" },
        },
      },
    },
  });
}

async function loadCity(cityId) {
  metaEl.textContent = "Loading…";
  const [latest, series] = await Promise.all([
    fetchJson(`/cities/${cityId}/latest`).catch(() => null),
    fetchJson(`/cities/${cityId}/timeseries?limit=120`),
  ]);
  if (!series.length) {
    metaEl.textContent = "No curated rows yet — run `weather-pipeline run` first.";
    rowsEl.innerHTML = "";
    if (chart) chart.destroy();
    return;
  }
  metaEl.textContent = latest
    ? `Latest ${latest.date}: mean ${fmt(latest.temperature_mean_c)}°C · 30d anomaly ${fmt(latest.temperature_anomaly_30d_c)}°C · ${series.length} days shown`
    : `${series.length} days`;
  renderChart(series);
  renderTable(series);
}

async function init() {
  const cities = await fetchJson("/cities");
  citySelect.innerHTML = cities
    .map((c) => `<option value="${c.city_id}">${c.name} (${c.country_code})</option>`)
    .join("");
  const firstWithData = cities.find((c) => c.day_count > 0) || cities[0];
  if (firstWithData) {
    citySelect.value = firstWithData.city_id;
    await loadCity(firstWithData.city_id);
  }
  citySelect.addEventListener("change", () => loadCity(citySelect.value));
}

init().catch((err) => {
  metaEl.textContent = `Failed to load: ${err.message}`;
});
