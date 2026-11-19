// Workshop map: Queens NTAs, census tracts, bus stops, NTA picker, two switches
// (tract shading and weekday/Sunday service).
// Static: reads precomputed GeoJSON from data/ (built by scripts/build_map_data.py).

// Data version stamped on this script's tag by build_map_data.py; added to data fetches
// so a rebuild isn't hidden by the browser cache.
const DATA_VERSION = new URL(document.currentScript.src).searchParams.get("v");

const QUEENS_CENTER = [40.705, -73.82];
const SNAPSHOT_LABELS = { weekday: "weekday midday", sunday: "Sunday midday" };

// Tract shading. Ordered single-hue scales (ColorBrewer Purples and Greens, 5 classes),
// light to dark; a value at a break falls in the higher class.
const MEASURES = {
  cars: {
    field: "vehicle_deficit_share",
    numerator: "vehicle_deficit_households",
    denominator: "households",
    title: "Darker = areas where more households are short on cars",
    breaks: [0.6, 0.7, 0.8, 0.9],
    labels: ["Under 60%", "60–70%", "70–80%", "80–90%", "90%+"],
    colors: ["#f2f0f7", "#cbc9e2", "#9e9ac8", "#756bb1", "#54278f"],
  },
  burden: {
    field: "burden_share",
    numerator: "burdened_households",
    denominator: "burden_households",
    title: "Darker = areas where more households spend 30% or more of income on housing",
    breaks: [0.3, 0.4, 0.5, 0.6],
    labels: ["Under 30%", "30–40%", "40–50%", "50–60%", "60%+"],
    colors: ["#edf8e9", "#bae4b3", "#74c476", "#31a354", "#006d2c"],
  },
};

const state = { nta: "", snapshot: "weekday", measure: "cars" };

const map = L.map("map", { preferCanvas: true, zoomControl: true }).setView(QUEENS_CENTER, 11);

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  maxZoom: 19,
}).addTo(map);

const picker = document.getElementById("nta-picker");
const summary = document.getElementById("summary");

let ntaLayer;
let tractLayer;
let stopLayer;
let stops = [];
let ntaFeatures = [];
const ntaNames = {};

function getCss(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function tractColor(value) {
  const { breaks, colors } = MEASURES[state.measure];
  let i = 0;
  while (i < breaks.length && value >= breaks[i]) i += 1;
  return colors[i];
}

function tractStyle(feature) {
  const value = feature.properties[MEASURES[state.measure].field];
  const inPick = !state.nta || feature.properties.nta2020 === state.nta;
  const noData = value === null || value === undefined;
  return {
    stroke: noData,
    color: getCss("--sand"),
    weight: 1,
    dashArray: "3 3",
    fillColor: noData ? getCss("--nodata") : tractColor(value),
    fillOpacity: inPick ? 0.72 : 0.3,
  };
}

function ntaStyle(feature) {
  const selected = feature.properties.nta2020 === state.nta;
  return {
    color: getCss("--ink"),
    opacity: selected ? 1 : 0.45,
    weight: selected ? 3 : 1,
    fillOpacity: 0,
  };
}

function stopStyle(feature) {
  const served = feature.properties[state.snapshot];
  const inPick = !state.nta || feature.properties.nta2020 === state.nta;
  return {
    radius: inPick ? 4 : 2.5,
    color: served ? "#ffffff" : getCss("--unserved"),
    weight: served ? 1 : 1.5,
    fillColor: served ? getCss("--served") : "#ffffff",
    fillOpacity: inPick ? 1 : 0.5,
    opacity: inPick ? 1 : 0.5,
  };
}

function formatShare(numerator, denominator) {
  return `${(100 * numerator / denominator).toFixed(1)}%`;
}

// NTA (or Queens) figures: sum tract counts, then divide. Never average tract shares.
function updateFigures() {
  const inArea = state.nta ? ntaFeatures.filter((f) => f.properties.nta2020 === state.nta) : ntaFeatures;
  document.getElementById("figures-place").textContent = state.nta ? ntaNames[state.nta] : "All of Queens";
  for (const [key, measure] of Object.entries(MEASURES)) {
    const num = inArea.reduce((sum, f) => sum + f.properties[measure.numerator], 0);
    const den = inArea.reduce((sum, f) => sum + f.properties[measure.denominator], 0);
    const value = document.getElementById(`fig-${key}`);
    const detail = document.getElementById(`fig-${key}-detail`);
    if (den === 0) {
      value.textContent = "No data";
      detail.textContent = "(no households)";
    } else {
      value.textContent = formatShare(num, den);
      detail.textContent = `(${num.toLocaleString("en-US")} of ${den.toLocaleString("en-US")} households)`;
    }
  }
}

function updateLegend() {
  const measure = MEASURES[state.measure];
  document.getElementById("legend-title").textContent = measure.title;
  const ramp = document.getElementById("ramp");
  ramp.replaceChildren(...measure.colors.map((color, i) => {
    const li = document.createElement("li");
    const swatch = document.createElement("span");
    swatch.className = "swatch";
    swatch.style.background = color;
    const label = document.createElement("span");
    label.textContent = measure.labels[i];
    li.append(swatch, label);
    return li;
  }));
  document.getElementById("cars-note").hidden = state.measure !== "cars";
}

function updateSummary() {
  const inArea = state.nta ? stops.filter((s) => s.properties.nta2020 === state.nta) : stops;
  const served = inArea.filter((s) => s.properties[state.snapshot]).length;
  const place = state.nta ? ntaNames[state.nta] : "Queens";
  summary.textContent = inArea.length === 0
    ? `${place}: no bus stops.`
    : `${place}: ${served} of ${inArea.length} bus stops have scheduled local service at ${SNAPSHOT_LABELS[state.snapshot]}.`;
}

function render() {
  if (tractLayer) tractLayer.setStyle(tractStyle);
  if (ntaLayer) ntaLayer.setStyle(ntaStyle);
  if (stopLayer) stopLayer.setStyle(stopStyle);
  updateFigures();
  updateLegend();
  updateSummary();
}

function zoomToSelection() {
  map.invalidateSize();
  if (map.getSize().x === 0) {
    // Page opened in a hidden tab or pane: fit once it has a size.
    window.addEventListener("resize", zoomToSelection, { once: true });
    return;
  }
  const padding = panelPadding();
  if (!state.nta) {
    map.fitBounds(ntaLayer.getBounds(), padding);
    return;
  }
  ntaLayer.eachLayer((layer) => {
    if (layer.feature.properties.nta2020 === state.nta) {
      map.fitBounds(layer.getBounds(), { ...padding, maxZoom: 15 });
    }
  });
}

// Keep the fitted area clear of the panel: left of it on desktop, above it on phones.
function panelPadding() {
  const panel = document.getElementById("panel").getBoundingClientRect();
  const gap = 16;
  return window.matchMedia("(max-width: 600px)").matches
    ? { paddingTopLeft: [gap, gap], paddingBottomRight: [gap, panel.height + gap] }
    : { paddingTopLeft: [panel.right + gap, gap], paddingBottomRight: [gap, gap] };
}

function fillPicker(features) {
  const sorted = [...features].sort((a, b) => a.properties.ntaname.localeCompare(b.properties.ntaname));
  for (const f of sorted) {
    const { nta2020, ntaname, nta_type_label } = f.properties;
    ntaNames[nta2020] = ntaname;
    const option = document.createElement("option");
    option.value = nta2020;
    option.textContent = nta_type_label === "Residential" ? ntaname : `${ntaname} (${nta_type_label.toLowerCase()})`;
    picker.appendChild(option);
  }
}

async function loadJson(path) {
  const response = await fetch(DATA_VERSION ? `${path}?v=${DATA_VERSION}` : path);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

async function init() {
  try {
    const [ntaData, tractData, stopData] = await Promise.all([
      loadJson("data/nta_queens.geojson"),
      loadJson("data/tracts_queens.geojson"),
      loadJson("data/stops_queens.geojson"),
    ]);

    // Drawn in order: tract shading under NTA outlines under stops.
    tractLayer = L.geoJSON(tractData, { style: tractStyle, interactive: false }).addTo(map);

    ntaFeatures = ntaData.features;
    ntaLayer = L.geoJSON(ntaData, {
      style: ntaStyle,
      onEachFeature: (feature, layer) => {
        layer.on("click", () => {
          picker.value = feature.properties.nta2020;
          picker.dispatchEvent(new Event("change"));
        });
      },
    }).addTo(map);

    stops = stopData.features;
    stopLayer = L.geoJSON(stopData, {
      pointToLayer: (feature, latlng) => L.circleMarker(latlng, stopStyle(feature)),
      onEachFeature: (feature, layer) => layer.bindPopup(() => stopPopup(feature)),
    }).addTo(map);

    fillPicker(ntaData.features);
    zoomToSelection();
    render();
  } catch (err) {
    summary.textContent = "Map data didn't load. Run scripts/build_map_data.py, then reload.";
    console.error(err);
  }
}

function stopPopup(feature) {
  const p = feature.properties;
  const div = document.createElement("div");
  const name = document.createElement("strong");
  name.textContent = p.stop_name;
  const detail = document.createElement("div");
  detail.textContent = p[state.snapshot]
    ? `Scheduled local service at ${SNAPSHOT_LABELS[state.snapshot]}`
    : `No scheduled local service at ${SNAPSHOT_LABELS[state.snapshot]}`;
  div.append(name, detail);
  return div;
}

picker.addEventListener("change", () => {
  state.nta = picker.value;
  render();
  zoomToSelection();
});

document.querySelectorAll('input[name="snapshot"]').forEach((input) => {
  input.addEventListener("change", () => {
    state.snapshot = input.value;
    map.closePopup();
    render();
  });
});

document.querySelectorAll('input[name="measure"]').forEach((input) => {
  input.addEventListener("change", () => {
    state.measure = input.value;
    render();
  });
});

updateLegend();
init();
