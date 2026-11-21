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

// Served stops are colored by food_score: plate shares come in quarters, so one class per
// quarter. Darker = less food, matching the tracts' darker = more need. The "None" class
// also differs in outline (white ring vs. ink ring), so it reads without color.
// Light classes (ColorBrewer YlOrBr) carry an ink ring for light tracts; "None" is ink with
// a white ring for dark tracts.
const FOOD_CLASSES = [
  { score: 0, label: "None", fill: "#1C1C1A", ring: "#ffffff" },
  { score: 0.25, label: "A quarter plate", fill: "#d95f0e", ring: "#1C1C1A" },
  { score: 0.5, label: "Half a plate", fill: "#fe9929", ring: "#1C1C1A" },
  { score: 0.75, label: "Three quarters of a plate", fill: "#fec44f", ring: "#1C1C1A" },
  { score: 1, label: "A full plate", fill: "#fff7bc", ring: "#1C1C1A" },
];

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

// The class at or below a score (food_score is rounded to 2 decimals in the data).
function foodClass(score) {
  let match = FOOD_CLASSES[0];
  for (const c of FOOD_CLASSES) if (score >= c.score) match = c;
  return match;
}

function stopStyle(feature) {
  const served = feature.properties[state.snapshot];
  const inPick = !state.nta || feature.properties.nta2020 === state.nta;
  const food = foodClass(feature.properties.food_score);
  return {
    radius: inPick ? 4 : 2.5,
    color: served ? food.ring : getCss("--unserved"),
    weight: served && food.score > 0 ? 1 : 1.5,
    fillColor: served ? food.fill : "#ffffff",
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
    : `${place}: ${served.toLocaleString("en-US")} of ${inArea.length.toLocaleString("en-US")} bus stops have scheduled local service at ${SNAPSHOT_LABELS[state.snapshot]}.`;

  // Food figure: counts of served stops, not an average of scores.
  const foodSummary = document.getElementById("food-summary");
  const noFood = inArea.filter((s) => s.properties[state.snapshot] && s.properties.food_score === 0).length;
  foodSummary.textContent = served === 0
    ? `${place}: no served stops in this snapshot.`
    : `${noFood.toLocaleString("en-US")} of ${served.toLocaleString("en-US")} served stops (${Math.round(100 * noFood / served)}%) have no nutritious, affordable food within a 5-minute walk.`;
}

function fillFoodLegend() {
  document.getElementById("food-legend").replaceChildren(...FOOD_CLASSES.map((c) => {
    const li = document.createElement("li");
    const dot = document.createElement("span");
    dot.className = "dot";
    dot.style.background = c.fill;
    dot.style.border = `${c.score === 0 ? 2 : 1}px solid ${c.ring}`;
    if (c.score === 0) dot.style.boxShadow = `0 0 0 1px ${c.fill}`;
    const label = document.createElement("span");
    label.textContent = c.label;
    li.append(dot, label);
    return li;
  }));
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
      // maxWidth keeps the longer food popup inside a 375 px phone screen.
      onEachFeature: (feature, layer) => layer.bindPopup(() => stopPopup(feature), { maxWidth: 260 }),
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

  const lines = [
    `Nutritious, affordable food within a 5-minute walk: ${foodClass(p.food_score).label.toLowerCase()}`,
    `Grocery and food stores within a 5-minute walk: ${p.food_store_count}`,
    `Other food retailers (convenience stores, pharmacies, dollar stores, Restaurant Meals Program restaurants): ${p.context_store_count}`,
  ];
  for (const text of lines) {
    const line = document.createElement("div");
    line.className = "food-line";
    line.textContent = text;
    div.append(line);
  }
  if (p.network_flag) {
    const note = document.createElement("div");
    note.className = "food-note";
    note.textContent = "This stop is far from the mapped sidewalks, so this score is less reliable.";
    div.append(note);
  }
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
fillFoodLegend();
init();
