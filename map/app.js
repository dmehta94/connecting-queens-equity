// Workshop map scaffold: Queens NTAs, bus stops, NTA picker, weekday/Sunday toggle.
// Static: reads precomputed GeoJSON from data/ (built by scripts/build_map_data.py).

const QUEENS_CENTER = [40.705, -73.82];
const SNAPSHOT_LABELS = { weekday: "weekday midday", sunday: "Sunday midday" };

const state = { nta: "", snapshot: "weekday" };

const map = L.map("map", { preferCanvas: true, zoomControl: true }).setView(QUEENS_CENTER, 11);

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  maxZoom: 19,
}).addTo(map);

const picker = document.getElementById("nta-picker");
const summary = document.getElementById("summary");

let ntaLayer;
let stopLayer;
let stops = [];
const ntaNames = {};

function ntaStyle(feature) {
  const selected = feature.properties.nta2020 === state.nta;
  return {
    color: selected ? getCss("--nta-highlight") : "#7b8794",
    weight: selected ? 3 : 1,
    fillOpacity: selected ? 0.06 : 0,
    fillColor: getCss("--nta-highlight"),
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

function getCss(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
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
  if (ntaLayer) ntaLayer.setStyle(ntaStyle);
  if (stopLayer) stopLayer.setStyle(stopStyle);
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
  const response = await fetch(path);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

async function init() {
  try {
    const [ntaData, stopData] = await Promise.all([
      loadJson("data/nta_queens.geojson"),
      loadJson("data/stops_queens.geojson"),
    ]);

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

init();
