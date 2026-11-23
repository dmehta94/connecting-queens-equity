# Connecting Queens: Equity in Transit & Access

*Deval Mehta | Independent Practice | Latest as of: November 2026*

**Stack**: Python · GeoPandas · NetworkX · Google BigQuery · Google Cloud Run · Google Cloud Scheduler · MTA BusTime API · Leaflet on GitHub Pages \
**Planned for v2**: HDBSCAN · Leiden Community Analysis · Louvain Community Analysis \
**Data**: Real-time bus positions every 15 minutes across 122 bus routes since April 2026 · Static sources listed in [Data Sources & Schema](#data-sources--schema) \
**Live map**: [dmehta94.github.io/connecting-queens-equity/map/](https://dmehta94.github.io/connecting-queens-equity/map/)

## Table of Contents
1. [What It Does](#what-it-does)
2. [Why I Built This](#why-i-built-this)
3. [What I Learned](#what-i-learned)
4. [Quick Start](#quick-start)
5. [Sample Output](#sample-output)
6. [Technical Details](#technical-details)
   - [Architecture](#architecture)
   - [Data Sources & Schema](#data-sources--schema)
   - [Methodology](#methodology)
   - [Key Methods](#key-methods)
   - [Dependencies](#dependencies)
7. [Limitations](#limitations)
8. [Next Steps – The Policy Product](#next-steps--the-policy-product)
9. [Credits](#credits)
10. [License](#license)

## What It Does
Assesses the equity of bus service across Queens on three dimensions:

* The concentration of low-service, low-access stops within transit-dependent communities.
* The distribution of essential services within walking distance of bus stops.
* The quality of bus service at each stop: excess wait time (actual vs. scheduled gaps between buses), missing service (scheduled vs. observed buses), and crowding (the Bus Time Occupancy field, once collection starts).

## Why I Built This
The original Connecting Queens (my General Assembly Bootcamp Capstone) is a passion project informed by experiences growing up in Eastern Queens, a plea to the MTA to improve our access to public transit and help connect those of us in the transit deserts to the City at large. While it was worthwhile and quantified the issue of low-frequency service, it was still limited in many ways. This iteration addresses its shortcomings.

* **Impacts of the QBNR.** Over the course of 2025, the MTA implemented the Queens Bus Network Redesign, a plan that promised to provide much better service to Queens residents reliant on the bus network. The previous project completed while the QBNR was reaching implementation, so the landscape of the transit network in Queens has changed considerably. I'm looking at a brand new network, in principle, and this project analyzes service and access on the redesigned network.
* **Fully mapping Queens.** I previously considered only those bus routes which serve Eastern Queens, meaning my analysis only compared neighborhoods against the Eastern Queens baseline, establishing five locally relative transit hubs and 17 low-service zones among 27 communities. With data for all 122 routes across Queens now, I can truly draw a comparison across the entire borough to better demonstrate disparities in the transit network.
* **Cloud-native architecture.** The original project ran entirely on my local machine, from collection and scheduling to attempted deployment. Between that and my limited data collection window, I could only gather a week of real-time bus data across 23 bus routes once every 30 minutes. The redesigned Connecting Queens collects data once every 15 minutes across all 122 bus routes serving Queens.
* **Access to Essential Services.** The last iteration of Connecting Queens focused solely on developing frequency-based bus communities using clustering and network analysis methods, as a means of assessing accessibility. This time around, I've accounted for access to essential services within walking distance of bus stops. The services I've considered are drawn from the Universal Declaration of Human Rights.
* **Accounting for transit-dependency.** I had fully depersonalized the previous Connecting Queens, focusing only the movement and locations of buses. This time around, I've derived transit-dependency from ACS data. Socioeconomic wellbeing is now at the heart of Connecting Queens: Equity in Transit & Access.

## What I Learned
### Technical Skills
This was my first time working with Google Cloud Platform (GCP) in this capacity. Google Cloud Run, Cloud Scheduler, and BigQuery work together masterfully to collect and maintain data. The only major change I needed to make to `collection.py` was to switch from writing data to a CSV to writing to BigQuery tables. The same script runs both locally and in Cloud Run without much modification because the authentication is handled outside the code.

### Data Science Insights
*Planned: insights from the proximity, service quality, and equity analyses.*

### MLOps Practices
This is my first time working with a dataset on this scale, which meant monitoring by inspection or during the data cleaning stage before EDA alone no longer cut it. Instead, I adopted the industry-standard practice of looping in health and monitoring checks into my data collection cycle.

### Software Engineering Practices
*Planned: notes on testing, code structure, and review practices.*

### Unexpected Learnings
Permission wrangling on a cloud solutions platform is tough, to say the least. I had a persistent problem with GCP not allowing anyone on the project to create new credentials. As a result, I needed to hold off on authenticating Streamlit Cloud's access to BigQuery and use placeholder DataFrames in the meantime.

### Design Decisions
*Planned: the reasoning behind key methodology choices.*

## Quick Start
*Planned: setup steps to run collection and analysis locally.*

## Sample Output
*See the [live workshop map](https://dmehta94.github.io/connecting-queens-equity/map/). Planned: example images and tables.*

## Technical Details

### Architecture
This is a three-layered project:

**Collection** (`src/collection.py`, `scripts/health_check.sql`): \
Static data is downloaded directly from the sources (found below). 
Real-time vehicle position data is cloud operated:
* Google Cloud Scheduler pings Google Cloud Run every 15 minutes to pull the collection job container image from the Artifact Registry. The container image then runs `collection.py`.
* `collection.py` queries Google BigQuery for active routes, then fetches positions for the vehicles on each active route from the MTA BusTime API, before writing them as rows in the `vehicle_positions` table on BigQuery.

**Analysis** (`src/proximity.py`, `src/transit_dependency.py`, `src/tract_nta.py`, `scripts/service_snapshots.py`, `scripts/transit_dependency.py`):
<!-- TODO: fill in at S3 -->

**Presentation** (`scripts/build_map_data.py`, `map/`; the static workshop map is published with GitHub Pages):
<!-- TODO: fill in at S3 -->

### Data Sources & Schema
<!-- TODO: fill in precise dates where available -->
| Source | Format | As-of Date | Status |
|---|---|---|---|
| MTA GTFS static feed: Queens (April 2026) | GTFS TXT files | Valid Apr 11 – Jun 27, 2026 | In use |
| MTA GTFS static feed: MTA Bus Co (April 2026) | GTFS TXT files | Valid Apr 12 – Jun 27, 2026 | In use |
| MTA GTFS static feed: Queens (October 2026), [gtfs_q.zip](https://rrgtfsfeeds.s3.amazonaws.com/gtfs_q.zip) via [MTA Developer Resources](https://www.mta.info/developers) | ZIP | Valid Oct 12, 2026 – Jan 2, 2027 | In use |
| MTA GTFS static feed: MTA Bus Co (October 2026), [gtfs_busco.zip](https://rrgtfsfeeds.s3.amazonaws.com/gtfs_busco.zip) via [MTA Developer Resources](https://www.mta.info/developers) | ZIP | Valid Oct 12, 2026 – Jan 2, 2027 | In use |
| MTA GTFS static feed: subway (regular), [gtfs_subway.zip](https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip) via [MTA Developer Resources](https://www.mta.info/developers) | ZIP | Valid Nov 1, 2026 – May 15, 2027 | Downloaded; not yet used |
| NYC borough boundaries (NYC Open Data `gthc-hcne`) | GeoJSON | 2024 | In use |
| MTA subway entrances (data.ny.gov `i9wp-a4ja`) | GeoJSON | Dec 5, 2025 | Planned; downloaded, not yet used |
| NYC school locations (NYC Open Data `wg9x-4ke6`) | SHP | Aug 28, 2024 | Planned; downloaded, not yet used |
| FQHCs (DCP Facilities Database, NYC Open Data `ji82-xba5`) | GeoJSON | 2026 | Planned; downloaded, not yet used |
| NYC libraries (Queens Public Library, NYC Open Data `kh3d-xhq7`) | CSV | Sep 10, 2018 | Planned; downloaded, not yet used |
| NYC parks (NYC Open Data `enfh-gkve`) | GeoJSON | Apr 18, 2026 | Planned; downloaded, not yet used |
| USDA SNAP Retailer Location Data ([USDA FNS on ArcGIS Hub](https://usda-snap-retailers-usda-fns.hub.arcgis.com/datasets/USDA-FNS::snap-retailer-location-data)); food-group tags for specialty stores in `data/reference/usda_snap_food_group_tags_2026.csv` | GeoJSON | 2026 | In use |
| NYC Farmers Markets (NYC Open Data `8vwk-6iz2`) | TODO | Not yet pulled | Planned |
| NY State Farmers' Markets (data.ny.gov `qq4h-8p86`) | TODO | Not yet pulled | Planned |
| NYC Walks pedestrian network: Sevtsuk, A., Basu, R., Liu, L., Alhassan, A., & Kollar, J. (2026). Spatial distribution of foot traffic in New York City and applications for urban planning. *Nature Cities*, 3, 136–145. [doi:10.1038/s44284-025-00383-y](https://doi.org/10.1038/s44284-025-00383-y). Used by permission of the authors, with credit to the paper; not redistributed in this repo. | GeoJSON | 2026 | In use |
| USDA Food Access Atlas | XLSX | 2019 data (USDA ERS last updated Apr 27, 2021) | Validation only |
| [ACS 2020–2024 5-year estimates](https://www.census.gov/data/developers/data-sets/acs-5year.html) (Census API): B08201, B25070, B25091, B25092, B08301 | CSV | 2020–2024 | In use; BigQuery load pending |
| [2020 Decennial Census Redistricting Data (P.L. 94-171)](https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html), used to confirm zero-household tracts | Census API; used as a check, not stored | 2020 | Validation only |
| [DCP 2020 Census Tracts to 2020 NTAs and CDTAs Equivalency](https://data.cityofnewyork.us/d/hm78-6dwm) (NYC Open Data `hm78-6dwm`) | CSV | 2020 geographies (published Sep 2021) | In use; BigQuery load pending |
| [TIGER/Line 2022 census tracts](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.2022.html), New York ([tl_2022_36_tract.zip](https://www2.census.gov/geo/tiger/TIGER2022/TRACT/tl_2022_36_tract.zip)) | SHP | 2022 | In use |
| HUD AMI thresholds | XLSX | FY2025 | TODO |
| NYC NTA boundaries (NYC Open Data `9nt8-h7nd`) | GeoJSON | 2020 | In use |
| MTA BusTime Vehicle Monitoring (real-time) | API | Ongoing | In use |

For full column-level schema documentation, see `schemas/`.

### Methodology
*Planned: how proximity, service quality, and transit dependency are measured and combined.*

### Key Methods
*Planned: the spatial, statistical, and network methods used.*

### Dependencies
See `requirements.txt`. Key packages: <!-- TODO: Fill in later, once key packages are determined -->

## Limitations
* **Streamlit Cloud BigQuery credentials blocked by IAM policy**: a project-level permission prevents any user from generating a credential file to allow Streamlit Cloud to read data from BigQuery. This no longer blocks the workshop tool, which is a static, precomputed map with no live BigQuery connection.
* **Social services layer evaluated and dropped**: While I would have wanted to round out my essential service access coverage with other social services, no NYC Open Data dataset passes my coverage, point-level geometry, and recency criteria simultaneously.
* **Queens Library branch data current as of September 2018**: The data here is stable and treated as current, though the age should be noted.
* **Food access uses point-level SNAP retailer data**: the tract-level USDA Food Access Atlas method is retired. Food access is measured from point-level SNAP retailer locations, and the Atlas is kept only as a validation check.
* **No saved GTFS schedule covers Jun 28 – Oct 11, 2026**: the April 2026 feeds end Jun 27, 2026, and the October 2026 feeds begin Oct 12, 2026. Service measures for bus positions collected in that period can't be matched to a schedule. Final reported numbers use one shared window; its start date is still open, since no saved schedule covers Oct 1–11, 2026, and crowding data starts once Occupancy collection is deployed.

## Next Steps – The Policy Product
* **Workshop**: *The Connecting Queens Digital Scavenger Hunt: A Single Borough, a Single Fare*, a one-hour hands-on hybrid workshop proposed for NYC Open Data Week 2027.
* **Workshop map (live)**: [dmehta94.github.io/connecting-queens-equity/map/](https://dmehta94.github.io/connecting-queens-equity/map/). A static, precomputed map of Queens with neighborhood (NTA) views, an NTA picker, and a weekday/Sunday toggle. It shows a transit dependency layer by census tract and a food access layer at each bus stop. At borough zoom, each NTA is shown as one circle; NTAs with no households (or too few served stops) are shown as hollow circles.
* The map needs no login and is built to work on phones, venue Wi-Fi, or a video call.
* Service quality layers (excess wait time, missing service, crowding) are added to the map as they are completed.

## Credits
I'm responsible for the research question, methodology, and every design decision. I used Claude (Anthropic) in three distinct ways:
1.  Claude helped me organize my thoughts, maintain the architecture of the project, and keep notes so I could write prose more cleanly and coherently. Claude also wrote the first draft of the CLAUDE.md file.
2.  I wrote the proximity and cloud collection pipelines myself, as well as the Dockerfile, BigQuery schemas, routes and stops loaders, and original SQL health checks, guided by Claude as I needed in Socratic sessions. This practice ensured that the project never strayed too far or expanded too much in scope.
3.  Claude Code did a good share of the mechanical and front-end work under my direction: the data downloads and read-only checks, the transit dependency analysis and tract-to-NTA junction, the map's data build script and every layer of the workshop map, the pytest setup and parser tests for the collection pipeline, and the service snapshots and their split into local and express service. It also drafted the SNAP food-group tags, which I reviewed and corrected, and made later edits to the README and CLAUDE.md files. I reviewed and approved every change before pushing, independently verified the headline numbers, and spot-checked scores for essential service access. See [CLAUDE.md](CLAUDE.md) for how agentic work is scoped and reviewed.

Real-time bus data from the [MTA BusTime API](https://bustime.mta.info/wiki/Developers/Index); schedule data from MTA GTFS feeds; geographic and essential service data from [NYC Open Data](https://opendata.cityofnewyork.us/) and [USDA SNAP retailer data](https://usda-snap-retailers-usda-fns.hub.arcgis.com/datasets/USDA-FNS::snap-retailer-location-data); transit dependency from the U.S. Census Bureau's American Community Survey; and walking distances from the NYC Walks pedestrian network ([Sevtsuk et al., *Nature Cities*, 2026](https://doi.org/10.1038/s44284-025-00383-y)), used by permission of the authors. Other sources are listed in [Data Sources & Schema](#data-sources--schema).

## License
MIT License. See `LICENSE` for details.

**Contact:** [github.com/dmehta94](https://github.com/dmehta94)