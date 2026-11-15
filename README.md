# Connecting Queens: Equity in Transit & Access

*Deval Mehta | Freelance Data Scientist | Latest as of: April 2026*

**Stack**: Python · GeoPandas · Google BigQuery · Google Cloud Run · Google Cloud Scheduler · HDBSCAN · Leiden Community Analysis · Louvain Community Analysis · MTA BusTime API \
**Data**: Real-time bus positions every 15 minutes across 122 bus routes since April 2026 · Static sources listed in [Data Sources & Schema](#data-sources--schema)

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
*Planned: example maps and tables from the workshop map.*

## Technical Details

### Architecture
This is a three-layered project:

**Collection** (Collection file names go here)<!-- TODO: fill in at S6 -->: \
Static data is downloaded directly from the sources (found below). 
Real-time vehicle position data is cloud operated:
* Google Cloud Scheduler pings Google Cloud Run every 15 minutes to pull the collection job container image from the Artifact Registry. The container image then runs `collection.py`.
* `collection.py` queries Google BigQuery for active routes, then fetches positions for the vehicles on each active route from the MTA BusTime API, before writing them as rows in the `vehicle_positions` table on BigQuery.

**Analysis** (Analysis file names go here):
<!-- TODO: fill in at S3 -->

**Presentation** (Presentation file names go here):
<!-- TODO: fill in at S3 -->

### Data Sources & Schema
<!-- TODO: fill in precise dates where available -->
| Source | Format | As-of Date | Status |
|---|---|---|---|
| MTA GTFS static feed: Queens (April 2026) | GTFS TXT files | Valid Apr 11 – Jun 27, 2026 | In use |
| MTA GTFS static feed: MTA Bus Co (April 2026) | GTFS TXT files | Valid Apr 12 – Jun 27, 2026 | In use |
| MTA GTFS static feed: Queens (October 2026) | ZIP | Valid Oct 12, 2026 – Jan 2, 2027 | In use |
| MTA GTFS static feed: MTA Bus Co (October 2026) | ZIP | Valid Oct 12, 2026 – Jan 2, 2027 | In use |
| MTA GTFS static feed: subway (regular) | ZIP | Valid Nov 1, 2026 – May 15, 2027 | Planned |
| NYC borough boundaries | GeoJSON | 2024 | In use |
| MTA subway entrances | GeoJSON | Dec 5, 2025 | In use |
| NYC school locations | SHP | Aug 28, 2024 | In use |
| FQHCs (DCP Facilities Database) | GeoJSON | 2026 | In use |
| NYC libraries (Queens Public Library) | CSV | Sep 10, 2018 | In use |
| NYC parks | GeoJSON | Apr 18, 2026 | In use |
| USDA SNAP Retailer Locator | TODO | Not yet pulled | Planned |
| NYC Farmers Markets (NYC Open Data `8vwk-6iz2`) | TODO | Not yet pulled | Planned |
| NY State Farmers' Markets (data.ny.gov `qq4h-8p86`) | TODO | Not yet pulled | Planned |
| NYC Walks pedestrian network (Sevtsuk et al., *Nature Cities*, 2026) | TODO | Not yet pulled | Planned |
| USDA Food Access Atlas | XLSX | Jan 5, 2025 | Validation only |
| ACS variables + TIGER/Line census tracts | SHP | 2022 | In use |
| HUD AMI thresholds | XLSX | FY2025 | TODO |
| NYC NTA boundaries | GeoJSON | 2020 | In use |
| MTA BusTime Vehicle Monitoring (real-time) | API | Ongoing | In use |

For full column-level schema documentation, see `schemas/`.

### Methodology
*Planned: how proximity, service quality, and transit dependency are measured and combined.*

### Key Methods
*Planned: the spatial, statistical, and network methods used.*

### Dependencies
See `requirements.txt`. Key packages: <!-- TODO: Fill in later, once key packages are determined -->

## Limitations
* **Streamlit Cloud BigQuery credentials blocked by IAM policy**: a project-level permission prevents any user from generating a credential file to allow Streamlit Cloud to read data from BigQuery. This no longer blocks the workshop tool, which will be a static, precomputed map with no live BigQuery connection.
* **Social services layer evaluated and dropped**: While I would have wanted to round out my essential service access coverage with other social services, no NYC Open Data dataset passes my coverage, point-level geometry, and recency criteria simultaneously.
* **Queens Library branch data current as of September 2018**: The data here is stable and treated as current, though the age should be noted.
* **Food access uses point-level SNAP retailer data**: the tract-level USDA Food Access Atlas method is retired. Food access will be measured from point-level SNAP retailer locations, and the Atlas is kept only as a validation check.
* **No saved GTFS schedule covers Jun 28 – Oct 11, 2026**: the April 2026 feeds end Jun 27, 2026, and the October 2026 feeds begin Oct 12, 2026. Service measures for bus positions collected in that period can't be matched to a schedule. Final reported numbers use a shared window from October 2026 onward.

## Next Steps – The Policy Product
* A hands-on workshop for NYC Open Data Week 2027 (proposal in progress).
* A static, precomputed map of Queens by NTA for the workshop, with an NTA picker and a weekday/Sunday toggle.
* The map needs no login and is built to work on phones, venue Wi-Fi, or a video call.
* The map starts with proximity to essential services; service quality layers are added as they are completed.

## Credits
I'm responsible for the research question, methodology, every design decision, and the code written in guided sessions, where Claude (Anthropic) served as a Socratic guide. Claude Code carried out agentic tasks (data downloads, health checks, service snapshots, documentation structure) under my direction; I reviewed and approved every change before committing. See `CLAUDE.md` for how agentic work is scoped and reviewed.

Real-time bus data from the [MTA BusTime API](https://bustime.mta.info/wiki/Developers/Index); schedule data from MTA GTFS feeds; geographic and essential service data from [NYC Open Data](https://opendata.cityofnewyork.us/) and the other sources listed in [Data Sources & Schema](#data-sources--schema).

## License
MIT License. See `LICENSE` for details.

**Contact:** [github.com/dmehta94](https://github.com/dmehta94)