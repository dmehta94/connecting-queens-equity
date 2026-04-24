# Connecting Queens: Equity in Transit & Access

*Deval Mehta | Freelance Data Scientist | Latest as of: April 2026*

**Stack**: Python · GeoPandas · Google BigQuery · Google Cloud Run · Google Cloud Scheduler · HDBSCAN · Leiden Community Analysis · Louvain Community Analysis · MTA BusTime API \
**Data**: 4 weeks of real-time bus location data across 122 bus routes · <!-- Insert all sources of static data here -->

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
8. [Next Steps – The Policy Product](#next-steps---the-policy-product)
9. [Credits](#credits)
10. [License](#license)

## What It Does
Assesses the equity of bus service across Queens on three dimensions:

* The concentration of low-service, low-access stops within transit-dependent communities.
* The distribution of essential services within walking distance of bus stops.
* The frequency of bus service at each stop.

## Why I Built This
The original Connecting Queens (my General Assembly Bootcamp Capstone) is a passion project informed by experiences growing up in Eastern Queens, a plea to the MTA to improve our access to public transit and help connect those of us in the transit deserts to the City at large. While it was worthwhile and quantified the issue of low-frequency service, it was still limited in many ways. This iteration addresses its shortcomings.

* **Impacts of the QBNR.** Over the course of 2025, the MTA implemented the Queens Bus Network Redesign, a plan that promised to provide much better service to Queens residents reliant on the bus network. The previous project completed while the QBNR was reaching implementation, so the landscape of the transit network in Queens has changed considerably. I'm looking at a brand new network, in principle, and this project assesses the success of the QBNR.
* **Fully mapping Queens.** I previously considered only those bus routes which serve Eastern Queens, meaning my analysis only compared neighborhoods against the Eastern Queens baseline, establishing five locally relative transit hubs and 17 low-service zones among 27 communities. With data for all 122 routes across Queens now, I can truly draw a comparison across the entire borough to better demonstrate disparities in the transit network.
* **Cloud-native architecture.** The original project ran entirely on my local machine, from collection and scheduling to attempted deployment. Between that and my limited data collection window, I could only gather a week of real-time bus data across 23 bus routes once every 30 minutes. The redesigned Connecting Queens collects data once every 15 minutes across all 122 bus routes serving Queens.
* **Access to Essential Services.** The last iteration of Connecting Queens focused solely on developing frequency-based bus communities using clustering and network analysis methods, as a means of assessing accessibility. This time around, I've accounted for access to essential services within walking distance of bus stops. The services I've considered are drawn from the Universal Declaration of Human Rights.
* **Accounting for transit-dependency.** I had fully depersonalized the previous Connecting Queens, focusing only the movement and locations of buses. This time around, I've derived transit-dependency from ACS data. Socioeconomic wellbeing is now at the heart of Connecting Queens: Equity in Transit & Access.

## What I Learned

## Quick Start

## Sample Output

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
| Source | Format | As-of Date |
|---|---|---|
| MTA GTFS static feed — Queens | ZIP | 2025 |
| MTA GTFS static feed — MTA Bus Co | ZIP | 2025 |
| NYC borough boundaries | GeoJSON | 2024 |
| MTA subway entrances | GeoJSON | Dec 5, 2025 |
| NYC school locations | SHP | Aug 28, 2024 |
| FQHCs (DCP Facilities Database) | GeoJSON | 2026 |
| NYC libraries (Queens Public Library) | CSV | Sep 10, 2018 |
| NYC parks | GeoJSON | Apr 18, 2026 |
| USDA Food Access Atlas | XLSX | Jan 5, 2025 |
| ACS variables + TIGER/Line census tracts | SHP | 2022 |
| HUD AMI thresholds | XLSX | FY2025 |
| NYC NTA boundaries | GeoJSON | 2020 |
| MTA BusTime Vehicle Monitoring (real-time) | API | Ongoing |

For full column-level schema documentation, see `schemas/`.

### Methodology

### Key Methods

### Dependencies
See `requirements.txt`. Key packages: <!-- TODO: Fill in later, once key packages are determined -->

## Limitations

## Next Steps – The Policy Product

## Credits
Data collection and analysis by Deval Mehta. Claude (Anthropic) served as a Socratic guide throughout the build — explaining concepts, asking questions, and letting me write the code.  Real-time and static bus data from the [MTA BusTime API](https://bustime.mta.info/wiki/Developers/Index). Geographic boundary data from [NYC Open Data](https://opendata.cityofnewyork.us/).

## License
MIT License. See `LICENSE` for details.

**Contact:** [github.com/dmehta94](https://github.com/dmehta94)