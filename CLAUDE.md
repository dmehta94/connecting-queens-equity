# Connecting Queens: Equity in Transit & Access (v2)

## What this project is
A transit equity analysis of the Queens bus network, rooted in Dev's experience growing up in Bellerose, Eastern Queens. v2 is a GCP-native rebuild of his GA capstone (v1 repo: dmehta94/project-capstone-transit-inequity-eastern-queens). The near-term goal is an NYC Open Data Week 2027 workshop proposal, due Dec 6, 2026 (Dev's target: submit Nov 20).

Research question: Which bus stops have the worst combined physical access to essential services and the lowest service quality, and are those stops disproportionately located in transit-dependent communities?

## Who writes what
Dev and Claude split the work. Claude Code builds only the agentic tasks below. For a guided task, do not write the implementation: explain that it is a guided task and suggest Dev take it to his chat project.

Agentic (Claude writes, Dev reviews and approves):
- Health check: run `scripts/health_check.sql`, review `collection_log`, report row volume and costs
- Add Bus Time `Occupancy` to the parser and schema, then redeploy collection
- GTFS downloads: current MTA bus feeds and the subway feed
- Service snapshots: which stops have scheduled service at weekday midday and on Sunday
- Static workshop map (one screen; see "Workshop map" below)
- Transit dependency by tract (ACS pull and joins)
- Missing service (scheduled vs. observed), reusing Dev's schedule-matching code from excess wait time
- Library data refresh and the venue reachability score
- Dataset list with URLs for the proposal form

Guided (Dev writes; do not implement):
- Proximity (buffers, spatial joins, CRS handling)
- EDA and excess wait time, including how buses are matched to the schedule
- Equity overlay by NTA
- Connectivity graph and community detection (HDBSCAN, Leiden vs. Louvain)

Dev only: proposal prose, lesson plan, task card, README "What I Learned" entries.

## Ship review protocol
Nothing ships until Dev could explain it himself. "Ships" means committed, pushed, written to BigQuery, deployed, or run against real services.

Before writing anything:
- Restate the goal in one sentence and list assumptions. Ask instead of picking silently.
- State the risk tier and a short plan where each step has a verification check.

Risk tiers (use the highest any part touches; when in doubt, go higher):
- Low: docs, comments, tests, or a small fix in one function with no data, credential, service, or public-output change. Gate: tests pass; show the diff summary and test output.
- Medium: behavior changes, new features, parsing or classification rules, dependency changes, multi-file refactors. Gate: tests pass, including at least one new test that fails without the change; an explanation; Dev approves before commit.
- High: anything touching BigQuery tables or schemas, credentials or `.env`, the public repo's contents, Cloud Run or Cloud Scheduler, anything that runs unattended, or deleting files or history. Gate: everything in Medium, plus a dry run or a run on a copy first, a clear way to undo, and Dev's explicit approval of the specific action before it runs.

Verify before claiming done: run it, read the output, quote the result (test counts, row counts, before/after queries). If something can't be verified from here, say so and give Dev the exact command.

Review rule: report an issue only with a concrete failing scenario. Check for changes outside the request, leftover files, and line-ending or encoding changes.

Explain before approval (Medium and High), in five lines: what changed; why; how it was verified; what could go wrong and how it would show up; how to undo it. Then offer two or three quick questions Dev should be able to answer about the change.

Record who did what: commit messages say what changed and why, with a Co-Authored-By line for Claude.

Known gotcha: this Windows repo uses CRLF line endings. Preserve each file's existing line endings.

## Conventions
- Look up external URLs, API parameters, census table IDs, and dataset details instead of guessing.
- Unit tests apply to `src/` modules. One-time `scripts/` are verified by BigQuery row counts.
- Commit per function.
- Use "essential service," never "amenity," in user-facing text.
- American spelling.
- Keep changes small and scoped to the task asked for. Log out-of-scope ideas in the session summary instead of pursuing them.

## Infrastructure
- GCP project `connecting-queens-equity`; BigQuery dataset `connecting_queens_equity`.
- Collection: `src/collection.py`, containerized as a Cloud Run Job, triggered by Cloud Scheduler every 15 minutes (`*/15 * * * *`), running since April 2026. Functions: `fetch_vehicle_positions()`, `parse_vehicle_positions()`, `insert_vehicle_positions()`, `log_collection_sweep()`. Image built with `gcloud builds submit` and pushed to Artifact Registry.
- Credentials: Application Default Credentials locally. Service account `connecting-queens-equity-sa` is attached to Cloud Run (bigquery.dataEditor, bigquery.jobUser, run.invoker). Key creation is blocked by `constraints/iam.disableServiceAccountKeyCreation`.
- `.env` (gitignored): `MTA_API_KEY`, `CENSUS_API_KEY`. Never print or commit their values.
- Layout: `src/` (collection, clustering, proximity, equity, visualization), `app/` (Streamlit scaffold, six stubbed tabs; predates the re-scope), `scripts/` (one-time scripts), `schemas/` (BigQuery schema JSON), `data/raw/` (static sources; gitignored).

## Data
- 122 Queens routes (MTA's headline 124 double-counts two variants). 8 shuttles excluded: Q92, Q93, Q96, Q97, Q107, Q108, Q109, Q121.
- SIRI LineRef formats: `MTA NYCT_Q44%2B` (NYCT), `MTABC_Q06` (MTABC; leading zeros preserved).
- BigQuery tables: `routes` (122), `stops` (3,250; 2,973 in Queens), `stop_routes` (5,332), `vehicle_positions` (partitioned on `collected_at`, clustered on `route_id`; includes `direction_ref`, `next_stop_ref`, `bearing`), `tract_nta` (junction; tracts are not assigned to a single NTA), `proximity_scores`, `census_tracts`, `collection_log`.
- Bus Time's optional `Occupancy` field (full / seatsAvailable / standingAvailable) is currently dropped by the parser. Rows before the fix will have it null.
- Static sources in `data/raw/`, named `{source}_{descriptor}_{asof_YYYY}.{ext}`, never modified after pull. Read directly at analysis time; only derived outputs go to BigQuery.
- Library data is from Sept 2018 (NYC Open Data `kh3d-xhq7`).
- Only the April 2026 GTFS is saved. Always match observations against the schedule in effect at that time.

## Locked methodology
- Service quality: excess wait time (actual vs. scheduled gaps), missing service (scheduled vs. observed buses), and crowding (Occupancy, Oct 2026 onward).
- Final reported numbers use one shared window from Oct 2026 onward. April–Oct 2026 data is only for building and testing methods.
- Transit dependency: households with fewer vehicles than household members (ACS B08201), weighted by housing cost burden for renters (B25070) and owners (B25091; medians in B25092). Commute mode (B08301) is a validation check only. No separate student component.
- Proximity is time-aware, with two snapshots: weekday midday and Sunday. A stop counts only if it has scheduled service in that snapshot.
- Essential service layers: six, grounded in UDHR articles, weighted from Neutens 2015 and Grengs 2010.
- Public-facing geography: NTAs, all of Queens.
- Connectivity graph: Queens bus routes plus subway links within Queens only. No LIRR, no subway outside Queens. Community definitions use only spatial and network structure, never outcome variables.

## Workshop map
- One screen, two or three controls: NTA picker and a weekday/Sunday toggle.
- Works on phones, venue Wi-Fi, or a video call, with no login. Precomputed and static, not a live BigQuery connection.
- Starts with proximity; service-quality layers are added as they are completed.

## Open issues
- `parse_errors` in `collection_log` is initialized but never incremented.
- "Amenity" remains in `schemas/proximity_scores.json`.
- Subway GTFS still needed for the connectivity graph.