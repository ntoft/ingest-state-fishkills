// ingest-state-fishkills — scrape MDE fish-kill portal + parse annual PDF
// reports for Chesapeake Bay incidents.
//
// Data constraints (as of 2026-04):
//   - MD DNR / MDE has NO machine-readable live feed. Reports come in by phone.
//   - The only public archive is the MDE annual PDF report (one per year).
//   - VA DEQ blocks scraping via WAF.
//
// Therefore this sprite does discovery + PDF parsing:
//   1. Fetch the MDE Fish Kills landing page.
//   2. Extract any linked PDF under /Fish_Kill_Reports/.
//   3. Download + text-extract each PDF with pdf-parse.
//   4. Heuristically extract incident rows (date + waterbody + cause patterns).
//   5. Emit FishKillReport things (home repo) + FishKillEvent things
//      (cross-repo into chesapeake-attribution) for incidents in the Chesapeake bbox.
//
// The CRON cadence is daily — PDFs update at most once a year, daily discovery
// is cheap, and skipExisting keeps reruns idempotent.

import type { AddOperation, Operation } from "@warmhub/sdk-ts";
import { clientFromEnv, homeRepo, splitRepo } from "./warmhub";
// pdf-parse is CJS — use default import under Bun.
import pdfParse from "pdf-parse";

const MDE_URL = "https://mde.maryland.gov/programs/water/FishandShellfish/Pages/mdfishkills.aspx";
const MDE_ORIGIN = "https://mde.maryland.gov";
const CURATED_REPO = "fish-kill-attribution/chesapeake-attribution";
const CHESAPEAKE_WATERSHED = "Watershed/chesapeake-bay";
const BBOX = { minLat: 36.8, maxLat: 39.7, minLon: -77.5, maxLon: -75.3 };

interface Incident {
  reportId: string;
  date: string;          // ISO YYYY-MM-DD
  locationName: string;
  waterbody: string | null;
  species: string | null;
  mortality: number;
  officialCause: string | null;
  lat: number | null;
  lon: number | null;
  rawPayload: string;
  sourcePdf: string;
}

// Discover PDF URLs listed on the MDE landing page.
async function discoverPdfUrls(): Promise<string[]> {
  const res = await fetch(MDE_URL, { headers: { "User-Agent": "fish-kill-attribution-sprite/0.1" } });
  if (!res.ok) throw new Error(`MDE landing ${res.status}: ${await res.text()}`);
  const html = await res.text();
  const matches = html.matchAll(/\/programs\/water\/FishandShellfish\/Documents\/Fish_Kill_Reports\/([^"'?#\s<>]+\.pdf)/gi);
  const urls = new Set<string>();
  for (const m of matches) {
    urls.add(`${MDE_ORIGIN}/programs/water/FishandShellfish/Documents/Fish_Kill_Reports/${m[1]}`);
  }
  return [...urls];
}

async function fetchPdfText(url: string): Promise<string> {
  const res = await fetch(url, { headers: { "User-Agent": "fish-kill-attribution-sprite/0.1" } });
  if (!res.ok) throw new Error(`PDF ${res.status}: ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const parsed = await pdfParse(buf);
  return parsed.text ?? "";
}

// Very lightweight heuristic incident parser.
// MDE annual PDFs list incidents in rough tables with columns like:
//   Date | Location | Waterbody | Species | Estimated # | Cause
// Rather than model the whole PDF, we find dated rows and capture the
// following chunk up to the next date line.
function parseIncidents(text: string, sourcePdf: string): Incident[] {
  const year = (sourcePdf.match(/(\d{4})/) ?? [])[1] ?? new Date().getUTCFullYear().toString();
  const dateRe = /\b(\d{1,2})\/(\d{1,2})\/(\d{2,4})\b/g;
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const incidents: Incident[] = [];

  let currentDate: string | null = null;
  let buffer: string[] = [];

  const flush = () => {
    if (!currentDate || buffer.length === 0) return;
    const payload = buffer.join(" ").slice(0, 4000);
    const mortalityMatch = payload.match(/\b(\d{1,3}(?:,\d{3})*|\d+)\b\s*(?:fish|dead|mortalit|kill)/i);
    const mortality = mortalityMatch ? Number(mortalityMatch[1].replace(/,/g, "")) : 0;
    const waterbodyMatch = payload.match(/\b(?:River|Creek|Bay|Pond|Lake|Reservoir|Branch|Run)\b[^,\.]{0,40}/i);
    const speciesMatch = payload.match(/\b(?:Menhaden|Striped Bass|Rockfish|Catfish|Bluegill|Sunfish|Perch|Shad|Herring|Carp|Trout|Largemouth|Smallmouth|Minnow|Eel|Bass|Shiner|Killifish|Silverside|Anchovy)\b/i);
    const causeMatch = payload.match(/\b(?:low (?:DO|dissolved oxygen|oxygen)|algal bloom|Karlodinium|HAB|disease|spill|runoff|pesticide|ammonia|chlorine|thermal|stormwater|natural causes?|unknown)\b/i);

    incidents.push({
      reportId: `MDE-${currentDate}-${incidents.length}`,
      date: currentDate,
      locationName: (waterbodyMatch?.[0] ?? buffer[0] ?? "Unknown").slice(0, 200),
      waterbody: waterbodyMatch?.[0]?.trim() ?? null,
      species: speciesMatch?.[0] ?? null,
      mortality: Number.isFinite(mortality) ? mortality : 0,
      officialCause: causeMatch?.[0] ?? null,
      lat: null,
      lon: null,
      rawPayload: payload,
      sourcePdf,
    });
  };

  for (const line of lines) {
    dateRe.lastIndex = 0;
    const dateHit = dateRe.exec(line);
    if (dateHit) {
      flush();
      const [_, m, d, y] = dateHit;
      const fullYear = y.length === 4 ? Number(y) : 2000 + Number(y);
      const mm = String(Number(m)).padStart(2, "0");
      const dd = String(Number(d)).padStart(2, "0");
      currentDate = `${fullYear}-${mm}-${dd}`;
      buffer = [line];
      continue;
    }
    if (currentDate) buffer.push(line);
  }
  flush();

  // Sanity filter — only keep incidents whose date year matches the file year.
  const fileYear = Number(year);
  return incidents.filter((i) => {
    const y = Number(i.date.slice(0, 4));
    return Number.isFinite(y) && (Math.abs(y - fileYear) <= 1);
  });
}

function inBbox(lat: number | null, lon: number | null): boolean {
  if (lat == null || lon == null) return false;
  return lat >= BBOX.minLat && lat <= BBOX.maxLat && lon >= BBOX.minLon && lon <= BBOX.maxLon;
}

function buildReportOps(incidents: Incident[]): AddOperation[] {
  const ops: AddOperation[] = [];
  for (const i of incidents) {
    ops.push({
      operation: "add",
      kind: "thing",
      name: `FishKillReport/${i.reportId}`,
      data: {
        report_id: i.reportId,
        date: i.date,
        lat: i.lat ?? 0,
        lon: i.lon ?? 0,
        location_name: i.locationName,
        waterbody: i.waterbody,
        status: "reported",
        source_agency: "md-dnr",
        primary_species: i.species,
        estimated_mortality: i.mortality,
        official_cause: i.officialCause,
        raw_payload: i.rawPayload,
      },
      skipExisting: true,
    });
  }
  return ops;
}

function buildEventOps(incidents: Incident[], homeRepoName: string): AddOperation[] {
  const ops: AddOperation[] = [];
  // Bootstrap: ensure the Chesapeake watershed placeholder exists (skipExisting).
  ops.push({
    operation: "add",
    kind: "thing",
    name: CHESAPEAKE_WATERSHED,
    data: {
      name: "Chesapeake Bay",
      region: "Mid-Atlantic",
      huc_code: "02060000",
      bounds_geojson: JSON.stringify({
        type: "Polygon",
        coordinates: [[
          [BBOX.minLon, BBOX.minLat], [BBOX.maxLon, BBOX.minLat],
          [BBOX.maxLon, BBOX.maxLat], [BBOX.minLon, BBOX.maxLat],
          [BBOX.minLon, BBOX.minLat],
        ]],
      }),
    },
    skipExisting: true,
  });

  for (const i of incidents) {
    if (!inBbox(i.lat, i.lon)) continue;
    const slug = `${i.date}-${i.reportId}`.replace(/[^a-zA-Z0-9-]/g, "-").toLowerCase();
    ops.push({
      operation: "add",
      kind: "thing",
      name: `FishKillEvent/${slug}`,
      data: {
        lat: i.lat,
        lon: i.lon,
        date: i.date,
        status: "reported",
        watershed: CHESAPEAKE_WATERSHED,
        location_name: i.locationName,
        source_report: `wh:${homeRepoName}/FishKillReport/${i.reportId}`,
        primary_species: i.species,
        estimated_mortality: i.mortality,
      },
      skipExisting: true,
    });
  }
  return ops;
}

async function main() {
  const client = clientFromEnv();
  const home = homeRepo();
  const { orgName, repoName } = splitRepo(home);

  const pdfUrls = await discoverPdfUrls();
  const incidents: Incident[] = [];
  const perPdf: Record<string, number> = {};
  for (const url of pdfUrls) {
    try {
      const text = await fetchPdfText(url);
      const found = parseIncidents(text, url);
      perPdf[url] = found.length;
      incidents.push(...found);
    } catch (err) {
      perPdf[url] = -1;
      console.error(`pdf-parse failed on ${url}:`, (err as Error).message);
    }
  }

  const reportOps = buildReportOps(incidents);
  const today = new Date().toISOString().slice(0, 10);

  let reportCommitId: string | null = null;
  if (reportOps.length > 0) {
    const reportResult = await client.commit.apply(
      orgName,
      repoName,
      `MDE state fish-kill ingest ${today}`,
      reportOps as Operation[],
      { chunkSize: 200 },
    );
    reportCommitId = reportResult.commitId;
  }

  // Cross-repo write into chesapeake-attribution. Requires the sprite token to
  // have write scope for BOTH repos — see runbook Step 6.4.
  const { orgName: curOrg, repoName: curRepo } = splitRepo(CURATED_REPO);
  const eventOps = buildEventOps(incidents, home);
  let eventCommitId: string | null = null;
  try {
    const eventResult = await client.commit.apply(
      curOrg,
      curRepo,
      `MDE fish-kill events ${today}`,
      eventOps as Operation[],
      { chunkSize: 100 },
    );
    eventCommitId = eventResult.commitId;
  } catch (err) {
    console.error(`chesapeake-attribution write failed:`, (err as Error).message);
  }

  console.log(JSON.stringify({
    reportCommitId,
    eventCommitId,
    pdfUrlsFound: pdfUrls.length,
    incidentsParsed: incidents.length,
    perPdf,
  }));
}

main().catch((err) => { console.error(err); process.exit(1); });
