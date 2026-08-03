"""What was NOT entered today — the exception panel that opens the daily report.

WHY THIS EXISTS
    The rest of the report answers "what happened". A supervisor also needs the
    opposite: which factory raised no job card, which warehouse keyed nothing,
    which cards were worked on but never closed. None of that is visible in a
    summary assembled from rows that exist — a warehouse with no rows simply has
    no row to look at, so it disappears instead of standing out.

WHEN SILENCE COUNTS AS A GAP
    Only for a site that is normally active, judged from the trailing 30 days
    rather than a hardcoded list: sites open, close and get renamed, and a fixed
    list would quietly stop being true without anyone noticing. A site qualifies
    when it was active on at least 3 days AND on at least 40% of the days the
    module itself ran. On 31 Jul 2026 that keeps W-202 (job cards on 10 of the
    16 days the module ran) and drops a warehouse that took one delivery all
    month, which is the difference between a panel people read and one they
    learn to scroll past.

    A module with an occasional cadence — fewer than 8 active days in 30, which
    is samples/NPD — gets no per-site silence flags at all, only whole-module
    silence. "No sample raised at W202 today" every single day is noise, not a
    finding.

    When a module is silent EVERYWHERE, one module-level line is raised instead
    of one per site, so a genuinely quiet day reads as a sentence rather than a
    wall of red.

JOB CARD STATE VOCABULARY  (verified against the live table, not assumed)
    locked / unlocked / assigned  planned only — 0 of 435 such cards have a
                                  start_time and none carry an accounting row
    in_progress                   started, not ended
    completed / closed            started and ended — the only two states that
                                  actually mean the card is finished
    So "planning done but nothing closed" is precisely: cards active today, none
    of them in completed/closed, and none ended today.

CANONICALISATION
    `canon_site` is passed in rather than imported. Warehouse aliasing lives in
    `daily_report`, which imports this module; taking the function as an argument
    keeps the dependency one-way instead of circular, and guarantees the gap
    panel and the tables agree on what "A185" is called.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.ims_service.daily_report_ops import canon_factory

logger = get_logger("daily_report_gaps")

LOOKBACK_DAYS = 30      # trailing window that defines "normally active"
MIN_ACTIVE_DAYS = 3     # absolute floor, so a one-off site is never flagged
MIN_SHARE = 0.40        # ...and it must cover 40% of the module's own active days
MIN_MODULE_DAYS = 8     # below this the module is occasional: no per-site flags

# Rank orders the panel: the whole module being silent outranks one site being
# silent, which outranks work left unfinished, which outranks a blank field.
R_MODULE_SILENT = 0
R_SITE_SILENT = 1
R_UNFINISHED = 2
R_MISSING_DATA = 3

ALL_SITES = ""          # a gap that belongs to the company, not to one warehouse

_UNKNOWN = {"(unassigned)", "(uncategorised)", "(blank)", "-", "", "none", "null"}


def _unknown(site) -> bool:
    return str(site or "").strip().lower() in _UNKNOWN


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


def _gap(module: str, site: str, message: str, rank: int) -> dict:
    return {"module": module, "site": site or ALL_SITES, "text": message, "rank": rank}


# ═════════════════════════════════════════════════════════════════════════
#  BASELINE — which sites are normally active in each module
# ═════════════════════════════════════════════════════════════════════════
# Each query returns (site, d) pairs: one row per day a site did something.
# LEFT(...) rather than LIKE 'cold%' on purpose — a literal % in text() is a
# paramstyle hazard under psycopg2, and this needs no wildcard to say the same
# thing. It mirrors `src_site()`: 'Cold Storage' is generic, from_cold_unit
# names the actual store.
_BASELINE_SQL = {
    "inward": """
        SELECT DISTINCT TRIM(COALESCE(warehouse, '')) AS site, entry_date::date AS d
        FROM cfpl_transactions_v2 WHERE entry_date::date BETWEEN :a AND :b
        UNION
        SELECT DISTINCT TRIM(COALESCE(warehouse, '')), entry_date::date
        FROM cdpl_transactions_v2 WHERE entry_date::date BETWEEN :a AND :b
    """,
    "transfers": """
        SELECT DISTINCT CASE
                 WHEN LEFT(LOWER(TRIM(COALESCE(from_site, ''))), 4) = 'cold'
                      AND TRIM(COALESCE(from_cold_unit, '')) <> ''
                 THEN TRIM(from_cold_unit)
                 ELSE TRIM(COALESCE(from_site, '')) END AS site,
               stock_trf_date AS d
        FROM interunit_transfers_header WHERE stock_trf_date BETWEEN :a AND :b
        UNION
        SELECT DISTINCT COALESCE(NULLIF(TRIM(COALESCE(i.receiving_warehouse, '')), ''),
                                 TRIM(COALESCE(o.to_site, ''))),
               i.grn_date::date
        FROM interunit_transfer_in_header i
        LEFT JOIN interunit_transfers_header o ON o.id = i.transfer_out_id
        WHERE i.grn_date::date BETWEEN :a AND :b
    """,
    "jobcards": """
        SELECT DISTINCT TRIM(COALESCE(j.factory, '')) AS site, v.d
        FROM job_card_v2 j
        CROSS JOIN LATERAL (VALUES (j.created_at::date), (j.updated_at::date),
                                   (j.start_time::date), (j.end_time::date)) AS v(d)
        WHERE j.deleted_at IS NULL AND v.d BETWEEN :a AND :b
    """,
    "samples": """
        SELECT DISTINCT TRIM(COALESCE(warehouse, '')) AS site, created_at::date AS d
        FROM sample_requisitions
        WHERE deleted_at IS NULL AND created_at::date BETWEEN :a AND :b
        UNION
        SELECT DISTINCT TRIM(COALESCE(warehouse, '')), created_at::date
        FROM npd_dev_job_cards WHERE created_at::date BETWEEN :a AND :b
    """,
}


def expected_sites(db: Session, day: date, canon_site) -> dict[str, set[str]]:
    """Sites that are normally active per module, over the 30 days BEFORE `day`.

    The report day is excluded deliberately — a site's own silence today must not
    be what decides whether today's silence is worth mentioning.
    """
    a = day - timedelta(days=LOOKBACK_DAYS)
    b = day - timedelta(days=1)
    out: dict[str, set[str]] = {}

    for module, sql in _BASELINE_SQL.items():
        canon = canon_factory if module == "jobcards" else canon_site
        days_by_site: dict[str, set[date]] = defaultdict(set)
        module_days: set[date] = set()
        try:
            for r in db.execute(text(sql), {"a": a, "b": b}):
                # Sundays are not business days here. Counting them would put
                # every site's share against a denominator it can never reach and
                # drag routine sites below the threshold.
                if r.d is None or r.d.weekday() == 6:
                    continue
                module_days.add(r.d)
                site = canon(r.site)
                if not _unknown(site):
                    days_by_site[site].add(r.d)
        except Exception as exc:                                   # noqa: BLE001
            # A failed statement poisons the transaction for everything after it,
            # so clear it — a missing baseline must cost the panel, not the report.
            logger.error("Gap baseline for %s failed: %s", module, exc)
            db.rollback()
            out[module] = set()
            continue

        if len(module_days) < MIN_MODULE_DAYS:
            out[module] = set()
            continue
        need = max(MIN_ACTIVE_DAYS, MIN_SHARE * len(module_days))
        out[module] = {s for s, ds in days_by_site.items() if len(ds) >= need}

    return out


# ═════════════════════════════════════════════════════════════════════════
#  RULES
# ═════════════════════════════════════════════════════════════════════════
def _inward_gaps(agg, expected: set[str]) -> list[dict]:
    gaps: list[dict] = []
    active = {name for name, w in agg["wh"].items() if w["itx"]}

    if not agg["head"]["inw_txns"]:
        gaps.append(_gap("inward", ALL_SITES, "No inward anywhere", R_MODULE_SILENT))
    else:
        for site in sorted(expected - active):
            gaps.append(_gap("inward", site, "No inward entered", R_SITE_SILENT))

    for name, w in sorted(agg["wh"].items()):
        if not w["itx"]:
            continue
        if _unknown(name):
            gaps.append(_gap("inward", "(Unassigned)",
                             f"{_plural(len(w['itx']), 'entry')} without a warehouse",
                             R_MISSING_DATA))
        lines, miss = w.get("ilines", 0), w.get("imiss", 0)
        if miss and lines:
            where = "the only line" if lines == 1 else (
                f"all {lines} lines" if miss == lines else f"{miss} of {lines} lines")
            gaps.append(_gap("inward", name, f"Value blank on {where}", R_MISSING_DATA))
    return gaps


def _transfer_gaps(agg, expected: set[str]) -> list[dict]:
    gaps: list[dict] = []
    h = agg["head"]
    active = {name for name, w in agg["wh"].items() if w["och"] or w["igrn"]}

    if not (h["out_chl"] or h["in_grn"]):
        gaps.append(_gap("transfers", ALL_SITES, "No transfers anywhere", R_MODULE_SILENT))
    else:
        for site in sorted(expected - active):
            gaps.append(_gap("transfers", site, "No transfers in or out", R_SITE_SILENT))
    return gaps


def _jobcard_gaps(jc, expected: set[str]) -> list[dict]:
    gaps: list[dict] = []

    if jc["empty"]:
        gaps.append(_gap("jobcards", ALL_SITES, "No job cards anywhere", R_MODULE_SILENT))
        return gaps

    for site in sorted(expected - set(jc["wh"])):
        gaps.append(_gap("jobcards", site, "No job cards entered", R_SITE_SILENT))

    for name, v in sorted(jc["wh"].items()):
        n = len(v["cards"])
        if not n:
            continue
        closed = len(v.get("closed", ()))
        closed_today = len(v.get("closed_today", ()))
        started = len(v.get("started", ()))

        if not closed and not closed_today:
            state = "open, none closed" if started else "planned, none started"
            gaps.append(_gap("jobcards", name,
                             f"{_plural(n, 'job card')} {state}", R_UNFINISHED))
        elif not closed_today:
            gaps.append(_gap("jobcards", name, "None closed today", R_UNFINISHED))

        if v.get("no_acct"):
            gaps.append(_gap("jobcards", name,
                             f"{v['no_acct']} of {n} without accounting", R_MISSING_DATA))
        if not v["users"]:
            gaps.append(_gap("jobcards", name,
                             f"{_plural(n, 'job card')} without a team leader",
                             R_MISSING_DATA))
        if v.get("no_fg"):
            gaps.append(_gap("jobcards", name,
                             f"{v['no_fg']} of {n} without an FG item", R_MISSING_DATA))

    loss = jc.get("loss") or {}
    if loss.get("unbalanced"):
        gaps.append(_gap("jobcards", ALL_SITES,
                         f"{loss['unbalanced']} of {loss['rows']} job cards unbalanced",
                         R_MISSING_DATA))
    return gaps


def _sample_gaps(sm, expected: set[str], canon_site) -> list[dict]:
    gaps: list[dict] = []

    if sm["empty"]:
        gaps.append(_gap("samples", ALL_SITES, "No sample or NPD activity", R_MODULE_SILENT))
        return gaps

    active = {canon_site(r.get("warehouse")) for r in sm["requisitions"]}
    active |= {canon_site(n.get("warehouse")) for n in sm["npd_jobcards"]}
    for site in sorted(expected - active):
        gaps.append(_gap("samples", site, "No sample or NPD activity", R_SITE_SILENT))

    total = len(sm["requisitions"])
    unmapped = [r for r in sm["requisitions"] if not (r.get("sale_groups") or "").strip()]
    if unmapped:
        gaps.append(_gap("samples", ALL_SITES,
                         f"{len(unmapped)} of {total} requisitions without a sales group",
                         R_MISSING_DATA))

    faceless = [r for r in sm["requisitions"]
                if not (r.get("customer_name") or r.get("company_name") or "").strip()
                and not (r.get("npd_target_name") or "").strip()]
    if faceless:
        gaps.append(_gap("samples", ALL_SITES,
                         f"{_plural(len(faceless), 'requisition')} without a customer",
                         R_MISSING_DATA))

    open_npd = [n for n in sm["npd_jobcards"]
                if str(n.get("status") or "").strip().upper() not in
                ("CLOSED", "CANCELLED") and not n.get("output_qty")]
    if open_npd:
        gaps.append(_gap("samples", ALL_SITES,
                         f"{_plural(len(open_npd), 'NPD card')} without output qty",
                         R_MISSING_DATA))
    return gaps


# ═════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════
def compute_gaps(db: Session, day: date, agg: dict, ops: dict, *, canon_site) -> list[dict]:
    """Everything that was not entered or not finished on `day`, ready to render.

    Never raises: a report that lists yesterday's tonnage is worth sending even
    if the exception panel could not be built, so a failure here degrades to an
    empty panel and a log line.
    """
    if day.weekday() == 6:
        # A Sunday report only goes out when something happened on it. That a
        # warehouse stayed shut is not a finding — whole-module silence and
        # blank fields still are, so only the per-site silence rules stand down.
        expected = {k: set() for k in _BASELINE_SQL}
    else:
        try:
            expected = expected_sites(db, day, canon_site)
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Gap baseline unavailable for %s: %s", day, exc)
            expected = {k: set() for k in _BASELINE_SQL}

    try:
        gaps = (_inward_gaps(agg, expected.get("inward", set()))
                + _transfer_gaps(agg, expected.get("transfers", set()))
                + _jobcard_gaps(ops["jobcards"], expected.get("jobcards", set()))
                + _sample_gaps(ops["samples"], expected.get("samples", set()), canon_site))
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Gap computation failed for %s: %s", day, exc)
        return []

    logger.info("Daily report %s: %d gap(s) across %d site(s)",
                day, len(gaps), len({g["site"] for g in gaps if g["site"]}))
    return gaps


# Reading order inside a warehouse's line. Grouping by module keeps the
# same-coloured phrases adjacent, which is what makes a run-on line scannable;
# ordering by rank instead would interleave two modules and read as noise.
_MODULE_ORDER = {"inward": 0, "transfers": 1, "jobcards": 2, "samples": 3}


def group_by_site(gaps: list[dict]) -> list[tuple[str, list[dict]]]:
    """Warehouse-first ordering, because that is how the panel is read.

    A site is judged by its worst gap, then by how many it has. Company-wide
    items are pushed to the end: they are real, but a supervisor scans for their
    own warehouse first.
    """
    by_site: dict[str, list[dict]] = defaultdict(list)
    for g in gaps:
        by_site[g["site"]].append(g)
    for rows in by_site.values():
        rows.sort(key=lambda g: (_MODULE_ORDER.get(g["module"], 9), g["rank"], g["text"]))

    def order(item):
        site, rows = item
        return (1 if site == ALL_SITES else 0,
                min(r["rank"] for r in rows), -len(rows), site)

    return sorted(by_site.items(), key=order)
