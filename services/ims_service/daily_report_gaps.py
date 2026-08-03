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

# Rank orders the phrases inside a warehouse's line: nothing entered at all
# outranks work left unfinished, which outranks a field left blank.
R_SILENT = 0
R_UNFINISHED = 1
R_MISSING_DATA = 2

# EVERY gap names a warehouse. There was briefly an "All sites" bucket for the
# figures that are aggregated company-wide — unbalanced job cards, requisitions
# with no sales group — but nobody owns "All sites", so nobody acts on it. Each
# of those is attributable: accounting rows carry a factory, requisitions carry
# a warehouse. They are counted per site now, and a module that is silent
# everywhere reports once per site that should have reported rather than once
# for the company.
_UNKNOWN = {"(uncategorised)", "(blank)", "-", "", "none", "null"}
UNASSIGNED = "(No warehouse)"


def _unknown(site) -> bool:
    return str(site or "").strip().lower() in _UNKNOWN


def _site(raw) -> str:
    """A blank warehouse is still a bucket — one that says its own name."""
    s = str(raw or "").strip()
    return UNASSIGNED if not s or s.lower() in _UNKNOWN or s == "(Unassigned)" else s


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


# "1 of 2 job cards do not balance" reads as a typo, and a summary that looks
# careless gets treated as one.
def _has(n: int) -> str:
    return "has" if n == 1 else "have"


def _does(n: int) -> str:
    return "does" if n == 1 else "do"


def _gap(module: str, site: str, message: str, rank: int) -> dict:
    return {"module": module, "site": _site(site), "text": message, "rank": rank}


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

    for site in sorted(expected - active):
        gaps.append(_gap("inward", site, "No inward entry made", R_SILENT))

    for name, w in sorted(agg["wh"].items()):
        if not w["itx"]:
            continue
        if _unknown(name) or name == "(Unassigned)":
            gaps.append(_gap("inward", name,
                             f"{_plural(len(w['itx']), 'inward entry')} saved with no "
                             f"warehouse", R_MISSING_DATA))
        lines, miss = w.get("ilines", 0), w.get("imiss", 0)
        if miss and lines:
            where = "its only line" if lines == 1 else (
                f"all {lines} lines" if miss == lines else f"{miss} of {lines} lines")
            gaps.append(_gap("inward", name,
                             f"Inward value not entered on {where}", R_MISSING_DATA))
    return gaps


def _transfer_gaps(agg, expected: set[str]) -> list[dict]:
    gaps: list[dict] = []
    active = {name for name, w in agg["wh"].items() if w["och"] or w["igrn"]}
    return gaps + [_gap("transfers", site, "No transfer dispatched or received", R_SILENT)
                   for site in sorted(expected - active)]


def _jobcard_gaps(jc, expected: set[str]) -> list[dict]:
    gaps: list[dict] = []

    for site in sorted(expected - set(jc["wh"])):
        gaps.append(_gap("jobcards", site, "No job card initiated or entered", R_SILENT))

    for name, v in sorted(jc["wh"].items()):
        n = len(v["cards"])
        if not n:
            continue
        closed = len(v.get("closed", ()))
        closed_today = len(v.get("closed_today", ()))
        started = len(v.get("started", ()))

        if not closed and not closed_today:
            state = ("open, none closed today" if started
                     else "planned, none started today")
            gaps.append(_gap("jobcards", name,
                             f"{_plural(n, 'job card')} {state}", R_UNFINISHED))
        elif not closed_today:
            gaps.append(_gap("jobcards", name, "No job card closed today", R_UNFINISHED))

        if v.get("no_acct"):
            k = v["no_acct"]
            gaps.append(_gap("jobcards", name,
                             f"{k} of {n} job cards {_has(k)} no accounting entry",
                             R_MISSING_DATA))
        if v.get("unbalanced"):
            u = v["unbalanced"]
            gaps.append(_gap("jobcards", name,
                             f"{u} of {v['acct_rows']} accounted job cards {_does(u)} "
                             f"not balance", R_MISSING_DATA))
        if not v["users"]:
            gaps.append(_gap("jobcards", name,
                             f"{_plural(n, 'job card')} {_has(n)} no team leader named",
                             R_MISSING_DATA))
        if v.get("no_fg"):
            k = v["no_fg"]
            gaps.append(_gap("jobcards", name,
                             f"{k} of {n} job cards {_has(k)} no FG item", R_MISSING_DATA))
    return gaps


def _sample_gaps(sm, expected: set[str], canon_site) -> list[dict]:
    """Requisitions and NPD cards both carry a warehouse, so both are filed under
    one rather than pooled into a company-wide count nobody owns."""
    gaps: list[dict] = []

    by_site: dict[str, dict] = defaultdict(lambda: {"reqs": [], "npd": []})
    for r in sm["requisitions"]:
        by_site[_site(canon_site(r.get("warehouse")))]["reqs"].append(r)
    for n in sm["npd_jobcards"]:
        by_site[_site(canon_site(n.get("warehouse")))]["npd"].append(n)

    for site in sorted(expected - set(by_site)):
        gaps.append(_gap("samples", site,
                         "No sample requisition or NPD card raised", R_SILENT))

    for site, v in sorted(by_site.items()):
        reqs, npd = v["reqs"], v["npd"]

        unmapped = [r for r in reqs if not (r.get("sale_groups") or "").strip()]
        if unmapped:
            k = len(unmapped)
            gaps.append(_gap("samples", site,
                             f"{k} of {len(reqs)} sample requisitions {_has(k)} no sales "
                             f"group in all_sku", R_MISSING_DATA))

        faceless = [r for r in reqs
                    if not (r.get("customer_name") or r.get("company_name") or "").strip()
                    and not (r.get("npd_target_name") or "").strip()]
        if faceless:
            k = len(faceless)
            gaps.append(_gap("samples", site,
                             f"{_plural(k, 'sample requisition')} {_has(k)} no customer "
                             f"named", R_MISSING_DATA))

        open_npd = [n for n in npd
                    if str(n.get("status") or "").strip().upper() not in
                    ("CLOSED", "CANCELLED") and not n.get("output_qty")]
        if open_npd:
            gaps.append(_gap("samples", site,
                             f"{_plural(len(open_npd), 'NPD card')} open with no output "
                             f"quantity", R_MISSING_DATA))
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


# Tie-break only. Severity leads inside a warehouse's line — "no job card
# entered" has to come before "one card has no FG item" — and each phrase names
# its own subject, so modules interleaving costs nothing.
_MODULE_ORDER = {"inward": 0, "transfers": 1, "jobcards": 2, "samples": 3}


def group_by_site(gaps: list[dict]) -> list[tuple[str, list[dict]]]:
    """Warehouse-first ordering, because that is how the panel is read.

    A site is judged by its worst gap, then by how many it has. The bucket for
    records saved without a warehouse sorts last — it is a data-entry fault
    rather than a place, and no one goes looking for it first.
    """
    by_site: dict[str, list[dict]] = defaultdict(list)
    for g in gaps:
        by_site[g["site"]].append(g)
    for rows in by_site.values():
        rows.sort(key=lambda g: (g["rank"], _MODULE_ORDER.get(g["module"], 9), g["text"]))

    def order(item):
        site, rows = item
        return (1 if site == UNASSIGNED else 0,
                min(r["rank"] for r in rows), -len(rows), site)

    return sorted(by_site.items(), key=order)
