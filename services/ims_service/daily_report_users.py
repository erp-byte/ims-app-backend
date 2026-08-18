"""Who did NOTHING today — the roster block that closes the daily report.

WHAT IT ANSWERS
    Every other section of the report is assembled from rows that exist, so a
    person who keyed nothing has no row and simply vanishes. This block is the
    inverse: the named list of people who took no action anywhere in IMS or ERP
    on the business day — 0 entries, 0 approvals, 0 anything.

WHY THE ROSTER IS NOT `auth_user`
    The obvious roster is `auth_user WHERE is_active`, and it is wrong. Those 36
    rows do not contain Vaibhav Kumkar, Vaishali Dhuri, Pankaj Ranga or Sumit
    Baikar — between them the majority of every day's inward. Most job-card team
    leaders (Monika, Nishant, Samiksha, Namrata) have no login row either; they
    are typed as free text onto the card. A roster taken from the login table
    would report the busiest keyers in the company as "no activity", every day,
    which is not a report — it is an accusation, and a false one.

    So the roster is derived from what people actually DID: everyone seen acting
    in any module across the trailing 30 days, unioned with the active logins.
    A name has to have earned its way onto the list before its silence counts.

TWO TIERS, BECAUSE "IDLE" MEANS TWO DIFFERENT THINGS
    Regular    active on >= 3 of the last 30 business days. These are the people
               whose silence is a finding — a keyer who works most days and
               keyed nothing today.
    Occasional everyone else on the roster. Listed too (the ask was for the
               total, and a business head who never keys anything is still a
               true zero), but compactly and below, so tier 1 stays readable.

    The threshold also disposes of typo identities for free: 'Samikshq', 'sdf'
    and 'Test' appear once and never clear it, so they never get reported as a
    person who did not work.

WHICH SYSTEM IS WHICH
    IMS  inward, bulk entry, interunit transfers, job work, customer returns,
         stock take
    ERP  job cards, production plan, samples/NPD, material documents, x-ray,
         utility & meter readings
    A person counts as active for the day if ANY of those recorded them, so
    "no action" means no action in either system — which is what was asked for.
    The mail prints this coverage list under the block: a reader has to be able
    to see what "no action" was measured against, or the block is unfalsifiable.

DATE BASIS
    Each source is matched on the same date the report itself uses for that
    module (inward `entry_date`, transfers `stock_trf_date` / `grn_date`), so
    this block and the tables above it can never disagree about who worked.
    Sources with no business date of their own (job work, material documents,
    readings) are matched on `created_at`.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("daily_report_users")

LOOKBACK_DAYS = 30      # trailing window that defines the roster
MIN_REGULAR_DAYS = 3    # ...and the bar for "works here regularly"

IMS = "IMS"
ERP = "ERP"


# ═════════════════════════════════════════════════════════════════════════
#  ACTIVITY SOURCES
# ═════════════════════════════════════════════════════════════════════════
# Every query returns (actor, d) pairs — one row per person per day they did
# something. `actor` is text in most tables and an integer user id in the
# sample/NPD ones; `_resolve` handles both, so a source only has to say which.
#
# Declared as data rather than as one giant UNION on purpose: these tables are
# owned by different teams and several have been renamed or retyped mid-life.
# A UNION makes one bad column take the whole block down, where a list lets the
# runner skip the broken source, log it, and still report the other fifteen.
_SOURCES: list[tuple[str, str, str, bool]] = [
    # (module label, system, sql, actor_is_user_id)
    ("Inward", IMS, """
        SELECT approved_by AS actor, entry_date::date AS d FROM cfpl_transactions_v2
        WHERE entry_date::date BETWEEN :a AND :b
        UNION ALL
        SELECT approval_authority, entry_date::date FROM cfpl_transactions_v2
        WHERE entry_date::date BETWEEN :a AND :b
        UNION ALL
        SELECT approved_by, entry_date::date FROM cdpl_transactions_v2
        WHERE entry_date::date BETWEEN :a AND :b
        UNION ALL
        SELECT approval_authority, entry_date::date FROM cdpl_transactions_v2
        WHERE entry_date::date BETWEEN :a AND :b
    """, False),
    ("Bulk entry", IMS, """
        SELECT approved_by AS actor, NULLIF(entry_date::text, '')::date AS d
        FROM cfpl_bulk_entry_transactions
        WHERE NULLIF(entry_date::text, '')::date BETWEEN :a AND :b
        UNION ALL
        SELECT approved_by, NULLIF(entry_date::text, '')::date
        FROM cdpl_bulk_entry_transactions
        WHERE NULLIF(entry_date::text, '')::date BETWEEN :a AND :b
    """, False),
    ("Transfer out", IMS, """
        SELECT created_by AS actor, stock_trf_date AS d FROM interunit_transfers_header
        WHERE stock_trf_date BETWEEN :a AND :b
    """, False),
    ("Transfer in", IMS, """
        SELECT received_by AS actor, grn_date::date AS d FROM interunit_transfer_in_header
        WHERE grn_date::date BETWEEN :a AND :b
    """, False),
    ("Job work", IMS, """
        SELECT created_by AS actor, created_at::date AS d FROM jb_materialout_header
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, created_at::date FROM jb_work_inward_receipt
        WHERE created_at::date BETWEEN :a AND :b
    """, False),
    ("Customer returns", IMS, """
        SELECT created_by AS actor, rtv_date::date AS d FROM cfpl_rtv_header
        WHERE rtv_date::date BETWEEN :a AND :b
        UNION ALL
        SELECT approved_by, approved_at::date FROM cfpl_rtv_header
        WHERE approved_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, rtv_date::date FROM cdpl_rtv_header
        WHERE rtv_date::date BETWEEN :a AND :b
        UNION ALL
        SELECT approved_by, approved_at::date FROM cdpl_rtv_header
        WHERE approved_at::date BETWEEN :a AND :b
    """, False),
    ("Stock take", IMS, """
        SELECT entered_by AS actor, created_at::date AS d FROM stocktake_entries
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT verified_by, verified_at::date FROM stocktake_entries
        WHERE verified_at::date BETWEEN :a AND :b
    """, False),
    ("Job cards", ERP, """
        SELECT j.assigned_to_team_leader AS actor, v.d
        FROM job_card_v2 j
        CROSS JOIN LATERAL (VALUES (j.created_at::date), (j.updated_at::date),
                                   (j.start_time::date), (j.end_time::date)) AS v(d)
        WHERE j.deleted_at IS NULL AND v.d BETWEEN :a AND :b
        UNION ALL
        SELECT j.updated_by, v.d
        FROM job_card_v2 j
        CROSS JOIN LATERAL (VALUES (j.created_at::date), (j.updated_at::date),
                                   (j.start_time::date), (j.end_time::date)) AS v(d)
        WHERE j.deleted_at IS NULL AND v.d BETWEEN :a AND :b
    """, False),
    ("Production plan", ERP, """
        SELECT created_by AS actor, created_at::date AS d FROM production_plan_v2
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT approved_by, approved_at::date FROM production_plan_v2
        WHERE approved_at::date BETWEEN :a AND :b
    """, False),
    ("Material documents", ERP, """
        SELECT created_by AS actor, created_at::date AS d FROM material_document
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT verified_by, created_at::date FROM md_entry
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT verified_by, created_at::date FROM md_entry_185
        WHERE created_at::date BETWEEN :a AND :b
    """, False),
    ("X-ray", ERP, """
        SELECT verified_by AS actor, created_at::date AS d FROM xray_entries
        WHERE created_at::date BETWEEN :a AND :b
    """, False),
    ("Readings", ERP, """
        SELECT created_by AS actor, reading_date AS d FROM mt_utility_diesel
        WHERE reading_date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, reading_date FROM mt_utility_gas
        WHERE reading_date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, reading_date FROM mt_utility_water
        WHERE reading_date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, reading_date FROM mt_w202_meterreading
        WHERE reading_date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, reading_date FROM mt_a185_readings
        WHERE reading_date BETWEEN :a AND :b
    """, False),
    # Integer user ids, resolved against auth_user.
    ("Samples / NPD", ERP, """
        SELECT created_by AS actor, created_at::date AS d FROM sample_requisitions
        WHERE deleted_at IS NULL AND created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT actor_user_id, created_at::date FROM sample_audit_log
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, created_at::date FROM npd_dev_job_cards
        WHERE created_at::date BETWEEN :a AND :b
        UNION ALL
        SELECT created_by, created_at::date FROM npd_dev_promote_request
        WHERE created_at::date BETWEEN :a AND :b
    """, True),
]

MODULES = [(label, system) for label, system, _, _ in _SOURCES]


# ═════════════════════════════════════════════════════════════════════════
#  IDENTITY
# ═════════════════════════════════════════════════════════════════════════
_HONORIFIC = re.compile(r"\b(SIR|MADAM|MAM|JI)\b")

# Spellings that fold to different keys but are one person. Verified against the
# live tables — every entry here was observed, none is speculative. Keys and
# values are both folded forms, and the value is always the FULLEST spelling,
# because that is what the report prints.
#
# Deliberately absent: SHUBHAM. Four of them work here (Shivekar, Seth, Mhatre,
# Lohar), so a bare 'Shubham' cannot be resolved to any of them and guessing
# would put one man's silence against another man's name.
NAME_ALIASES = {
    "MADIRI": "MADHURI",
    "MADURI": "MADHURI",
    "SAMIKSHQ": "SAMIKSHA",
    "SHABHANA SAYYED": "SHABANA SAYYED",
    "SAMAL KUAMR": "SAMAL KUMAR",
    "SAMAL KUMKAR": "SAMAL KUMAR",
    "SAMAL": "SAMAL KUMAR",
    "MAHESH TAPARIYA": "MAHESH TAPARIA",
    "MAHESH": "MAHESH TAPARIA",
    "MAYURESH DATES": "MAYURESH MAHADIK",
    "MAYURESH": "MAYURESH MAHADIK",
    "HRITHIK": "B HRITHIK",
    "BHRITHIK": "B HRITHIK",
    "ROSHANSAPKAL": "ROSHAN SAKPAL",
    "HRITHIK B": "B HRITHIK",
    "SABANA ANSARI": "SHABANA ANSARI",
    "NAMARTA NACHARE": "NAMRATA NACHARE",
    "ARBAAZSHAIKH": "ARBAJ SHAIKH",
    # Stock Take logins with no spaced spelling anywhere in the data. Without
    # these the roster block prints a login where it promised a person.
    "SAMALKUMKAR": "SAMAL KUMAR",
    "DASHRATBIRAJDAR": "DASHRATH BIRAJDAR",
    "SHAKIRASHAIKH": "SHAKIRA SHAIKH",
    "MADHURISHEWALE": "MADHURI SHEWALE",
    "PAWANJAMBLE": "PAWAN JAMBLE",
    "RITESHDIGE": "RITESH DIGE",
    "SHUBHAMMHATRE": "SHUBHAM MHATRE",
    "SWADHINJOSHI": "SWADHIN JOSHI",
    "SHUBHAMLOHAR": "SHUBHAM LOHAR",
    "LOHARSHUBHAM31": "SHUBHAM LOHAR",
    "GANANANDGHODEKAR": "GANANAND GHODEKAR",
    "GANESHSHINDE": "GANESH SHINDE",
    "VILASRAVANANG": "VILAS RAVANANG",
    "PRIYANSHUSHRIVASTAV": "PRIYANSHU SHRIVASTAV",
    "YASHGAWADI": "YASH GAWDI",
    "SUNILJASORIA": "SUNIL JASORIA",
    "YASH": "YASH GAWDI",
    "VAIBHAV": "VAIBHAV KUMKAR",
    "VAISHALI": "VAISHALI DHURI",
    "SUMIT": "SUMIT BAIKAR",
    "NARESH": "NARESH JADHAV",
    "PANKAJ": "PANKAJ RANGA",
}

# Logins that are a place or a role rather than a person. They key real work, so
# they belong in the activity totals, but "Stores A185 took no action today" is
# not a sentence anyone can act on — a shared store login has no one to ask.
NOT_A_PERSON = {"STORES", "STORES A185", "ADMIN", "PROMOTE", "TEST", "SDF",
                "MANAGER", "INVENTORY MANAGER", "SUPERUSER", "FLOORHEAD",
                "FLOOR MANAGER", "SYSTEM", "NULL", "NONE"}


def base_fold(raw) -> str:
    """Normalise a name or email to a comparison key, before aliasing.

    Emails collapse to their local part with separators opened out, so
    `soham.damgude@…`, `Soham Damgude` and `SOHAMDAMGUDE` all reach the same
    place once `squash` has taken the spaces back out.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    if "@" in s:
        s = s.split("@", 1)[0]
    s = s.replace(".", " ").replace("_", " ").replace("-", " ")
    s = _HONORIFIC.sub(" ", s.upper())
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def fold(raw) -> str:
    key = base_fold(raw)
    return NAME_ALIASES.get(key, key)


def squash(key: str) -> str:
    """Space-insensitive form: 'SOHAM DAMGUDE' and 'SOHAMDAMGUDE' are one man."""
    return key.replace(" ", "")


def is_person(key: str) -> bool:
    return bool(key) and key not in NOT_A_PERSON and not key.isdigit()


def clean_raw(raw: str) -> str:
    """Open out the separators a login uses, so 'B.hrithik' prints as two words."""
    return re.sub(r"\s+", " ", re.sub(r"[._\-]+", " ", str(raw))).strip()


def rank(s: str) -> tuple[int, int]:
    """How good a spelling is: word count first, then letters.

    Word count leads because the failure it prevents is the visible one. A login
    ('rajupaikrao') and the name it belongs to ('Raju Paikrao') have nearly the
    same letter count, so a letters-only contest is a coin toss that prints the
    login about half the time.
    """
    return (s.count(" "), len(re.sub(r"[^A-Za-z]", "", s)))


def titlecase(raw: str) -> str:
    """'shabana sayyed' -> 'Shabana Sayyed', and 'R M PATIL' -> 'R M Patil'.

    Single letters are initials and stay upper; everything else is capitalised,
    because the same person is keyed as MONIKA, Monika and monika on three
    different days and the report must print one name, not three.
    """
    return " ".join(w.upper() if len(w) == 1 else w.capitalize()
                    for w in re.split(r"\s+", clean_raw(raw)) if w)


def display(key: str, seen: dict[str, str] | None = None) -> str:
    """Human-readable name — the better of the spelling seen and the resolved key.

    The key wins whenever it is the fuller form. Once 'rajupaikrao' has been
    resolved to RAJU PAIKRAO through the directory, printing the login that got
    us there would throw away the whole point of resolving it.
    """
    raw = clean_raw((seen or {}).get(key) or "")
    return titlecase(raw if rank(raw) >= rank(key) else key)


def _merge_partials(keys: set[str], directory: dict[str, set[str]]) -> dict[str, str]:
    """Map a bare first name onto the full name when exactly one person matches.

    Job cards carry 'Namrata' on one card and 'Namrata nachare' on the next, and
    left alone that is two people, one of whom is always idle.

    Ambiguity is judged against the whole staff directory, not just the names
    seen this month: 'Shubham' resolves to one full name in a quiet window and
    to four across the year, and a merge that depends on who happened to work
    that week would put one man's silence against another man's name.
    """
    out: dict[str, str] = {}
    for k in keys:
        if " " in k:
            continue
        known = directory.get(k, set())
        if len(known) > 1:
            continue                       # several people share this first name
        hits = {m for m in keys if " " in m and m.split(" ", 1)[0] == k}
        if len(known) == 1:
            hits |= known
        if len(hits) == 1:
            out[k] = hits.pop()
    return out


# ═════════════════════════════════════════════════════════════════════════
#  COLLECTION
# ═════════════════════════════════════════════════════════════════════════
def _user_names(db: Session) -> dict[str, str]:
    """auth_user ids -> full name, for the sources that store an id."""
    try:
        return {str(r.user_id): (r.full_name or f"User {r.user_id}")
                for r in db.execute(text(
                    "SELECT user_id, full_name FROM auth_user"))}
    except Exception as exc:                                       # noqa: BLE001
        logger.error("auth_user lookup failed: %s", exc)
        db.rollback()
        return {}


# Where properly spelt, spaced human names can be found. Activity tables store
# logins ('rajupaikrao'), so without a directory the report prints logins; these
# three carry the same people written out as names.
_DIRECTORY_SQL = [
    "SELECT DISTINCT full_name AS n FROM auth_user WHERE full_name IS NOT NULL",
    "SELECT DISTINCT name FROM stocktake_users WHERE name IS NOT NULL",
    "SELECT DISTINCT authority FROM stocktake_entries WHERE authority IS NOT NULL",
]


def name_directory(db: Session) -> tuple[dict[str, set[str]], dict[str, str]]:
    """Known staff names, indexed for the two lookups the merges need.

    Returns (by first name, by squashed spelling). The first decides whether a
    bare 'Namrata' is unambiguous; the second turns a login like 'rajupaikrao'
    back into 'Raju Paikrao'.
    """
    by_first: dict[str, set[str]] = defaultdict(set)
    by_squash: dict[str, str] = {}
    for sql in _DIRECTORY_SQL:
        try:
            rows = db.execute(text(sql)).fetchall()
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Name directory source failed: %s", exc)
            db.rollback()
            continue
        for r in rows:
            key = fold(r[0])
            if not is_person(key) or " " not in key:
                continue
            by_first[key.split(" ", 1)[0]].add(key)
            # Longest spelling wins, so 'SURAJ SANJAY BHILARE' is preferred over
            # 'SURAJ BHILARE' only when the shorter one was never keyed itself.
            prev = by_squash.get(squash(key))
            if prev is None or len(key) > len(prev):
                by_squash[squash(key)] = key
    return dict(by_first), by_squash


def collect(db: Session, day: date, lookback: int = LOOKBACK_DAYS) -> dict:
    """Every person seen acting, per day, over [day - lookback, day].

    One pass answers both questions the block needs — who belongs on the roster
    at all, and who was active today — so the two can never be computed from
    different windows and disagree.
    """
    a = day - timedelta(days=lookback)
    ids = _user_names(db)
    by_first, by_squash = name_directory(db)

    raw_seen: dict[str, str] = {}          # fold key -> fullest clean spelling
    days: dict[str, set[date]] = defaultdict(set)
    today_mods: dict[str, set[str]] = defaultdict(set)
    today_systems: dict[str, set[str]] = defaultdict(set)
    failed: list[str] = []

    def remember(key: str, base: str, raw: str) -> None:
        """Keep the best spelling to print for this person.

        An email is never it — 'vaibhav.kumkar@candorfoods.in' is longer than
        'Vaibhav Kumkar' and would win a naive longest-wins contest, putting an
        address where the report promised a name. Neither is a spelling the
        alias table had to correct: 'Maduri' folds to MADHURI, and reprinting
        the typo would undo the correction.
        """
        if "@" in raw or squash(base) != squash(key):
            return
        cleaned = clean_raw(raw)
        if rank(cleaned) > rank(raw_seen.get(key, "")):
            raw_seen[key] = cleaned

    for label, system, sql, by_id in _SOURCES:
        try:
            rows = db.execute(text(sql), {"a": a, "b": day}).fetchall()
        except Exception as exc:                                   # noqa: BLE001
            # One missing column must cost this source, not the whole block.
            logger.error("Activity source %s failed: %s", label, exc)
            db.rollback()
            failed.append(label)
            continue

        for r in rows:
            actor, d = r.actor, r.d
            if actor is None or d is None:
                continue
            raw = ids.get(str(actor), "") if by_id else str(actor)
            base = base_fold(raw)
            key = NAME_ALIASES.get(base, base)
            if not is_person(key):
                continue
            remember(key, base, raw)
            days[key].add(d)
            if d == day:
                today_mods[key].add(label)
                today_systems[key].add(system)

    # Fold 'SOHAMDAMGUDE' into 'SOHAM DAMGUDE', then 'Namrata' into
    # 'Namrata Nachare' — spacing first, because the partial-name merge needs
    # the spaced forms to compare against.
    _apply_merges(days, today_mods, today_systems, raw_seen, by_first, by_squash)

    return {"days": dict(days), "today_modules": dict(today_mods),
            "today_systems": dict(today_systems), "seen": raw_seen,
            "failed_sources": failed, "since": a}


def _remap(store: dict, src: str, dst: str) -> None:
    if src in store:
        store.setdefault(dst, type(store[src])()).update(store.pop(src))


def _apply_merges(days, today_mods, today_systems, raw_seen,
                  dir_by_first, dir_by_squash) -> None:
    """Collapse spelling variants of one person onto a single key, in place."""
    def move(src: str, dst: str) -> None:
        if src == dst or src not in days:
            return
        _remap(days, src, dst)
        _remap(today_mods, src, dst)
        _remap(today_systems, src, dst)
        best = raw_seen.pop(src, "")
        if rank(best) > rank(raw_seen.get(dst, "")):
            raw_seen[dst] = best

    # Spacing variants seen in the data: SOHAMDAMGUDE -> SOHAM DAMGUDE.
    groups: dict[str, list[str]] = defaultdict(list)
    for k in list(days):
        groups[squash(k)].append(k)
    for group in groups.values():
        if len(group) < 2:
            continue
        winner = max(group, key=lambda k: (k.count(" "), len(k)))
        for k in group:
            move(k, winner)

    # ...and against the staff directory, which is how a login that was never
    # once keyed as a name ('rajupaikrao') still prints as 'Raju Paikrao'.
    for k in list(days):
        if " " in k:
            continue
        full = dir_by_squash.get(squash(k))
        if full:
            move(k, full)

    # first-name-only variants: NAMRATA -> NAMRATA NACHARE
    for src, dst in _merge_partials(set(days), dir_by_first).items():
        move(src, dst)


# ═════════════════════════════════════════════════════════════════════════
#  THE ROSTER — accounts that exist on the system
# ═════════════════════════════════════════════════════════════════════════
# The three login systems this report covers. `users` is the IMS login table —
# not `auth_user`, which is the ERP's: Vaishali Dhuri and Pankaj Ranga key
# inward every week and appear only in `users`, so an ERP-only roster would
# never mention them.
#
# Deliberately NOT included: `mt_users` (maintenance technicians), `qc_users`,
# `ipqc_users`, `mis_users`, `ims_users`. Those are module rosters and mostly
# shared mailboxes rather than people; adding one is a line in this list.
ROSTER_SOURCES = [
    ("IMS", """
        SELECT COALESCE(NULLIF(TRIM(name), ''), NULLIF(TRIM(display_name), '')) AS nm,
               email AS em, NULL AS un
        FROM users WHERE is_active
    """),
    ("ERP", """
        SELECT NULLIF(TRIM(full_name), '') AS nm, email AS em, NULL AS un
        FROM auth_user WHERE is_active
    """),
    ("Stock Take", """
        SELECT NULLIF(TRIM(name), '') AS nm, email AS em, username AS un
        FROM stocktake_users WHERE is_active
    """),
]

# Logins that are a desk, a mailbox or a robot rather than a person. They key
# real work and their activity still counts, but "Quality A185 took no action
# today" names nobody, so there is nobody to ask about it.
FUNCTIONAL_ACCOUNTS = {
    "STORES", "STORESA185", "ADMIN", "AIADMIN", "AI1", "AI2", "AI",
    "SYSTEMADMINISTRATOR", "QUALITYA185", "QUALITYW202", "QUALITYINWARD",
    "QUALITYASSURANCE", "PRINTING", "PRINTINGA185", "A185QC", "CANDORA185",
    "CANDORW202", "ITSUPPORT", "TEST", "DEMO", "SUPPORT", "NOREPLY",
}


# Nine `stocktake_users` rows carry the four-character string 'null' in the name
# column — not SQL NULL, the word. Folded into an identity key it becomes 'NULL',
# which every one of those rows then shares, and all nine merge into a single
# account: Vaibhav Kumkar, Samal Kumar, Raju Paikrao and Harsh Arora arrive as
# one person whose activity is the union of theirs.
_NON_NAMES = {"", "-", "null", "none", "nil", "na", "n/a", "undefined", "false"}


def usable(v) -> str:
    s = re.sub(r"\s+", " ", str(v or "").strip())
    return "" if s.lower() in _NON_NAMES else s


def _functional(keys: set[str]) -> bool:
    return any(k in FUNCTIONAL_ACCOUNTS for k in keys)


def _account_rows(db: Session) -> list[dict]:
    rows = []
    for system, sql in ROSTER_SOURCES:
        try:
            for r in db.execute(text(sql)):
                rows.append({"system": system, "name": usable(r.nm),
                             "email": usable(r.em), "username": usable(r.un)})
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Roster source %s failed: %s", system, exc)
            db.rollback()
    return rows


def _compatible(a: str, b: str) -> bool:
    """True when two spellings could be one person: one name contains the other.

    'Digamber' / 'Digamber Sawant' and 'Satyendra Garg' / 'Satyendra Kumar Garg'
    are the same man written twice. 'Purva Nalawade' / 'Aakanksha Padwal' are
    not, and they share a mailbox.
    """
    ta, tb = set(a.split()), set(b.split())
    return ta <= tb or tb <= ta


def _shared_emails(rows: list[dict]) -> set[str]:
    """Addresses genuinely used by two different people — never an identity.

    `npd1@candorfoods.in` is Purva Nalawade AND Aakanksha Padwal;
    `stores-a185@candorfoods.in` is the store login AND Swapnil Raikar. Keying
    on those merges two people into one row, and one of them then vanishes from
    a report whose entire subject is who is missing.

    Two spellings of one name is NOT sharing — treating it as such splits one
    person into two accounts, and the quieter half is reported idle on a day
    they worked under the other spelling.
    """
    names_by_email: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        key = squash(base_fold(r["email"]))
        nm = fold(r["name"])
        if key and nm:
            names_by_email[key].add(nm)
    return {e for e, names in names_by_email.items()
            if any(not _compatible(x, y) for x in names for y in names)}


def _union(groups: list[set[str]]) -> list[list[int]]:
    """Indices of accounts that share at least one identity key."""
    parent = list(range(len(groups)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    seen: dict[str, int] = {}
    for i, keys in enumerate(groups):
        for k in keys:
            j = seen.setdefault(k, i)
            a, b = find(i), find(j)
            if a != b:
                parent[b] = a

    out: dict[int, list[int]] = defaultdict(list)
    for i in range(len(groups)):
        out[find(i)].append(i)
    return list(out.values())


def system_roster(db: Session) -> list[dict]:
    """Every active account on IMS, ERP and Stock Take, one row per person.

    A person holds up to three logins under three spellings — 'harsh@' on ERP,
    'harsharora@' on Stock Take, 'Harsh Arora' as a name — so accounts sharing
    any identity key are folded into one. Without that fold the block reports
    the same person twice and calls them idle under whichever login was quiet.
    """
    rows = _account_rows(db)
    shared = _shared_emails(rows)
    _, dir_by_squash = name_directory(db)

    keysets: list[set[str]] = []
    for r in rows:
        keys = set()
        for raw in (r["name"], r["username"]):
            for k in (squash(base_fold(raw)), squash(fold(raw))):
                if k:
                    keys.add(k)
        em = squash(base_fold(r["email"]))
        if em and em not in shared:
            keys.add(em)
            aliased = squash(fold(r["email"]))
            if aliased:
                keys.add(aliased)
        keysets.append(keys)

    accounts = []
    for group in _union(keysets):
        keys: set[str] = set()
        names: list[str] = []
        emails: list[str] = []
        systems: set[str] = set()
        for i in group:
            keys |= keysets[i]
            r = rows[i]
            systems.add(r["system"])
            if r["name"]:
                names.append(r["name"])
            if r["email"]:
                emails.append(r["email"])
        if not keys or _functional(keys):
            continue
        # The fullest spelling wins, and the staff directory is consulted for
        # logins that were never written out as a name ('rajupaikrao').
        pool = [n for n in names if n]
        for k in keys:
            hit = dir_by_squash.get(k)
            if hit:
                pool.append(hit)
        for k in keys:
            aliased = NAME_ALIASES.get(k)
            if aliased and " " in aliased:
                pool.append(aliased)
        display_name = titlecase(max(pool, key=rank)) if pool else titlecase(
            sorted(keys, key=len)[-1])
        accounts.append({"name": display_name, "keys": keys,
                         "email": sorted(emails)[0] if emails else "",
                         "systems": sorted(systems)})

    accounts.sort(key=lambda a: a["name"])
    logger.info("System roster: %d active accounts across IMS, ERP and Stock Take",
                len(accounts))
    return accounts


def _activity_by_key(data: dict) -> dict[str, set[date]]:
    """Actor days re-keyed on the squashed spelling, to match account keys."""
    out: dict[str, set[date]] = defaultdict(set)
    for actor, days in data["days"].items():
        out[squash(actor)] |= days
    return out


def _modules_by_key(store: dict) -> dict[str, set[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    for actor, vals in store.items():
        out[squash(actor)] |= vals
    return out


# ═════════════════════════════════════════════════════════════════════════
#  THE BLOCK
# ═════════════════════════════════════════════════════════════════════════
def compute_idle(db: Session, day: date) -> dict:
    """Which SYSTEM ACCOUNTS recorded nothing anywhere on `day`.

    The roster is the login tables, never the activity. That direction matters:
    an activity-derived roster can only contain people who did something, so the
    accounts that do nothing at all — the ones this block exists to surface —
    would be the exact set it could never see. It also keeps free text out: a
    job card assigned to 'Monika' or 'Nishant' names nobody with a login, and
    those are floor-staff names typed into a box, not users.

    Never raises: the rest of the report is worth sending even when this block
    cannot be built, so a failure degrades to an empty block and a log line.
    """
    blank = {"regular": [], "occasional": [], "dormant": [], "active": [],
             "total": 0, "idle": 0, "active_count": 0, "failed_sources": [],
             "unavailable": True, "since": day}
    try:
        roster = system_roster(db)
        data = collect(db, day)
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Idle-user block failed for %s: %s", day, exc)
        return blank

    by_key = _activity_by_key(data)
    mods_by_key = _modules_by_key(data["today_modules"])
    sys_by_key = _modules_by_key(data["today_systems"])

    # Business days only. Counting Sundays would put every account's tally
    # against a denominator it can never reach and demote the whole workforce.
    def business(ds: set[date]) -> int:
        return len({d for d in ds if d.weekday() != 6})

    regular, occasional, dormant, active = [], [], [], []
    for acc in roster:
        ds: set[date] = set()
        mods: set[str] = set()
        systems: set[str] = set()
        for k in acc["keys"]:
            ds |= by_key.get(k, set())
            mods |= mods_by_key.get(k, set())
            systems |= sys_by_key.get(k, set())

        base = {"name": acc["name"], "logins": acc["systems"],
                "email": acc["email"]}
        if day in ds:
            active.append({**base, "modules": sorted(mods),
                           "systems": sorted(systems)})
            continue

        n = business(ds)
        row = {**base, "active_days": n,
               "last_active": max(ds) if ds else None,
               "days_since": (day - max(ds)).days if ds else None}
        if n >= MIN_REGULAR_DAYS:
            regular.append(row)
        elif ds:
            occasional.append(row)
        else:
            # No trace in 30 days. A standing condition rather than today's
            # news, so it is named but kept out of the two working tiers.
            dormant.append(row)

    # Idle longest first: someone who last worked three weeks ago is a different
    # conversation from someone who was here yesterday.
    regular.sort(key=lambda r: (-(r["days_since"] or 0), r["name"]))
    occasional.sort(key=lambda r: (-(r["days_since"] or 0), r["name"]))
    dormant.sort(key=lambda r: r["name"])
    active.sort(key=lambda r: r["name"])

    idle = len(regular) + len(occasional) + len(dormant)
    logger.info("Daily report %s: %d of %d system accounts took no action "
                "(%d regular, %d occasional, %d dormant)",
                day, idle, len(roster), len(regular), len(occasional),
                len(dormant))

    return {"regular": regular, "occasional": occasional, "dormant": dormant,
            "active": active, "total": len(roster), "idle": idle,
            "active_count": len(active),
            "failed_sources": data["failed_sources"],
            "unavailable": False, "since": data["since"]}


def idle_over(db: Session, a: date, b: date, lookback: int = LOOKBACK_DAYS) -> dict:
    """Which system accounts did nothing across a whole range — the weekly form.

    Same roster as the daily block (the login tables), so the weekly and the
    seven dailies it summarises can never disagree about who exists.
    """
    try:
        roster = system_roster(db)
        data = collect(db, b, lookback=max(lookback, (b - a).days))
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Weekly idle block failed for %s..%s: %s", a, b, exc)
        return {"silent": [], "worked": [], "total": 0, "days": 0,
                "unavailable": True, "failed_sources": []}

    by_key = _activity_by_key(data)
    span = {a + timedelta(days=i) for i in range((b - a).days + 1)}
    business = {d for d in span if d.weekday() != 6}

    silent, worked = [], []
    for acc in roster:
        ds: set[date] = set()
        for k in acc["keys"]:
            ds |= by_key.get(k, set())
        inside = ds & span
        row = {"name": acc["name"], "logins": acc["systems"],
               "days": len(inside & business)}
        if inside:
            row["last_active"] = max(inside)
            worked.append(row)
        else:
            row["last_active"] = max(ds) if ds else None
            row["days_since"] = (b - max(ds)).days if ds else None
            silent.append(row)

    # Never-seen accounts last: an unused login is a standing fact, where a
    # keyer who stopped three weeks ago is a question for someone.
    silent.sort(key=lambda r: (r["days_since"] is None,
                               -(r["days_since"] or 0), r["name"]))
    worked.sort(key=lambda r: (-r["days"], r["name"]))
    logger.info("Weekly %s..%s: %d of %d system accounts did nothing all week",
                a, b, len(silent), len(roster))
    return {"silent": silent, "worked": worked, "total": len(roster),
            "days": len(business), "unavailable": False,
            "failed_sources": data["failed_sources"]}
