"""One renderer for all four weekly roll-ups.

WHY ONE RENDERER AND NOT FOUR
    The streams differ entirely in what they measure and not at all in how a
    week reads: a headline row, seven days down the left, then the breakdowns.
    Four renderers would drift into four different-looking mails from one
    system, and every Gmail lesson already paid for in `daily_report_html`
    would have to be relearned in each of them.

    So each stream's builder returns a normalised structure — tiles, flags, a
    day-by-day block and a list of tables — and this walks it. Adding a fifth
    weekly means writing a builder, not a template.

COLOUR CARRIES THE STREAM
    Four weeklies land in the same inbox within seconds of each other at 10:00
    on Monday. Each takes its parent report's colour so the one you want is
    identifiable before a word is read — and so the weekly is visibly the same
    family as the daily it summarises.
"""
from __future__ import annotations

from datetime import date, datetime

from services.ims_service.daily_report_html import (
    BAND, GREY, INK, RULE, FS_H3, FS_NOTE, FS_SECTION,
    _tone_helpers, e, flag, h3, tiles,
)

MAIL_ROW_CAP = 14
MAIL_SAFE_BYTES = 92_000
MAIL_MIN_ROW_CAP = 4


def _span(start: date, end: date) -> str:
    if start.month == end.month:
        return f"{start:%d}–{end:%d %B %Y}"
    return f"{start:%d %B}–{end:%d %B %Y}"


def _render_tables(T, blocks, cap) -> str:
    out = ""
    for b in blocks:
        if not b:
            continue
        out += h3(b["title"], tone=_TONE[0])
        kw = {"cap": b.get("cap", cap) if b.get("cap") is not None else cap,
              "total": b.get("total"), "note": b.get("note"),
              "widths": b.get("widths"), "keep": b.get("keep")}
        if b.get("empty"):
            kw["empty"] = b["empty"]
        out += T(b["headers"], b["rows"], b.get("aligns"),
                 **{k: v for k, v in kw.items() if v is not None})
    return out


# The tone in play for the block currently being rendered. Set once per render
# call: `h3` needs it and threading it through every helper for one colour is
# more machinery than the problem deserves.
_TONE = [None]


def render_email(rep: dict, generated: datetime, *,
                 view_url: str | None = None, _cap: int | None = None) -> str:
    cap = MAIL_ROW_CAP if _cap is None else _cap
    tone = rep["tone"]
    _TONE[0] = tone
    T, _, _ = _tone_helpers(tone, True)

    body = tiles(rep["tiles"], tone=tone)
    for text_, kind in rep.get("flags", ()):
        body += flag(e(text_), kind)

    d = rep["days"]
    body += h3("Day by day", tone=tone)
    body += T(d["headers"], d["rows"], d.get("aligns"), keep=d.get("keep"),
              note="Monday to Sunday. A day with no activity is shown as a dash "
                   "rather than left out, so a gap in the week is visible.",
              empty="No activity recorded in this week")
    body += _render_tables(T, rep["tables"], cap)

    cta = ""
    if view_url:
        cta = (
            f'<div style="margin:0 0 16px;">'
            f'<a href="{e(view_url)}" style="display:inline-block;padding:13px 22px;'
            f'background:{tone["deep"]};color:#fff;text-decoration:none;'
            f'border-radius:8px;font:700 15px Arial,Helvetica,sans-serif;">'
            f'Open the full week &rarr;</a></div>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{e(rep["title"])} — {_span(rep["start"], rep["end"])}</title>
<style>
  body {{ margin:0; padding:0; background:#F1F4F9;
          -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; }}
  a {{ color:{tone["deep"]}; }}
  .wrap {{ max-width:920px; margin:0 auto; }}
  table {{ mso-table-lspace:0; mso-table-rspace:0; }}
  @media only screen and (max-width:600px) {{
    .pad {{ padding:14px !important; }}
    .tiles td.tile {{ display:block !important; width:auto !important; margin-bottom:8px; }}
    .tiles td {{ width:auto !important; }}
    .hdr h1 {{ font-size:19px !important; }}
    .scroll table {{ font-size:20px !important; }}
    .scroll td, .scroll th {{ padding:11px 7px !important; }}
    .wrap {{ padding:8px 4px !important; }}
  }}
</style></head>
<body>
<div class="wrap" style="padding:16px 10px;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
         style="background:#fff;border-radius:10px;overflow:hidden;
                box-shadow:0 1px 4px rgba(0,0,0,.09);">
    <tr><td class="hdr" style="background:{tone["deep"]};padding:16px 18px;">
      <h1 style="margin:0;font:700 18px Arial,Helvetica,sans-serif;color:#fff;">
        {e(rep["title"])}</h1>
      <div style="font:12px Arial,Helvetica,sans-serif;color:#D6DEEC;margin-top:3px;">
        {_span(rep["start"], rep["end"])} &nbsp;·&nbsp; {e(rep["subtitle"])}
      </div>
    </td></tr>
    <tr><td class="pad" style="padding:16px 18px 20px;">{cta}{body}</td></tr>
    <tr><td style="background:{BAND};padding:11px 18px;text-align:center;
                   font:11px Arial,Helvetica,sans-serif;color:{GREY};">
      Candor Foods — weekly roll-up of {e(rep["source"])}, sent every Monday at
      10:00 AM IST to that report's own recipients.
      <br>Week {rep["start"]:%d %b} to {rep["end"]:%d %b %Y} &nbsp;·&nbsp;
      generated {generated:%d %b %Y, %I:%M:%S %p} IST
    </td></tr>
  </table>
</div>
</body></html>"""

    if len(html.encode("utf-8")) > MAIL_SAFE_BYTES and cap > MAIL_MIN_ROW_CAP:
        return render_email(rep, generated, view_url=view_url,
                            _cap=max(MAIL_MIN_ROW_CAP, cap - 4))
    return html


def render_page(rep: dict, generated: datetime) -> str:
    """The hosted week — every row, no caps."""
    tone = rep["tone"]
    _TONE[0] = tone
    T, _, _ = _tone_helpers(tone, False)

    body = tiles(rep["tiles"], tone=tone)
    for text_, kind in rep.get("flags", ()):
        body += flag(e(text_), kind)
    d = rep["days"]
    body += h3("Day by day", tone=tone)
    body += T(d["headers"], d["rows"], d.get("aligns"),
              empty="No activity recorded in this week")
    body += _render_tables(T, rep["tables"], None)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{e(rep["title"])} — {_span(rep["start"], rep["end"])}</title>
<style>
  *,*::before,*::after{{box-sizing:border-box}}
  body{{margin:0;background:#F1F4F9;color:{INK};
       font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif}}
  header{{background:{tone["deep"]};color:#fff;padding:18px 0}}
  header .in{{max-width:1120px;margin:0 auto;padding:0 12px}}
  h1{{margin:0;font-size:20px}}
  .sub{{color:#D6DEEC;font-size:13px;margin-top:3px}}
  .wrap{{max-width:1120px;margin:0 auto;padding:16px 12px 48px;
         background:#fff;border-radius:0 0 10px 10px}}
  .scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
  table{{width:100%}}
  @media (prefers-color-scheme:dark){{
    body{{background:#10151F;color:#E5E9F0}}
    .wrap{{background:#18202E}}
  }}
</style></head>
<body>
<header><div class="in">
  <h1>{e(rep["title"])}</h1>
  <div class="sub">{_span(rep["start"], rep["end"])} &nbsp;·&nbsp;
       {e(rep["subtitle"])} &nbsp;·&nbsp; generated
       {generated:%d %b %Y, %I:%M %p} IST</div>
</div></header>
<div class="wrap">{body}</div>
</body></html>"""


def render_plain(rep: dict, generated: datetime) -> str:
    """Text alternative — the same week, for clients that refuse HTML."""
    lines = [f'{rep["title"]}: {_span(rep["start"], rep["end"])}', "",
             rep["subtitle"], ""]

    lines.append("HEADLINE")
    for label, value, sub in rep["tiles"]:
        lines.append(f"  {label}: {value}" + (f" ({sub})" if sub else ""))

    for text_, _kind in rep.get("flags", ()):
        lines += ["", f"! {text_}"]

    d = rep["days"]
    lines += ["", "DAY BY DAY", "  " + " | ".join(d["headers"])]
    for r in d["rows"]:
        lines.append("  " + " | ".join(str(c) for c in r))

    for b in rep["tables"]:
        if not b:
            continue
        lines += ["", b["title"].upper()]
        if not b["rows"]:
            lines.append("  " + (b.get("empty") or "Nothing recorded"))
            continue
        lines.append("  " + " | ".join(b["headers"]))
        for r in b["rows"][:40]:
            lines.append("  " + " | ".join(str(c) for c in r))
        if len(b["rows"]) > 40:
            lines.append(f"  ... +{len(b['rows']) - 40} more rows")
        if b.get("total"):
            lines.append("  " + " | ".join(str(c) for c in b["total"]))

    lines += ["", f'Weekly roll-up of {rep["source"]} — generated '
                  f'{generated:%d %b %Y, %I:%M %p} IST']
    return "\n".join(lines)
