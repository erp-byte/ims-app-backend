# Mail identity — IMS + ERP

Every notification from both systems used to arrive as **“erp”** in the sender column,
because each sender set `From` to the bare address with no display name. The sender
column is fixed-width, always visible and first in the reading path, so it now carries
the module.

Implemented in `shared/mail_identity.py` (IMS) and `app/core/mail_identity.py` (ERP).
Same file, two codebases — they have separate settings objects and no shared package,
so a copy is the honest option.

---

## What lands on every mail

```
From:            "Candor · Job Work" <erp@candorfoods.in>
List-Id:         Candor Job Work <jobwork.candorfoods.in>
Auto-Submitted:  auto-generated
X-Candor-System: IMS
X-Candor-Module: Job Work
X-Candor-Entity-Type / -Entity-ID / -Event / -Status / -Stage / -Actor / -Env
```

`Auto-Submitted` stops out-of-office auto-replies bouncing back into the ERP mailbox.

---

## The two subject policies — read this before changing a subject

Gmail groups a conversation by **normalised subject *plus* References**. Change the
subject between two mails and the thread splits, even with a perfect References chain.
`sample_mail_service._thread_subject` documents this in its own docstring.

| Policy | Subject | Used by |
|---|---|---|
| **ANCHOR** | untouched, with a **constant module glyph** prefixed | Customer returns, NPD, job work, production |
| **EVENT** | rewritten: `<glyph> <ID> — <what changed>` | Inward deletion, daily report, weekly digest, verification codes |

ANCHOR is the safe default. The glyph it adds is per **module**, never per event, so the
normalised subject is identical across a whole trail and no existing conversation can be
split. `Re:` is kept at the very front, where Gmail needs it.

EVENT is only for mails that are genuinely one-off. Those also get `X-Entity-Ref-ID`,
which keeps unrelated mails with similar subjects out of each other's threads.

**Nothing in this layer touches `Message-ID`, `In-Reply-To`, `References` or any mail
body.** Existing threading and the per-event banners keep working exactly as before.

---

## Modules

| Sender shows as | List-Id | Glyph | Policy |
|---|---|---|---|
| Candor · Returns | `returns.candorfoods.in` | ↩️ | ANCHOR |
| Candor · NPD | `npd.candorfoods.in` | 🧪 | ANCHOR |
| Candor · Job Work | `jobwork.candorfoods.in` | 🔧 | ANCHOR (digest: EVENT) |
| Candor · Production | `production.candorfoods.in` | 🏭 | ANCHOR |
| Candor · Inward | `inward.candorfoods.in` | 📥 | EVENT |
| Candor · Transfers | `transfers.candorfoods.in` | 🔄 | — (reserved) |
| Candor · Daily Report | `reports.candorfoods.in` | 📊 | EVENT |
| Candor · Security | `security.candorfoods.in` | 🔐 | EVENT |

Event glyphs (EVENT policy only): 🆕 raised · ✅ approved/issued · ❌ rejected ·
🚫 cancelled · 🗑️ deleted · 🚚 dispatched · ⏳ awaiting · ⏸️ hold · ⚠️ action needed ·
🔄 updated · 📦 ready · 📊 report.

---

## Gmail filters — paste one per module

Gmail cannot filter on `X-` headers, but it **does** support `List-Id` through the
`list:` search operator, and that works inside filters. No plus-addressing, no extra
mailboxes, no change to any recipient list.

**Settings → Filters and blocked addresses → Create a new filter → Has the words:**

| Has the words | Then | Label |
|---|---|---|
| `list:returns.candorfoods.in` | Apply label | `Candor/Returns` |
| `list:npd.candorfoods.in` | Apply label | `Candor/NPD` |
| `list:jobwork.candorfoods.in` | Apply label | `Candor/Job Work` |
| `list:production.candorfoods.in` | Apply label | `Candor/Production` |
| `list:inward.candorfoods.in` | Apply label | `Candor/Inward` |
| `list:reports.candorfoods.in` | Apply label | `Candor/Daily Report` |
| `list:security.candorfoods.in` | Apply label, Never send to Spam | `Candor/Security` |

Worth adding on top:

- `subject:(❌ OR ⚠️ OR 🚫)` → **Star it** and **Mark as important**, so rejections and
  escalations surface above everything else.
- Colour each label; the sender name and the label colour then agree at a glance.
- Settings → Inbox → **Multiple inboxes** with `list:npd.candorfoods.in` etc. as sections
  gives each module its own pane on one screen.

---

## Test / staging sends

Set `MAIL_ENV=TEST` in the environment. The sender becomes `[TEST] Candor · NPD`, EVENT
subjects gain a `[TEST]` prefix, and `X-Candor-Env: TEST` is set — so a rehearsal can
never be mistaken for the real thing. Unset (or `PROD`) leaves everything clean.

---

## Adding a new mail

1. Pick the module — add one to `Module` + `MODULES` only if it is genuinely new.
2. Pick the policy. If any other mail about the same entity shares this subject,
   it is **ANCHOR**. If this mail happens once and stands alone, it is **EVENT**.
3. Call `stamp(msg, module=…, policy=…, entity_type=…, entity_id=…, event=…, status=…)`
   after the message is built and after any threading headers are set.
