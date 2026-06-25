#!/usr/bin/env python3
"""
trump_trade_monitor.py  --  all-in-one watcher
==============================================

Three jobs, one email pipeline:

  1) FILINGS   -- watches the OGE public disclosure database and alerts you the
                  moment a new Trump filing posts (new trades / annual report).
  2) MENTIONS  -- scans news coverage for fresh articles that tie Trump to a
                  company he holds, and flags them for your review.
  3) TRACKER   -- pulls real stock prices and reports each holding's return
                  since the buy date (and since any public-comment date),
                  plus flags closed positions.

EMAILS:
  - INSTANT  -> sent any run where a new filing or new news mention is found.
  - DAILY    -> one digest each morning with the return tracker table.
  - WEEKLY   -> a Monday recap of the week's filings, mentions, and returns.

HONEST LIMITS (so the email never oversells what it knows):
  - Filings are lagged ~45 days by law. This catches them when PUBLISHED.
  - News matching is a heuristic flag, not proof he "named a stock he owns."
    It errs toward over-flagging. You review the article.
  - Tracker shows the STOCK's performance, not his actual dollar profit
    (the filings disclose only value ranges, never share counts or prices).

NO external libraries. Pure Python 3 standard library. Designed to be run
once per scheduled tick (GitHub Actions calls it on a cron; see SETUP_GUIDE).
"""

import os
import re
import ssl
import csv
import io
import json
import html
import smtplib
import logging
import urllib.request
import urllib.error
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

# ----------------------------------------------------------------------------
# CONFIG  -- in GitHub Actions these come from "Secrets". Locally you can edit
# the defaults, but DO NOT paste your password here if you'll upload the file.
# ----------------------------------------------------------------------------
CFG = {
    "NAME_FILTER": os.getenv("OGE_NAME_FILTER", "Trump"),
    "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", "465")),
    "SMTP_USER": os.getenv("SMTP_USER", ""),
    "SMTP_PASS": os.getenv("SMTP_PASS", ""),
    "EMAIL_FROM": os.getenv("EMAIL_FROM", os.getenv("SMTP_USER", "")),
    "EMAIL_TO": os.getenv("EMAIL_TO", os.getenv("SMTP_USER", "")),
    # Hour (UTC) after which the daily digest / weekly summary may send.
    # 13 UTC is ~9am US Eastern in summer.
    "DAILY_HOUR_UTC": int(os.getenv("DAILY_HOUR_UTC", "13")),
    "HOLDINGS_FILE": os.getenv("HOLDINGS_FILE", "holdings.json"),
    "STATE_FILE": os.getenv("STATE_FILE", "state.json"),
}

OGE_BASE = "https://extapps2.oge.gov"
VIEW_URLS = [
    f"{OGE_BASE}/201/Presiden.nsf/PAS+Index?OpenView&ExpandView&Count=5000",
    f"{OGE_BASE}/201/Presiden.nsf/PAS+Index?OpenView&Count=5000",
]
LINK_RE = re.compile(
    r"/201/Presiden\.nsf/PAS\+Index/([0-9A-Fa-f]{16,})/\$[Ff][Ii][Ll][Ee]/([^\"'>\s]+?\.pdf)"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (TradeMonitor/1.0; personal use)",
           "Accept-Encoding": "identity"}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")


# ---------------------------- tiny helpers ----------------------------------
def http_get(url, binary=False, timeout=45):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout,
                                context=ssl.create_default_context()) as r:
        data = r.read()
    return data if binary else data.decode("utf-8", errors="replace")


def unq(s):
    try:
        return urllib.request.unquote(s)
    except Exception:
        return s


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning("Bad %s (%s); using default.", path, e)
        return default


def save_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def now_utc():
    return datetime.now(timezone.utc)


# ---------------------------- 1) FILINGS ------------------------------------
def fetch_filings():
    page = None
    for u in VIEW_URLS:
        try:
            page = http_get(u)
            if LINK_RE.search(page):
                break
        except Exception as e:
            log.warning("View fetch failed %s: %s", u, e)
    if not page:
        return []
    out, seen = [], set()
    for uid, fname in LINK_RE.findall(page):
        uid = uid.upper()
        if uid in seen:
            continue
        seen.add(uid)
        nm = unq(fname).replace(".pdf", "")
        m = re.match(r"(.+?)-(\d{1,2}\.\d{1,2}\.\d{4})-(.+)$", nm)
        filer = m.group(1).strip() if m else nm
        date = m.group(2) if m else ""
        ftype = m.group(3) if m else ("278e" if "278" in nm else "filing")
        out.append({"id": uid, "filer": filer, "date": date, "ftype": ftype,
                    "url": f"{OGE_BASE}/201/Presiden.nsf/PAS+Index/{uid}/$FILE/{fname}"})
    return out


def check_filings(state):
    alerts = []
    nf = CFG["NAME_FILTER"].lower()
    filings = fetch_filings()
    targets = [f for f in filings if nf in f["filer"].lower()]
    seen = set(state.get("seen_filings", []))
    first_run = not seen
    for f in targets:
        if f["id"] not in seen:
            seen.add(f["id"])
            if not first_run:
                alerts.append(
                    f"NEW FILING: {f['filer']} — {f['ftype']} (filed {f['date']})\n"
                    f"  {f['url']}\n"
                    f"  Worth a look: does it record a SALE of a tracked name "
                    f"shortly before bad news for that stock?")
    state["seen_filings"] = sorted(seen)
    return alerts


# ---------------------------- 2) MENTIONS -----------------------------------
def google_news(query):
    url = ("https://news.google.com/rss/search?q="
           + urllib.request.quote(query) + "&hl=en-US&gl=US&ceid=US:en")
    try:
        xml = http_get(url)
    except Exception as e:
        log.warning("News fetch failed (%s): %s", query, e)
        return []
    items = []
    for block in re.findall(r"<item>(.*?)</item>", xml, re.S):
        t = re.search(r"<title>(.*?)</title>", block, re.S)
        l = re.search(r"<link>(.*?)</link>", block, re.S)
        d = re.search(r"<pubDate>(.*?)</pubDate>", block, re.S)
        if not t:
            continue
        items.append({"title": html.unescape(t.group(1).strip()),
                      "link": (l.group(1).strip() if l else ""),
                      "pub": (d.group(1).strip() if d else "")})
    return items


def check_mentions(state, holdings):
    alerts = []
    seen = set(state.get("seen_news", []))
    first_run = not seen
    for h in holdings:
        if h.get("status") == "closed":
            continue
        for alias in h.get("aliases", [h["name"]]):
            for it in google_news(f'Trump {alias}'):
                key = f"{h['ticker']}|{it['title']}"
                hid = str(abs(hash(key)))
                if hid in seen:
                    continue
                seen.add(hid)
                if first_run:
                    continue
                # Only flag if the holding name actually appears in the headline.
                if alias.lower() in it["title"].lower():
                    alerts.append(
                        f"NEWS MENTION — {h['name']} ({h['ticker']}), a tracked holding:\n"
                        f"  \"{it['title']}\"\n"
                        f"  {it['link']}\n"
                        f"  ({it['pub']}) — heuristic flag; confirm he actually named it.")
    state["seen_news"] = sorted(seen)[-4000:]  # cap memory
    return alerts


# ---------------------------- 3) TRACKER ------------------------------------
def stooq_latest(ticker):
    url = f"https://stooq.com/q/l/?s={ticker.lower()}.us&f=sd2t2ohlcv&h&e=csv"
    try:
        rows = list(csv.DictReader(io.StringIO(http_get(url))))
        c = rows[0].get("Close")
        return float(c) if c not in (None, "", "N/D") else None
    except Exception as e:
        log.warning("Latest price failed %s: %s", ticker, e)
        return None


def stooq_close_on(ticker, date_str):
    """Close on/near a date (widen a few days for weekends/holidays)."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
    d1 = d.strftime("%Y%m%d")
    d2 = (d + timedelta(days=6)).strftime("%Y%m%d")
    url = (f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&d1={d1}&d2={d2}&i=d")
    try:
        rows = list(csv.DictReader(io.StringIO(http_get(url))))
        for r in rows:
            if r.get("Close") not in (None, "", "N/D"):
                return float(r["Close"])
    except Exception as e:
        log.warning("Hist price failed %s: %s", ticker, e)
    return None


def build_tracker(holdings):
    lines, rows = [], []
    for h in holdings:
        entry = stooq_close_on(h["ticker"], h["buy_date"])
        cur = stooq_latest(h["ticker"])
        ret = None
        if entry and cur:
            ret = (cur - entry) / entry * 100.0
        comment_ret = None
        if h.get("comment_date"):
            cp = stooq_close_on(h["ticker"], h["comment_date"])
            if cp and cur:
                comment_ret = (cur - cp) / cp * 100.0
        rows.append({"h": h, "entry": entry, "cur": cur,
                     "ret": ret, "cret": comment_ret})
    # text table
    lines.append(f"{'TICKER':7} {'BUY DATE':11} {'ENTRY':>9} {'NOW':>9} "
                 f"{'RET%':>8} {'SINCE CMT%':>11}  STATUS")
    lines.append("-" * 72)
    for r in rows:
        h = r["h"]
        e = f"${r['entry']:.2f}" if r["entry"] else "  n/a"
        c = f"${r['cur']:.2f}" if r["cur"] else "  n/a"
        rr = f"{r['ret']:+.1f}%" if r["ret"] is not None else "   n/a"
        cr = f"{r['cret']:+.1f}%" if r["cret"] is not None else "    n/a"
        lines.append(f"{h['ticker']:7} {h['buy_date']:11} {e:>9} {c:>9} "
                     f"{rr:>8} {cr:>11}  {h.get('status','open')}")
    lines.append("\nReturns are the STOCK's move, not his realized profit. "
                 "'Since cmt%' = move since a date he publicly named it.")
    return "\n".join(lines)


# ---------------------------- email -----------------------------------------
def send_email(subject, body):
    if not CFG["SMTP_USER"] or not CFG["SMTP_PASS"]:
        log.error("SMTP not configured; printing instead:\n%s\n%s", subject, body)
        return
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, CFG["EMAIL_FROM"], CFG["EMAIL_TO"]
    msg.set_content(body)
    ctx = ssl.create_default_context()
    if CFG["SMTP_PORT"] == 465:
        with smtplib.SMTP_SSL(CFG["SMTP_HOST"], CFG["SMTP_PORT"], context=ctx) as s:
            s.login(CFG["SMTP_USER"], CFG["SMTP_PASS"]); s.send_message(msg)
    else:
        with smtplib.SMTP(CFG["SMTP_HOST"], CFG["SMTP_PORT"]) as s:
            s.starttls(context=ctx); s.login(CFG["SMTP_USER"], CFG["SMTP_PASS"])
            s.send_message(msg)
    log.info("Email sent: %s", subject)


# ---------------------------- orchestration ---------------------------------
def main():
    holdings = load_json(CFG["HOLDINGS_FILE"], {"holdings": []})["holdings"]
    state = load_json(CFG["STATE_FILE"], {})
    now = now_utc()
    today = now.strftime("%Y-%m-%d")
    iso_week = now.strftime("%G-W%V")
    log_lines = state.get("event_log", [])

    first_run = "seen_filings" not in state

    # --- instant checks every run ---
    alerts = []
    alerts += check_filings(state)
    alerts += check_mentions(state, holdings)

    if first_run:
        latest = next((f for f in fetch_filings()
                       if CFG["NAME_FILTER"].lower() in f["filer"].lower()), None)
        intro = ("Trump trade monitor is live.\n\n"
                 f"Watching filer: {CFG['NAME_FILTER']}\n"
                 f"Tracking {len(holdings)} holdings: "
                 f"{', '.join(h['ticker'] for h in holdings)}\n\n")
        if latest:
            intro += f"Latest filing on record: {latest['ftype']} ({latest['date']})\n{latest['url']}\n\n"
        intro += "You'll get: instant alerts on new filings/mentions, a daily tracker digest, and a weekly recap.\n\n"
        intro += build_tracker(holdings)
        send_email("[Trump Monitor] Setup complete \u2014 it's running", intro)
    elif alerts:
        for a in alerts:
            log_lines.append(f"{today}: " + a.splitlines()[0])
        send_email(f"[Trump Monitor] {len(alerts)} new alert(s)",
                   "\n\n".join(alerts))

    # --- daily digest ---
    if (not first_run and now.hour >= CFG["DAILY_HOUR_UTC"]
            and state.get("last_daily") != today):
        send_email(f"[Trump Monitor] Daily tracker \u2014 {today}",
                   "Return on tracked holdings:\n\n" + build_tracker(holdings))
        state["last_daily"] = today

    # --- weekly summary (Mondays) ---
    if (not first_run and now.weekday() == 0 and now.hour >= CFG["DAILY_HOUR_UTC"]
            and state.get("last_weekly") != iso_week):
        recent = [l for l in log_lines if l >= (now - timedelta(days=7)).strftime("%Y-%m-%d")]
        body = ("Weekly recap.\n\nEvents in the last 7 days:\n"
                + ("\n".join(f"  - {l}" for l in recent) if recent else "  (none)")
                + "\n\nCurrent tracker:\n\n" + build_tracker(holdings))
        send_email(f"[Trump Monitor] Weekly summary \u2014 {iso_week}", body)
        state["last_weekly"] = iso_week

    state["event_log"] = log_lines[-500:]
    save_json(CFG["STATE_FILE"], state)
    log.info("Run complete. %d instant alert(s).", 0 if first_run else len(alerts))


if __name__ == "__main__":
    main()
