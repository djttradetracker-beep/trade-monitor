#!/usr/bin/env python3
"""
trump_trade_monitor.py  --  all-in-one watcher  (v2: reliable prices)
=====================================================================

Three jobs, one email pipeline:
  1) FILINGS   -- new Trump OGE filings -> instant alert.
  2) MENTIONS  -- news tying Trump to a held stock -> instant flag.
  3) TRACKER   -- real prices: return since buy date (and since comment date),
                  plus closed-position flags. Daily digest + weekly recap.

v2 change: prices now come from Twelve Data (a real keyed API) with Stooq as a
silent fallback. This fixes the "n/a" columns. You add ONE secret named
TWELVEDATA_API_KEY (see SETUP). Price lookups only happen on the first run,
the daily digest, and the weekly recap -- so you use ~14 of your 800 free
daily calls. No external libraries; pure Python 3 standard library.
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
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

CFG = {
    "NAME_FILTER": os.getenv("OGE_NAME_FILTER", "Trump"),
    "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", "465")),
    "SMTP_USER": os.getenv("SMTP_USER", ""),
    "SMTP_PASS": os.getenv("SMTP_PASS", ""),
    "EMAIL_FROM": os.getenv("EMAIL_FROM", os.getenv("SMTP_USER", "")),
    "EMAIL_TO": os.getenv("EMAIL_TO", os.getenv("SMTP_USER", "")),
    "TD_KEY": os.getenv("TWELVEDATA_API_KEY", ""),
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
HEADERS = {"User-Agent": "Mozilla/5.0 (TradeMonitor/2.0; personal use)",
           "Accept-Encoding": "identity"}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")


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
        log.warning("Bad %s (%s); default.", path, e)
        return default


def save_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def now_utc():
    return datetime.now(timezone.utc)


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
    targets = [f for f in fetch_filings() if nf in f["filer"].lower()]
    seen = set(state.get("seen_filings", []))
    first = not seen
    for f in targets:
        if f["id"] not in seen:
            seen.add(f["id"])
            if not first:
                alerts.append(
                    f"NEW FILING: {f['filer']} \u2014 {f['ftype']} (filed {f['date']})\n"
                    f"  {f['url']}\n"
                    f"  Check: does it record a SALE of a tracked name shortly "
                    f"before bad news for that stock?")
    state["seen_filings"] = sorted(seen)
    return alerts


def google_news(query):
    url = ("https://news.google.com/rss/search?q="
           + urllib.request.quote(query) + "&hl=en-US&gl=US&ceid=US:en")
    try:
        xml = http_get(url)
    except Exception as e:
        log.warning("News failed (%s): %s", query, e)
        return []
    items = []
    for b in re.findall(r"<item>(.*?)</item>", xml, re.S):
        t = re.search(r"<title>(.*?)</title>", b, re.S)
        l = re.search(r"<link>(.*?)</link>", b, re.S)
        d = re.search(r"<pubDate>(.*?)</pubDate>", b, re.S)
        if t:
            items.append({"title": html.unescape(t.group(1).strip()),
                          "link": l.group(1).strip() if l else "",
                          "pub": d.group(1).strip() if d else ""})
    return items


def check_mentions(state, holdings):
    alerts = []
    seen = set(state.get("seen_news", []))
    first = not seen
    for h in holdings:
        if h.get("status") == "closed":
            continue
        for alias in h.get("aliases", [h["name"]]):
            for it in google_news(f'Trump {alias}'):
                hid = str(abs(hash(f"{h['ticker']}|{it['title']}")))
                if hid in seen:
                    continue
                seen.add(hid)
                if first:
                    continue
                if alias.lower() in it["title"].lower():
                    alerts.append(
                        f"NEWS MENTION \u2014 {h['name']} ({h['ticker']}), a tracked holding:\n"
                        f"  \"{it['title']}\"\n  {it['link']}\n"
                        f"  ({it['pub']}) \u2014 heuristic flag; confirm he actually named it.")
    state["seen_news"] = sorted(seen)[-4000:]
    return alerts


def td_series(ticker, start_date):
    if not CFG["TD_KEY"]:
        return None
    end = now_utc().strftime("%Y-%m-%d")
    url = (f"https://api.twelvedata.com/time_series?symbol={ticker}"
           f"&interval=1day&start_date={start_date}&end_date={end}"
           f"&outputsize=5000&apikey={CFG['TD_KEY']}")
    try:
        data = json.loads(http_get(url))
    except Exception as e:
        log.warning("TD fetch failed %s: %s", ticker, e)
        return None
    if data.get("status") != "ok" or "values" not in data:
        log.warning("TD no data %s: %s", ticker,
                    data.get("message", data.get("status")))
        return None
    return {r["datetime"]: float(r["close"]) for r in data["values"] if r.get("close")}


def close_on_or_after(series, target):
    if not series:
        return None
    for d in sorted(series):
        if d >= target:
            return series[d]
    return None


def stooq_latest(ticker):
    try:
        rows = list(csv.DictReader(io.StringIO(http_get(
            f"https://stooq.com/q/l/?s={ticker.lower()}.us&f=sd2t2ohlcv&h&e=csv"))))
        c = rows[0].get("Close")
        return float(c) if c not in (None, "", "N/D") else None
    except Exception:
        return None


def stooq_close_on(ticker, date_str):
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
    url = (f"https://stooq.com/q/d/l/?s={ticker.lower()}.us"
           f"&d1={d.strftime('%Y%m%d')}&d2={(d+timedelta(days=6)).strftime('%Y%m%d')}&i=d")
    try:
        for r in csv.DictReader(io.StringIO(http_get(url))):
            if r.get("Close") not in (None, "", "N/D"):
                return float(r["Close"])
    except Exception:
        pass
    return None


def prices_for(h):
    series = td_series(h["ticker"], h["buy_date"])
    entry = cur = cbase = None
    if series:
        cur = series[sorted(series)[-1]]
        entry = close_on_or_after(series, h["buy_date"])
        if h.get("comment_date"):
            cbase = close_on_or_after(series, h["comment_date"])
    if cur is None:
        cur = stooq_latest(h["ticker"])
    if entry is None:
        entry = stooq_close_on(h["ticker"], h["buy_date"])
    if cbase is None and h.get("comment_date"):
        cbase = stooq_close_on(h["ticker"], h["comment_date"])
    return entry, cur, cbase


def build_tracker(holdings):
    lines = [f"{'TICKER':7} {'BUY DATE':11} {'ENTRY':>9} {'NOW':>9} "
             f"{'RET%':>8} {'SINCE CMT%':>11}  STATUS", "-" * 72]
    for h in holdings:
        entry, cur, cbase = prices_for(h)
        ret = (cur - entry) / entry * 100 if entry and cur else None
        cret = (cur - cbase) / cbase * 100 if cbase and cur else None
        e = f"${entry:.2f}" if entry else "  n/a"
        c = f"${cur:.2f}" if cur else "  n/a"
        rr = f"{ret:+.1f}%" if ret is not None else "   n/a"
        cr = f"{cret:+.1f}%" if cret is not None else "    n/a"
        lines.append(f"{h['ticker']:7} {h['buy_date']:11} {e:>9} {c:>9} "
                     f"{rr:>8} {cr:>11}  {h.get('status','open')}")
    lines.append("\nReturns are the STOCK's move, not his realized profit. "
                 "'Since cmt%' = move since a date he publicly named it.")
    return "\n".join(lines)


def send_email(subject, body):
    if not CFG["SMTP_USER"] or not CFG["SMTP_PASS"]:
        log.error("SMTP not set; printing:\n%s\n%s", subject, body)
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


def main():
    holdings = load_json(CFG["HOLDINGS_FILE"], {"holdings": []})["holdings"]
    state = load_json(CFG["STATE_FILE"], {})
    now = now_utc()
    today = now.strftime("%Y-%m-%d")
    iso_week = now.strftime("%G-W%V")
    elog = state.get("event_log", [])
    first = "seen_filings" not in state

    alerts = check_filings(state) + check_mentions(state, holdings)

    if first:
        latest = next((f for f in fetch_filings()
                       if CFG["NAME_FILTER"].lower() in f["filer"].lower()), None)
        intro = ("Trump trade monitor is live.\n\n"
                 f"Watching filer: {CFG['NAME_FILTER']}\n"
                 f"Tracking: {', '.join(h['ticker'] for h in holdings)}\n\n")
        if latest:
            intro += f"Latest filing: {latest['ftype']} ({latest['date']})\n{latest['url']}\n\n"
        intro += build_tracker(holdings)
        send_email("[Trump Monitor] Setup complete \u2014 it's running", intro)
    elif alerts:
        for a in alerts:
            elog.append(f"{today}: " + a.splitlines()[0])
        send_email(f"[Trump Monitor] {len(alerts)} new alert(s)", "\n\n".join(alerts))

    if (not first and now.hour >= CFG["DAILY_HOUR_UTC"]
            and state.get("last_daily") != today):
        send_email(f"[Trump Monitor] Daily tracker \u2014 {today}",
                   "Return on tracked holdings:\n\n" + build_tracker(holdings))
        state["last_daily"] = today

    if (not first and now.weekday() == 0 and now.hour >= CFG["DAILY_HOUR_UTC"]
            and state.get("last_weekly") != iso_week):
        recent = [l for l in elog if l >= (now - timedelta(days=7)).strftime("%Y-%m-%d")]
        body = ("Weekly recap.\n\nEvents in the last 7 days:\n"
                + ("\n".join(f"  - {l}" for l in recent) if recent else "  (none)")
                + "\n\nCurrent tracker:\n\n" + build_tracker(holdings))
        send_email(f"[Trump Monitor] Weekly summary \u2014 {iso_week}", body)
        state["last_weekly"] = iso_week

    state["event_log"] = elog[-500:]
    save_json(CFG["STATE_FILE"], state)
    log.info("Run complete. first=%s alerts=%d", first, 0 if first else len(alerts))


if __name__ == "__main__":
    main()
