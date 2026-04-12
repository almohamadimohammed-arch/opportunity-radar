import requests
import json
import os
import re
import time
from datetime import datetime, timedelta

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
NEWS_KEY = os.environ.get("NEWS_API_KEY", "")
GNEWS_KEY = os.environ.get("GNEWS_API_KEY", "")
REDDIT_HEADERS = {"User-Agent": "OpportunityRadar/4.0"}

TIER1_NAMES = ["reuters","bloomberg","arab news","spa","saudi press agency","al arabiya",
         "cnbc","financial times","ft.com","associated press","wall street journal",
         "wsj","bbc","guardian","new york times","nyt","economist","politico",
         "government","ministry"]

TIER2_NAMES = ["techcrunch","wired","the verge","ars technica","zdnet","cnn","al jazeera",
         "independent","fortune","business insider","mashable","engadget","vice",
         "mit technology review","nature","science","ieee","axios","the information",
         "rest of world","semafor","nikkei"]

def get_source_tier(source_name, origin):
    s = source_name.lower()
    for t in TIER1_NAMES:
        if t in s:
            return 1
    for t in TIER2_NAMES:
        if t in s:
            return 2
    if origin in ["newsapi", "gnews"]:
        return 2
    if origin in ["reddit", "reddit_search"]:
        return 3
    return 4

RELEVANCE_KEYWORDS = [
    "saudi","vision 2030","neom","sama","pif","gulf","gcc","aramco","stc",
    "qiddiya","sdaia","riyadh","jeddah","mena","middle east","bahrain","kuwait",
    "uae","qatar","oman","opec","oil price","crude",
    "artificial intelligence","ai ","machine learning","deep learning","llm",
    "quantum","cybersecurity","cyber attack","data breach","ransomware",
    "fintech","blockchain","digital currency","cbdc",
    "semiconductor","chip","autonomous","drone","space","satellite",
    "hydrogen","renewable","solar","nuclear","carbon",
    "regulation","policy","sanction","tariff","trade war","export control",
    "investment","venture capital","ipo","funding round","acquisition",
    "startup","enterprise","cloud","saas","infrastructure",
    "defense","military","surveillance","intelligence"
]

def is_relevant(title, desc):
    text = (title + " " + (desc or "")).lower()
    matches = sum(1 for kw in RELEVANCE_KEYWORDS if kw in text)
    return matches >= 1

STOP_WORDS = {"the","a","an","is","are","was","were","be","been","being","have","has",
              "had","do","does","did","will","would","could","should","may","might",
              "can","shall","to","of","in","for","on","with","at","by","from","as",
              "into","through","during","before","after","above","below","between",
              "out","off","over","under","again","further","then","once","here","there",
              "when","where","why","how","all","both","each","few","more","most","other",
              "some","such","no","nor","not","only","own","same","so","than","too","very",
              "just","but","and","or","if","about","up","its","it","this","that","these",
              "new","said","says","also","one","two","first","last","us","we","they","he",
              "she","his","her","their","my","your","our","what","which","who","whom"}

def get_topic_key(title):
    words = re.sub(r'[^a-z0-9\s]', '', title.lower()).split()
    meaningful = [w for w in words if w not in STOP_WORDS and len(w) > 2]
    return set(meaningful[:8])

def deduplicate_by_topic(articles):
    groups = []
    for a in articles:
        key = get_topic_key(a["title"])
        if not key:
            continue
        merged = False
        for g in groups:
            overlap = len(key & g["key"])
            union = len(key | g["key"])
            if union > 0 and overlap / union > 0.4:
                if a["source_tier"] < g["best"]["source_tier"]:
                    g["best"] = a
                g["sources"].append(a["source"])
                g["key"] = key | g["key"]
                merged = True
                break
        if not merged:
            groups.append({"key": key, "best": a, "sources": [a["source"]]})
    result = []
    for g in groups:
        item = g["best"].copy()
        item["corroborating_sources"] = list(set(g["sources"]))
        item["source_count"] = len(set(g["sources"]))
        result.append(item)
    return result

def fetch_and_filter():
    raw = []
    cutoff = (datetime.now() - timedelta(hours=72)).strftime("%Y-%m-%d")

    for q in ["Saudi Arabia Vision 2030","NEOM Saudi investment","SAMA fintech regulation","Saudi oil energy","Gulf geopolitics trade","Saudi tourism","PIF Saudi fund"]:
        try:
            r = requests.get("https://newsapi.org/v2/everything", params={"q": q, "sortBy": "publishedAt", "pageSize": 5, "language": "en", "apiKey": NEWS_KEY, "from": cutoff}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    raw.append({"title": a.get("title","").strip(), "source": a.get("source",{}).get("name",""), "desc": (a.get("description","") or "").strip(), "date": a.get("publishedAt","")[:10], "origin": "newsapi"})
        except:
            pass

    for q in ["AI breakthrough","quantum computing","cybersecurity breach","semiconductor chip","autonomous AI","space technology"]:
        try:
            r = requests.get("https://gnews.io/api/v4/search", params={"q": q, "max": 5, "lang": "en", "token": GNEWS_KEY}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    raw.append({"title": a.get("title","").strip(), "source": a.get("source",{}).get("name",""), "desc": (a.get("description","") or "").strip(), "date": a.get("publishedAt","")[:10], "origin": "gnews"})
        except:
            pass

    for sub in ["worldnews","news","geopolitics","saudiarabia","business","economics","finance","technology","TechNews","ArtificialIntelligence","MachineLearning","Futurology","netsec","DataBreaches","hardware","gadgets","startups","Entrepreneur","SaaS"]:
        try:
            r = requests.get("https://www.reddit.com/r/" + sub + "/hot.json?limit=8&t=week", headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                for post in r.json().get("data", {}).get("children", []):
                    d = post.get("data", {})
                    if d.get("stickied") or d.get("score", 0) < 20:
                        continue
                    created = d.get("created_utc", 0)
                    raw.append({"title": d.get("title","").strip(), "source": "Reddit r/" + sub + " (" + str(d.get("score",0)) + " pts)", "desc": (d.get("selftext","") or "")[:300].strip(), "date": datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else "", "origin": "reddit"})
            time.sleep(1)
        except:
            pass

    for q, sub in [("Saudi Arabia","worldnews"),("Vision 2030","saudiarabia"),("NEOM","saudiarabia"),("AI regulation","technology"),("quantum computing","technology"),("semiconductor","geopolitics"),("cybersecurity breach","netsec"),("data breach","DataBreaches"),("AI startup","startups")]:
        try:
            r = requests.get("https://www.reddit.com/r/" + sub + "/search.json?q=" + q + "&sort=new&t=week&limit=3", headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                for post in r.json().get("data", {}).get("children", []):
                    d = post.get("data", {})
                    created = d.get("created_utc", 0)
                    raw.append({"title": d.get("title","").strip(), "source": "Reddit r/" + sub + " (" + str(d.get("score",0)) + " pts)", "desc": (d.get("selftext","") or "")[:300].strip(), "date": datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else "", "origin": "reddit_search"})
            time.sleep(1)
        except:
            pass

    print(f"  Raw fetched: {len(raw)}")
    raw = [a for a in raw if a["title"] and len(a["title"]) > 15]
    print(f"  After title filter: {len(raw)}")
    raw = [a for a in raw if a["origin"] in ["reddit","reddit_search"] or len(a.get("desc","")) >= 50]
    print(f"  After desc length filter: {len(raw)}")
    raw = [a for a in raw if is_relevant(a["title"], a.get("desc",""))]
    print(f"  After relevance filter: {len(raw)}")
    for a in raw:
        a["source_tier"] = get_source_tier(a["source"], a["origin"])
    t1 = len([a for a in raw if a['source_tier']==1])
    t2 = len([a for a in raw if a['source_tier']==2])
    t3 = len([a for a in raw if a['source_tier']==3])
    t4 = len([a for a in raw if a['source_tier']==4])
    print(f"  Tier distribution: T1={t1}, T2={t2}, T3={t3}, T4={t4}")
    raw = [a for a in raw if a["source_tier"] <= 3]
    print(f"  After tier filter (drop T4): {len(raw)}")
    seen = set()
    unique = []
    for a in raw:
        t = a["title"].strip().lower()
        if t not in seen:
            seen.add(t)
            unique.append(a)
    print(f"  After exact dedup: {len(unique)}")
    deduped = deduplicate_by_topic(unique)
    print(f"  After topic dedup: {len(deduped)}")
    deduped.sort(key=lambda a: a["source_tier"])
    capped = deduped[:30]
    print(f"  Final to model: {len(capped)} (capped at 30)")
    return capped

def generate_radar(articles):
    feed = ""
    for a in articles:
        tier_label = "T" + str(a["source_tier"])
        sources = str(a.get("source_count", 1)) + " source(s)"
        feed += "[" + tier_label + "][" + sources + "] " + a["title"] + " | " + a["source"] + " | " + a["date"] + "\n  " + (a["desc"] or "")[:200] + "\n"
    today = datetime.now().strftime("%Y-%m-%d")
    prompt = "You are the Intelligence Layer of a strategic sensing system for Saudi Arabia.\n\nTODAY: " + today + "\n\nBelow are pre-filtered, source-scored articles. Each is tagged with source tier (T1=authoritative like Reuters/Bloomberg, T2=major publications or professional news APIs, T3=Reddit community) and number of corroborating sources.\n\n" + feed + "\n\nYour job: surface NEWLY OBSERVABLE information with possible strategic consequence.\n\nRULES:\n- You may return between 0 and 15 items.\n- If the source material today is weak, return fewer items. Do NOT promote weak material.\n- Do NOT invent or embellish. Stick to what the sources actually report.\n- Each item must have a verification_status: confirmed (multiple authoritative sources), reported (single authoritative source), alleged (single non-authoritative source), rumored (unverified claim).\n- Do NOT assign numeric novelty or credibility scores. Those are not computable.\n\nRespond ONLY with a JSON array:\n[{\"title\":\"what was newly observed - factual\",\"source\":\"original source\",\"source_tier\":1,\"time\":\"YYYY-MM-DD\",\"category\":\"technology|business|geopolitics|security|regulation|science|market|infrastructure\",\"geography\":\"Saudi|Gulf|Global|specific\",\"affected_domains\":[\"domain1\",\"domain2\"],\"summary\":\"2-3 factual sentences on what was observed\",\"why_it_matters\":\"1-2 sentences on possible strategic consequence for Saudi Arabia\",\"verification_status\":\"confirmed|reported|alleged|rumored\"}]"
    result = call_groq(prompt)
    if not result:
        return None
    valid_statuses = ["confirmed", "reported", "alleged", "rumored"]
    cleaned = []
    for item in result:
        if item.get("verification_status") not in valid_statuses:
            item["verification_status"] = "alleged"
        cleaned.append(item)
    return cleaned

def generate_signals(radar_items):
    intel = ""
    for r in radar_items:
        intel += "- [" + r.get("verification_status","?") + "][T" + str(r.get("source_tier",4)) + "] " + r.get("title","") + "\n  " + r.get("summary","") + "\n"
    prompt = "You are the Signal Layer. Your job is NOT to repeat headlines. Your job is to identify WHAT CHANGED.\n\nA signal is NOT the event. It is the CHANGE that follows from the event.\n\nExample: Event='SAMA releases fintech rule' -> Signal='Regulatory barrier to fintech integration has reduced, making bank-fintech partnerships more feasible'\n\nRULES:\n- You may return between 0 and 10 signals.\n- Each signal MUST cite at least 2 intelligence items as evidence. List their exact titles.\n- Exception: a single-item signal is allowed ONLY if the source is T1 and verification_status is confirmed.\n- For each signal, state ONE THING that would invalidate it.\n- Do NOT assign numeric confidence scores.\n- Assign evidence_strength: strong (3+ corroborating items), moderate (2 items), weak (1 T1 confirmed item).\n- If you cannot find enough evidence for a change, do NOT include it.\n\nINTELLIGENCE ITEMS:\n" + intel + "\n\nRespond ONLY with a JSON array:\n[{\"statement\":\"What is different now - the validated change\",\"change_type\":\"demand|regulation|cost|supply|competition|risk|feasibility|trust|behavior|infrastructure\",\"mechanism\":\"How this change works - based on evidence, not speculation\",\"evidence\":[\"exact title from intelligence 1\",\"exact title from intelligence 2\"],\"evidence_strength\":\"strong|moderate|weak\",\"affected_actors\":[\"who is affected\"],\"time_horizon\":\"now|1-2yr|3-5yr|later\",\"upside\":\"what good could come\",\"downside\":\"what risk this creates\",\"first_order\":\"direct effect\",\"second_order\":\"downstream effect\",\"invalidation\":\"what would prove this signal wrong\"}]"
    result = call_groq(prompt)
    if not result:
        return None
    cleaned = []
    for sig in result:
        evidence = sig.get("evidence", [])
        if not evidence:
            print(f"  DROPPED signal (no evidence): {sig.get('statement','')[:60]}")
            continue
        ev_count = len(evidence)
        if ev_count >= 3:
            sig["evidence_strength"] = "strong"
        elif ev_count >= 2:
            sig["evidence_strength"] = "moderate"
        else:
            sig["evidence_strength"] = "weak"
        if not sig.get("invalidation"):
            sig["invalidation"] = "Not stated - treat with caution"
        cleaned.append(sig)
    return cleaned

def generate_plays(signals):
    qualified = [s for s in signals if s.get("evidence_strength") in ["strong", "moderate"]]
    if not qualified:
        print("  No signals with sufficient evidence strength for play generation")
        return []
    sig_text = ""
    for s in qualified:
        sig_text += "- [" + s.get("evidence_strength","?") + "][" + s.get("change_type","?") + "][" + s.get("time_horizon","?") + "] " + s.get("statement","") + "\n  Mechanism: " + s.get("mechanism","") + "\n  Invalidation: " + s.get("invalidation","") + "\n"
    prompt = "You are the Plays Layer. Translate validated signals into SPECIFIC ACTION PATHS.\n\nRULES:\n- You may return between 0 and 8 plays. Only generate plays you can ground in the signals.\n- A play must specify WHO can act, WHY NOW, WHAT EXACTLY, WHAT CAPABILITY is needed.\n- Do NOT include market sizes. You have no market model.\n- Do NOT include numeric scores. You have no scoring methodology.\n- For each play state ASSUMPTIONS (what must be true for this to work) and INVALIDATION (what would kill it).\n- Use qualitative assessment only.\n- Map each play to an action posture: Monitor, Prepare, Defend, Build, or Invest.\n\nA BAD play: 'This creates opportunities in cybersecurity'\nA GOOD play: 'Saudi enterprises building internal AI need model access governance and environment hardening services - buildable by cybersecurity firms with cloud-native capabilities'\n\nQUALIFIED SIGNALS:\n" + sig_text + "\n\nRespond ONLY with a JSON array:\n[{\"title\":\"Specific play\",\"actor\":\"Who can act\",\"customer\":\"Who buys or benefits\",\"problem\":\"What problem the signal created\",\"action_path\":\"What exactly to do - 2-3 sentences\",\"entry_mode\":\"build|buy|partner|invest|wait\",\"monetization\":\"How it creates value - qualitative\",\"capabilities\":\"What is needed to execute\",\"barriers\":\"What blocks execution\",\"timing\":\"enter now|prepare now|monitor only\",\"action_posture\":\"Monitor|Prepare|Defend|Build|Invest\",\"linked_signals\":[\"signal statement\"],\"sector\":\"sector name\",\"assumptions\":\"What must be true for this to work\",\"invalidation\":\"What would kill this play\"}]"
    result = call_groq(prompt)
    if not result:
        return []
    cleaned = []
    for play in result:
        play.pop("market_size", None)
        play.pop("marketSize", None)
        play.pop("scores", None)
        play.pop("score", None)
        play.pop("confidence", None)
        if not play.get("assumptions"):
            play["assumptions"] = "Not stated - requires investigation"
        if not play.get("invalidation"):
            play["invalidation"] = "Not stated - treat as hypothesis"
        if play.get("action_posture") not in ["Monitor","Prepare","Defend","Build","Invest"]:
            play["action_posture"] = "Monitor"
        cleaned.append(play)
    return cleaned

def call_groq(prompt):
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 6000},
            timeout=120)
        if r.status_code == 200:
            return extract_json(r.json()["choices"][0]["message"]["content"])
        elif r.status_code == 429:
            print("  Rate limited. Waiting 30s...")
            time.sleep(30)
            r2 = requests.post("https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"},
                json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 6000},
                timeout=120)
            if r2.status_code == 200:
                return extract_json(r2.json()["choices"][0]["message"]["content"])
            print("  Groq error after retry:", r2.status_code)
        else:
            print("  Groq error:", r.status_code, r.text[:200])
    except Exception as e:
        print("  Groq exception:", e)
    return None

def extract_json(text):
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = text.strip()
    try:
        return json.loads(text)
    except:
        pass
    depth = 0
    start = -1
    for i, c in enumerate(text):
        if c in '[{':
            if depth == 0:
                start = i
            depth += 1
        if c in ']}':
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i+1])
                except:
                    start = -1
    return None

def load_existing(filename):
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            return data.get('items', []) if isinstance(data, dict) else data if isinstance(data, list) else []
    except:
        return []

def merge(existing, new_items, key='title'):
    keys = set(s.get(key, '').lower().strip() for s in existing)
    fresh = [dict(s, _at=datetime.now().isoformat()) for s in new_items if s.get(key, '').lower().strip() not in keys]
    cutoff = datetime.now().timestamp() - 30 * 86400
    old = [s for s in existing if datetime.fromisoformat(s.get('_at', datetime.now().isoformat())).timestamp() > cutoff]
    return fresh + old

def save_json(filename, items, extra=None):
    out = {"items": items, "lastRefresh": datetime.now().isoformat()}
    if extra:
        out.update(extra)
    with open(filename, 'w') as f:
        json.dump(out, f, indent=2)

def main():
    print("=" * 60)
    print("OPPORTUNITY RADAR v4")
    print("Architecture: Fetch(filtered) -> Radar -> Signals -> Plays")
    print("Gates: source tiers, relevance, dedup, evidence, strength")
    print("=" * 60)

    print("\n[1/5] Fetching and filtering...")
    articles = fetch_and_filter()
    if not articles:
        print("  No articles survived filtering. Exiting.")
        return

    print("\n[2/5] RADAR: Generating intelligence (no quota)...")
    new_radar = generate_radar(articles)
    if not new_radar:
        print("  FAILED"); return
    confirmed = len([r for r in new_radar if r.get("verification_status") == "confirmed"])
    reported = len([r for r in new_radar if r.get("verification_status") == "reported"])
    alleged = len([r for r in new_radar if r.get("verification_status") == "alleged"])
    rumored = len([r for r in new_radar if r.get("verification_status") == "rumored"])
    print(f"  Generated: {len(new_radar)} items (confirmed:{confirmed} reported:{reported} alleged:{alleged} rumored:{rumored})")

    existing_radar = load_existing('radar.json')
    merged_radar = merge(existing_radar, new_radar, 'title')
    print(f"  Merged: {len(merged_radar)} total")
    time.sleep(5)

    print("\n[3/5] SIGNALS: Identifying validated changes (evidence required)...")
    new_signals = generate_signals(new_radar)
    if not new_signals:
        print("  FAILED or no signals met evidence threshold"); new_signals = []
    else:
        strong = len([s for s in new_signals if s.get("evidence_strength") == "strong"])
        moderate = len([s for s in new_signals if s.get("evidence_strength") == "moderate"])
        weak = len([s for s in new_signals if s.get("evidence_strength") == "weak"])
        print(f"  Generated: {len(new_signals)} signals (strong:{strong} moderate:{moderate} weak:{weak})")
        for s in new_signals:
            print(f"    [{s.get('evidence_strength','?')}][{s.get('change_type','?')}] {s.get('statement','')[:70]}")

    existing_signals = load_existing('signals.json')
    merged_signals = merge(existing_signals, new_signals, 'statement')
    time.sleep(5)

    print("\n[4/5] PLAYS: Generating action paths (strong/moderate only)...")
    new_plays = generate_plays(new_signals)
    if new_plays:
        print(f"  Generated: {len(new_plays)} plays")
        for p in new_plays:
            print(f"    [{p.get('action_posture','?')}] {p.get('title','')[:60]}")
    else:
        print("  No plays generated (insufficient signal strength)")
        new_plays = []

    existing_plays = load_existing('plays.json')
    merged_plays = merge(existing_plays, new_plays, 'title')

    print("\n[5/5] Saving...")
    gate_report = {
        "articles_to_model": len(articles),
        "radar_items": len(new_radar),
        "radar_confirmed": confirmed,
        "radar_reported": reported,
        "radar_alleged": alleged,
        "signals_generated": len(new_signals),
        "signals_strong": len([s for s in new_signals if s.get("evidence_strength") == "strong"]),
        "signals_moderate": len([s for s in new_signals if s.get("evidence_strength") == "moderate"]),
        "signals_weak": len([s for s in new_signals if s.get("evidence_strength") == "weak"]),
        "plays_generated": len(new_plays)
    }
    save_json('radar.json', merged_radar, {"gate_report": gate_report})
    save_json('signals.json', merged_signals)
    save_json('plays.json', merged_plays)

    print(f"\n{'=' * 60}")
    print(f"v4 COMPLETE")
    print(f"  Radar:   {len(merged_radar)} intelligence items")
    print(f"  Signals: {len(merged_signals)} validated changes")
    print(f"  Plays:   {len(merged_plays)} action paths")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
