import requests
import json
import os
import re
import time
from datetime import datetime, timedelta

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
NEWS_KEY = os.environ.get("NEWS_API_KEY", "")
GNEWS_KEY = os.environ.get("GNEWS_API_KEY", "")
REDDIT_HEADERS = {"User-Agent": "OpportunityRadar/4.1"}

# ═══════════════════════════════════════════════════════════════
# SOURCE TIERS
# ═══════════════════════════════════════════════════════════════

TIER1_NAMES = ["reuters","bloomberg","arab news","spa","saudi press agency","al arabiya",
         "cnbc","financial times","ft.com","associated press","wall street journal",
         "wsj","bbc","guardian","new york times","nyt","economist","politico",
         "government","ministry","al monitor","al jazeera"]

TIER2_NAMES = ["techcrunch","wired","the verge","ars technica","zdnet","cnn",
         "independent","fortune","business insider","mashable","engadget","vice",
         "mit technology review","nature","science","ieee","axios","the information",
         "rest of world","semafor","nikkei","decrypt","rigzone","oilprice",
         "the register","dark reading","bleeping computer","krebs on security",
         "hacker news","venture beat","information week","globe newswire",
         "pr newswire","yahoo finance","seeking alpha","market watch"]

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

# ═══════════════════════════════════════════════════════════════
# RELEVANCE KEYWORDS
# ═══════════════════════════════════════════════════════════════

RELEVANCE_KEYWORDS = [
    "saudi","vision 2030","neom","sama","pif","gulf","gcc","aramco","stc",
    "qiddiya","sdaia","riyadh","jeddah","mena","middle east","bahrain","kuwait",
    "uae","qatar","oman","opec","oil price","crude","roshn","acwa","humain",
    "artificial intelligence","ai ","machine learning","deep learning","llm",
    "quantum","cybersecurity","cyber attack","data breach","ransomware","zero day",
    "fintech","blockchain","digital currency","cbdc","neobank",
    "semiconductor","chip","autonomous","drone","space","satellite","lidar",
    "hydrogen","renewable","solar","nuclear","carbon","green energy",
    "regulation","policy","sanction","tariff","trade war","export control",
    "investment","venture capital","ipo","funding round","acquisition","merger",
    "startup","enterprise","cloud","saas","infrastructure","5g","6g",
    "defense","military","surveillance","intelligence","geopolitics",
    "robotics","automation","manufacturing","supply chain","logistics",
    "tourism","entertainment","healthcare","biotech","education","edtech"
]

def is_relevant(title, desc):
    text = (title + " " + (desc or "")).lower()
    return sum(1 for kw in RELEVANCE_KEYWORDS if kw in text) >= 1

# ═══════════════════════════════════════════════════════════════
# TOPIC CLUSTERING (not just dedup - builds corroboration chains)
# ═══════════════════════════════════════════════════════════════

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

def get_topic_words(title):
    words = re.sub(r'[^a-z0-9\s]', '', title.lower()).split()
    return set(w for w in words if w not in STOP_WORDS and len(w) > 2)

def cluster_articles(articles):
    clusters = []
    for a in articles:
        key = get_topic_words(a["title"])
        if not key:
            continue
        merged = False
        for cl in clusters:
            overlap = len(key & cl["key"])
            union = len(key | cl["key"])
            if union > 0 and overlap / union > 0.35:
                cl["articles"].append(a)
                cl["sources"].add(a["source"])
                cl["tiers"].append(a["source_tier"])
                cl["key"] = key | cl["key"]
                merged = True
                break
        if not merged:
            clusters.append({
                "key": key,
                "articles": [a],
                "sources": {a["source"]},
                "tiers": [a["source_tier"]]
            })

    result = []
    for cl in clusters:
        best = min(cl["articles"], key=lambda x: x["source_tier"])
        item = best.copy()
        item["cluster_size"] = len(cl["articles"])
        item["unique_sources"] = len(cl["sources"])
        item["best_tier"] = min(cl["tiers"])
        item["all_sources"] = list(cl["sources"])[:5]
        if item["unique_sources"] >= 3:
            item["corroboration"] = "strong"
        elif item["unique_sources"] >= 2:
            item["corroboration"] = "moderate"
        else:
            item["corroboration"] = "single"
        result.append(item)

    result.sort(key=lambda x: (x["best_tier"], -x["unique_sources"]))
    return result

# ═══════════════════════════════════════════════════════════════
# FETCH LAYER - MASSIVELY EXPANDED DISCOVERY
# ═══════════════════════════════════════════════════════════════

def fetch_and_filter():
    raw = []
    cutoff = (datetime.now() - timedelta(hours=72)).strftime("%Y-%m-%d")

    # ─── NewsAPI: 20 queries (entity-led + event-led) ───
    newsapi_queries = [
        # Saudi entity-led
        "Saudi Arabia Vision 2030",
        "NEOM project Saudi",
        "PIF Public Investment Fund Saudi",
        "SAMA Saudi regulation",
        "SDAIA Saudi data AI",
        "Aramco Saudi energy",
        "Saudi tourism Red Sea",
        "Qiddiya Saudi entertainment",
        "Saudi defense military",
        "Saudi digital transformation",
        "Riyadh investment summit",
        "Saudi startup funding",
        "ROSHN Saudi real estate",
        "ACWA Power renewable",
        # Regional + global with Saudi relevance
        "OPEC oil production cut",
        "Gulf cooperation trade",
        "Middle East investment",
        "sovereign wealth fund Gulf",
        "Red Sea shipping Houthi",
        "Saudi Arabia diplomatic"
    ]
    for q in newsapi_queries:
        try:
            r = requests.get("https://newsapi.org/v2/everything",
                params={"q": q, "sortBy": "publishedAt", "pageSize": 5,
                         "language": "en", "apiKey": NEWS_KEY, "from": cutoff},
                timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    raw.append({"title": a.get("title","").strip(),
                                "source": a.get("source",{}).get("name",""),
                                "desc": (a.get("description","") or "").strip(),
                                "date": a.get("publishedAt","")[:10],
                                "origin": "newsapi"})
            elif r.status_code == 429:
                print("  NewsAPI rate limited, stopping news queries")
                break
        except:
            pass

    print(f"  NewsAPI: {len([a for a in raw if a['origin']=='newsapi'])} articles from {len(newsapi_queries)} queries")

    # ─── GNews: 12 queries (tech-specific + innovation) ───
    gnews_queries = [
        "artificial intelligence enterprise",
        "quantum computing breakthrough",
        "cybersecurity attack breach",
        "semiconductor manufacturing chip",
        "space technology satellite launch",
        "autonomous vehicles drones",
        "blockchain CBDC digital currency",
        "cloud computing sovereign",
        "5G 6G telecommunications",
        "robotics automation industrial",
        "green hydrogen energy transition",
        "AI regulation governance policy"
    ]
    for q in gnews_queries:
        try:
            r = requests.get("https://gnews.io/api/v4/search",
                params={"q": q, "max": 5, "lang": "en", "token": GNEWS_KEY},
                timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    raw.append({"title": a.get("title","").strip(),
                                "source": a.get("source",{}).get("name",""),
                                "desc": (a.get("description","") or "").strip(),
                                "date": a.get("publishedAt","")[:10],
                                "origin": "gnews"})
            elif r.status_code == 429:
                print("  GNews rate limited, stopping tech queries")
                break
        except:
            pass

    print(f"  GNews: {len([a for a in raw if a['origin']=='gnews'])} articles from {len(gnews_queries)} queries")

    # ─── Reddit: 19 subreddits hot posts ───
    reddit_subs = ["worldnews","news","geopolitics","saudiarabia","business","economics",
                   "finance","technology","TechNews","ArtificialIntelligence",
                   "MachineLearning","Futurology","netsec","DataBreaches",
                   "hardware","gadgets","startups","Entrepreneur","SaaS"]
    for sub in reddit_subs:
        try:
            r = requests.get("https://www.reddit.com/r/" + sub + "/hot.json?limit=8&t=week",
                headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                for post in r.json().get("data", {}).get("children", []):
                    d = post.get("data", {})
                    if d.get("stickied") or d.get("score", 0) < 20:
                        continue
                    created = d.get("created_utc", 0)
                    raw.append({"title": d.get("title","").strip(),
                                "source": "Reddit r/" + sub + " (" + str(d.get("score",0)) + " pts)",
                                "desc": (d.get("selftext","") or "")[:300].strip(),
                                "date": datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else "",
                                "origin": "reddit"})
            time.sleep(1)
        except:
            pass

    # ─── Reddit: 15 targeted searches ───
    reddit_searches = [
        ("Saudi Arabia","worldnews"),
        ("Vision 2030","saudiarabia"),
        ("NEOM","saudiarabia"),
        ("Gulf investment","finance"),
        ("AI regulation","technology"),
        ("quantum computing","technology"),
        ("semiconductor export","geopolitics"),
        ("cybersecurity breach","netsec"),
        ("data breach","DataBreaches"),
        ("AI startup","startups"),
        ("OPEC oil","economics"),
        ("Saudi","business"),
        ("sovereign AI","ArtificialIntelligence"),
        ("machine learning deployment","MachineLearning"),
        ("SaaS enterprise","SaaS")
    ]
    for q, sub in reddit_searches:
        try:
            r = requests.get("https://www.reddit.com/r/" + sub + "/search.json?q=" + q + "&sort=new&t=week&limit=3",
                headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                for post in r.json().get("data", {}).get("children", []):
                    d = post.get("data", {})
                    created = d.get("created_utc", 0)
                    raw.append({"title": d.get("title","").strip(),
                                "source": "Reddit r/" + sub + " (" + str(d.get("score",0)) + " pts)",
                                "desc": (d.get("selftext","") or "")[:300].strip(),
                                "date": datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else "",
                                "origin": "reddit_search"})
            time.sleep(1)
        except:
            pass

    reddit_count = len([a for a in raw if a["origin"] in ["reddit","reddit_search"]])
    print(f"  Reddit: {reddit_count} posts from {len(reddit_subs)} subs + {len(reddit_searches)} searches")
    print(f"  Raw total: {len(raw)}")

    # ─── CODE-ENFORCED GATES ───
    raw = [a for a in raw if a["title"] and len(a["title"]) > 15]
    print(f"  After title filter: {len(raw)}")

    raw = [a for a in raw if a["origin"] in ["reddit","reddit_search"] or len(a.get("desc","")) >= 40]
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
    print(f"  After tier filter: {len(raw)}")

    # Exact title dedup
    seen = set()
    unique = []
    for a in raw:
        t = a["title"].strip().lower()
        if t not in seen:
            seen.add(t)
            unique.append(a)
    print(f"  After exact dedup: {len(unique)}")

    # Topic clustering (not just dedup - builds corroboration)
    clustered = cluster_articles(unique)
    print(f"  After topic clustering: {len(clustered)} clusters")

    # Report corroboration
    strong_corr = len([c for c in clustered if c.get("corroboration") == "strong"])
    mod_corr = len([c for c in clustered if c.get("corroboration") == "moderate"])
    single_corr = len([c for c in clustered if c.get("corroboration") == "single"])
    print(f"  Corroboration: strong={strong_corr}, moderate={mod_corr}, single={single_corr}")

    # Cap at 40 (increased from 30 to accommodate broader discovery)
    capped = clustered[:40]
    print(f"  Final to model: {len(capped)} (capped at 40)")

    return capped

# ═══════════════════════════════════════════════════════════════
# LAYER 1: RADAR - with cluster corroboration info
# ═══════════════════════════════════════════════════════════════

def generate_radar(articles):
    feed = ""
    for a in articles:
        tier = "T" + str(a["source_tier"])
        corr = a.get("corroboration", "single")
        nsrc = a.get("unique_sources", 1)
        srcs = ", ".join(a.get("all_sources", [a["source"]])[:3])
        feed += "[" + tier + "][corroboration:" + corr + "][" + str(nsrc) + " sources: " + srcs + "] "
        feed += a["title"] + " | " + a["date"] + "\n"
        feed += "  " + (a["desc"] or "")[:200] + "\n"

    today = datetime.now().strftime("%Y-%m-%d")

    prompt = "You are the Intelligence Layer of a strategic sensing system for Saudi Arabia Vision 2030.\n\nTODAY: " + today + "\n\nBelow are pre-filtered, source-scored, topic-clustered articles. Each shows:\n- Source tier (T1=authoritative, T2=professional, T3=community)\n- Corroboration level (strong=3+ sources, moderate=2 sources, single=1 source)\n- Source names\n\n" + feed + "\n\nYour job: surface NEWLY OBSERVABLE information with possible strategic consequence.\n\nRULES:\n- Return between 0 and 15 items. Quality over quantity.\n- Strongly corroborated items (3+ sources) should almost always be included.\n- Single-source T3 items should only be included if genuinely novel and strategically relevant.\n- Do NOT invent or embellish beyond what sources report.\n- verification_status: confirmed (multi-source corroborated), reported (single authoritative source), alleged (single non-authoritative source), rumored (unverified claim)\n- Do NOT assign numeric novelty or credibility scores.\n\nRespond ONLY with a JSON array:\n[{\"title\":\"factual statement of what was observed\",\"source\":\"primary source\",\"source_tier\":2,\"time\":\"YYYY-MM-DD\",\"category\":\"technology|business|geopolitics|security|regulation|science|market|infrastructure\",\"geography\":\"Saudi|Gulf|Global|specific\",\"affected_domains\":[\"domain1\",\"domain2\"],\"summary\":\"2-3 factual sentences\",\"why_it_matters\":\"1-2 sentences on Saudi strategic consequence\",\"verification_status\":\"confirmed|reported|alleged|rumored\",\"source_count\":2}]"

    result = call_groq(prompt)
    if not result:
        return None

    valid_statuses = ["confirmed", "reported", "alleged", "rumored"]
    for item in result:
        if item.get("verification_status") not in valid_statuses:
            item["verification_status"] = "alleged"
    return result

# ═══════════════════════════════════════════════════════════════
# LAYER 2: SIGNALS - evidence-gated, single T1 confirmed allowed
# ═══════════════════════════════════════════════════════════════

def generate_signals(radar_items):
    intel = ""
    for r in radar_items:
        intel += "- [" + r.get("verification_status","?") + "][T" + str(r.get("source_tier",4)) + "][sources:" + str(r.get("source_count",1)) + "] " + r.get("title","") + "\n  " + r.get("summary","") + "\n"

    prompt = "You are the Signal Layer. Identify WHAT CHANGED - not what happened.\n\nA signal is the CHANGE INFERRED from intelligence, not the headline itself.\n\nGood: 'SAMA releases fintech rule' -> Signal: 'Regulatory barrier to fintech integration has reduced'\nBad: Signal that just restates the headline\n\nRULES:\n- Return between 0 and 10 signals.\n- Each signal MUST cite at least 2 intelligence items as evidence.\n- EXCEPTION: A single-item signal is allowed if that item is T1, verification_status is confirmed, AND source_count >= 2. This covers official regulations, major confirmed milestones, and authoritative disclosures.\n- For each signal, state what would INVALIDATE it.\n- evidence_strength: strong (3+ items), moderate (2 items), weak (1 confirmed T1 item).\n- Do NOT generate signals you cannot ground in the intelligence provided.\n\nINTELLIGENCE:\n" + intel + "\n\nRespond ONLY JSON array:\n[{\"statement\":\"The validated change\",\"change_type\":\"demand|regulation|cost|supply|competition|risk|feasibility|trust|behavior|infrastructure\",\"mechanism\":\"How this change works\",\"evidence\":[\"exact title 1\",\"exact title 2\"],\"evidence_strength\":\"strong|moderate|weak\",\"affected_actors\":[\"who\"],\"time_horizon\":\"now|1-2yr|3-5yr|later\",\"upside\":\"potential benefit\",\"downside\":\"potential risk\",\"first_order\":\"direct effect\",\"second_order\":\"downstream effect\",\"invalidation\":\"what would prove this wrong\"}]"

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

# ═══════════════════════════════════════════════════════════════
# LAYER 3: PLAYS - from strong/moderate signals only
# ═══════════════════════════════════════════════════════════════

def generate_plays(signals):
    qualified = [s for s in signals if s.get("evidence_strength") in ["strong", "moderate"]]
    if not qualified:
        print("  No signals with sufficient evidence for plays")
        return []

    sig_text = ""
    for s in qualified:
        sig_text += "- [" + s.get("evidence_strength","?") + "][" + s.get("change_type","?") + "][" + s.get("time_horizon","?") + "] " + s.get("statement","") + "\n  Mechanism: " + s.get("mechanism","") + "\n  Invalidation: " + s.get("invalidation","") + "\n"

    prompt = "You are the Plays Layer. Translate validated signals into SPECIFIC ACTION PATHS.\n\nRULES:\n- Return between 0 and 8 plays.\n- Each play must specify WHO, WHY NOW, WHAT EXACTLY, WHAT CAPABILITY.\n- No market sizes. No numeric scores.\n- State ASSUMPTIONS and INVALIDATION for each.\n- Action posture: Monitor / Prepare / Defend / Build / Invest.\n\nBAD: 'Opportunities in cybersecurity'\nGOOD: 'Saudi enterprises building internal AI need model access governance - buildable by cybersecurity firms with cloud-native capabilities'\n\nSIGNALS:\n" + sig_text + "\n\nRespond ONLY JSON array:\n[{\"title\":\"specific play\",\"actor\":\"who can act\",\"customer\":\"who buys\",\"problem\":\"what problem the signal created\",\"action_path\":\"what to do - 2-3 sentences\",\"entry_mode\":\"build|buy|partner|invest|wait\",\"monetization\":\"how it creates value\",\"capabilities\":\"what is needed\",\"barriers\":\"what blocks\",\"timing\":\"enter now|prepare now|monitor only\",\"action_posture\":\"Monitor|Prepare|Defend|Build|Invest\",\"linked_signals\":[\"signal statement\"],\"sector\":\"sector\",\"assumptions\":\"what must be true\",\"invalidation\":\"what would kill it\"}]"

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

# ═══════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════

def call_groq(prompt):
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.2, "max_tokens": 6000},
            timeout=120)
        if r.status_code == 200:
            return extract_json(r.json()["choices"][0]["message"]["content"])
        elif r.status_code == 429:
            print("  Groq rate limited. Waiting 30s...")
            time.sleep(30)
            r2 = requests.post("https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"},
                json={"model": "llama-3.3-70b-versatile",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.2, "max_tokens": 6000},
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

# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("OPPORTUNITY RADAR v4.1")
    print("Expanded discovery: 20 news + 12 tech + 19 subs + 15 searches")
    print("Pre-Radar clustering with corroboration chains")
    print("Same strict gates: evidence-gated signals, grounded plays")
    print("=" * 60)

    print("\n[1/5] DISCOVERY: Fetching from all sources...")
    articles = fetch_and_filter()
    if not articles:
        print("  No articles survived filtering. Exiting.")
        return

    print("\n[2/5] RADAR: Intelligence extraction (no forced quota)...")
    new_radar = generate_radar(articles)
    if not new_radar:
        print("  FAILED to generate radar items"); return

    confirmed = len([r for r in new_radar if r.get("verification_status") == "confirmed"])
    reported = len([r for r in new_radar if r.get("verification_status") == "reported"])
    alleged = len([r for r in new_radar if r.get("verification_status") == "alleged"])
    print(f"  Generated: {len(new_radar)} items (confirmed:{confirmed} reported:{reported} alleged:{alleged})")

    existing_radar = load_existing('radar.json')
    merged_radar = merge(existing_radar, new_radar, 'title')
    print(f"  Merged: {len(merged_radar)} total (30-day window)")
    time.sleep(5)

    print("\n[3/5] SIGNALS: Validated changes (evidence required)...")
    new_signals = generate_signals(new_radar)
    if not new_signals:
        print("  No signals met evidence threshold"); new_signals = []
    else:
        strong = len([s for s in new_signals if s.get("evidence_strength") == "strong"])
        moderate = len([s for s in new_signals if s.get("evidence_strength") == "moderate"])
        weak = len([s for s in new_signals if s.get("evidence_strength") == "weak"])
        print(f"  Generated: {len(new_signals)} signals (strong:{strong} moderate:{moderate} weak:{weak})")
        for s in new_signals:
            print(f"    [{s.get('evidence_strength','?')}] {s.get('statement','')[:70]}")

    existing_signals = load_existing('signals.json')
    # Filter old-format signals that lack 'statement' field
    existing_signals = [s for s in existing_signals if s.get('statement')]
    merged_signals = merge(existing_signals, new_signals, 'statement')
    time.sleep(5)

    print("\n[4/5] PLAYS: Action paths (strong/moderate signals only)...")
    new_plays = generate_plays(new_signals)
    if new_plays:
        print(f"  Generated: {len(new_plays)} plays")
        for p in new_plays:
            print(f"    [{p.get('action_posture','?')}] {p.get('title','')[:60]}")
    else:
        print("  No plays (insufficient signal strength)")
        new_plays = []

    existing_plays = load_existing('plays.json')
    merged_plays = merge(existing_plays, new_plays, 'title')

    print("\n[5/5] Saving...")
    gate_report = {
        "discovery_queries": "20 news + 12 tech + 19 reddit_subs + 15 reddit_searches",
        "articles_to_model": len(articles),
        "radar_generated": len(new_radar),
        "radar_confirmed": confirmed,
        "radar_reported": reported,
        "radar_alleged": alleged,
        "signals_generated": len(new_signals),
        "signals_strong": len([s for s in new_signals if s.get("evidence_strength") == "strong"]),
        "signals_moderate": len([s for s in new_signals if s.get("evidence_strength") == "moderate"]),
        "plays_generated": len(new_plays)
    }
    save_json('radar.json', merged_radar, {"gate_report": gate_report})
    save_json('signals.json', merged_signals)
    save_json('plays.json', merged_plays)

    print(f"\n{'=' * 60}")
    print(f"v4.1 COMPLETE")
    print(f"  Radar:   {len(merged_radar)} intelligence items")
    print(f"  Signals: {len(merged_signals)} validated changes")
    print(f"  Plays:   {len(merged_plays)} action paths")
    print(f"  Gates:   {json.dumps(gate_report, indent=2)}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
