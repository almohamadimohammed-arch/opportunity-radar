import requests
import json
import os
import re
from datetime import datetime

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
NEWS_KEY = os.environ.get("NEWS_API_KEY", "")
GNEWS_KEY = os.environ.get("GNEWS_API_KEY", "")

REDDIT_HEADERS = {"User-Agent": "OpportunityRadar/1.0 (signal-scanner)"}

def fetch_news():
    articles = []

    # ─── NewsAPI: Saudi business + policy ───
    queries_news = [
        "Saudi Arabia Vision 2030",
        "Saudi investment NEOM Qiddiya",
        "SAMA fintech regulation Saudi",
        "Saudi oil energy renewable",
        "Gulf geopolitics trade",
        "Saudi Arabia tourism entertainment",
        "PIF Saudi fund investment"
    ]
    for q in queries_news:
        try:
            r = requests.get("https://newsapi.org/v2/everything", params={"q": q, "sortBy": "publishedAt", "pageSize": 5, "language": "en", "apiKey": NEWS_KEY}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    articles.append({"title": a.get("title",""), "source": "NewsAPI: " + a.get("source",{}).get("name",""), "description": a.get("description",""), "date": a.get("publishedAt","")[:10], "category": "business"})
        except:
            pass

    # ─── GNews: emerging tech + innovation ───
    queries_tech = [
        "artificial intelligence breakthrough",
        "quantum computing",
        "cybersecurity threat breach",
        "space technology satellite",
        "semiconductor chip export",
        "autonomous AI agent",
        "blockchain digital currency"
    ]
    for q in queries_tech:
        try:
            r = requests.get("https://gnews.io/api/v4/search", params={"q": q, "max": 5, "lang": "en", "token": GNEWS_KEY}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    articles.append({"title": a.get("title",""), "source": "GNews: " + a.get("source",{}).get("name",""), "description": a.get("description",""), "date": a.get("publishedAt","")[:10], "category": "technology"})
        except:
            pass

    # ─── Reddit: full subreddit scan (no API key needed) ───
    reddit_subs = [
        # News & Geopolitics
        {"sub": "worldnews", "label": "World news"},
        {"sub": "news", "label": "General news"},
        {"sub": "geopolitics", "label": "Geopolitical analysis"},
        {"sub": "saudiarabia", "label": "Saudi discussions"},
        # Business & Finance
        {"sub": "business", "label": "Business news"},
        {"sub": "economics", "label": "Economic analysis"},
        {"sub": "finance", "label": "Finance"},
        # Technology & AI
        {"sub": "technology", "label": "Tech trends"},
        {"sub": "TechNews", "label": "Tech news"},
        {"sub": "ArtificialIntelligence", "label": "AI developments"},
        {"sub": "MachineLearning", "label": "ML research"},
        {"sub": "Futurology", "label": "Emerging trends"},
        # Cybersecurity
        {"sub": "netsec", "label": "Network security"},
        {"sub": "DataBreaches", "label": "Data breaches"},
        # Hardware & Devices
        {"sub": "hardware", "label": "Hardware trends"},
        {"sub": "gadgets", "label": "Consumer tech"},
        # Startups & Entrepreneurship
        {"sub": "startups", "label": "Startup ecosystem"},
        {"sub": "Entrepreneur", "label": "Entrepreneurship"},
        {"sub": "SaaS", "label": "SaaS market"},
    ]

    for src in reddit_subs:
        try:
            url = "https://www.reddit.com/r/" + src["sub"] + "/hot.json?limit=5&t=week"
            r = requests.get(url, headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                posts = r.json().get("data", {}).get("children", [])
                for post in posts:
                    d = post.get("data", {})
                    if d.get("stickied"):
                        continue
                    title = d.get("title", "")
                    score = d.get("score", 0)
                    if score < 10:
                        continue
                    created = d.get("created_utc", 0)
                    date_str = datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else ""
                    selftext = (d.get("selftext", "") or "")[:300]
                    articles.append({
                        "title": title,
                        "source": "Reddit r/" + src["sub"] + " (" + str(score) + " upvotes)",
                        "description": selftext if selftext else title,
                        "date": date_str,
                        "category": "reddit_" + src["label"].lower().replace(" ", "_")
                    })
            import time
            time.sleep(1)
        except:
            pass

    # ─── Reddit: targeted searches ───
    reddit_searches = [
        {"q": "Saudi Arabia", "sub": "worldnews"},
        {"q": "Vision 2030", "sub": "saudiarabia"},
        {"q": "NEOM", "sub": "saudiarabia"},
        {"q": "Gulf investment", "sub": "finance"},
        {"q": "AI regulation", "sub": "technology"},
        {"q": "quantum computing", "sub": "technology"},
        {"q": "semiconductor export", "sub": "geopolitics"},
        {"q": "cybersecurity breach", "sub": "netsec"},
        {"q": "SaaS startup", "sub": "SaaS"},
        {"q": "data breach", "sub": "DataBreaches"},
        {"q": "AI startup", "sub": "startups"},
        {"q": "machine learning production", "sub": "MachineLearning"},
    ]
    for rs in reddit_searches:
        try:
            url = "https://www.reddit.com/r/" + rs["sub"] + "/search.json?q=" + rs["q"] + "&sort=new&t=week&limit=3"
            r = requests.get(url, headers=REDDIT_HEADERS, timeout=15)
            if r.status_code == 200:
                posts = r.json().get("data", {}).get("children", [])
                for post in posts:
                    d = post.get("data", {})
                    title = d.get("title", "")
                    score = d.get("score", 0)
                    created = d.get("created_utc", 0)
                    date_str = datetime.fromtimestamp(created).strftime("%Y-%m-%d") if created else ""
                    selftext = (d.get("selftext", "") or "")[:300]
                    articles.append({
                        "title": title,
                        "source": "Reddit r/" + rs["sub"] + " search:'" + rs["q"] + "' (" + str(score) + " pts)",
                        "description": selftext if selftext else title,
                        "date": date_str,
                        "category": "reddit_search"
                    })
            import time
            time.sleep(1)
        except:
            pass

    # Deduplicate by title
    seen = set()
    unique = []
    for a in articles:
        t = a["title"].strip().lower()
        if t and t not in seen and len(t) > 10:
            seen.add(t)
            unique.append(a)

    return unique

def generate_signals(articles):
    news_articles = [a for a in articles if a["source"].startswith("NewsAPI")]
    tech_articles = [a for a in articles if a["source"].startswith("GNews")]
    reddit_articles = [a for a in articles if a["source"].startswith("Reddit")]

    news_text = "=== NEWS (Saudi business, policy, investment) ===\n"
    for a in news_articles[:25]:
        news_text += a["title"] + " | " + a["source"] + " | " + a["date"] + " | " + (a["description"] or "") + "\n"

    news_text += "\n=== TECHNOLOGY (emerging tech, innovation) ===\n"
    for a in tech_articles[:20]:
        news_text += a["title"] + " | " + a["source"] + " | " + a["date"] + " | " + (a["description"] or "") + "\n"

    news_text += "\n=== REDDIT (community intelligence, discussions, weak signals) ===\n"
    for a in reddit_articles[:30]:
        news_text += a["title"] + " | " + a["source"] + " | " + a["date"] + " | " + (a["description"] or "")[:200] + "\n"

    today = datetime.now().strftime("%Y-%m-%d")

    prompt = """You are a Signal Detection Engine for Saudi Arabia Vision 2030.

TODAY: """ + today + """

Below are REAL articles from 3 source types:
- NewsAPI: professional news on Saudi business, policy, investment
- GNews: technology and innovation news
- Reddit: community discussions from 19 subreddits covering world news, geopolitics, Saudi Arabia, business, economics, finance, technology, AI, machine learning, cybersecurity, data breaches, hardware, startups, entrepreneurship, SaaS, and futurology

""" + news_text + """

Generate exactly 10 Saudi local signals and 8 global signals.

LOCAL: events directly about Saudi Arabia
GLOBAL: international events that impact Saudi Arabia

RULES:
- Only use information from the articles above
- Reddit discussions reveal emerging sentiment and weak signals that mainstream news misses
- Each signal must cite its source
- Prioritize high-impact, actionable signals over noise

For EACH signal, apply 8-question classification:
1. What happened? (headline)
2. Driver? (regulation / technology / customer_behavior / capital / geopolitics / competition)
3. Affected layer? (supplier / platform / distributor / operator / customer / regulator)
4. Likely effect? (cost / revenue / demand / adoption / compliance / speed / trust / differentiation)
5. Time horizon? (now / 1-2yr / 3-5yr / later)
6. Classification? (threat / opportunity / both / noise)
7. Who benefits / who loses?
8. Recommended action? (monitor / study / defend / partner / pilot / invest / escalate)

Respond ONLY with a JSON array:
[{
  "scope": "local or global",
  "headline": "concise factual headline",
  "sector": "Tourism / Fintech / Energy / AI & Digital / Manufacturing / Education / Healthcare / Entertainment / Logistics / Cybersecurity / Technology / Defense / Real Estate",
  "region": "specific Saudi region or Global",
  "date": "YYYY-MM-DD",
  "impact": "critical / high / medium",
  "source": "original source name",
  "type": "Regulation / Investment / Policy / Milestone / Incentive / Industrial / Cyber Threat / Geopolitical / Technology / Benchmark / Tender / Community Signal",
  "context": "2-3 sentences on what happened and Saudi impact",
  "driver": "regulation / technology / customer_behavior / capital / geopolitics / competition",
  "affected_layer": "supplier / platform / distributor / operator / customer / regulator",
  "likely_effect": "cost / revenue / demand / adoption / compliance / speed / trust / differentiation",
  "time_horizon": "now / 1-2yr / 3-5yr / later",
  "classification": "threat / opportunity / both / noise",
  "beneficiaries": "who benefits",
  "losers": "who loses",
  "recommended_action": "monitor / study / defend / partner / pilot / invest / escalate"
}]"""

    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 6000}, timeout=90)
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"]
            return extract_json(text)
        else:
            print("Groq signals error:", r.status_code, r.text[:200])
    except Exception as e:
        print("Groq signals error:", e)
    return None

def generate_opportunities(signals):
    sig_text = ""
    for s in signals:
        sig_text += "[" + s.get("scope","") + "][" + s.get("impact","") + "][" + s.get("classification","") + "] "
        sig_text += s.get("headline","") + " | " + s.get("sector","")
        sig_text += " | Driver:" + s.get("driver","") + " | Effect:" + s.get("likely_effect","")
        sig_text += " | Horizon:" + s.get("time_horizon","") + " | Action:" + s.get("recommended_action","")
        sig_text += "\n  Context: " + s.get("context","") + "\n"

    prompt = """Analyze these classified signals. Generate 5-8 scored Saudi business opportunities.

SIGNALS:
""" + sig_text + """

For each opportunity:
- Link to at least 2 signals
- Score 0-100 on 6 dimensions
- Estimate TAM / SAM / SOM
- Recommend entry mode: build / buy / partner / invest / wait
- Classify timing: enter now / prepare now / monitor only

Respond ONLY with a JSON array:
[{
  "title": "opportunity name",
  "sector": "primary sector",
  "region": "Saudi region or National",
  "score": 85,
  "urgency": "Emerging / Growing / Mature / Declining",
  "marketSize": "$X.XB (TAM)",
  "sam": "$X.XB",
  "som": "$X.XM",
  "growth": "+XX%",
  "description": "3-4 sentences linking signals to business need",
  "customers": "target segments",
  "competition": "competitive landscape",
  "capabilities": "required tech, talent, partnerships",
  "risks": "key risks",
  "investment": "$X-XM range",
  "returnEst": "Xx over Xyr",
  "entryMode": "build / buy / partner / invest / wait",
  "timing": "enter now / prepare now / monitor only",
  "linkedSignals": ["signal headline 1", "signal headline 2"],
  "scores": {
    "v2030": 90,
    "regulatory": 85,
    "market": 88,
    "ease": 70,
    "competition": 75,
    "investReturn": 80
  }
}]"""

    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 5000}, timeout=90)
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"]
            return extract_json(text)
        else:
            print("Groq opps error:", r.status_code, r.text[:200])
    except Exception as e:
        print("Groq opps error:", e)
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
            if isinstance(data, dict) and 'items' in data:
                return data['items']
            if isinstance(data, list):
                return data
    except:
        pass
    return []

def merge_items(existing, new_items, key_field='headline'):
    keys = set()
    for s in existing:
        keys.add(s.get(key_field, ''))
    fresh = []
    for s in new_items:
        if s.get(key_field, '') not in keys:
            s['_at'] = datetime.now().isoformat()
            fresh.append(s)
    cutoff = datetime.now().timestamp() - 30 * 86400
    merged = fresh + [s for s in existing if datetime.fromisoformat(s.get('_at', datetime.now().isoformat())).timestamp() > cutoff]
    return merged

def main():
    print("=" * 50)
    print("OPPORTUNITY RADAR - Signal Scraper v2")
    print("Sources: NewsAPI + GNews + Reddit (19 subreddits)")
    print("=" * 50)

    print("\n[1/4] Fetching from all sources...")
    articles = fetch_news()
    news_count = len([a for a in articles if a["source"].startswith("NewsAPI")])
    tech_count = len([a for a in articles if a["source"].startswith("GNews")])
    reddit_count = len([a for a in articles if a["source"].startswith("Reddit")])
    print(f"  NewsAPI:  {news_count} articles")
    print(f"  GNews:    {tech_count} articles")
    print(f"  Reddit:   {reddit_count} posts (19 subreddits + 12 searches)")
    print(f"  Total:    {len(articles)} unique items")

    print("\n[2/4] Generating classified signals via Groq...")
    new_signals = generate_signals(articles)
    if not new_signals:
        print("  FAILED to generate signals")
        return

    local = len([s for s in new_signals if s.get('scope') == 'local'])
    global_s = len([s for s in new_signals if s.get('scope') == 'global'])
    threats = len([s for s in new_signals if s.get('classification') in ['threat', 'both']])
    opps_c = len([s for s in new_signals if s.get('classification') in ['opportunity', 'both']])
    print(f"  Generated: {len(new_signals)} signals ({local} local, {global_s} global)")
    print(f"  Threats: {threats} | Opportunities: {opps_c}")

    existing_signals = load_existing('signals.json')
    merged_signals = merge_items(existing_signals, new_signals, 'headline')
    print(f"  Merged: {len(merged_signals)} total (30-day rolling window)")

    print("\n[3/4] Generating opportunities via Groq...")
    new_opps = generate_opportunities(merged_signals[:20])
    if not new_opps:
        print("  FAILED to generate opportunities")
        new_opps = []
    else:
        print(f"  Generated: {len(new_opps)} opportunities")

    existing_opps = load_existing('opps.json')
    merged_opps = merge_items(existing_opps, [dict(o, headline=o.get('title','')) for o in new_opps], 'headline')
    for o in merged_opps:
        if 'headline' in o and 'title' in o:
            del o['headline']
    print(f"  Merged: {len(merged_opps)} total opportunities")

    stats = {
        "totalSignals": len(merged_signals),
        "localSignals": len([s for s in merged_signals if s.get('scope') == 'local']),
        "globalSignals": len([s for s in merged_signals if s.get('scope') == 'global']),
        "threats": len([s for s in merged_signals if s.get('classification') in ['threat', 'both']]),
        "opportunities_count": len([s for s in merged_signals if s.get('classification') in ['opportunity', 'both']]),
        "criticalSignals": len([s for s in merged_signals if s.get('impact') == 'critical']),
        "totalOpps": len(merged_opps),
        "avgScore": round(sum(o.get('score', 0) for o in merged_opps) / max(len(merged_opps), 1)),
        "sources": {"newsapi": news_count, "gnews": tech_count, "reddit": reddit_count}
    }

    print("\n[4/4] Saving files...")
    with open('signals.json', 'w') as f:
        json.dump({"items": merged_signals, "lastRefresh": datetime.now().isoformat(), "stats": stats}, f, indent=2)
    with open('opps.json', 'w') as f:
        json.dump({"items": merged_opps, "lastRefresh": datetime.now().isoformat()}, f, indent=2)

    print(f"\nDONE.")
    print(f"  Signals:       {len(merged_signals)}")
    print(f"  Opportunities: {len(merged_opps)} (avg score: {stats['avgScore']})")
    print(f"  Sources:       NewsAPI({news_count}) + GNews({tech_count}) + Reddit({reddit_count})")

if __name__ == "__main__":
    main()
