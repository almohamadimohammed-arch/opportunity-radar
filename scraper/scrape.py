import requests
import json
import os
from datetime import datetime

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
NEWS_KEY = os.environ.get("NEWS_API_KEY", "")
GNEWS_KEY = os.environ.get("GNEWS_API_KEY", "")

def fetch_news():
    articles = []
    queries_news = [
        "Saudi Arabia Vision 2030",
        "Saudi investment NEOM Qiddiya",
        "SAMA fintech regulation Saudi",
        "Saudi oil energy renewable",
        "Gulf geopolitics trade"
    ]
    for q in queries_news:
        try:
            r = requests.get("https://newsapi.org/v2/everything", params={"q": q, "sortBy": "publishedAt", "pageSize": 5, "language": "en", "apiKey": NEWS_KEY}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    articles.append({"title": a.get("title",""), "source": a.get("source",{}).get("name",""), "description": a.get("description",""), "date": a.get("publishedAt","")[:10], "category": "business"})
        except:
            pass

    queries_tech = [
        "artificial intelligence breakthrough",
        "quantum computing",
        "cybersecurity threat",
        "space technology",
        "semiconductor chip"
    ]
    for q in queries_tech:
        try:
            r = requests.get("https://gnews.io/api/v4/search", params={"q": q, "max": 5, "lang": "en", "token": GNEWS_KEY}, timeout=15)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    articles.append({"title": a.get("title",""), "source": a.get("source",{}).get("name",""), "description": a.get("description",""), "date": a.get("publishedAt","")[:10], "category": "technology"})
        except:
            pass

    return articles

def generate_signals(articles):
    news_text = ""
    for a in articles[:40]:
        news_text += a["title"] + " | " + a["source"] + " | " + a["date"] + " | " + (a["description"] or "") + "\n"

    prompt = """You are a Signal Detection Engine for Saudi Arabia Vision 2030.

TODAY: """ + datetime.now().strftime("%Y-%m-%d") + """

Here are real news articles from today:

""" + news_text + """

From these REAL articles, generate exactly 10 Saudi local signals and 8 global signals.

LOCAL: events directly about Saudi Arabia - regulations, investments, tenders, mega-projects, economic indicators
GLOBAL: international events that impact Saudi Arabia - geopolitics, tech disruptions, trade policy, energy shifts, cybersecurity

IMPORTANT: Only use information from the articles above. Do not invent events.

Respond ONLY with a JSON array:
[{"scope":"local","headline":"...","sector":"...","region":"...","date":"YYYY-MM-DD","impact":"critical|high|medium","source":"...","type":"...","context":"2-3 sentences explaining what happened and why it matters for Saudi Arabia"}]"""

    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 4000}, timeout=60)
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"]
            return extract_json(text)
    except Exception as e:
        print("Groq signals error:", e)
    return None

def generate_opportunities(signals):
    sig_text = ""
    for s in signals:
        sig_text += "[" + s.get("scope","") + "][" + s.get("impact","") + "] " + s.get("headline","") + " | " + s.get("sector","") + ": " + s.get("context","") + "\n"

    prompt = """Analyze these signals. Generate 5-8 scored Saudi business opportunities.

SIGNALS:
""" + sig_text + """

Respond ONLY with a JSON array:
[{"title":"...","sector":"...","region":"...","score":85,"urgency":"Emerging","marketSize":"$X.XB","growth":"+XX%","description":"3-4 sentences","investment":"$X-XM","scores":{"v2030":90,"regulatory":85,"market":88,"ease":70,"competition":75}}]"""

    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 4000}, timeout=60)
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"]
            return extract_json(text)
    except Exception as e:
        print("Groq opps error:", e)
    return None

def extract_json(text):
    import re
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

def merge_signals(existing, new_signals):
    headlines = set()
    for s in existing:
        headlines.add(s.get('headline',''))
    fresh = []
    for s in new_signals:
        if s.get('headline','') not in headlines:
            s['_at'] = datetime.now().isoformat()
            fresh.append(s)
    cutoff = datetime.now().timestamp() - 30 * 86400
    merged = fresh + [s for s in existing if datetime.fromisoformat(s.get('_at', datetime.now().isoformat())).timestamp() > cutoff]
    return merged

def main():
    print("Fetching news...")
    articles = fetch_news()
    print(f"Got {len(articles)} articles")

    print("Generating signals...")
    new_signals = generate_signals(articles)
    if not new_signals:
        print("Failed to generate signals")
        return

    print(f"Generated {len(new_signals)} signals")
    existing_signals = load_existing('signals.json')
    merged_signals = merge_signals(existing_signals, new_signals)

    print("Generating opportunities...")
    new_opps = generate_opportunities(merged_signals[:20])
    if not new_opps:
        print("Failed to generate opportunities")
        new_opps = []

    print(f"Generated {len(new_opps)} opportunities")
    existing_opps = load_existing('opps.json')
    merged_opps = merge_signals(existing_opps, [dict(o, headline=o.get('title','')) for o in new_opps])
    for o in merged_opps:
        if 'headline' in o and 'title' in o:
            del o['headline']

    signals_out = {"items": merged_signals, "lastRefresh": datetime.now().isoformat()}
    opps_out = {"items": merged_opps, "lastRefresh": datetime.now().isoformat()}

    with open('signals.json', 'w') as f:
        json.dump(signals_out, f, indent=2)
    with open('opps.json', 'w') as f:
        json.dump(opps_out, f, indent=2)

    print(f"Done. {len(merged_signals)} signals, {len(merged_opps)} opportunities saved.")

if __name__ == "__main__":
    main()
