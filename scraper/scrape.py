import requests
import json
import os
from datetime import datetime

GROQ_KEY = os.environ.get(“GROQ_API_KEY”, “”)
NEWS_KEY = os.environ.get(“NEWS_API_KEY”, “”)
GNEWS_KEY = os.environ.get(“GNEWS_API_KEY”, “”)

def fetch_news():
articles = []
queries_news = [
“Saudi Arabia Vision 2030”,
“Saudi investment NEOM Qiddiya”,
“SAMA fintech regulation Saudi”,
“Saudi oil energy renewable”,
“Gulf geopolitics trade”,
“Saudi Arabia tourism entertainment”,
“PIF Saudi fund investment”
]
for q in queries_news:
try:
r = requests.get(“https://newsapi.org/v2/everything”, params={“q”: q, “sortBy”: “publishedAt”, “pageSize”: 5, “language”: “en”, “apiKey”: NEWS_KEY}, timeout=15)
if r.status_code == 200:
for a in r.json().get(“articles”, []):
articles.append({“title”: a.get(“title”,””), “source”: a.get(“source”,{}).get(“name”,””), “description”: a.get(“description”,””), “date”: a.get(“publishedAt”,””)[:10], “category”: “business”})
except:
pass

```
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
                articles.append({"title": a.get("title",""), "source": a.get("source",{}).get("name",""), "description": a.get("description",""), "date": a.get("publishedAt","")[:10], "category": "technology"})
    except:
        pass

return articles
```

def generate_signals(articles):
news_text = “”
for a in articles[:50]:
news_text += a[“title”] + “ | “ + a[“source”] + “ | “ + a[“date”] + “ | “ + (a[“description”] or “”) + “\n”

```
today = datetime.now().strftime("%Y-%m-%d")

prompt = """You are a Signal Detection Engine for Saudi Arabia Vision 2030.
```

TODAY: “”” + today + “””

Here are real news articles:

“”” + news_text + “””

From these REAL articles, generate exactly 10 Saudi local signals and 8 global signals.

LOCAL: events directly about Saudi Arabia
GLOBAL: international events that impact Saudi Arabia

IMPORTANT: Only use information from the articles above. Do not invent events.

For EACH signal, apply this 8-question classification:

1. What exactly happened? (the headline)
1. What driver is behind it? (regulation / technology / customer_behavior / capital / geopolitics / competition)
1. Which market layer is affected? (supplier / platform / distributor / operator / customer / regulator)
1. What is the likely effect? (cost / revenue / demand / adoption / compliance / speed / trust / differentiation)
1. Time horizon? (now / 1-2yr / 3-5yr / later)
1. Is it a threat, opportunity, both, or noise?
1. Who benefits and who loses?
1. Recommended action? (monitor / study / defend / partner / pilot / invest / escalate)

Respond ONLY with a JSON array:
[{
“scope”: “local or global”,
“headline”: “concise factual headline”,
“sector”: “Tourism / Fintech / Energy / AI & Digital / Manufacturing / Education / Healthcare / Entertainment / Logistics / Cybersecurity / Technology / Defense / Real Estate”,
“region”: “specific Saudi region or Global”,
“date”: “YYYY-MM-DD”,
“impact”: “critical / high / medium”,
“source”: “original source name”,
“type”: “Regulation / Investment / Policy / Milestone / Incentive / Industrial / Cyber Threat / Geopolitical / Technology / Benchmark / Tender”,
“context”: “2-3 sentences on what happened and Saudi impact”,
“driver”: “regulation / technology / customer_behavior / capital / geopolitics / competition”,
“affected_layer”: “supplier / platform / distributor / operator / customer / regulator”,
“likely_effect”: “cost / revenue / demand / adoption / compliance / speed / trust / differentiation”,
“time_horizon”: “now / 1-2yr / 3-5yr / later”,
“classification”: “threat / opportunity / both / noise”,
“beneficiaries”: “who benefits”,
“losers”: “who loses”,
“recommended_action”: “monitor / study / defend / partner / pilot / invest / escalate”
}]”””

```
try:
    r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 6000}, timeout=90)
    if r.status_code == 200:
        text = r.json()["choices"][0]["message"]["content"]
        return extract_json(text)
except Exception as e:
    print("Groq signals error:", e)
return None
```

def generate_opportunities(signals):
sig_text = “”
for s in signals:
sig_text += “[” + s.get(“scope”,””) + “][” + s.get(“impact”,””) + “][” + s.get(“classification”,””) + “] “ + s.get(“headline”,””) + “ | “ + s.get(“sector”,””) + “ | Driver:” + s.get(“driver”,””) + “ | Effect:” + s.get(“likely_effect”,””) + “ | Horizon:” + s.get(“time_horizon”,””) + “ | Action:” + s.get(“recommended_action”,””) + “\n  Context: “ + s.get(“context”,””) + “\n”

```
prompt = """Analyze these classified signals. Generate 5-8 scored Saudi business opportunities.
```

SIGNALS WITH CLASSIFICATION:
“”” + sig_text + “””

For each opportunity, provide:

- Link to at least 2 signals
- Score 0-100 on 6 dimensions
- TAM/SAM/SOM estimation
- Entry mode recommendation (build / buy / partner / invest / wait)
- Timing classification (enter now / prepare now / monitor only)

Respond ONLY with a JSON array:
[{
“title”: “opportunity name”,
“sector”: “primary sector”,
“region”: “Saudi region or National”,
“score”: 85,
“urgency”: “Emerging / Growing / Mature / Declining”,
“marketSize”: “$X.XB (TAM)”,
“sam”: “$X.XB”,
“som”: “$X.XM”,
“growth”: “+XX%”,
“description”: “3-4 sentences linking signals to business need”,
“customers”: “target segments”,
“competition”: “competitive landscape”,
“capabilities”: “required tech, talent, partnerships”,
“risks”: “key risks”,
“investment”: “$X-XM range”,
“returnEst”: “Xx over Xyr”,
“entryMode”: “build / buy / partner / invest / wait”,
“timing”: “enter now / prepare now / monitor only”,
“linkedSignals”: [“signal headline 1”, “signal headline 2”],
“scores”: {
“v2030”: 90,
“regulatory”: 85,
“market”: 88,
“ease”: 70,
“competition”: 75,
“investReturn”: 80
}
}]”””

```
try:
    r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + GROQ_KEY, "Content-Type": "application/json"}, json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 5000}, timeout=90)
    if r.status_code == 200:
        text = r.json()["choices"][0]["message"]["content"]
        return extract_json(text)
except Exception as e:
    print("Groq opps error:", e)
return None
```

def extract_json(text):
import re
text = re.sub(r’`json\s*', '', text) text = re.sub(r'`\s*’, ‘’, text)
text = text.strip()
try:
return json.loads(text)
except:
pass
depth = 0
start = -1
for i, c in enumerate(text):
if c in ‘[{’:
if depth == 0:
start = i
depth += 1
if c in ‘]}’:
depth -= 1
if depth == 0 and start >= 0:
try:
return json.loads(text[start:i+1])
except:
start = -1
return None

def load_existing(filename):
try:
with open(filename, ‘r’) as f:
data = json.load(f)
if isinstance(data, dict) and ‘items’ in data:
return data[‘items’]
if isinstance(data, list):
return data
except:
pass
return []

def merge_items(existing, new_items, key_field=‘headline’):
keys = set()
for s in existing:
keys.add(s.get(key_field, ‘’))
fresh = []
for s in new_items:
if s.get(key_field, ‘’) not in keys:
s[’_at’] = datetime.now().isoformat()
fresh.append(s)
cutoff = datetime.now().timestamp() - 30 * 86400
merged = fresh + [s for s in existing if datetime.fromisoformat(s.get(’_at’, datetime.now().isoformat())).timestamp() > cutoff]
return merged

def main():
print(“Fetching news…”)
articles = fetch_news()
print(f”Got {len(articles)} articles”)

```
print("Generating classified signals...")
new_signals = generate_signals(articles)
if not new_signals:
    print("Failed to generate signals")
    return

print(f"Generated {len(new_signals)} signals")

# Count classifications
threats = len([s for s in new_signals if s.get('classification') == 'threat'])
opps = len([s for s in new_signals if s.get('classification') == 'opportunity'])
both = len([s for s in new_signals if s.get('classification') == 'both'])
print(f"Classification: {threats} threats, {opps} opportunities, {both} both")

existing_signals = load_existing('signals.json')
merged_signals = merge_items(existing_signals, new_signals, 'headline')

print("Generating opportunities with entry mode analysis...")
new_opps = generate_opportunities(merged_signals[:20])
if not new_opps:
    print("Failed to generate opportunities")
    new_opps = []

print(f"Generated {len(new_opps)} opportunities")
existing_opps = load_existing('opps.json')
merged_opps = merge_items(existing_opps, [dict(o, headline=o.get('title','')) for o in new_opps], 'headline')
for o in merged_opps:
    if 'headline' in o and 'title' in o:
        del o['headline']

# Stats
stats = {
    "totalSignals": len(merged_signals),
    "localSignals": len([s for s in merged_signals if s.get('scope') == 'local']),
    "globalSignals": len([s for s in merged_signals if s.get('scope') == 'global']),
    "threats": len([s for s in merged_signals if s.get('classification') in ['threat', 'both']]),
    "opportunities_count": len([s for s in merged_signals if s.get('classification') in ['opportunity', 'both']]),
    "criticalSignals": len([s for s in merged_signals if s.get('impact') == 'critical']),
    "totalOpps": len(merged_opps),
    "avgScore": round(sum(o.get('score', 0) for o in merged_opps) / max(len(merged_opps), 1)),
}

signals_out = {"items": merged_signals, "lastRefresh": datetime.now().isoformat(), "stats": stats}
opps_out = {"items": merged_opps, "lastRefresh": datetime.now().isoformat()}

with open('signals.json', 'w') as f:
    json.dump(signals_out, f, indent=2)
with open('opps.json', 'w') as f:
    json.dump(opps_out, f, indent=2)

print(f"Done. {len(merged_signals)} signals, {len(merged_opps)} opportunities saved.")
print(f"Stats: {json.dumps(stats)}")
```

if **name** == “**main**”:
main()
