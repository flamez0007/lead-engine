import asyncio
import json
import logging
import os
import sqlite3
import websockets
import aiohttp
from aiohttp import web
from datetime import datetime
from groq import AsyncGroq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# =====================================================================
# CONFIGURATION & ENDPOINTS
# =====================================================================
JETSTREAM_URL = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

AFTERCHOICE_WEBHOOK = "https://discord.com/api/webhooks/1528859955212451880/7eqlB5irt32mFz-q534hZS7N8J0wMG9lf4bCYiEqGFtwvhdLdbRiA4usgpeycXhS9Oo4"
DECO_WEBHOOK = "https://discord.com/api/webhooks/1528870135731851396/PORbJqM0VsXIUfsmKBXRQ3TUCti1uopo7uKzs-IG1WcRFIHrZEDsuXC_ZC5Qtiiiz3bC"

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client = AsyncGroq(api_key=GROQ_API_KEY)

# =====================================================================
# TIER 1 SIGNAL GROUPS (Tightened to Protect Groq Rate Limits)
# =====================================================================
# Hard drop words: instantly discard before calling AI
NEGATIVE_NOISE = [
    "hiring", "job opening", "apply now", "remote manager", "full-time", "part-time",
    "salary", "resume", "giveaway", "free download", "trump", "biden", "election",
    "porn", "nsfw", "onlyfans", "dating", "ngl.link"
]

# Targeted Afterchoice phrases
HIGH_INTENT_AFTERCHOICE_PHRASES = [
    "looking for agency", "recommend an agency", "need a developer", "need a designer",
    "need a web dev", "looking for web dev", "looking for a designer", "fired our agency",
    "rebrand our", "redesign our", "rebuilding our site", "rebuilding our website",
    "new landing page", "website isn't converting", "site isn't converting",
    "redesigning our website", "shopify developer", "nextjs developer"
]

SITUATION_SIGNALS = ["launching", "rebranding", "relaunching", "redesigning", "rebuilding", "pivoting", "revamping"]
PAIN_SIGNALS = ["broken", "outdated", "failed", "conversion dropped", "amateurish", "frustrated with our website"]
BUSINESS_SIGNALS = ["startup", "brand", "agency", "ecommerce", "saas", "our website", "our store"]

DECO_PHRASES = [
    "bare walls", "empty walls", "wall art", "textured art", "interior design ideas",
    "looking for art", "decorating my apartment", "decorating our living room"
]

# =====================================================================
# TIER 2 SYSTEM PROMPT SPECIFICATION
# =====================================================================
EVALUATOR_SYSTEM_PROMPT = """
You are the Lead Evaluation Intelligence Engine for Afterchoice, a premium branding and web development agency.

Your duty is to analyze social media posts to determine if the author is in a real-world situation involving a MEANINGFUL GAP that Afterchoice can plausibly solve.

DEFINITIONS:
- Current State: The existing condition of their brand, website, or digital platform.
- Desired State: The goal, milestone, pivot, or launch they are pursuing.
- Meaningful Gap: A clear disconnect where the current digital asset is failing, slowing down, or misrepresenting business goals.

DISQUALIFICATION PROFILES (Set is_disqualified to true if ANY match):
- Author is a developer, designer, agency, or freelancer sharing work, asking dev questions, or offering services.
- Author is a student, job seeker, commentator, educator, or news account.
- Author is discussing a consumer experience without owning the business or decision.

SCORING VECTORS (Score each 0 to 5):
1. meaningful_gap: Real friction/transition between Current and Desired state? (GATING VECTOR)
2. actionability: Evidence of near-term intent or active search? (GATING VECTOR)
3. service_match: Maps directly to branding, web dev, UI/UX, or e-commerce rebuilds? (GATING VECTOR)
4. business_context: Real business, startup, or commercial project? (MULTIPLIER VECTOR)
5. authority: Likely a decision-maker (founder, CEO, owner)? (MULTIPLIER VECTOR)

Respond ONLY with valid JSON:
{
  "is_disqualified": boolean,
  "disqualification_reason": "string or null",
  "situation_analysis": {
    "current_state": "string",
    "desired_state": "string",
    "identified_gap": "string"
  },
  "scores": {
    "meaningful_gap": integer,
    "actionability": integer,
    "service_match": integer,
    "business_context": integer,
    "authority": integer
  },
  "summary_reasoning": "string"
}
"""

# =====================================================================
# DATABASE & CACHE DEDUPLICATION
# =====================================================================
def init_db():
    conn = sqlite3.connect("processed_leads.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_posts (
            post_uri TEXT PRIMARY KEY,
            processed_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def is_already_processed(post_uri: str) -> bool:
    conn = sqlite3.connect("processed_leads.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_posts WHERE post_uri = ?", (post_uri,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def mark_as_processed(post_uri: str):
    conn = sqlite3.connect("processed_leads.db")
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed_posts (post_uri, processed_at) VALUES (?, ?)", 
                       (post_uri, datetime.now()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

# =====================================================================
# TIER 1 FILTER
# =====================================================================
def passes_tier1_filter(clean_text: str) -> tuple[bool, str]:
    # 1. Hard noise discard
    if any(noise in clean_text for noise in NEGATIVE_NOISE):
        return False, ""

    # 2. Check Deco signals
    if any(phrase in clean_text for phrase in DECO_PHRASES):
        return True, "DECO"

    # 3. Check High-Intent Direct Phrases for Afterchoice
    if any(phrase in clean_text for phrase in HIGH_INTENT_AFTERCHOICE_PHRASES):
        return True, "AFTERCHOICE"

    # 4. Strict Combinations (Must have Situation + Pain + Business)
    has_sit = any(s in clean_text for s in SITUATION_SIGNALS)
    has_pain = any(p in clean_text for p in PAIN_SIGNALS)
    has_biz = any(b in clean_text for b in BUSINESS_SIGNALS)

    if (has_sit and has_pain) or (has_pain and has_biz):
        return True, "AFTERCHOICE"

    return False, ""

# =====================================================================
# TIER 2 & 3 QUALIFICATION & GATING
# =====================================================================
def evaluate_lead_qualification(eval_data: dict) -> dict:
    if eval_data.get("is_disqualified"):
        return {
            "qualified": False, 
            "status": "DISQUALIFIED",
            "reason": f"Disqualified profile: {eval_data.get('disqualification_reason')}"
        }
    
    scores = eval_data.get("scores", {})
    gap = scores.get("meaningful_gap", 0)
    act = scores.get("actionability", 0)
    match = scores.get("service_match", 0)
    biz = scores.get("business_context", 0)
    auth = scores.get("authority", 0)
    
    if gap < 3 or act < 3 or match < 3:
        failed_gates = []
        if gap < 3: failed_gates.append(f"meaningful_gap ({gap}/5)")
        if act < 3: failed_gates.append(f"actionability ({act}/5)")
        if match < 3: failed_gates.append(f"service_match ({match}/5)")
        return {
            "qualified": False,
            "status": "REJECTED (Gating Failure)",
            "reason": f"Failed gate(s): {', '.join(failed_gates)}"
        }
    
    gating_score = ((gap + act + match) / 15.0) * 70.0
    multiplier_score = ((biz + auth) / 10.0) * 30.0
    total_score = round(gating_score + multiplier_score, 1)
    
    is_qualified = total_score >= 65.0
    return {
        "qualified": is_qualified,
        "status": "QUALIFIED" if is_qualified else "REJECTED (Low Score)",
        "total_score": total_score,
        "reason": f"Final Weighted Score: {total_score}/100"
    }

async def evaluate_intent_with_groq_api(post_text: str, venture: str) -> tuple[dict, dict]:
    if venture == "DECO":
        try:
            res = await groq_client.chat.completions.create(
                model="openai/gpt-oss-20b",
                messages=[{"role": "user", "content": f"Does this post show someone looking for wall art, decor, or redecorating? Answer YES or NO. Post: {post_text}"}]
            )
            verdict = "YES" in res.choices[0].message.content.upper()
            return {}, {"qualified": verdict, "total_score": 100 if verdict else 0, "status": "QUALIFIED" if verdict else "REJECTED"}
        except Exception:
            return {}, {"qualified": False, "status": "REJECTED"}

    try:
        response = await groq_client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[
                {"role": "system", "content": EVALUATOR_SYSTEM_PROMPT},
                {"role": "user", "content": f"Analyze this post: \"{post_text}\""}
            ],
            response_format={"type": "json_object"}
        )
        raw_json = response.choices[0].message.content
        eval_data = json.loads(raw_json)
        qual_result = evaluate_lead_qualification(eval_data)
        return eval_data, qual_result
    except Exception as ai_err:
        # Handles 400 safety refusals or rate limits smoothly
        logging.info(f"Skipping unprocessable or throttled post: {ai_err}")
        return {}, {"qualified": False, "status": "SKIPPED", "reason": str(ai_err)}

# =====================================================================
# DISCORD DISPATCHER
# =====================================================================
async def send_discord_webhook(webhook_url: str, venture: str, post_text: str, user_did: str, post_uri: str, eval_data: dict, qual_result: dict):
    situation = eval_data.get("situation_analysis", {})
    scores = eval_data.get("scores", {})
    
    if venture == "AFTERCHOICE":
        embed = {
            "title": "🚀 High-Intent Opportunity Detected",
            "color": 5814783,
            "fields": [
                {"name": "📊 Lead Score", "value": f"**{qual_result.get('total_score', 'N/A')} / 100**", "inline": True},
                {"name": "🔍 Identified Gap", "value": situation.get("identified_gap", "N/A"), "inline": False},
                {"name": "📍 Current vs. Desired State", "value": f"**Current:** {situation.get('current_state', 'N/A')}\n**Desired:** {situation.get('desired_state', 'N/A')}", "inline": False},
                {"name": "📝 Post Content", "value": f"\"{post_text}\"", "inline": False},
                {"name": "📈 Vector Breakdown", "value": f"Gap: `{scores.get('meaningful_gap')}/5` | Act: `{scores.get('actionability')}/5` | Service: `{scores.get('service_match')}/5`", "inline": False},
                {"name": "🔗 Post Address", "value": f"`{post_uri}`", "inline": False}
            ],
            "footer": {"text": "Afterchoice Intelligence Engine • 24/7 Cloud"}
        }
    else:
        embed = {
            "title": "🖼️ DECO SIGNAL DETECTED",
            "color": 15105570,
            "fields": [
                {"name": "Content", "value": post_text, "inline": False},
                {"name": "User DID", "value": f"`{user_did}`", "inline": True},
                {"name": "Post URI", "value": f"`{post_uri}`", "inline": False}
            ],
            "footer": {"text": "DECO Filter • 24/7 Cloud"}
        }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json={"embeds": [embed]}) as response:
                if response.status in (200, 204):
                    logging.info(f"🚀 Dispatched {venture} lead to Discord.")
    except Exception as e:
        logging.error(f"❌ Webhook dispatch error: {e}")

# =====================================================================
# REAL-TIME STREAM PIPELINE
# =====================================================================
async def stream_bluesky_firehose():
    logging.info("Connecting to Bluesky Jetstream Firehose...")
    try:
        async with websockets.connect(JETSTREAM_URL) as websocket:
            logging.info("Connected! Tier 1 Combinatorial + Groq AI Judge Active...")
            
            async for raw_message in websocket:
                try:
                    data_packet = json.loads(raw_message)
                    if data_packet.get("kind") == "commit" and data_packet.get("commit", {}).get("operation") == "create":
                        record = data_packet["commit"].get("record", {})
                        post_text = record.get("text", "")
                        user_did = data_packet.get("did", "")
                        post_uri = f"at://{user_did}/app.bsky.feed.post/{data_packet['commit'].get('rkey')}"
                        
                        if post_text:
                            clean_text = post_text.lower()
                            
                            passed_tier1, assigned_venture = passes_tier1_filter(clean_text)
                            if not passed_tier1:
                                continue
                                
                            if is_already_processed(post_uri):
                                continue
                                
                            mark_as_processed(post_uri)
                            target_webhook = AFTERCHOICE_WEBHOOK if assigned_venture == "AFTERCHOICE" else DECO_WEBHOOK
                            
                            eval_data, qual_result = await evaluate_intent_with_groq_api(post_text, assigned_venture)
                            
                            if qual_result.get("qualified"):
                                print(f"\n[💎 VERIFIED OPPORTUNITY] ── {assigned_venture} | Score: {qual_result.get('total_score')}/100")
                                asyncio.create_task(
                                    send_discord_webhook(target_webhook, assigned_venture, post_text, user_did, post_uri, eval_data, qual_result)
                                )
                            else:
                                logging.info(f"Dropped post [{qual_result.get('status')}]: '{post_text[:35]}...'")
                                
                except json.JSONDecodeError:
                    continue
    except Exception as network_error:
        logging.error(f"WebSocket disconnected: {network_error}")
        await asyncio.sleep(5)
        await stream_bluesky_firehose()

# =====================================================================
# KEEP-ALIVE SERVER (FOR 24/7 RENDER HOSTING)
# =====================================================================
async def handle_health_check(request):
    return web.Response(text="Afterchoice Listener is Active and Healthy!", status=200)

async def start_app():
    init_db()
    
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    app.router.add_get('/health', handle_health_check)
    
    port = int(os.getenv("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Keep-Alive HTTP Server running on port {port}")

    await stream_bluesky_firehose()

if __name__ == "__main__":
    try:
        asyncio.run(start_app())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Terminated cleanly.")
