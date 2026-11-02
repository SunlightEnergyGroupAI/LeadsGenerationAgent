from sklearn.ensemble import GradientBoostingClassifier, VotingClassifier, RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from langchain.chat_models import init_chat_model
from langchain.messages import SystemMessage, HumanMessage
from crawl4ai import AsyncWebCrawler
from urllib.parse import urlparse
from ddgs import DDGS
from contact import Contact
from filter import FilterResponse
import pandas as pd
import numpy as np
import asyncio
import re


"""
Lead filter, 3 stages:
1. ML model classifies the lead by company name + role (+ state) -> Good Fit / Potential Fit / Bad Fit
2. If it's a Good Fit or Potential Fit, Crawl4AI crawls the company's website to research what they do / use
3. Small AI model reads the research and gives the final verdict
"""

TRAINING_DATA = "seg_icp_training_data.csv"
# classes from the ML model that move on to the research stage
PASSING_CLASSES = {"Good Fit", "Potential Fit"}
# the ML model only rejects a lead when it's this sure, borderline leads go to the AI
BAD_FIT_CONFIDENCE = 0.75
# roles that never make energy decisions, rejected before spending time on the AI
BAD_ROLE_WORDS = ("sales", "marketing", "business development", "human resources", "recruit", "talent",
                  "information technology", "communications", "creative", "editor", "events")
# these always get through, an "Owner, Marketing Manager" still owns the business
TOP_ROLE_WORDS = ("owner", "founder", "president", "ceo", "chief executive", "principal")


def isBadRole(role: str | None) -> bool:
    role = (role or "").lower()
    # "vice president" contains "president" but isn't a top role
    if any(word in role.replace("vice president", "") for word in TOP_ROLE_WORDS):
        return False
    # "it" has to be its own word so titles like "Quality Director" don't match
    return any(word in role for word in BAD_ROLE_WORDS) or "it" in re.split(r"\W+", role)
MAX_PAGE_CHARS = 6000

BLOCKED = ("yelp.", "linkedin.", "facebook.", "indeed.", "glassdoor.", "yellowpages.",
           "tripadvisor.", "zoominfo.", "mapquest.", "bbb.org", "wikipedia.", "reddit.",
           "instagram.", "youtube.", "manta.", "dnb.com", "expedia.", "booking.com",
           "allbiz.", "bizapedia.", "opencorporates.", "buzzfile.", "chamberofcommerce.",
           "crunchbase.", "bloomberg.", "rocketreach.", "apollo.io",
           "yahoo.", "bing.", "google.", "msn.", "duckduckgo.", "aol.")

classifier = None
# reasoning=False turns off the long hidden thinking (verdicts were taking minutes or hanging),
# num_predict caps how much it can write, and the timeout skips a lead if the model still gets stuck
verdictLLM = init_chat_model(
    model_provider="ollama",
    model="qwen3.5:4b",
    reasoning=False,
    num_predict=512,
    client_kwargs={"timeout": 120},
).with_structured_output(FilterResponse)


# Stage 1: ML model

def buildClassifier() -> Pipeline:
    features = ColumnTransformer([
        ("company", TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4)), "company"),
        ("company_role", TfidfVectorizer(ngram_range=(1, 2)), "company_role"),
        ("state", CountVectorizer(token_pattern=r".+"), "state"),
    ])
    voting = VotingClassifier(
        estimators=[
            ("lr", LogisticRegression(max_iter=1000)),
            ("rf", RandomForestClassifier(n_estimators=200, n_jobs=-1, random_state=42)),
            ("gb", GradientBoostingClassifier(random_state=42)),
        ],
        voting="soft",
    )
    return Pipeline([("features", features), ("classifier", voting)])


def trainClassifier() -> Pipeline:
    data = pd.read_csv(TRAINING_DATA).fillna("")
    X = data[["company", "company_role", "state"]]
    y = data["classification"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    model = buildClassifier()
    model.fit(X_train, y_train)
    print(classification_report(y_test, model.predict(X_test)))
    return model


def classifyLead(contact: Contact) -> str:
    global classifier
    if classifier is None:
        classifier = trainClassifier()
    row = pd.DataFrame([{
        "company": contact.company or "",
        "company_role": contact.company_role or "",
        "state": contact.state or "",
    }])
    probabilities = dict(zip(classifier.classes_, classifier.predict_proba(row)[0]))
    if probabilities["Bad Fit"] >= BAD_FIT_CONFIDENCE:
        return "Bad Fit"
    if probabilities["Good Fit"] >= probabilities["Potential Fit"]:
        return "Good Fit"
    return "Potential Fit"


# Stage 2: Crawl4AI research

def findCompanyWebsite(contact: Contact) -> str | None:
    query = f"{contact.company} {contact.city or ''} {contact.state or ''}".strip()
    try:
        results = DDGS().text(query, max_results=5, backend="bing")
    except Exception as e:
        print("search failed:", contact.company, e)
        return None
    for result in results:
        href = result.get("href", "")
        domain = urlparse(href).netloc.lower()
        if domain and not any(b in domain for b in BLOCKED):
            return href
    return None


async def researchCompany(crawler: AsyncWebCrawler, contact: Contact) -> str:
    url = findCompanyWebsite(contact)
    if not url:
        return ""
    try:
        page = await asyncio.wait_for(crawler.arun(url=url), timeout=20)
        return str(page.markdown or "")[:MAX_PAGE_CHARS]
    except Exception as e:
        print("skip", url, e)
        return ""


# Stage 3: small AI model verdict

verdict_prompt = """
You decide if a lead is a good fit for Sunlight Energy Group, an energy consulting company
that helps businesses lower their electricity and natural gas costs.

A good lead is a small or mid-sized business with a physical location that uses real energy,
where the contact can make or influence decisions about costs, utilities, or facilities
(owner, co-owner, founder, CEO, president, general manager, CFO, COO, operations, facilities, plant, purchasing).

Every one of these industries is a good fit, you do not need to see heavy equipment:
manufacturing, machine shops, plastics, metal fabrication, commercial printing, food and beverage
production (coffee roasters, breweries, distilleries, wineries, bakeries, food processors), hotels, inns,
resorts, dry cleaners and laundries, restaurants, warehouses and cold storage, multi-location retail,
and property management.

A bad lead:
- Large companies we are unlikely to win: publicly traded companies, REITs, and large national chains,
  brands, or franchise locations of a national brand
- Software, consulting, media, directories, blogs, or remote-only companies with little energy usage
- Contacts in sales, marketing, HR, IT, or business development
- Businesses clearly located outside New York, New Jersey, Maryland, and Pennsylvania

Location: cities like Newark, Jersey City, Brooklyn, Buffalo, Baltimore, Philadelphia, and Pittsburgh are inside
the target states. Only reject for location if the research clearly shows the business is in another state or country.
If the location is unknown, do not reject for location.

Use the website research to judge what the company does. If the research is empty, or is about a different
company or an unrelated page, ignore it and judge from the company name and role.
When a lead is borderline, approve it, the sales team reviews every lead.
Give short reasoning, then the verdict. The verdict must match your reasoning: if your reasoning
says the lead is a likely fit, the verdict is true.
"""


def getVerdict(contact: Contact, research: str) -> FilterResponse:
    lead_info = f"""
Company: {contact.company}
Contact role: {contact.company_role}
Industry: {contact.industry}
Location: {contact.city}, {contact.state}

Website research:
{research or "No research found."}
"""
    return verdictLLM.invoke([SystemMessage(verdict_prompt), HumanMessage(lead_info)])


# Full filter

async def isGoodLead(contact: Contact, crawler: AsyncWebCrawler | None = None, research: str | None = None) -> bool:
    if isBadRole(contact.company_role):
        print(f"{contact.company} | {contact.company_role} | skip, role doesn't make energy decisions")
        return False

    ml_class = classifyLead(contact)
    print(f"{contact.company} | {contact.company_role} | ML: {ml_class}")
    if ml_class not in PASSING_CLASSES:
        return False

    # web search leads already crawled the site, so reuse that text instead of crawling again
    if research is not None:
        research = research[:MAX_PAGE_CHARS]
    elif crawler is None:
        async with AsyncWebCrawler() as crawler:
            research = await researchCompany(crawler, contact)
    else:
        research = await researchCompany(crawler, contact)

    try:
        verdict = getVerdict(contact, research)
    except Exception as e:
        print("  AI verdict failed, skipping lead:", e)
        return False
    print(f"  AI verdict: {verdict['icp_verdict']} | {verdict['reasoning']}")
    if verdict["icp_verdict"]:
        contact.fit_score_reasoning = verdict["reasoning"]
    return verdict["icp_verdict"]


async def filterLeads(leads: list[Contact]) -> list[Contact]:
    good_leads = []
    async with AsyncWebCrawler() as crawler:
        for contact in leads:
            if await isGoodLead(contact, crawler):
                good_leads.append(contact)

    print(f"{len(good_leads)}/{len(leads)} leads passed the filter")
    return good_leads
