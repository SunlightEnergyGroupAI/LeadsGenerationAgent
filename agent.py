from langgraph.graph import StateGraph, START, END
from langchain.chat_models import init_chat_model
from tavily import TavilyClient  
from ddgs import DDGS  
from dotenv import load_dotenv 
import os 
import requests
import pandas as pd 
from state import StateSchema 
from langgraph.checkpoint.memory import InMemorySaver 
from crawl4ai import AsyncWebCrawler 
from typing import Literal 
from langchain.mcp import MCPAdapter 
from langchain_core.documents import Document 
from source import Source 
import asyncio 
from langchain.messages import SystemMessage  
from supabase import create_client 
import time  
from langchain.agents import create_agent 
from langchain_ollama import ChatOllama
from fastmcp.client import Client 
from contact import Contact   
from fastmcp.client.transports import StreamableHttpTransport  
from langchain_core.tools import Tool 
from SearchQuery import Query
import re
from urllib.parse import urlparse 
import random
from langchain.agents import create_agent 
from filter import FilterResponse

load_dotenv()  
tavilyapikey = os.getenv("TAVILYAPI")   
hunterapikey = os.getenv("HUNTERAPI") 
zmClientId = os.getenv("ZOOMINFOCLIENTID") 
zmClientSecret = os.getenv("ZOOMINFOCLIENTSECRET") 
hubspotKey =  os.getenv("HUBSPOT_TOKEN") 
geoapikey = os.getenv("GEOPLACES_KEY")


def getLocationForCompany():
    #TODO: helper function that calls an api for the hunter io and zoom info pipeline source 
    ... 

def namesZoomInfo(): 
    #fix this for testing and then make ml model with attributes used from Contact type
    states = {1: "New York", 2: "Maryland", 3: "New Jersey", 4: "Pennsylvania"}
    chosenState = states.get(random.randint(1, 4))
    nameOnlyContacts: list[Contact] = []

    zmclientid = os.getenv("ZOOMINFOCLIENTID") 
    zmclientsecret = os.getenv("ZOOMINFOCLIENTSECRET") 

    tokenResponse = requests.post(
        url="https://api.zoominfo.com/gtm/oauth/v1/token",
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}, 
        auth=(zmclientid,zmclientsecret), 
        data={"grant_type": "client_credentials"},        
        timeout=30
    )
    token = tokenResponse.json() 
    token = token["access_token"] 
    print(token)

    # page = fetchNewZoomInfoPageNumber()
    url = "https://api.zoominfo.com/gtm/data/v1/contacts/search?page[number]=1&page[size]=1"
    headers = {
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "data": {
            "type": "ContactSearch",
            "attributes": {
                "managementLevel": "C Level Exec, VP Level Exec,Director",
                "executivesOnly": True,
                "contactAccuracyScoreMin": "75",
                "contactAccuracyScoreMax": "99",
                "employeeRangeMin": "20",
                "employeeRangeMax": "800",
                "state": chosenState,
                "industryCodes": "Manufacturing,Hospitality,Retail"
            }
        }
    }
    searchResponse = requests.post(url, json=payload, headers=headers, timeout=30)
    searchResponse.raise_for_status()

    print(searchResponse.json())  



config = {"configurable": {"thread_id": "1"}} 
checkpointer = InMemorySaver() 
agent = init_chat_model(model_provider="ollama", model="qwen3.5:4b")  
graph = StateGraph(StateSchema)  



def hubspotSearch(objectType: str, propertyName: str, value: str) -> list: 
    HUBSPOT_URL = "https://api.hubapi.com/crm/objects/2026-09" 
    headers = {
        "Authorization": f"Bearer {hubspotKey}",
        "Content-Type": "application/json"
    }
    body = {
        "filterGroups": [
            {"filters": [{"propertyName": propertyName, "operator": "EQ", "value": value}]}
        ],
        "properties": [],
        "sorts": [],
        "limit": 1,
        "after": "0"
    }
    response = requests.post(f"{HUBSPOT_URL}/{objectType}/search", headers=headers, json=body, timeout=30)
    response.raise_for_status()
    return response.json().get("results", [])


def isInHubspot(contact: Contact) -> bool:
    companies = hubspotSearch("companies", "name", contact.company.strip())
    if not companies:
        return False
    leads = hubspotSearch("leads", "associations.company", companies[0]["id"])
    return len(leads) > 0

def initalize_agent(state: StateSchema) -> dict:
    return {}  

def fetchNewZoomInfoPageNumber() -> int: 
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("zoominfopages").select("*", count="exact").execute() 
    return response.count + 1 

def writeNewZoomInfoPageNumber(pageNumber: int) -> None:
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("zoominfopages").insert({"pagenum": pageNumber}).execute()
    return  


def enrichContactZoomInfo(contact: Contact, token: str) -> Contact:
    url = "https://api.zoominfo.com/gtm/data/v1/contacts/enrich"
    headers = {
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "data": {
            "type": "ContactEnrich",
            "attributes": {
                "outputFields": [
                        "email", "phone", "mobilePhone",
                        "state", "street", "country",
                        "companyState", "companyStreet",
                        "directPhoneDoNotCall", "mobilePhoneDoNotCall",
                        "companyPrimaryIndustry", "companyWebsite"
                ],
                "matchPersonInput": [{"personId": int(contact.id)}]
            }
        }
    }
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    results = response.json().get("data", [])
    if results:
        attrs = results[0].get("attributes", {})
        contact.email = attrs.get("email")
        contact.phone = attrs.get("phone")
        contact.company_address = attrs.get("street") 
        contact.state = attrs.get("state")  
        contact.industry = (attrs.get("company", {}).get("primaryIndustry") or [None])[0]    
        contact.data_source = "Zoom Info"
    return contact


def getFullContact(state: StateSchema) -> dict:
    states = {1: "New York", 2: "Maryland", 3: "New Jersey", 4: "Pennsylvania"}
    chosenState = states.get(random.randint(1, 4))
    uniqueContacts: list[Contact] = []

    token = state.get("zm_access_token")
    if not token:
        tokenResponse = requests.post(
            url="https://api.zoominfo.com/gtm/oauth/v1/token",
            headers={"Accept": "application/json"},
            auth=(zmClientId, zmClientSecret),
            data={"grant_type": "client_credentials"},
            timeout=30
        )
        tokenResponse.raise_for_status()
        token = tokenResponse.json()["access_token"]

    page = fetchNewZoomInfoPageNumber()
    url = f"https://api.zoominfo.com/gtm/data/v1/contacts/search?page[number]={page}&page[size]=1"
    headers = {
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "data": {
            "type": "ContactSearch",
            "attributes": {
                "managementLevel": "C Level Exec, VP Level Exec,Director",
                "executivesOnly": True,
                "contactAccuracyScoreMin": "75",
                "contactAccuracyScoreMax": "99",
                "employeeRangeMin": "20",
                "employeeRangeMax": "800",
                "state": chosenState,
                "industryCodes": "Manufacturing,Hospitality,Retail"
            }
        }
    }
    searchResponse = requests.post(url, json=payload, headers=headers, timeout=30)
    searchResponse.raise_for_status()
    seen_companies = {c.company for c in state.get("leads", [])}

    for record in searchResponse.json()["data"]:
        attrs = record["attributes"]
        currLead = Contact(
            company=attrs["company"]["name"],
            lead_first_name=attrs["firstName"],
            lead_last_name=attrs["lastName"],
            company_role=attrs.get("jobTitle"),
            state=attrs.get("state"),  
            company_address=attrs.get("street"),
            id=record["id"]
        )
        if currLead.company in seen_companies:
            continue
        elif isInHubspot(currLead):
            continue
        seen_companies.add(currLead.company) 
        addedContact = enrichContactZoomInfo(currLead, token) 
        addedContact.data_source = "Zoom Info" 

        uniqueContacts.append(addedContact)
    writeNewZoomInfoPageNumber(page) 
    return {
        "leads": uniqueContacts,
        "zm_access_token": token,
        "zoom_info_calls": state["zoom_info_calls"]+1
    }


def getNamesFromZoomInfo(state: StateSchema) -> dict:
    states = {1: "New York", 2: "Maryland", 3: "New Jersey", 4: "Pennsylvania"}
    chosenState = states.get(random.randint(1, 4))
    nameOnlyContacts: list[Contact] = []

    token = state.get("zm_access_token")
    if not token:
        tokenResponse = requests.post(
            url="https://api.zoominfo.com/gtm/oauth/v1/token",
            headers={"Accept": "application/json"},
            json={"grant_type": "client_credentials", "client_id": zmClientId, "client_secret": zmClientSecret},
            timeout=30
        )
        tokenResponse.raise_for_status()
        token = tokenResponse.json()["access_token"]

    page = fetchNewZoomInfoPageNumber()
    url = f"https://api.zoominfo.com/gtm/data/v1/contacts/search?page[number]={page}&page[size]=1"
    headers = {
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "data": {
            "type": "ContactSearch",
            "attributes": {
                "managementLevel": "C Level Exec, VP Level Exec,Director",
                "executivesOnly": True,
                "contactAccuracyScoreMin": "75",
                "contactAccuracyScoreMax": "99",
                "employeeRangeMin": "20",
                "employeeRangeMax": "800",
                "state": chosenState,
                "industryCodes": "Manufacturing,Hospitality,Retail"
            }
        }
    }
    searchResponse = requests.post(url, json=payload, headers=headers, timeout=30)
    searchResponse.raise_for_status()
    seen_companies = {c.company for c in state.get("leads", [])}
    seen_companies |= {c.company for c in state.get("leads_to_get_email", [])} 

    print(searchResponse.json())

    for record in searchResponse.json()["data"]:
        attrs = record["attributes"]
        currLead = Contact(
            company=attrs["company"]["name"],
            lead_first_name=attrs["firstName"],
            lead_last_name=attrs["lastName"],
            company_role=attrs.get("jobTitle"),
            data_source="Zoom Info",
            id=record["id"]
        )
        if currLead.company in seen_companies:
            continue
        elif isInHubspot(currLead):
            continue 
        #where we modify the currLead variable by calling our func that gets the 
        # location and state 

        seen_companies.add(currLead.company)
        nameOnlyContacts.append(currLead)
    writeNewZoomInfoPageNumber(page) 
    return {
        "leads_to_get_email": nameOnlyContacts,
        "zm_access_token": token, 
        "zoom_info_name_counter": state["zoom_info_name_counter"] + 1
    } 

def findemailWithHunter(state: StateSchema) -> dict: 
    contacts: list[Contact] = []  
    url = "https://api.hunter.io/v2/email-finder"
    for contact in state["leads_to_get_email"]:
        first_name = contact.lead_first_name 
        last_name = contact.lead_last_name 
        company_name = contact.company  
        role = contact.company_role  
        person_state = contact.state
        response = requests.get(url, params={"first_name": first_name, "last_name": last_name, "company": company_name, "api_key": hunterapikey}, timeout=30) 
        data = response.json() 
        lead_email = data["data"]["email"] 
        contacts.append(Contact(company_name, first_name, last_name, role, email=lead_email, state=person_state, data_source="Zoom info & Hunter.io")) 
    return {"leads": contacts}  


def fetchNewApolloPageNumber() -> int:  
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("apollopages").select("*", count="exact").execute() 
    return response.count + 1  


def writeNewApolloPageNumber(pageNumber: int) -> None:
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("apollopages").insert({"page": pageNumber}).execute()
    return 



def enrichApolloContact(apollo_id: str) -> Contact | None:
    response = requests.post(
        "https://api.apollo.io/api/v1/people/match",
        headers={
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "accept": "application/json",
            "x-api-key": os.getenv("APOLLOAPIKEY"),
        },
        json={"id": apollo_id},
    )
    response.raise_for_status()

    person = response.json().get("person")
    if not person:
        return None 

    organization = person.get("organization") or {}
    phone_numbers = person.get("phone_numbers") or []

    return Contact(
        company=organization.get("name"),
        lead_first_name=person.get("first_name"),
        lead_last_name=person.get("last_name"),
        company_role=person.get("title"),
        data_source="Apollo",
        email=person.get("email"),
        phone=phone_numbers[0].get("sanitized_number") if phone_numbers else None,
        state=person.get("state"), 
        industry=organization.get("industry"),
        id=apollo_id,
    )

def getApolloContacts(state: StateSchema) -> dict: 
    contacts: list[Contact] = [] 
    url = "https://api.apollo.io/api/v1/mixed_people/api_search?person_seniorities[]=c_suite&person_seniorities[]=director&person_seniorities[]=vp&person_seniorities[]=founder&organization_locations[]=New%20Jersey%2C%20US&organization_locations[]=New%20York%2C%20US&organization_locations[]=Maryland%2C%20US&organization_locations[]=Pennsylvania%2C%20US&person_locations[]=New%20Jersey%2C%20US&person_locations[]=New%20York%2C%20US&person_locations[]=Maryland%2C%20US&person_locations[]=Pennsylvania%2C%20US&q_organization_keyword_tags[]=manufacturing&q_organization_keyword_tags[]=machinery&q_organization_keyword_tags[]=plastics&q_organization_keyword_tags[]=pharmaceuticals&q_organization_keyword_tags[]=building%20materials&q_organization_keyword_tags[]=food%20production&q_organization_keyword_tags[]=hospitality&q_organization_keyword_tags[]=hotels&q_organization_keyword_tags[]=supermarkets&q_organization_keyword_tags[]=resorts&contact_email_status[]=verified&contact_email_status[]=likely%20to%20engage&contact_email_status[]=unverified&organization_num_employees_ranges[]=10%2C1000&page=1&per_page=1"
    pageNumber = fetchNewApolloPageNumber()  
    url = url.replace("&page=1&", f"&page={pageNumber}&")

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "accept": "application/json",
        "x-api-key": os.getenv("APOLLOAPIKEY"),
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()  
    dataDict = response.json()

    people = dataDict.get("people", [])
    if not people:
        print(f"No people on page {pageNumber}. Reached the end of the results.")
        return {"leads": [], "apollo_calls": state["apollo_calls"] + 1}

    for person in people:
        organization = person.get("organization") or {}
        curr_lead = Contact(
            organization.get("name"),
            person.get("first_name"),
            company_role=person.get("title"),
            data_source="Apollo",
            id=person["id"],
        )
        if not isInHubspot(curr_lead):
            enriched = enrichApolloContact(curr_lead.id)
            if enriched is None:
                continue
            contacts.append(enriched)
    writeNewApolloPageNumber(pageNumber) 
    return {"leads": contacts, "apollo_calls": state["apollo_calls"]+1} 


        
def model_classifier():  
    #ignore this is optional and for later in order to improve lead filtering after retreival 

    ...




def leadsFilter(state: StateSchema) -> dict: 
    #TODO: build the ML model to filter the leads 
    return {}
        



def leadsToCSV(state: StateSchema): 
    #change to filtered_leads later
    
    dataframe = pd.DataFrame({
        "company": [contact.company for contact in state["leads"]], 
        "lead_first_name": [contact.lead_first_name for contact in state["leads"]], 
        "lead_last_name": [contact.lead_last_name for contact in state["leads"]], 
        "company_role": [contact.company_role for contact in state["leads"]],
        "phone": [contact.phone for contact in state["leads"]], 
        "email": [contact.email for contact in state["leads"]], 
        "state": [contact.state for contact in state["leads"]], 
        "data_source": [contact.data_source for contact in state["leads"]]
    })  
    dataframe.to_csv("newleads.csv", index=False)  
    return {} 






async def composioOutlook(state: StateSchema) -> dict:
    composioKey = os.getenv("COMPOSIO_KEY")
    if not composioKey:
        raise ValueError("COMPOSIO_KEY missing from .env")

    senderAccount = os.getenv("COMPOSIO_OUTLOOK_ACCOUNT")
    accountArg = f", account={senderAccount!r}" if senderAccount else ""

    transport = StreamableHttpTransport(
        url="https://connect.composio.dev/mcp",
        headers={"x-consumer-api-key": composioKey}
    )
    with open("newleads.csv") as f:
        csvText = f.read()
    leadCount = len(state.get("filtered_leads", []))

    code = f"""
path = "/home/user/newleads.csv"
with open(path, "w") as f:
    f.write({csvText!r})

up, err = upload_local_file(path)
if err:
    print("upload error:", err)
else:
    data = up.get("data", up)
    res, err = run_composio_tool("OUTLOOK_SEND_EMAIL", {{
        "to": "kevin.kairouz@sunlightenergygroup.com",
        "subject": "New leads",
        "body": "{leadCount} new leads attached.",
        "attachment": {{"name": "newleads.csv", "mimetype": "text/csv", "s3key": data["s3key"]}}
    }}{accountArg})
    print(err or res)
"""

    async with Client(transport) as c:
        result = await c.call_tool("COMPOSIO_REMOTE_WORKBENCH", {"code_to_execute": code})
    print(result)
    return {}

async def searchWeb(state: StateSchema):
    """
    Step 1: search Bing, Yandex and Tavily and keep the first 10 company websites.
    Step 2: crawl each company's site (homepage plus contact/about/team pages)
    and pull emails, phones and exec-title lines.

    Returns:
    - list[dict]: one dict per company with company_domain, website, emails, phones, title_lines
    """ 
    contact: list[Contact] = [] 

    #check if in hubspot, check if its in current list ie state["leads"] 
    #if its unique lead append to contact list 


    # at end of the function return {"leads": contact}

    tavilyEngine = TavilyClient(api_key=tavilyapikey)
    searchEngine = DDGS()
    queryMakerLLM = init_chat_model(model_provider="ollama", model="nemotron-3-nano:30b-cloud")

    sq = queryMakerLLM.invoke([icp_prompt]).content
    sq2 = queryMakerLLM.invoke([icp_prompt2]).content
    print("BING QUERY:", sq)
    print("YANDEX QUERY:", sq2)
    urls = []
    try:
        urls += [r["href"] for r in searchEngine.text(sq, max_results=15, backend="bing")]
    except Exception as e:
        print("bing failed:", e)
    try:
        urls += [r["href"] for r in searchEngine.text(sq2, max_results=15, backend="yandex")]
    except Exception as e:
        print("yandex failed:", e)
    try:
        urls += [r["url"] for r in tavilyEngine.search(sq, max_results=10)["results"]]
    except Exception as e:
        print("tavily failed:", e)

    companies = {}  
    for u in urls:
        domain = urlparse(u).netloc.lower().removeprefix("www.")
        if not domain or any(b in domain for b in BLOCKED) or domain in companies:
            continue
        companies[domain] = u
        if len(companies) == 10:
            break
    print(f"{len(companies)} companies found:", list(companies))
    email_re = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
    phone_re = re.compile(r"\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}")
    title_re = re.compile(
        r"^.*\b(CEO|Chief \w+ Officer|President|Vice President|VP|Director|Manager|Owner|Pastor)\b.*$",
        re.MULTILINE,
    )
    junk = ("example.com", "sentry", "wixpress", ".png", ".jpg", ".webp")

    results = []
    async with AsyncWebCrawler() as crawler:
        for domain, home_url in companies.items():
            print("crawling:", domain)
            text = ""
            try:
                home = await asyncio.wait_for(crawler.arun(url=home_url), timeout=20)
                text += str(home.markdown or "")
                pages = []
                for link in (home.links or {}).get("internal", []):
                    href = link.get("href", "")
                    if any(w in href.lower() for w in CONTACT_WORDS) and href not in pages:
                        pages.append(href)

                for page in pages[:3]:
                    try:
                        res = await asyncio.wait_for(crawler.arun(url=page), timeout=20)
                        text += "\n" + str(res.markdown or "")
                    except Exception as e:
                        print("  skip page", page, e)
            except Exception as e:
                print("skip", domain, e)
                continue

            emails = sorted({e.lower().strip(".") for e in email_re.findall(text)
                             if not any(j in e.lower() for j in junk)})
            phones = sorted(set(phone_re.findall(text)))[:3]
            title_lines = [m.group(0).strip()[:120] for m in title_re.finditer(text)][:5]

            results.append({
                "company_domain": domain,
                "website": home_url,
                "emails": emails[:5],
                "phones": phones,
                "title_lines": title_lines,
            })
            print(f"{domain} | emails: {len(emails)} | phones: {len(phones)} | title lines: {len(title_lines)}")

    return results


icp_prompt = """
Write one web search query that finds the websites of individual businesses.

Target: a small manufacturer in Maryland.

Pick ONE specific trade (for example machine shop, sheet metal fabrication, plastics molding,
food production, printing, woodworking) and ONE Maryland city or town.

Rules:
- Plain keywords only
- No quotes, no parentheses, no OR or AND, no site: filters
- Do not use the words best, top, list, companies, small, or directory
- Under 10 words
- Do not mention job titles, emails, or LinkedIn
- Return only the query, with no explanation

Example of the style: sheet metal fabrication Pittsburgh Pennsylvania
"""

icp_prompt2 = """
Write one web search query that finds the websites of individual businesses.

Target: an independent hotel, inn or motel in New Jersey.

Pick ONE type (for example boutique hotel, family owned inn, bed and breakfast, motel,
resort) and ONE New Jersey city or town.

Rules:
- Plain keywords only
- No quotes, no parentheses, no OR or AND, no site: filters
- Do not use the words best, top, list, hotels, small, or directory
- Under 10 words
- Do not mention job titles, emails, or LinkedIn
- Return only the query, with no explanation

Example of the style: family owned inn Austin Texas
"""

BLOCKED = ("yelp.", "linkedin.", "facebook.", "indeed.", "glassdoor.", "yellowpages.",
           "tripadvisor.", "zoominfo.", "mapquest.", "bbb.org", "wikipedia.", "reddit.",
           "instagram.", "youtube.", "manta.", "dnb.com", "expedia.", "booking.com")
CONTACT_WORDS = ("contact", "about", "team", "leadership", "staff", "people", "management")


#Routing Nodes (Conditional Logic) 

def useZoomInfoAgain(state: StateSchema) -> str: 
    if state["zoom_info_calls"] < 2: 
        return "use_again"
    else: 
        return "next_tool"

def useHunterAndZoomAgain(state: StateSchema) -> str: 
    if state["zoom_info_name_counter"] < 5: 
        return "use_again" 
    else: 
        return "next_tool" 

def useApolloAgain(state: StateSchema) -> str: 
    if state["apollo_calls"] < 5: 
        return "use_again" 
    else: 
        return "filter" 






graph.add_node(getFullContact) 
graph.add_node(getNamesFromZoomInfo) 
graph.add_node(findemailWithHunter)  
graph.add_node(getApolloContacts)
graph.add_node(leadsToCSV) 
graph.add_node(leadsFilter)
graph.add_node(composioOutlook) 

graph.add_edge(START, "getFullContact") 
graph.add_conditional_edges(
    "getFullContact", 
    useZoomInfoAgain, 
    {"use_again": "getFullContact", "next_tool": "getNamesFromZoomInfo"} 
) 
graph.add_edge("getNamesFromZoomInfo", "findemailWithHunter") 
graph.add_conditional_edges(
    "findemailWithHunter", 
    useHunterAndZoomAgain, 
    {"use_again": "getNamesFromZoomInfo", "next_tool": "getApolloContacts"}
) 
graph.add_conditional_edges("getApolloContacts", useApolloAgain, {"use_again": "getApolloContacts", "filter": "leadsToCSV"}) 
graph.add_edge("leadsToCSV", "composioOutlook")
graph.add_edge("composioOutlook", END)
app = graph.compile(checkpointer=checkpointer) 
app.get_graph().draw_png("workflow.png")

# getApolloContacts()

# result = asyncio.run(app.ainvoke(
#     {
#         "leads": [],
#         "leads_to_get_email": [],
#         "zoom_info_calls": 0,
#         "zoom_info_name_counter": 0,
#         "apollo_calls": 0,
#         "names_page": 0,
#     },
#     config
# ))
# print(len(result["leads"]), "leads")




















    

    














