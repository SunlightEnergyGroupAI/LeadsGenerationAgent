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
from langchain.messages import SystemMessage, HumanMessage
from supabase import create_client 
import time  
from langchain.agents import create_agent 
from langchain_ollama import ChatOllama
from fastmcp.client import Client 
from contact import Contact   
from fastmcp.client.transports import StreamableHttpTransport  
from langchain_core.tools import Tool 
from urllib.parse import urlparse, urljoin
import random
from itertools import zip_longest
from langchain.agents import create_agent 
from filter import FilterResponse, PersonResponse
from filteringModel import isGoodLead
import re

load_dotenv()  
tavilyapikey = os.getenv("TAVILYAPI")   
hunterapikey = os.getenv("HUNTERAPI") 
zmClientId = os.getenv("ZOOMINFOCLIENTID") 
zmClientSecret = os.getenv("ZOOMINFOCLIENTSECRET") 
hubspotKey =  os.getenv("HUBSPOT_TOKEN") 
geoapikey = os.getenv("GEOPLACES_KEY") 
locationiqkey = os.getenv("LOCATIONIQ_KEY")

ICP_STATES = {"New York", "New Jersey", "Maryland", "Pennsylvania"}
ICP_STATE_CODES = {"NY": "New York", "NJ": "New Jersey", "MD": "Maryland", "PA": "Pennsylvania"}


def toFullStateName(state: str | None) -> str | None:
    # so the csv always says "New York" and never "NY"
    state = (state or "").strip()
    return ICP_STATE_CODES.get(state.upper(), state.title()) or None

def getLocationForCompany(company_name: str, state: str | None = None) -> dict | None:
    try:
        q = f"{company_name}, {state}" if state else company_name
        response = requests.get(
            "https://us1.locationiq.com/v1/search",
            params={"q": q, "format": "json", "addressdetails": 1, "countrycodes": "us",
                    "limit": 1, "key": locationiqkey},
            timeout=15,
        )
        time.sleep(0.6)
        address = response.json()[0]["address"]
        return {
            "city": address.get("city") or address.get("town") or address.get("village"),
            "state": address.get("state"),
        }
    except Exception:
        return None





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
                "country": "United States",
                "industryCodes": "Manufacturing,Hospitality,Retail"
            }
        }
    }
    searchResponse = requests.post(url, json=payload, headers=headers, timeout=30)
    searchResponse.raise_for_status()

    print(searchResponse.json())   

# getLocationForCompany("Commonplace Coffee")
# namesZoomInfo()


config = {"configurable": {"thread_id": "1"}, "recursion_limit": 10000}
checkpointer = InMemorySaver() 
agent = init_chat_model(model_provider="ollama", model="qwen3.5:4b")
# same settings as the verdict model so it can't get stuck thinking
personLLM = init_chat_model(
    model_provider="ollama",
    model="qwen3.5:4b",
    reasoning=False,
    num_predict=512,
    client_kwargs={"timeout": 120},
).with_structured_output(PersonResponse)
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
    name = (contact.company or "").strip()
    if not name:
        return False
    companies = hubspotSearch("companies", "name", name)
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
        # contact's work address first, then the company HQ, then what we already had (crawl address, searched state)
        contact.company_address = attrs.get("street") or attrs.get("company", {}).get("street") or contact.company_address
        contact.state = attrs.get("state") or attrs.get("company", {}).get("state") or contact.state
        contact.industry = (attrs.get("company", {}).get("primaryIndustry") or [None])[0]    
        contact.data_source = "Zoom Info"
    return contact


async def zoomInfoFindAndEnrich(state: StateSchema) -> dict:
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
                "revenueMax": "50000",  # in thousands, so $50M
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
            # search doesn't return a state, but we searched in chosenState
            state=chosenState,
            company_address=attrs.get("street"),
            id=record["id"]
        )
        # zoominfo has nothing to enrich for these, they'd get dropped from the csv anyway
        hasContactInfo = attrs.get("hasEmail") or attrs.get("hasDirectPhone") or attrs.get("hasMobilePhone")
        if not hasContactInfo:
            continue
        elif currLead.company in seen_companies:
            continue
        elif isInHubspot(currLead):
            continue
        seen_companies.add(currLead.company)
        # filter before enriching so we don't spend ZoomInfo credits on bad leads
        if not await isGoodLead(currLead):
            continue
        addedContact = enrichContactZoomInfo(currLead, token)
        addedContact.data_source = "Zoom Info"  

    

        uniqueContacts.append(addedContact)
    writeNewZoomInfoPageNumber(page) 
    return {
        "leads": uniqueContacts,
        "zm_access_token": token,
        # only count leads that passed the filter, so we keep searching until we get good ones
        "zoom_info_calls": state["zoom_info_calls"] + len(uniqueContacts)
    }


async def zoomInfoFindNames(state: StateSchema) -> dict:
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
                "revenueMax": "50000",  # in thousands, so $50M
                "state": chosenState,
                "country": "United States",
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
            # search doesn't return a state, but we searched in chosenState
            state=chosenState,
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
        # filter before Hunter so we don't spend Hunter credits on bad leads
        if not await isGoodLead(currLead):
            continue
        nameOnlyContacts.append(currLead)
    writeNewZoomInfoPageNumber(page) 
    return {
        "leads_to_get_email": nameOnlyContacts,
        "zm_access_token": token
    }

def hunterFindEmails(state: StateSchema) -> dict:  
    contacts: list[Contact] = []  
    url = "https://api.hunter.io/v2/email-finder"
    for contact in state.get("leads_to_get_email"):
        first_name = contact.lead_first_name 
        last_name = contact.lead_last_name 
        company_name = contact.company  
        role = contact.company_role
        response = requests.get(url, params={"first_name": first_name, "last_name": last_name, "company": company_name, "api_key": hunterapikey}, timeout=30) 
        data = response.json() 
        lead_email = data.get("data",{}).get("email")
        if not lead_email:
            print("Hunter found no email for", first_name, last_name, company_name)
            continue
        # look up the city in the state we searched, not wherever the HQ is
        companyInfo = getLocationForCompany(company_name, contact.state) or {}
        contacts.append(Contact(company_name, first_name, last_name, role, email=lead_email, state=contact.state, data_source="Zoom info & Hunter.io", city=companyInfo.get("city"), fit_score_reasoning=contact.fit_score_reasoning, company_address=contact.company_address))
    return {
        "leads": contacts,
        # only count leads Hunter found an email for, so we keep going until we have 5 reachable ones
        "zoom_info_name_counter": state["zoom_info_name_counter"] + len(contacts)
    }
    


def fetchNewApolloPageNumber() -> int:  
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("apollopages").select("*", count="exact").execute() 
    return response.count + 1  


def writeNewApolloPageNumber(pageNumber: int) -> None:
    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASEAPIKEY") )
    response = supabase.table("apollopages").insert({"page": pageNumber}).execute()
    return 



def enrichApolloContact(apollo_id: str) -> Contact | None:
    return matchApolloPerson({"id": apollo_id})


def enrichApolloByName(firstName: str, lastName: str, domain: str) -> Contact | None:
    return matchApolloPerson({"first_name": firstName, "last_name": lastName, "domain": domain})


def matchApolloPerson(matchInput: dict) -> Contact | None:
    # costs 1 apollo credit
    response = requests.post(
        "https://api.apollo.io/api/v1/people/match",
        headers={
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "accept": "application/json",
            "x-api-key": os.getenv("APOLLOAPIKEY"),
        },
        json=matchInput,
    )
    response.raise_for_status() 
    print(response.json())

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
        company_address=organization.get("street_address"),
        id=person.get("id"),
        city=person.get("city")
    )

async def apolloFindAndEnrich(state: StateSchema) -> dict:
    print("USING APOLLO") 
    contacts: list[Contact] = [] 
    url = "https://api.apollo.io/api/v1/mixed_people/api_search?person_seniorities[]=c_suite&person_seniorities[]=director&person_seniorities[]=vp&person_seniorities[]=founder&organization_locations[]=New%20Jersey%2C%20US&organization_locations[]=New%20York%2C%20US&organization_locations[]=Maryland%2C%20US&organization_locations[]=Pennsylvania%2C%20US&person_locations[]=New%20Jersey%2C%20US&person_locations[]=New%20York%2C%20US&person_locations[]=Maryland%2C%20US&person_locations[]=Pennsylvania%2C%20US&q_organization_keyword_tags[]=manufacturing&q_organization_keyword_tags[]=machinery&q_organization_keyword_tags[]=plastics&q_organization_keyword_tags[]=pharmaceuticals&q_organization_keyword_tags[]=building%20materials&q_organization_keyword_tags[]=food%20production&q_organization_keyword_tags[]=hospitality&q_organization_keyword_tags[]=hotels&q_organization_keyword_tags[]=supermarkets&q_organization_keyword_tags[]=resorts&contact_email_status[]=verified&contact_email_status[]=likely%20to%20engage&contact_email_status[]=unverified&organization_num_employees_ranges[]=10%2C1000&revenue_range[max]=50000000&page=1&per_page=1"
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

    print(response.json())

    people = dataDict.get("people", [])
    if not people:
        print(f"No people on page {pageNumber}. Reached the end of the results.")
        return {"leads": [], "apollo_calls": state["apollo_calls"] + 1}

    seen_companies = {(c.company or "").lower() for c in state.get("leads", [])}
    for person in people:
        organization = person.get("organization") or {}
        curr_lead = Contact(
            organization.get("name"),
            person.get("first_name"),
            company_role=person.get("title"),
            data_source="Apollo",
            id=person["id"],
        )
        if (curr_lead.company or "").lower() in seen_companies:
            print("skip, already have this company:", curr_lead.company)
            continue
        if isInHubspot(curr_lead):
            print("skip, already in HubSpot:", curr_lead.company)
        # filter before enriching so we don't spend Apollo credits on bad leads
        elif await isGoodLead(curr_lead):
            enriched = enrichApolloContact(curr_lead.id)
            if enriched is None:
                continue
            enriched.fit_score_reasoning = curr_lead.fit_score_reasoning
            # apollo's address first, the crawl address from the filter is the backup
            enriched.company_address = enriched.company_address or curr_lead.company_address
            contacts.append(enriched)

    if contacts: 
        writeNewApolloPageNumber(pageNumber)
        return {"leads": contacts, "apollo_calls": state["apollo_calls"]+1, "new_apollo_leads": state["new_apollo_leads"]+1}
    writeNewApolloPageNumber(pageNumber) 
    return {"leads": contacts, "apollo_calls": state["apollo_calls"]+1} 









def leadsToCSV(state: StateSchema): 
    #change to filtered_leads later
    # skip leads with no way to reach them (no phone and no email)
    leads = [
        contact for contact in state["leads"]
        if (contact.phone or "").strip() or (contact.email or "").strip()
    ]
    print(f"Dropped {len(state['leads']) - len(leads)} leads with no phone or email")

    for contact in leads:
        if not contact.city or not contact.state:
            loc = getLocationForCompany(contact.company, contact.state) or {}
            contact.city = contact.city or loc.get("city")
            contact.state = contact.state or loc.get("state")
        contact.state = toFullStateName(contact.state)

    # zoominfo sometimes returns leads outside our states, blank states get dropped too
    inStateLeads = [contact for contact in leads if contact.state in ICP_STATES]
    print(f"Dropped {len(leads) - len(inStateLeads)} leads outside NY/NJ/MD/PA")
    leads = inStateLeads

    print(f"{len(leads)} leads for the CSV")


    dataframe = pd.DataFrame({
        "company": [contact.company for contact in leads],
        "lead_first_name": [contact.lead_first_name for contact in leads],
        "lead_last_name": [contact.lead_last_name for contact in leads],
        "company_role": [contact.company_role for contact in leads],
        "phone": [contact.phone for contact in leads],
        "email": [contact.email for contact in leads],
        "company_address": [contact.company_address for contact in leads],
        "city": [contact.city for contact in leads],
        "state": [contact.state for contact in leads],
        "data_source": [contact.data_source for contact in leads],
        "fit_score_reasoning": [contact.fit_score_reasoning for contact in leads]
    })
    dataframe.to_csv("newleads.csv", index=False)  
    return {} 






async def SendCSV_ToEmail(state: StateSchema) -> dict:
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
    # count the rows in the csv, not state["leads"] which still has the dropped leads
    leadCount = len(pd.read_csv("newleads.csv"))

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

async def crawlCompanySite(crawler: AsyncWebCrawler, homeUrl: str) -> str:
    try:
        home = await asyncio.wait_for(crawler.arun(url=homeUrl), timeout=20)
    except Exception as e:
        print("skip", homeUrl, e)
        return ""

    pages = []
    for link in (home.links or {}).get("internal", []):
        # links like "/about-us" are relative, the crawler needs the full url
        href = urljoin(homeUrl, link.get("href", ""))
        if any(w in href.lower() for w in CONTACT_WORDS) and href not in pages:
            pages.append(href)

    # about/contact pages first since they say what the business does and where it is
    text = ""
    for page in pages[:4]:
        try:
            res = await asyncio.wait_for(crawler.arun(url=page), timeout=20)
            text += str(res.markdown or "") + "\n"
        except Exception as e:
            print("  skip page", page, e)
    text += str(home.markdown or "")
    return text


def findDecisionMakerApollo(domain: str) -> dict | None:
    # apollo search is free, credits only get spent later in enrichApolloContact
    url = f"https://api.apollo.io/api/v1/mixed_people/api_search?q_organization_domains_list[]={domain}&person_seniorities[]=owner&person_seniorities[]=founder&person_seniorities[]=c_suite&person_seniorities[]=vp&person_seniorities[]=director&per_page=10"
    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "accept": "application/json",
        "x-api-key": os.getenv("APOLLOAPIKEY"),
    }
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    people = response.json().get("people", [])

    # pick the person closest to the top, in the order of DECISION_MAKER_TITLES
    for titleWord in DECISION_MAKER_TITLES:
        for person in people:
            if titleWord in (person.get("title") or "").lower():
                return person
    return people[0] if people else None


async def searchWeb(state: StateSchema) -> dict:
    """
    Step 1: search Bing, Yandex and Tavily for ICP businesses and keep the first 10 new company websites.
    Step 2: look up each company's top decision-maker in Apollo by its website (free search).
    Step 3: crawl the site and run the lead through the filter with what we crawled.
    Step 4: only if it passes, enrich in Apollo and keep it if we got a direct email or phone.

    Returns:
    - dict: {"leads": contact, "web_leads": count, "searched_domains": domains}
    """
    contact: list[Contact] = []
    searchedDomains: list[str] = []

    tavilyEngine = TavilyClient(api_key=tavilyapikey)
    searchEngine = DDGS()
    # plain text output since the prompt asks for just the query on one line
    queryMakerLLM = init_chat_model(model_provider="ollama", model="nemotron-3-nano:30b-cloud")

    sq = queryMakerLLM.invoke([makeIcpPrompt(random.choice(WEB_TRADES), random.choice(list(ICP_STATES)))]).content.strip()
    sq2 = queryMakerLLM.invoke([makeIcpPrompt(random.choice(WEB_TRADES), random.choice(list(ICP_STATES)))]).content.strip()
    print("BING QUERY:", sq)
    print("YANDEX QUERY:", sq2)
    bingUrls, yandexUrls, tavilyUrls = [], [], []
    try:
        bingUrls = [r["href"] for r in searchEngine.text(sq, max_results=15, backend="bing")]
    except Exception as e:
        print("bing failed:", e)
    try:
        yandexUrls = [r["href"] for r in searchEngine.text(sq2, max_results=15, backend="yandex")]
    except Exception as e:
        print("yandex failed:", e)
    try:
        tavilyUrls = [r["url"] for r in tavilyEngine.search(sq, max_results=10)["results"]]
    except Exception as e:
        print("tavily failed:", e)
    # take turns between sources so both queries get sites in the top 10
    urls = [u for group in zip_longest(bingUrls, yandexUrls, tavilyUrls) for u in group if u]

    alreadySearched = set(state.get("searched_domains", []))
    companies = {}
    for u in urls:
        domain = urlparse(u).netloc.lower().removeprefix("www.")
        if not domain or any(b in domain for b in BLOCKED) or domain in companies or domain in alreadySearched:
            continue
        companies[domain] = u
        if len(companies) == 10:
            break
    print(f"{len(companies)} companies found:", list(companies))

    seen_companies = {(c.company or "").lower() for c in state.get("leads", [])}
    async with AsyncWebCrawler() as crawler:
        for domain, home_url in companies.items():
            searchedDomains.append(domain)
            if hubspotSearch("companies", "domain", domain):
                print("skip, already in HubSpot:", domain)
                continue

            try:
                person = findDecisionMakerApollo(domain)
            except Exception as e:
                print("skip, Apollo search failed:", domain, e)
                continue
            if not person:
                print("skip, no decision-maker in Apollo:", domain)
                continue

            organization = person.get("organization") or {}
            currLead = Contact(
                company=organization.get("name") or domain,
                lead_first_name=person.get("first_name"),
                company_role=person.get("title"),
                data_source="Web Search & Apollo",
                id=person["id"],
            )
            if currLead.company.lower() in seen_companies:
                print("skip, already have this company:", currLead.company)
                continue

            print("crawling:", domain)
            text = await crawlCompanySite(crawler, home_url)
            if not await isGoodLead(currLead, research=text):
                continue

            # only spend an Apollo enrichment once the lead passed the filter
            enriched = enrichApolloContact(currLead.id)
            if enriched is None or not (enriched.email or enriched.phone):
                print("skip, Apollo has no email or phone:", domain)
                continue
            enriched.data_source = "Web Search & Apollo"
            enriched.fit_score_reasoning = currLead.fit_score_reasoning
            enriched.company_address = enriched.company_address or currLead.company_address
            seen_companies.add(currLead.company.lower())
            contact.append(enriched)

    return {
        "leads": contact,
        "web_leads": state["web_leads"] + len(contact),
        "web_search_rounds": state["web_search_rounds"] + 1,
        "searched_domains": searchedDomains,
    }


WEB_TRADES = [
    "coffee roaster", "craft brewery", "distillery", "winery", "commercial bakery", "food processing plant",
    "machine shop", "sheet metal fabrication", "plastics molding", "commercial printing", "woodworking shop",
    "boutique hotel", "family owned inn", "dry cleaner", "cold storage warehouse",
]


def makeIcpPrompt(trade: str, stateName: str) -> str:
    return f"""
Write one web search query that finds the websites of individual businesses.

Target: a {trade} in {stateName}.

Pick ONE {stateName} city or town and write a query for a {trade} there.

Rules:
- Plain keywords only
- No quotes, no parentheses, no OR or AND, no site: filters
- Do not use the words best, top, list, companies, small, or directory
- Under 10 words
- Do not mention job titles, emails, or LinkedIn
- Return only the query, with no explanation

Example of the style: sheet metal fabrication Pittsburgh Pennsylvania
"""


DECISION_MAKER_TITLES = ("owner", "founder", "president", "ceo", "chief executive", "general manager",
                         "coo", "chief operating", "cfo", "chief financial", "operations", "facilities", "plant")

BLOCKED = ("yelp.", "linkedin.", "facebook.", "indeed.", "glassdoor.", "yellowpages.",
           "tripadvisor.", "zoominfo.", "mapquest.", "bbb.org", "wikipedia.", "reddit.",
           "instagram.", "youtube.", "manta.", "dnb.com", "expedia.", "booking.com",
           "allbiz.", "bizapedia.", "opencorporates.", "buzzfile.", "chamberofcommerce.",
           "crunchbase.", "bloomberg.", "rocketreach.", "apollo.io",
           "yahoo.", "bing.", "google.", "msn.", "duckduckgo.", "aol.",
           "tiktok.", "pinterest.", "x.com", "twitter.", "momondo.", "kayak.", "hotels.com", "hotelplanner.",
           "trivago.", "priceline.", "orbitz.", "travelocity.", "agoda.", "airbnb.", "vrbo.", "hotelsidekick.",
           "americanhotels.", "wheretostay", "guide", "visit", "tourism", "untappd.", "beeradvocate.",
           "thumbtack.", "angi.", "houzz.", "nextdoor.", "groupon.", "doordash.", "ubereats.", "grubhub.",
           "amazon.", "ebay.", "etsy.", "alibaba.", "thomasnet.", ".gov", ".edu", "news", "patch.com",
           "propertyshark.", "loopnet.", "streeteasy.", "zillow.", "apartments.com", "trulia.", "realtor.com",
           "cityrealty.", "renthop.", "compass.com", "crexi.", "commercialcafe.", "hotpads.", "rentals.com")
CONTACT_WORDS = ("contact", "about", "team", "leadership", "staff", "people", "management")

NY_PUBLIC_DATA = "NYCPublicDataset.csv"
MIN_ELECTRICITY_KWH = 500000
BUILDINGS_PER_ROUND = 10
# the public data node stops after an hour even without 2 leads
MAX_PUBLIC_DATA_SECONDS = 3600
BOROUGH_CITIES = {"MANHATTAN": "New York", "BROOKLYN": "Brooklyn", "QUEENS": "Queens",
                  "BRONX": "Bronx", "STATEN IS": "Staten Island"}
# shared inboxes, we only want the decision-maker's own email
GENERIC_EMAIL_WORDS = ("info", "contact", "hello", "office", "admin", "sales", "support", "team", "general",
                       "inquiries", "reservations", "frontdesk", "booking", "events", "marketing", "careers", "hr")

person_prompt = """
You read a company's website and find the person Sunlight Energy Group should contact about energy costs.

Find the most senior decision-maker named on the page, in this order:
owner, founder, president, CEO, general manager, COO, CFO, head of operations, facilities, or plant.

Return their first name, last name, and title.
Return their own email and direct phone only if they are written on the page next to this person.
Copy everything exactly as written. Never guess or make anything up.
If a field is not on the page, return an empty string for it.
Do not return shared emails like info@ or the company's main phone line.
"""


def fetchUsedBuildings() -> set[str]:
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASEAPIKEY"))
    response = supabase.table("nypublicbuildings").select("building_name").execute()
    return {row["building_name"] for row in response.data}


def writeUsedBuilding(buildingName: str) -> None:
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASEAPIKEY"))
    supabase.table("nypublicbuildings").insert({"building_name": buildingName}).execute()


def loadBigBuildings(usedBuildings: set[str]) -> pd.DataFrame:
    df = pd.read_csv(NY_PUBLIC_DATA, low_memory=False)
    # electricity is stored as text like "70,601,035"
    df["kwh"] = pd.to_numeric(df["Grid electricity (kWh)"].astype(str).str.replace(",", ""), errors="coerce")
    df = df[df["kwh"] >= MIN_ELECTRICITY_KWH]
    df = df[~df["Property Name"].isin(usedBuildings)]
    # shuffle so each round looks at a mix of building types
    return df.sample(frac=1)


def cleanBuildingName(buildingName: str) -> str | None:
    # "Moishas Supermarket - 305 Avenue M" -> "Moishas Supermarket", "BCS: 48-43 32nd Pl." -> "BCS"
    name = re.split(r":| - |\(", buildingName)[0].strip()
    # names that are just an address have no business name in them
    if not name or name[0].isdigit():
        return None
    return name


def findBuildingWebsite(businessName: str | None, address: str, city: str | None) -> str | None:
    query = f"{businessName or ''} {address} {city or ''} NY".strip()
    try:
        results = DDGS().text(query, max_results=8, backend="bing")
    except Exception as e:
        print("search failed:", query, e)
        return None
    # when we have a business name it has to show up in the result's title or domain, so we don't crawl tiktok or walmart
    nameWords = [word for word in re.findall(r"[a-z]+", (businessName or "").lower()) if len(word) > 2]
    for result in results:
        domain = urlparse(result.get("href", "")).netloc.lower()
        title = result.get("title", "").lower()
        if not domain or any(b in domain for b in BLOCKED):
            continue
        if not nameWords or any(word in domain or word in title for word in nameWords):
            return result["href"]
    return None


def isOnPage(value: str, text: str) -> bool:
    return bool(value) and value.lower() in text.lower()


def isPhoneOnPage(phone: str, text: str) -> bool:
    # compare digits only since the model can change the formatting
    digits = re.sub(r"\D", "", phone)[-10:]
    return len(digits) == 10 and digits in re.sub(r"\D", "", text)


def isGenericEmail(email: str) -> bool:
    localPart = email.split("@")[0].lower()
    return any(word in localPart for word in GENERIC_EMAIL_WORDS)


def findPersonOnPage(text: str) -> PersonResponse | None:
    try:
        return personLLM.invoke([SystemMessage(person_prompt), HumanMessage(text[:8000])])
    except Exception as e:
        print("  AI person search failed:", e)
        return None


async def findBuildingLead(crawler: AsyncWebCrawler, building: pd.Series, seenCompanies: set[str]) -> Contact | None:
    buildingName = building["Property Name"]
    # mark it right away so we never try the same building twice
    writeUsedBuilding(buildingName)
    businessName = cleanBuildingName(buildingName)

    currLead = Contact(
        company=businessName or buildingName,
        state="New York",
        city=BOROUGH_CITIES.get(building["Borough"]),
        company_address=building["Address"],
        industry=building["Largest use"],
        data_source="NY Public Data",
    )
    if currLead.company.lower() in seenCompanies or isInHubspot(currLead):
        print("skip, already have this company:", currLead.company)
        return None

    website = findBuildingWebsite(businessName, building["Address"], currLead.city)
    if not website:
        print("skip, no website found:", buildingName)
        return None
    print("crawling:", website)
    text = await crawlCompanySite(crawler, website)
    domain = urlparse(website).netloc.lower().removeprefix("www.")

    # the website is free so we try it first, apollo is the backup when no one is named on it
    person = findPersonOnPage(text)
    if person and person["first_name"] and person["last_name"]:
        return await leadFromWebsite(currLead, person, text, domain)
    return await leadFromApollo(currLead, text, domain)


async def leadFromWebsite(currLead: Contact, person: PersonResponse, text: str, domain: str) -> Contact | None:
    currLead.lead_first_name = person["first_name"]
    currLead.lead_last_name = person["last_name"]
    currLead.company_role = person["title"]
    # only keep contact info that's really on the page and belongs to the person
    if isOnPage(person["email"], text) and not isGenericEmail(person["email"]):
        currLead.email = person["email"]
    if isPhoneOnPage(person["phone"], text):
        currLead.phone = person["phone"]

    if not await isGoodLead(currLead, research=text):
        return None
    if currLead.email or currLead.phone:
        return currLead

    # we only have their name, so apollo looks them up by name for 1 credit
    enriched = enrichApolloByName(currLead.lead_first_name, currLead.lead_last_name, domain)
    if enriched is None or not (enriched.email or enriched.phone):
        print("skip, no email or phone found:", currLead.company)
        return None
    return keepBuildingInfo(enriched, currLead)


def keepBuildingInfo(enriched: Contact, currLead: Contact) -> Contact:
    # keep the building's location since that's where the energy is used
    enriched.company = enriched.company or currLead.company
    enriched.company_address = currLead.company_address
    enriched.city = currLead.city
    enriched.state = currLead.state
    enriched.industry = currLead.industry
    enriched.data_source = "NY Public Data"
    enriched.fit_score_reasoning = currLead.fit_score_reasoning
    return enriched


async def leadFromApollo(currLead: Contact, text: str, domain: str) -> Contact | None:
    try:
        person = findDecisionMakerApollo(domain)
    except Exception as e:
        print("skip, Apollo search failed:", domain, e)
        return None
    # has_email comes free with the search, so we never spend a credit on someone with no email
    if not person or not person.get("has_email"):
        print("skip, no decision-maker with an email in Apollo:", domain)
        return None

    currLead.lead_first_name = person.get("first_name")
    currLead.company_role = person.get("title")
    currLead.id = person["id"]
    if not await isGoodLead(currLead, research=text):
        return None

    # only spend an Apollo enrichment once the lead passed the filter
    enriched = enrichApolloContact(currLead.id)
    if enriched is None or not (enriched.email or enriched.phone):
        print("skip, Apollo has no email or phone:", domain)
        return None
    return keepBuildingInfo(enriched, currLead)


async def UsePublicData(state: StateSchema) -> dict:
    """
    Step 1: take buildings from the NYC public energy data that use at least 500,000 kWh and haven't been tried.
    Step 2: find the business's website from the property name and address, and crawl it.
    Step 3: if the website names a decision-maker, the lead filter checks them with the crawled text.
            If their email or phone isn't on the page, Apollo looks them up by name.
    Step 4: if no one is named, find the decision-maker in Apollo by the website (free),
            run the lead filter, then enrich in Apollo.

    Returns:
    - dict: {"leads": leads, "ny_public_leads": count, "ny_public_rounds": rounds}
    """
    leads: list[Contact] = []
    buildings = loadBigBuildings(fetchUsedBuildings()).head(BUILDINGS_PER_ROUND)
    seenCompanies = {(c.company or "").lower() for c in state["leads"]}
    # the first round starts the clock
    startedAt = state["ny_public_started_at"] or time.time()

    async with AsyncWebCrawler() as crawler:
        for _, building in buildings.iterrows():
            # check before every building so we stop close to the hour, not at the end of a round
            if time.time() - startedAt >= MAX_PUBLIC_DATA_SECONDS:
                print("public data hit the 1 hour limit")
                break
            currLead = await findBuildingLead(crawler, building, seenCompanies)
            if currLead:
                seenCompanies.add(currLead.company.lower())
                leads.append(currLead)

    return {
        "leads": leads,
        "ny_public_leads": state["ny_public_leads"] + len(leads),
        "ny_public_rounds": state["ny_public_rounds"] + 1,
        "ny_public_started_at": startedAt,
    }


#Routing Nodes (Conditional Logic) 

def useZoomInfoAgain(state: StateSchema) -> str: 
    if state["zoom_info_calls"] < 4:
        return "use_again"
    else: 
        return "next_tool"

def useHunterAndZoomAgain(state: StateSchema) -> str: 
    if state["zoom_info_name_counter"] < 5: 
        return "use_again" 
    else: 
        return "next_tool" 

def useApolloAgain(state: StateSchema) -> str: 
    if state["new_apollo_leads"] < 10: 
        return "use_again" 
    else: 
        return "filter"

def useSearchWebAgain(state: StateSchema) -> str:
    # stop after 5 rounds even without 5 leads so it can't loop for hours
    if state["web_leads"] < 5 and state["web_search_rounds"] < 5:
        return "use_again"
    else:
        return "next_tool"

def usePublicDataAgain(state: StateSchema) -> str:
    # stop at 2 leads, or after an hour or 50 rounds (500 buildings) so it can't loop forever
    timeUsed = time.time() - state["ny_public_started_at"]
    if state["ny_public_leads"] < 2 and state["ny_public_rounds"] < 50 and timeUsed < MAX_PUBLIC_DATA_SECONDS:
        return "use_again"
    else:
        return "next_tool"






graph.add_node(zoomInfoFindAndEnrich) 
graph.add_node(zoomInfoFindNames) 
graph.add_node(hunterFindEmails)  
graph.add_node(apolloFindAndEnrich)
graph.add_node(leadsToCSV) 
graph.add_node(SendCSV_ToEmail)  
graph.add_node(searchWeb)
graph.add_node(UsePublicData)

graph.add_edge(START, "zoomInfoFindAndEnrich") 
graph.add_conditional_edges(
    "zoomInfoFindAndEnrich", 
    useZoomInfoAgain, 
    {"use_again": "zoomInfoFindAndEnrich", "next_tool": "zoomInfoFindNames"} 
) 
graph.add_edge("zoomInfoFindNames", "hunterFindEmails") 
graph.add_conditional_edges(
    "hunterFindEmails", 
    useHunterAndZoomAgain, 
    {"use_again": "zoomInfoFindNames", "next_tool": "apolloFindAndEnrich"}
) 
graph.add_conditional_edges("apolloFindAndEnrich", useApolloAgain, {"use_again": "apolloFindAndEnrich", "filter": "searchWeb"})
graph.add_conditional_edges(
    "searchWeb",
    useSearchWebAgain,
    {"use_again": "searchWeb", "next_tool": "UsePublicData"}
)
graph.add_conditional_edges(
    "UsePublicData",
    usePublicDataAgain,
    {"use_again": "UsePublicData", "next_tool": "leadsToCSV"}
)
graph.add_edge("leadsToCSV", "SendCSV_ToEmail")
graph.add_edge("SendCSV_ToEmail", END)
app = graph.compile(checkpointer=checkpointer) 
app.get_graph().draw_png("workflow.png")


if __name__ == "__main__":
    result = asyncio.run(app.ainvoke(
        {
            "leads": [],
            "leads_to_get_email": [],
            "zoom_info_calls": 0,
            "zoom_info_name_counter": 0,
            "apollo_calls": 0,
            "new_apollo_leads": 0,
            "web_leads": 0,
            "web_search_rounds": 0,
            "searched_domains": [],
            "ny_public_leads": 0,
            "ny_public_rounds": 0,
            "ny_public_started_at": 0.0,
            "names_page": 0,
        },
        config,

    ))
    print(len(result["leads"]), "leads")




















    

    














