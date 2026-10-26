from langgraph.graph import StateGraph, START, END
from langchain.chat_models import init_chat_model
from tavily import TavilyClient  
from langchain.tools import tool 
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
from bs4 import BeautifulSoup 
from langchain_core.documents import Document 
from source import Source 
import asyncio 
from langchain.messages import SystemMessage  
from supabase import create_client 
from supabase import Client as cli
import time  
from langchain.agents import create_agent 
from langchain_ollama import ChatOllama
from fastmcp.client import Client 
from contact import Contact   
from fastmcp.client.transports import StreamableHttpTransport  
from langchain_core.tools import Tool 
from langchain_core.utils.function_calling import convert_to_openai_tool 
from langchain_core.tools import BaseTool 
from SearchQuery import Query
import re 
from urllib.parse import urlparse 
import random

load_dotenv()  
tavilyapikey = os.getenv("TAVILYAPI")   
hunterapikey = os.getenv("HUNTERAPI") 
zmClientId = os.getenv("ZOOMINFOCLIENTID") 
zmClientSecret = os.getenv("ZOOMINFOCLIENTSECRET") 
hubspotKey =  os.getenv("HUBSPOT_TOKEN")

config = {"configurable": {"thread_id": "1"}} 
checkpointer = InMemorySaver() 
agent = init_chat_model(model_provider="ollama", model="qwen3.5:4b")  
graph = StateGraph(StateSchema) 
qwenSub = ChatOllama(model="qwen3.5:4b") 







def initalize_agent(state: StateSchema) -> dict:
    agentPrompt = SystemMessage("systemPrompt")
    inital_agent_response = agent.invoke([agentPrompt]) 
    return {}  


#make enrich function to pass the names and we do that only if we havent seen the lead already 

def getFullContact(state: StateSchema) -> dict:
    company_types = {1: "Manufacturing", 2: "Church", 3: "Hospitality", 4: "Retail"} 
    states = {1: "New York", 2: "Maryland", 3: "New Jersey", 4: "Pennsylvania"} 

    companyTypeKey = random.randint(1, 4)  
    stateKey = random.randint(1, 4)
    chosenIndustry = company_types.get(companyTypeKey) 
    chosenState = states.get(stateKey) 

    uniqueContacts: list[Contact] = [] 
     

    if not state["zm_access_token"]: 
        response = requests.post(
            url="https://api.zoominfo.com/gtm/oauth/v1/token", 
            headers={"Accept": "application/json"},
            json={"grant_type": "client_credentials", "client_id": zmClientId, "client_secret": zmClientSecret}, 
            timeout=30 
        ) 
        print(response)
        data = response.json() 
        token = data["access_token"] 
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
        response2 = requests.post(url, json=payload, headers=headers, timeout=30)
        print(response2) 
        jsonResponse = response2.json() 
        for record in jsonResponse["data"]:  
            outer_dictionary = record["attributes"] 
            zm_id = outer_dictionary["id"] 
            company_dict = outer_dictionary["company"] 
            company_name = company_dict["name"] 
            firstName = outer_dictionary["firstName"] 
            lastName = outer_dictionary["lastName"] 

            currentLead = Contact(comapny_name, firstName, lastName, role, "Zoom Info", id=zm_id)  
            if isInHubspot(currentLead): 
                continue 
            else:
                data = enrichContactZoomInfo(currentLead) 
                #make the data into a new instance of contact type/object and then append to the uniqueContacts list 
                # at end of func update the state of the agent/workflow and move to should continue  

                uniqueContacts.append(currentLead) 
                enrichContactZoomInfo(currentLead)
        return {"leads": uniqueContacts, "zm_access_token": token}  
    else: 
        url = "https://api.zoominfo.com/gtm/data/v1/contacts/search?page[number]=1&page[size]=1"
        headers = {
            "accept": "application/vnd.api+json",
            "content-type": "application/vnd.api+json",
            "Authorization": state.get("zm_access_token")
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
        response2 = requests.post(url, json=payload, headers=headers, timeout=30)
        print(response2) 
        jsonResponse = response2.json() 
        for record in data["data"]: 
            comapny_name = record["attributes"]["company"]["name"] 
            first_name = record["firstName"] 
            last_name = record["lastName"] 
            role = record["jobTitle"] 

            currentLead = Contact(comapny_name, first_name, last_name, role, "Zoom Info")  
            if isInHubspot(currentLead): 
                continue 
            else: 
                uniqueContacts.append(currentLead)
        return {"leads": uniqueContacts} 



    




def enrichContactZoomInfo(contact_information: Contact): 
    fn = contact_information.lead_first_name 
    ln = contact_information.lead_last_name 
    id = contact_information.id  
    companyName = contact_information.company 

    url = "https://api.zoominfo.com/gtm/data/v1/contacts/enrich" 
    outputFields = {"outputFields": ["email","phone"]} 
    
    

    ... 
    #enriches the info 
        


# then goes to zoom info again ce. 

def useZoomInfoAgain(state: StateSchema) -> dict: 
    if state["zoom_info_calls"] < 3: 
        return "use_again" 
    else: 
        return "next_tool"  


def getNamesFromZoomInfo(state: StateSchema) -> dict:  
    ...



def getContactsApollo(state: StateSchema) -> dict:  
    ...


async def composioOutlook():   
    transport = StreamableHttpTransport(url="https://connect.composio.dev/mcp", headers={"x-consumer-api-key": "ck_IPiV5AV1ejKX_m6QJmY-"}) 
    async with Client(transport) as c: 
        tools = await c.list_tools() 
    return tools


def checkDuplicateLead(state: StateSchema): 
    current_company_lead = state["lead_to_check"].company
    supabase_client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASEAPIKEY"))
    response = supabase_client.table("new_leads").select("*").eq("company_name", current_company_lead).execute()
    if response.data: 
        {"is_current_lead_duplicate": True} 
    else: 
        {"is_current_lead_duplicate": False} 


def isDuplicateLead(contact: Contact) -> bool: 
    first_name = contact.lead_first_name 
    last_name = contact.lead_last_name 
    company_name = contact.company 
    sbc = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASEAPIKEY")) 
    response = sbc.table("new_leads").select("*").eq("company_name", contact.company).execute()
    if response.data: 
        return True 
    return False   

def isInHubspot(contact: Contact) -> bool: 
    token = os.getenv("HUBSPOT_TOKEN") 
    url = ""

    ...


def insertLead(contact: Contact): 
    sbc = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASEAPIKEY")) 
    record = {"lead_name": contact.lead_first_name+contact.lead_last_name, "company_name": contact.company, "lead_role": contact.company_role}
    result = sbc.table("new_leads").insert(record).execute()    


def findemailWithHunter(state: StateSchema): 
    first_name = state["lead_to_get_email"].lead_first_name 
    last_name = state["lead_to_get_email"].lead_last_name 
    company_name = state["lead_to_get_email"].company   
    url = "https://api.hunter.io/v2/email-finder"
    response = requests.get(url, params={"first_name": first_name, "last_name": last_name, "company": company_name, "api_key": hunterapikey}) 
    data = response.json() 
    lead_email = data["data"]["email"] 
    return lead_email  


def makeCSV(state: StateSchema):  
    #get all first names, last names, company, role, etc and make into dataframe and then make it into a csv that always gets overwritten each time 
    #and then make that sent via outlook mcp using composio
    df = pd.DataFrame()

async def searchWeb(state: StateSchema):
    """
    Step 1: search Bing, Yandex and Tavily and keep the first 10 company websites.
    Step 2: crawl each company's site (homepage plus contact/about/team pages)
    and pull emails, phones and exec-title lines.

    Returns:
    - list[dict]: one dict per company with company_domain, website, emails, phones, title_lines
    """
    tavilyEngine = TavilyClient(api_key=tavilyapikey)
    searchEngine = DDGS()
    queryMakerLLM = init_chat_model(model_provider="ollama", model="nemotron-3-nano:30b-cloud")

    sq = queryMakerLLM.invoke([icp_prompt]).content.strip().strip('"')
    sq2 = queryMakerLLM.invoke([icp_prompt2]).content.strip().strip('"')
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



graph.add_node(initalize_agent) 
graph.add_node(getNamesFromZoomInfo)  
graph.add_node(getContactsApollo)  
graph.add_node(getFullContact)
graph.add_node(composioOutlook) 
graph.add_node(findemailWithHunter)  
graph.add_node(searchWeb)


graph.add_edge(START, "initalize_agent") 
graph.add_edge("initalize_agent", "getFullContact") 
graph.add_edge("getFullContact", "getNamesFromZoomInfo") 
graph.add_edge("getNamesFromZoomInfo", "findemailWithHunter") 
graph.add_edge("findemailWithHunter", "getContactsApollo") 
graph.add_edge("getContactsApollo", "searchWeb") 
graph.add_edge("searchWeb", "composioOutlook") 
app = graph.compile(checkpointer)  
app.get_graph().draw_png("workflow.png")













    

   

    

    # tools = await MCPAdapter(client(mcplink, "ck_IPiV5AV1ejKX_m6QJmY-")).list_tools() 
    # return [tool.name for tool in tools] 


# results = asyncio.run(getInfoTest())
    

    # time.sleep(125) #used to avoid rate limit however can introduce retryPolicy middleware down the line instead 
    # tavily_results = tavilyEngine.search(sq, "advanced", max_results=10)
 


# res = asyncio.run(getInfoTest()) 
# print(res)

















    

    









async def getInfo(searchQuery: str, searchDepth: Literal["basic", "fast", "ultra-fast", "advanced"], maxResults: int) -> list[Source]: 
    """
    Gets information from the web/internet and returns the most relevant information
    Multiple search engines are used in order to get information those being the following: Bing, Yandex, Tavily

    For the search query ut

    Args:
    - searchQuery: str (the query that gets placed into the search engine)
    - searchDepth: str (Controls the depth of the search when using TavilyClient)
    - maxResults: the amount of sites we get back in returns from a query

    Returns:
    - list[Source]: one Source per unique url, with the crawled page content (markdown)
      in `information` and the url in `sourceName`
    """ 
    tavilyEngine = TavilyClient(api_key=tavilyapikey) 

    searchEngine = DDGS() 
   
    bing_results = searchEngine.text(searchQuery, max_results=maxResults, backend="bing") 
    yandex_results = searchEngine.text(searchQuery, max_results=maxResults, backend="yandex") 
    # time.sleep(125) #used to avoid rate limit however can introduce retryPolicy middleware down the line instead 

    if searchDepth == "advanced": 
        tavily_results = tavilyEngine.search(searchQuery, search_depth=searchDepth, max_results=5)
    elif searchDepth == "basic":
        tavily_results = tavilyEngine.search(searchQuery, search_depth=searchDepth, max_results=20)
    elif searchDepth == "fast": 
        tavily_results = tavilyEngine.search(searchQuery, search_depth=searchDepth, max_results=20)
    elif searchDepth == "ultra-fast": 
        tavily_results = tavilyEngine.search(searchQuery, search_depth=searchDepth, max_results=5)

    sources = [] 
    seen = set()

    async with AsyncWebCrawler() as crawler:
        for r in bing_results:
            if r["href"] in seen:
                continue 
            else: 
                seen.add(r["href"])
                page = await crawler.arun(url=r["href"])
                information = str(page.markdown) if page.success and page.markdown else r.get("body", "")
                sources.append(Source(information=information, sourceName=r["href"]))

        for r in yandex_results:
            if r["href"] in seen:
                continue
            seen.add(r["href"])
            page = await crawler.arun(url=r["href"])
            information = str(page.markdown) if page.success and page.markdown else r.get("body", "")
            sources.append(Source(information=information, sourceName=r["href"]))

        
        for r in tavily_results.get("results", []):
            if r["url"] in seen:
                continue
            seen.add(r["url"])
            page = await crawler.arun(url=r["url"])
            information = str(page.markdown) if page.success and page.markdown else r.get("content", "")
            sources.append(Source(information=information, sourceName=r["url"]))

    return sources









"""
GRAPH DESIGN 

START -> uses zoom info to get the 83 contacts (try except)
-> uses apollo to find more contacts -> uses zoom info for searching names only 
-> places those 33 names into hunter.io to fetch email and verificaiton 
-> Pandas to format data into the csv 
-> use hubspot MCP to import csv in hubspot leads section 


extra nodes: 
Qualify node which is where crawl4ai comes in to see if they can qualify 
or subagents
"""














