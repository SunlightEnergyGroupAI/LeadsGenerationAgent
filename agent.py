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
from supabase import create_client, Client  
import time  
from langchain.agents import create_agent 
from langchain_ollama import ChatOllama
from fastmcp.client import Client



load_dotenv()  
tavilyapikey = os.getenv("TAVILYAPI")   
hunterapikey = os.getenv("HUNTERAPI") 
zmClientId = os.getenv("ZOOMINFOCLIENTID") 
zmClientSecret = os.getenv("ZOOMINFOCLIENTSECRET") 

config = {"configurable": {"thread_id": "1"}} 
checkpointer = InMemorySaver() 
agent = init_chat_model(model_provider="ollama", model="qwen3:8b")  
graph = StateGraph(StateSchema) 
qwenSub = ChatOllama(model="qwen3:8b")  


def get_zoominfo_token() -> str:
    r = requests.post(
        "https://api.zoominfo.com/gtm/oauth/v1/token",
        auth=(zmClientId, zmClientSecret),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

async def fetchZoomInfoTools():
    client = Client("https://mcp.zoominfo.com/mcp", auth=get_zoominfo_token())
    async with MCPAdapter(client) as adapter:
        tools = await adapter.list_tools()
    return tools 

async def fetchApolloTools(): 
    tools = await MCPAdapter("https://mcp.apollo.io/mcp").list_tools() 
     
    ...


zm_tools = asyncio.run(fetchZoomInfoTools()) 





#SUBAGENT USES THE ZOOM INFO 



systemPrompt = """




"""  



def initalize_agent(state: StateSchema):
    agentPrompt = SystemMessage(systemPrompt)
    inital_agent_response = agent.invoke([agentPrompt]) 
    return {} 




def searchNamesZoomInfo(): 
    searchNamesPrompt = """
    """
    

    

def checkForDuplicateLead(state: StateSchema):
    """Checks supabase"""  
    ... 












# @tool 
# def getContactInfoHunter(): 
#     ...  


# @tool 
# def getFullContactApollo(): 
#     ...






#TOOL DESCRIPTION NEEDS WORK
# @tool
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
    time.sleep(125) #used to avoid rate limit however can introduce retryPolicy middleware down the line instead 

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

graph.add_node(initalize_agent) 


graph.add_edge(START, "initalize_agent") 
graph.add_edge("initalize_agent", END)
app = graph.compile()  
app.get_graph().draw_png("workflow.png") 












