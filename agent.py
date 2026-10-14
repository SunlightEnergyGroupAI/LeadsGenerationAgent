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


tavilyapikey = os.getenv("TAVILYAPI")   
hunterapikey = os.getenv("HUNTERAPI") 
config = {"configurable": {"thread_id": "1"}} 
checkpointer = InMemorySaver() 
agent = init_chat_model(model_provider="ollama", model="qwen3:8b")   
graph = StateGraph(StateSchema)  


#TODO: add the doc strings to the tools inside of the func so llm can obtain context 

@tool  
def getFullZoomInfoContact(): 
    ... 

@tool 
def getPersonZoomInfo(): 
    ...  

@tool 
def getContactInfoHunter(): 
    ... 







@tool
def getInfo(searchQuery: str, searchDepth: Literal["basic", "fast", "ultra-fast", "advanced"], maxResults: int):  
    bingEngine = DDGS(backend="bing") 
    yahooEngine = DDGS(backend="yahoo") 
    ddgEngine = DDGS(backend="duckduckgo") 

    bingEngine.text(searchQuery, max_results=maxResults) 
    yahooEngine.text(searchQuery, max_results=maxResults) 
    ddgEngine.text(searchQuery, max_results=maxResults)
    
    






load_dotenv()  




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














