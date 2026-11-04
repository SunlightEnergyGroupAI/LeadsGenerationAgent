from typing import Literal, TypedDict
from ddgs import DDGS  
import crawl4ai


class FilterResponse(TypedDict):
    reasoning: str
    icp_verdict: bool


class PersonResponse(TypedDict):
    first_name: str
    last_name: str
    title: str
    email: str
    phone: str



def searchLeadInformation(companyName: str): 
    engine = DDGS() 
    results = engine.text(companyName, max_results=5, backend="bing") 
    top_link = results[0].get("href") 
    






