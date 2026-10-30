from typing import Literal, TypedDict
from ddgs import DDGS  
import crawl4ai


class FilterResponse(TypedDict):  
    reasoning: str
    icp_verdict: bool 


def searchLeadInformation(companyName: str): 
    engine = DDGS() 
    results = engine.text(companyName, max_results=5, backend="bing") 
    top_link = results[0].get("href") 
    




searchLeadInformation("kiva han coffee")


