from langgraph.graph import StateGraph, START, END
from langchain.chat_models import init_chat_model
from tavily import TavilyClient  
from langchain.tools import tool 
from ddgs import DDGS  
from dotenv import load_dotenv 
import os


load_dotenv() 


tavilyapikey = os.getenv("TAVILYAPI")  









agent = init_chat_model(model_provider="ollama", model="qwen3:8b")  



