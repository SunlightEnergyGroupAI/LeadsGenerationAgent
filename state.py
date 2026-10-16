from typing import TypedDict, Literal, Annotated
from dataclasses import dataclass
from contact import Contact 
import operator

"""
MONTHLY BASIS CAPS

zoom_info_calls ie enriching and getting full contact information 
to be set at a hard limit of 80 

hunter_io_calls ie getting emails is at a hard limit of 33 

"""


class StateSchema(TypedDict): 
    leads: Annotated[list[Contact], operator.add] 
    zoom_info_calls: int 
    hunter_io_calls: int 
    total_tokens_used: int 
    urls: list[str] 
    lead_to_check: Contact 
    current_lead_duplicate: bool











