from typing import TypedDict, Literal
from dataclasses import dataclass
from contact import Contact 


"""
MONTHLY BASIS CAPS

zoom_info_calls ie enriching and getting full contact information 
to be set at a hard limit of 80 

hunteR_io_calls ie getting emails is at a hard limit of 33 

"""


class StateSchema(TypedDict): 
    leads: list[Contact] 
    zoom_info_calls: int 
    hunter_io_calls: int 
    total_tokens_used: int 
       










