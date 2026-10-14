from dataclasses import dataclass  
from typing import Literal

@dataclass 
class Contact: 
    company: str 
    lead_name: str 
    company_role: str
    data_source: Literal["Zoom Info", "Zoom info & Hunter.io", "Apollo"] 
    fit_score: int 
    fit_score_reasoning: str 

