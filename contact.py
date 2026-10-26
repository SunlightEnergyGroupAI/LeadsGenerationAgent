from dataclasses import dataclass  
from typing import Literal

@dataclass 
class Contact: 
    company: str 
    lead_first_name: str  
    lead_last_name: str
    company_role: str | None = None
    data_source: Literal["Zoom Info", "Zoom info & Hunter.io", "Apollo"] | None
    fit_score: int | None = None 
    phone: str | None = None
    email: str | None = None 
    fit_score_reasoning: str | None = None
    state: str | None
    industry: str | None 
    id: str | None = None






