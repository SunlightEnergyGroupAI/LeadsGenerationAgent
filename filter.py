from dataclasses import dataclass  
from typing import Literal 


@dataclass
class FilterResponse: 
    verdict: bool
