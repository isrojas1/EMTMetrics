from typing import List, Dict, Any

from pydantic import BaseModel

from ..model.location_request import LocationRequest


class BusDetailsResponse(BaseModel):
    line: float
    direction: float
    last_position: LocationRequest
    last_distance_traveled: float
    total_route_distance: int
    stops: List[Dict[str, Any]]
