from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from ..model.location_request import LocationRequest


class PositionPredictionResponse(BaseModel):
    bus_id: str
    last_known_distance_traveled: Optional[float]
    predicted_location: LocationRequest
    predicted_distance_traveled: float
    target_arrival_time: Optional[datetime]
    target_seconds_to_arrival: int
    average_speed: Optional[float]
    message: str