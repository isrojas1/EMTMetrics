from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from src.emtmetrics.model.location_request import LocationRequest


class TimePredictionResponse(BaseModel):
    bus_id: str
    last_known_distance_traveled: Optional[float]
    target_location: LocationRequest
    target_distance_traveled: Optional[float]
    predicted_arrival_time: Optional[datetime]  # ISO format datetime
    predicted_seconds_to_arrival: Optional[float]
    current_speed: Optional[float]  # km/h
    message: str