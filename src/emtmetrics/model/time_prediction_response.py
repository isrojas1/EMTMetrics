from typing import Optional

from pydantic import BaseModel

from src.emtmetrics.model.location_request import LocationRequest


class TimePredictionResponse(BaseModel):
    bus_id: str
    predicted_location: LocationRequest
    predicted_arrival_time: Optional[str]  # ISO format datetime
    seconds_to_arrival: Optional[float]
    current_speed: Optional[float]  # km/h
    message: str