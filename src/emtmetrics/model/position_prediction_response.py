from typing import Optional

from pydantic import BaseModel

from src.emtmetrics.model.location_request import LocationRequest


class PositionPredictionResponse(BaseModel):
    bus_id: str
    predicted_location: LocationRequest
    distance_traveled: float
    # total_route_distance: int
    prediction_time_seconds: int
    current_speed: Optional[float]
    message: str