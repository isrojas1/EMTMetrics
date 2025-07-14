from pydantic import BaseModel

from ..model.location_request import LocationRequest


class TimePredictionByCoordinatesRequest(BaseModel):
    bus_id: str
    target_location: LocationRequest

class TimePredictionByDistanceTraveledRequest(BaseModel):
    bus_id: str
    target_location: int

class TimePredictionByStopRequest(BaseModel):
    bus_id: str
    stop_order: int