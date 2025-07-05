from pydantic import BaseModel


class PositionPredictionRequest(BaseModel):
    bus_id: str
    prediction_time_seconds: int  # seconds into the future (max 3600 = 1 hour)