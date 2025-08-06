from typing import List

from pydantic import BaseModel


class Point(BaseModel):
    latitude: float
    longitude: float


class ShapeDetailsResponse(BaseModel):
    points: List[Point]
