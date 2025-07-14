from datetime import datetime
from typing import Any, List, Tuple
from dataclasses import dataclass


@dataclass
class RouteData:
    """Encapsulates route information"""
    bus_shape: Any
    route_coordinates: List[Tuple]
    distance_traveled_list: List[int]

@dataclass
class PositionPair:
    """Represents a pair of GPS positions with metadata"""
    first_position: Tuple[float, float]
    last_position: Tuple[float, float]
    first_index: int
    last_index: int
    first_timestamp: datetime
    last_timestamp: datetime

@dataclass
class CorrectedPositions:
    """Holds corrected GPS positions and their route segments"""
    first_corrected: Tuple[float, float]
    last_corrected: Tuple[float, float]
    first_segment: Tuple
    last_segment: Tuple

@dataclass
class SegmentDistances:
    """Contains distance measurements for route segments"""
    first_segment_point_a: float
    first_segment_point_b: float
    last_segment_point_a: float
    last_segment_point_b: float

@dataclass
class AbsoluteDistances:
    """Absolute distance measurements along the route"""
    first_point_distance: float
    last_point_distance: float

@dataclass
class TravelMetrics:
    """Final travel calculation results"""
    distance_traveled: float
    time_elapsed_seconds: float
    last_timestamp: datetime
    average_speed: float
