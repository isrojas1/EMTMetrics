from bisect import bisect
from decimal import Decimal
from typing import List, Tuple

import numpy as np
from scipy.spatial import cKDTree
from math import radians, sin, cos, sqrt, atan2
import bisect

from src.emtmetrics.error.point_not_close_error import PointNotCloseError


def correct_position(
    route: list,
    bus_position: tuple[float, float],
    max_distance: float = 0.001
) -> Tuple[tuple[float, float], float, tuple[tuple[float, float]]]:
    """
    Corrects the bus position to the closest point on the route.
    Raises PointNotCloseError if the closest point is farther than max_distance.

    Args:
        route: List of (lat, lon) points
        bus_position: Dict with 'latitude' and 'longitude' or tuple (lat, lon)
        max_distance: Maximum allowed distance to consider the point close

    Returns:
        best_point: Closest point on the route (lon, lat)
        best_distance: Distance to the closest point
        best_segment: Segment (p1, p2) where the closest point lies
    """
    route_float = []
    for point in route:
        lat = point[0]
        lon = point[1]
        if isinstance(lat, Decimal):
            lat = float(lat)
        if isinstance(lon, Decimal):
            lon = float(lon)
        route_float.append((lat, lon))

    if isinstance(bus_position, dict):
        lon = float(bus_position['longitude'])
        lat = float(bus_position['latitude'])
        pos_float = (lat, lon)
    else:
        lat = float(bus_position[0])
        lon = float(bus_position[1])
        pos_float = (lat, lon)

    tree = cKDTree(route_float)
    distances, indices = tree.query(pos_float, k=2)

    segments = []
    for idx in indices:
        if idx > 0:
            segments.append((route_float[idx - 1], route_float[idx]))
        if idx < len(route_float) - 1:
            segments.append((route_float[idx], route_float[idx + 1]))

    segments = list(set(segments))

    best_distance = float('inf')
    best_point = None
    best_segment = None

    for p1, p2 in segments:
        p1_arr = np.array(p1)
        p2_arr = np.array(p2)
        pos_arr = np.array(pos_float)

        v = p2_arr - p1_arr
        w = pos_arr - p1_arr

        c = np.dot(w, v)
        b = np.dot(v, v)

        if b == 0:
            dist = np.linalg.norm(w)
            if dist < best_distance:
                best_distance = dist
                best_point = p1
                best_segment = (p1, p2)
            continue

        t = max(0, min(1, c / b))
        point_proy = p1_arr + t * v
        dist = np.linalg.norm(pos_arr - point_proy)

        if dist < best_distance:
            best_distance = dist
            best_point = tuple(point_proy)
            best_segment = (p1, p2)

    if best_distance > max_distance:
        raise PointNotCloseError(
            f"Point is too far from route: distance {best_distance} > max allowed {max_distance}"
        )

    return best_point, best_distance, best_segment


def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Calculate the great-circle distance (in meters) between two points."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return 6371000 * c  # Earth radius in meters


def calculate_distance_along_route(
        a: tuple[float, float],
        b: tuple[float, float],
        p: tuple[float, float],
        d_ab: float
) -> float:
    """Calculate distance traveled from a to p along the route.

    Args:
        a: (longitude, latitude) of start point.
        b: (longitude, latitude) of end point.
        p: (longitude, latitude) of intermediate point.
        d_ab: Total route distance from a to b (meters).

    Returns:
        Distance from a to p along the route (meters).
    """
    d_ap = haversine(a[0], a[1], p[0], p[1])
    d_ab_straight = haversine(a[0], a[1], b[0], b[1])

    if d_ab_straight == 0:  # Avoid division by zero
        return 0.0

    ratio = d_ap / d_ab_straight
    return ratio * d_ab


def find_surrounding_distances(distances: List[float], target: float) -> Tuple[float, float]:
    """
    Finds the two consecutive distances in a sorted list that surround a target value.

    Args:
        distances: Sorted list of distances in ascending order
        target: Target distance value to locate

    Returns:
        Tuple (left_dist, right_dist) representing the surrounding interval

    Raises:
        ValueError: If the list is empty or target is out of range
    """
    if not distances:
        raise ValueError("Distance list cannot be empty")

    n = len(distances)

    # Check if target is out of range
    if target < distances[0]:
        raise ValueError(f"Target {target} is below minimum distance {distances[0]}")
    if target > distances[-1]:
        raise ValueError(f"Target {target} is above maximum distance {distances[-1]}")

    # Find insertion position using binary search
    idx = bisect.bisect_left(distances, target)

    # Handle exact matches and edge cases
    if idx == 0:
        return distances[0], distances[0]
    if distances[idx] == target or idx == n:
        return distances[idx - 1], distances[idx] if idx < n else distances[-1]

    # Return surrounding distances
    return distances[idx - 1], distances[idx]


def interpolate_point(
        lat_a: float, lon_a: float, dist_a: float,
        lat_b: float, lon_b: float, dist_b: float,
        dist_p: float
) -> tuple[float, float]:
    """
    Interpolates the coordinates of a point between two known points using linear interpolation.

    Args:
        lat_a: Latitude of point A
        lon_a: Longitude of point A
        dist_a: Traveled distance at point A
        lat_b: Latitude of point B
        lon_b: Longitude of point B
        dist_b: Traveled distance at point B
        dist_p: Traveled distance at the target point (must be between dist_a and dist_b)

    Returns:
        Tuple (lat_p, lon_p) with coordinates of the interpolated point

    Raises:
        ValueError: If dist_p is not between dist_a and dist_b
    """
    # Convert all distance values to float
    dist_a = float(dist_a)
    dist_b = float(dist_b)
    dist_p = float(dist_p)

    # Validate distance
    if not (dist_a <= dist_p <= dist_b):
        raise ValueError("dist_p must be between dist_a and dist_b")

    # Handle coincident points
    if dist_a == dist_b:
        return lat_a, lon_a

    # Calculate interpolation fraction
    fraction = (dist_p - dist_a) / (dist_b - dist_a)

    # Perform linear interpolation
    lat_p = lat_a + fraction * (lat_b - lat_a)
    lon_p = lon_a + fraction * (lon_b - lon_a)

    return lat_p, lon_p
