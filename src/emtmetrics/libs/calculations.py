from decimal import Decimal

import numpy as np
from scipy.spatial import cKDTree
from math import radians, sin, cos, sqrt, atan2


def correct_position(route, bus_position):
    route_float = []
    for point in route:
        lat = point[0]
        lon = point[1]
        if isinstance(lat, Decimal):
            lat = float(lat)
        if isinstance(lon, Decimal):
            lon = float(lon)
        route_float.append((lon, lat))

    if isinstance(bus_position, dict):
        lon = float(bus_position['longitude'])
        lat = float(bus_position['latitude'])
        pos_float = (lon, lat)
    else:
        pos_float = (float(bus_position[0]), float(bus_position[1]))

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
