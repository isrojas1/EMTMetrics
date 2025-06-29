from math import radians, sin, cos, sqrt, atan2


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
