import logging

import mysql.connector
from mysql.connector import Error
from typing import List, Tuple, Optional


logger = logging.getLogger(__name__)


class MySQLManager:
    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Initialize the database connection manager.

        :param host: MySQL server host address
        :param user: Database username
        :param password: Database password
        :param database: Database name
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def _get_connection(self):
        """Create and return a new database connection"""
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def shape_points(self, shape_id: int) -> List[Tuple[float, float, int, float]]:
        """
        Get shape points for a given shape ID

        :param shape_id: Shape identifier
        :return: List of tuples (lat, lon, sequence, distance)
        """
        try:
            with self._get_connection() as conexion:
                if conexion.is_connected():
                    with conexion.cursor() as cursor:
                        query = """
                            SELECT shape_pt_lat, shape_pt_lon, 
                                   shape_pt_sequence, shape_dist_traveled 
                            FROM shapes 
                            WHERE shape_id = %s
                            ORDER BY shape_pt_sequence
                        """
                        cursor.execute(query, (shape_id,))
                        return cursor.fetchall()
        except Error as e:
            # Handle logging appropriately in your environment
            print(f"Database error: {e}")
            return []

    def dist_traveled(self, shape_id: int,
                      shape_pt_lat: float,
                      shape_pt_lon: float) -> Optional[float]:
        """
        Get distance traveled for a specific shape point

        :param shape_id: Shape identifier
        :param shape_pt_lat: Point latitude
        :param shape_pt_lon: Point longitude
        :return: Distance traveled or None if not found
        """
        try:
            with self._get_connection() as conexion:
                if conexion.is_connected():
                    with conexion.cursor() as cursor:
                        query = """
                            SELECT shape_dist_traveled 
                            FROM shapes 
                            WHERE shape_id = %s 
                            AND ABS(shape_pt_lat - %s) < 0.000001
                            AND ABS(shape_pt_lon - %s) < 0.000001
                            LIMIT 1
                        """
                        cursor.execute(query, (shape_id, shape_pt_lat, shape_pt_lon))
                        result = cursor.fetchone()
                        return result[0] if result else None
        except Error as e:
            print(f"Database error: {e}")
            return None

    def get_coordinates(self, shape_id: int, dist_traveled: int) -> Optional[tuple[float, float]]:
        """
        Get (lat, lon) for a specific shape point's dist traveled

        :param shape_id: Shape identifier
        :param dist_traveled: Point's distance traveled
        :return: (lat, lon) or None if not found
        """
        try:
            with self._get_connection() as conexion:
                if conexion.is_connected():
                    with conexion.cursor() as cursor:
                        query = """
                            SELECT shape_pt_lat, shape_pt_lon 
                            FROM shapes 
                            WHERE shape_id = %s 
                            AND shape_dist_traveled = %s
                            LIMIT 1
                        """
                        cursor.execute(query, (shape_id, dist_traveled))
                        result = cursor.fetchone()
                        return (result[0], result[1]) if result else None
        except Error as e:
            print(f"Database error: {e}")
            return None

    def get_bus_shape(self, line_id: str, direction_id: str) -> Optional[int]:
        try:
            with self._get_connection() as conexion:
                if conexion.is_connected():
                    with conexion.cursor() as cursor:
                        query = """
                            SELECT shape_id 
                            FROM trips_summary 
                            WHERE route_id = %s 
                            AND direction_id = %s
                            LIMIT 1
                        """
                        cursor.execute(query, (line_id, int(direction_id)))
                        result = cursor.fetchone()
                        if result and result[0] is not None:
                            try:
                                return int(result[0])
                            except ValueError:
                                logger.error(f"shape_id '{result[0]}' is not convertible to int.")
                                return None
                        else:
                            return None
        except Error as e:
            print(f"Database error: {e}")
            return None
