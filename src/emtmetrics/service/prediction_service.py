import logging
from datetime import datetime
from typing import Any, Dict

from ..model.location_request import LocationRequest
from ..utils.influxdb_manager import InfluxDBManager
from ..utils.mysql_manager import MySQLManager
from ..utils.calculations import *

logger = logging.getLogger()


class PredictionService:
    def __init__(self, influxdb_manager: InfluxDBManager, mysql_manager: MySQLManager):
        self.influxdb_manager = influxdb_manager
        self.mysql_manager = mysql_manager

    def get_bus_shape(self, bus_id: str) -> Any:
        route_info = self.influxdb_manager.get_bus_route(bus_id)
        if not route_info.get('linea') or not route_info.get('sentido'):
            return None

        # Extract numeric values from prefixed strings
        try:
            line_id = route_info['linea'].split(':')[-1]
            direction_id = route_info['sentido'].split(':')[-1]
        except (IndexError, TypeError):
            return None

        return self.mysql_manager.get_bus_shape(line_id, direction_id)

    def calculate_average_speed(self, bus_id: str, first_point_index: int, last_point_index: int) -> Tuple[
        float, float, List[int], int]:
        try:
            bus_shape = self.get_bus_shape(bus_id)
            logger.info(f"Retrieved bus shape: {bus_shape}")
            if not bus_shape:
                logger.error("No bus shape found. Exiting.")

            # Get shape points from MySQL
            shape_points = self.mysql_manager.shape_points(bus_shape)
            logger.info(f"Retrieved {len(shape_points)} route points from database")
            if not shape_points:
                logger.error("No route points found in database. Exiting.")
            route = [(row[1], row[0]) for row in shape_points]  # (lon, lat)
            distance_traveled_list = [row[3] for row in shape_points]

            # Get bus positions from InfluxDB
            bus_positions = self.influxdb_manager.bus_positions(bus_id)
            logger.info(f"Retrieved {len(bus_positions)} position points from InfluxDB")
            if len(bus_positions) < 3:
                logger.error("Insufficient position points (min 3 required). Exiting.")

            # TODO: Check for indexes

            # Extract positions
            first_position = (bus_positions[first_point_index]['longitude'],
                              bus_positions[first_point_index]['latitude'])
            last_position = (bus_positions[last_point_index]['longitude'], bus_positions[last_point_index]['latitude'])
            logger.debug(f"First position: {first_position}")
            logger.debug(f"Last position: {last_position}")

            # Position correction
            logger.info("Correcting first position...")
            first_position_corrected, _, first_segment = correct_position(shape_points, first_position)
            logger.info("Correcting last position...")
            last_position_corrected, _, last_segment = correct_position(shape_points, last_position)

            # Get segment distances
            logger.info("Calculating segment distances...")
            distance_traveled_first_point_segment_point_a = self.mysql_manager.dist_traveled(bus_shape,
                                                                                             first_segment[0][1],
                                                                                             first_segment[0][0])
            distance_traveled_first_point_segment_point_b = self.mysql_manager.dist_traveled(bus_shape,
                                                                                             first_segment[1][1],
                                                                                             first_segment[1][0])
            distance_traveled_last_point_segment_point_a = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            last_segment[0][1],
                                                                                            last_segment[0][0])
            distance_traveled_last_point_segment_point_b = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            last_segment[1][1],
                                                                                            last_segment[1][0])
            logger.debug(f"First segment's a point distance traveled: {distance_traveled_first_point_segment_point_a}m")
            logger.debug(f"First segment's b point distance traveled: {distance_traveled_first_point_segment_point_b}m")
            logger.debug(f"Last segment's a point distance traveled: {distance_traveled_last_point_segment_point_a}m")
            logger.debug(f"Last segment's b point distance traveled: {distance_traveled_last_point_segment_point_b}m")

            # Calculate traveled distances
            logger.info("Calculating route distances...")
            relative_initial_point_distance = calculate_distance_along_route(
                first_segment[0], first_segment[1], first_position_corrected,
                distance_traveled_first_point_segment_point_b - distance_traveled_first_point_segment_point_a
            )
            absolute_first_point_distance = relative_initial_point_distance + distance_traveled_first_point_segment_point_a
            relative_final_point_distance = calculate_distance_along_route(
                last_segment[0], last_segment[1], last_position_corrected,
                distance_traveled_last_point_segment_point_b - distance_traveled_last_point_segment_point_a
            )
            absolute_last_point_distance = relative_final_point_distance + distance_traveled_last_point_segment_point_a

            logger.info(
                f"First point distance traveled: {absolute_first_point_distance:.2f}m | Last point distance traveled: {absolute_last_point_distance:.2f}m")

            # Calculate avg speed
            distance_traveled_in_section = abs(absolute_last_point_distance - absolute_first_point_distance)
            time_passed_in_section = bus_positions[last_point_index]['time'] - bus_positions[first_point_index]['time']
            time_passed_in_section_secs = time_passed_in_section.total_seconds()
            logger.info(
                f"Time elapsed: {time_passed_in_section_secs} seconds or {time_passed_in_section_secs / 3600} h")
            speed = distance_traveled_in_section / time_passed_in_section_secs  # m/s
            logger.info(f"Average speed: {speed} m/s or {speed * 3.6} km/h")

            return speed, absolute_last_point_distance, distance_traveled_list, bus_shape

        except Exception as e:
            logger.error(f"Error calculating average speed: {e}")
            raise

    def calculate_predicted_position(self, bus_id: str, prediction_seconds: int,
                                     initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
                bus_id,
                initial_index, last_index)

            distance_traveled_to_next_position = speed * prediction_seconds
            absolute_distance_traveled_to_next_position = absolute_last_point_distance + distance_traveled_to_next_position
            left_distance, right_distance = find_surrounding_distances(distance_traveled_list,
                                                                       absolute_distance_traveled_to_next_position)
            left_point = self.mysql_manager.get_coordinates(bus_shape, left_distance)
            right_point = self.mysql_manager.get_coordinates(bus_shape, right_distance)
            latitude_predicted, longitude_predicted = interpolate_point(float(left_point[0]), float(left_point[1]),
                                                                        float(left_distance),
                                                                        float(right_point[0]), float(right_point[1]),
                                                                        float(right_distance),
                                                                        float(
                                                                            absolute_distance_traveled_to_next_position))

            return {
                "latitude": latitude_predicted,
                "longitude": longitude_predicted,
                "distance_traveled": absolute_distance_traveled_to_next_position,
                "current_speed": speed
            }

        except Exception as e:
            logger.error(f"Error calculating predicted position: {e}")
            raise

    def calculate_predicted_arrival_by_coords(self, bus_id: str, location: LocationRequest,
                                              initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
                bus_id,
                initial_index, last_index)
            route = self.mysql_manager.shape_points(bus_shape)

            # Predict time to achieve bus_positions[-1]
            point_to_predict = (location.longitude, location.latitude)
            point_to_predict_corrected, _, segment_to_predict = correct_position(route, point_to_predict)

            distance_traveled_segment_to_predict_point_a = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            segment_to_predict[0][1],
                                                                                            segment_to_predict[0][0])
            distance_traveled_segment_to_predict_point_b = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            segment_to_predict[1][1],
                                                                                            segment_to_predict[1][0])
            distance_segment_to_predict = distance_traveled_segment_to_predict_point_b - distance_traveled_segment_to_predict_point_a

            distance_to_predict_relative = calculate_distance_along_route(
                segment_to_predict[0], segment_to_predict[1], point_to_predict_corrected, distance_segment_to_predict
            )

            absolute_point_to_predict_distance = distance_traveled_segment_to_predict_point_a + distance_to_predict_relative
            logger.info(f"Distance to predict: {absolute_point_to_predict_distance:.2f}m")

            distancia_por_recorrer = absolute_point_to_predict_distance - absolute_last_point_distance
            tiempo_predicho = distancia_por_recorrer / speed
            logger.info(f"Predicted time: {tiempo_predicho} secs or {tiempo_predicho / 60} mins")

            return {
                "current_speed": speed,
                "predicted_time_seconds": tiempo_predicho
            }

        except Exception as e:
            logger.error(f"Error calculating arrival time: {e}")
            raise

    def calculate_predicted_arrival_time_by_distance(self, bus_id: str, distance_traveled: int,
                                                     initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
                bus_id,
                initial_index, last_index)

            distance_traveled_relative = distance_traveled - absolute_last_point_distance
            predicted_time = distance_traveled_relative / speed

            # calculate coords
            left_distance, right_distance = find_surrounding_distances(distance_traveled_list,
                                                                       distance_traveled)  # TODO: chequea que distance_traveled < max
            left_point = self.mysql_manager.get_coordinates(bus_shape, left_distance)
            right_point = self.mysql_manager.get_coordinates(bus_shape, right_distance)
            latitude_predicted, longitude_predicted = interpolate_point(float(left_point[0]), float(left_point[1]),
                                                                        float(left_distance),
                                                                        float(right_point[0]), float(right_point[1]),
                                                                        float(right_distance),
                                                                        float(
                                                                            distance_traveled))

            return {
                "latitude": latitude_predicted,
                "longitude": longitude_predicted,
                "current_speed": speed,
                "predicted_time_seconds": predicted_time
            }

        except Exception as e:
            logger.error(f"Error calculating arrival time: {e}")
            raise

    def calculate_predicted_arrival_time_by_stop(self, bus_id: str, stop_order: int,
                                                     initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            route_info = self.influxdb_manager.get_bus_route(bus_id)
            bus_shape = self.get_bus_shape(bus_id)

            self.influxdb_manager.get_stops_for_line_and_direction()


        except Exception as e:
            logger.error(f"Error calculating arrival time: {e}")
            raise
