import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from fastapi import HTTPException

from ..model.prediction_service_aux_data import TravelMetrics, AbsoluteDistances, PositionPair, CorrectedPositions, \
    SegmentDistances, RouteData
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
        """Get bus shape information for the given bus ID"""
        route_info = self.influxdb_manager.get_bus_route(bus_id)
        if not route_info.get('linea') or not route_info.get('sentido'):
            return None

        try:
            line_id = route_info['linea'].split(':')[-1]
            direction_id = route_info['sentido'].split(':')[-1]
        except (IndexError, TypeError):
            return None

        return self.mysql_manager.get_bus_shape(line_id, direction_id)

    def _get_route_data(self, bus_id: str) -> RouteData:
        """Extract and prepare route data for calculations"""
        bus_shape = self.get_bus_shape(bus_id)
        if not bus_shape:
            raise ValueError("No bus shape found")

        logger.info(f"Retrieved bus shape: {bus_shape}")

        shape_points = self.mysql_manager.shape_points(bus_shape)
        if not shape_points:
            raise ValueError("No route points found in database")

        logger.info(f"Retrieved {len(shape_points)} route points from database")

        route_coordinates = [(row[0], row[1]) for row in shape_points]  # lat, lon
        distance_traveled_list = [row[3] for row in shape_points]

        return RouteData(
            bus_shape=bus_shape,
            route_coordinates=route_coordinates,
            distance_traveled_list=distance_traveled_list
        )

    def _get_bus_positions(self, bus_id: str) -> List[Dict]:
        """Get bus positions from InfluxDB with validation"""
        bus_positions = self.influxdb_manager.bus_positions(bus_id)
        logger.info(f"Retrieved {len(bus_positions)} position points from InfluxDB")

        if len(bus_positions) < 2:
            raise ValueError("Insufficient position points (min 2 required)")

        return bus_positions

    def _extract_position_pair(self, bus_positions: List[Dict],
                               first_index: int, last_index: int) -> PositionPair:
        """Extract position pair data from bus positions"""
        first_pos_data = bus_positions[first_index]
        last_pos_data = bus_positions[last_index]

        first_position = (first_pos_data['latitude'], first_pos_data['longitude'])
        last_position = (last_pos_data['latitude'], last_pos_data['longitude'])

        logger.debug(f"First position: {first_position}")
        logger.debug(f"Last position: {last_position}")

        return PositionPair(
            first_position=first_position,
            last_position=last_position,
            first_index=first_index,
            last_index=last_index,
            first_timestamp=first_pos_data['time'],
            last_timestamp=last_pos_data['time']
        )

    def _correct_positions(self, route_data: RouteData,
                           position_pair: PositionPair) -> CorrectedPositions:
        """Correct positions using route shape points"""
        logger.info("Correcting first position...")
        first_corrected, _, first_segment = correct_position(
            route_data.route_coordinates, position_pair.first_position
        )

        logger.info("Correcting last position...")
        last_corrected, _, last_segment = correct_position(
            route_data.route_coordinates, position_pair.last_position
        )

        logger.debug(f"First position corrected: {first_corrected}")
        logger.debug(f"Last position corrected: {last_corrected}")

        return CorrectedPositions(
            first_corrected=first_corrected,
            last_corrected=last_corrected,
            first_segment=first_segment,
            last_segment=last_segment
        )

    def _calculate_segment_distances(self, route_data: RouteData,
                                     corrected_positions: CorrectedPositions) -> SegmentDistances:
        """Calculate distances for position segments"""
        logger.info("Calculating segment distances...")

        first_segment_point_a = self.mysql_manager.dist_traveled(
            route_data.bus_shape,
            corrected_positions.first_segment[0][0],
            corrected_positions.first_segment[0][1]
        )
        first_segment_point_b = self.mysql_manager.dist_traveled(
            route_data.bus_shape,
            corrected_positions.first_segment[1][0],
            corrected_positions.first_segment[1][1]
        )

        last_segment_point_a = self.mysql_manager.dist_traveled(
            route_data.bus_shape,
            corrected_positions.last_segment[0][0],
            corrected_positions.last_segment[0][1]
        )
        last_segment_point_b = self.mysql_manager.dist_traveled(
            route_data.bus_shape,
            corrected_positions.last_segment[1][0],
            corrected_positions.last_segment[1][1]
        )

        logger.debug(f"First segment distances: a={first_segment_point_a}m, b={first_segment_point_b}m")
        logger.debug(f"Last segment distances: a={last_segment_point_a}m, b={last_segment_point_b}m")

        return SegmentDistances(
            first_segment_point_a=first_segment_point_a,
            first_segment_point_b=first_segment_point_b,
            last_segment_point_a=last_segment_point_a,
            last_segment_point_b=last_segment_point_b
        )

    def _calculate_absolute_distances(self, corrected_positions: CorrectedPositions,
                                      segment_distances: SegmentDistances) -> AbsoluteDistances:
        """Calculate absolute distances along the route"""
        logger.info("Calculating route distances...")

        relative_first_distance = calculate_distance_along_route(
            corrected_positions.first_segment[0],
            corrected_positions.first_segment[1],
            corrected_positions.first_corrected,
            segment_distances.first_segment_point_b - segment_distances.first_segment_point_a
        )

        relative_last_distance = calculate_distance_along_route(
            corrected_positions.last_segment[0],
            corrected_positions.last_segment[1],
            corrected_positions.last_corrected,
            segment_distances.last_segment_point_b - segment_distances.last_segment_point_a
        )

        absolute_first_distance = relative_first_distance + segment_distances.first_segment_point_a
        absolute_last_distance = relative_last_distance + segment_distances.last_segment_point_a

        logger.info(f"Distances - First: {absolute_first_distance:.2f}m, Last: {absolute_last_distance:.2f}m")

        return AbsoluteDistances(
            first_point_distance=absolute_first_distance,
            last_point_distance=absolute_last_distance
        )

    def _calculate_travel_metrics(self, absolute_distances: AbsoluteDistances,
                                  position_pair: PositionPair) -> TravelMetrics:
        """Calculate travel distance, time, and average speed"""
        distance_traveled = abs(absolute_distances.last_point_distance - absolute_distances.first_point_distance)
        time_elapsed = position_pair.last_timestamp - position_pair.first_timestamp
        time_elapsed_seconds = time_elapsed.total_seconds()

        logger.info(f"Time elapsed: {time_elapsed_seconds} seconds ({time_elapsed_seconds / 3600:.4f} hours)")

        if time_elapsed_seconds <= 0:
            raise ValueError("Invalid time elapsed: must be positive")

        average_speed = distance_traveled / time_elapsed_seconds

        logger.info(f"Average speed: {average_speed:.2f} m/s ({average_speed * 3.6:.2f} km/h)")

        return TravelMetrics(
            distance_traveled=distance_traveled,
            time_elapsed_seconds=time_elapsed_seconds,
            last_timestamp=position_pair.last_timestamp,
            average_speed=average_speed
        )

    def calculate_average_speed(self, bus_id: str, first_point_index: int,
                                last_point_index: int) -> tuple[float, datetime, float, list[int], int]:
        """
        Calculate average speed between two bus positions.

        This method orchestrates the speed calculation process by delegating
        specific responsibilities to focused helper methods.
        """
        try:
            route_data = self._get_route_data(bus_id)
            bus_positions = self._get_bus_positions(bus_id)
            position_pair = self._extract_position_pair(bus_positions, first_point_index, last_point_index)
            corrected_positions = self._correct_positions(route_data, position_pair)
            segment_distances = self._calculate_segment_distances(route_data, corrected_positions)
            absolute_distances = self._calculate_absolute_distances(corrected_positions, segment_distances)
            travel_metrics = self._calculate_travel_metrics(absolute_distances, position_pair)

            return (
                travel_metrics.average_speed,
                travel_metrics.last_timestamp,
                absolute_distances.last_point_distance,
                route_data.distance_traveled_list,
                route_data.bus_shape
            )

        except Exception as e:
            logger.error(f"Error calculating average speed: {e}")
            raise

    def calculate_predicted_position(self, bus_id: str, prediction_seconds: int,
                                     initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, last_timestamp, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
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

            target_arrival_time = last_timestamp + timedelta(seconds=prediction_seconds)

            return {
                "latitude": latitude_predicted,
                "longitude": longitude_predicted,
                "last_known_distance_traveled": absolute_last_point_distance,
                "distance_traveled": absolute_distance_traveled_to_next_position,
                "target_arrival_time": target_arrival_time,
                "current_speed": speed,
            }

        except Exception as e:
            logger.error(f"Error calculating predicted position: {e}")
            raise

    def calculate_predicted_arrival_by_coords(self, bus_id: str, location: LocationRequest,
                                              initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, last_timestamp, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
                bus_id,
                initial_index, last_index)
            route_data = self._get_route_data(bus_id)

            # Predict time to achieve next position
            point_to_predict = (location.latitude, location.longitude)
            point_to_predict_corrected, _, segment_to_predict = correct_position(route_data.route_coordinates,
                                                                                 point_to_predict)

            distance_traveled_segment_to_predict_point_a = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            segment_to_predict[0][0],
                                                                                            segment_to_predict[0][1])
            distance_traveled_segment_to_predict_point_b = self.mysql_manager.dist_traveled(bus_shape,
                                                                                            segment_to_predict[1][0],
                                                                                            segment_to_predict[1][1])
            distance_segment_to_predict = distance_traveled_segment_to_predict_point_b - distance_traveled_segment_to_predict_point_a

            distance_to_predict_relative = calculate_distance_along_route(
                segment_to_predict[0], segment_to_predict[1], point_to_predict_corrected, distance_segment_to_predict
            )

            absolute_point_to_predict_distance = distance_traveled_segment_to_predict_point_a + distance_to_predict_relative
            logger.info(f"Distance to predict: {absolute_point_to_predict_distance:.2f}m")

            if absolute_point_to_predict_distance > absolute_last_point_distance:
                if absolute_point_to_predict_distance < absolute_last_point_distance:
                    raise HTTPException(status_code=400, detail=f"Point to predict distance in route "
                                                                f"({absolute_point_to_predict_distance}m) is behind last "
                                                                f"known point distance in route "
                                                                f"({absolute_last_point_distance}m)")

            distance_to_travel = absolute_point_to_predict_distance - absolute_last_point_distance
            predicted_time = distance_to_travel / speed
            logger.info(f"Predicted time: {predicted_time} secs or {predicted_time / 60} mins")

            predicted_arrival_time = last_timestamp + timedelta(seconds=predicted_time)

            return {
                "current_speed": speed,
                "predicted_arrival_time": predicted_arrival_time,
                "predicted_time_seconds": predicted_time,
                "last_known_distance_traveled": absolute_last_point_distance,
                "target_distance_traveled": absolute_point_to_predict_distance
            }

        except Exception as e:
            logger.error(f"Error calculating arrival time: {e}")
            raise

    def calculate_predicted_arrival_time_by_distance(self, bus_id: str, distance_traveled: int,
                                                     initial_index: int = 0, last_index: int = -1) -> Dict[str, Any]:
        try:
            # calculate time
            speed, last_timestamp, absolute_last_point_distance, distance_traveled_list, bus_shape = self.calculate_average_speed(
                bus_id,
                initial_index, last_index)

            if distance_traveled < absolute_last_point_distance:
                raise HTTPException(status_code=400, detail=f"Point to predict distance in route "
                                                            f"({distance_traveled}m) is behind last "
                                                            f"known point distance in route "
                                                            f"({absolute_last_point_distance}m)")

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

            predicted_arrival_time = last_timestamp + timedelta(seconds=predicted_time)

            return {
                "latitude": latitude_predicted,
                "longitude": longitude_predicted,
                "current_speed": speed,
                "predicted_arrival_time": predicted_arrival_time,
                "last_known_distance_traveled": absolute_last_point_distance,
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
