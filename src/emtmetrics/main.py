import sys
from typing import Any

import logging
import folium
from folium.plugins import AntPath
from libs.influxdb_manager import InfluxDBManager
from libs.mysql_manager import MySQLManager
import libs.calculations as calcs
from libs.calculations import interpolate_point

logger = logging.getLogger(__name__)


def get_bus_shape(bus_id: str, influxdb_manager: InfluxDBManager, mysql_manager: MySQLManager) -> Any:
    route_info = influxdb_manager.get_bus_route(bus_id)
    if not route_info.get('linea') or not route_info.get('sentido'):
        return None

    # Extract numeric values from prefixed strings
    try:
        line_id = route_info['linea'].split(':')[-1] 
        direction_id = route_info['sentido'].split(':')[-1]
    except (IndexError, TypeError):
        return None
    
    return mysql_manager.get_bus_shape(line_id, direction_id)


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    influxdb_manager = InfluxDBManager("http://192.168.32.131:30716", "opentwins")
    mysql_manager = MySQLManager("127.0.0.1", "root", "tfgautobuses", "emtdata")

    BUS_ID = "buses:714"
    logger.info(f"Starting processing for bus: {BUS_ID}")

    try:
        # Get bus shape
        bus_shape = get_bus_shape(BUS_ID, influxdb_manager, mysql_manager)
        logger.info(f"Retrieved bus shape: {bus_shape}")
        if not bus_shape:
            logger.error("No bus shape found. Exiting.")
            sys.exit(1)

        # Get shape points from MySQL
        shape_points = mysql_manager.shape_points(bus_shape)
        logger.info(f"Retrieved {len(shape_points)} route points from database")
        if not shape_points:
            logger.error("No route points found in database. Exiting.")
            sys.exit(1)
        route = [(row[1], row[0]) for row in shape_points]  # (lon, lat)
        distance_traveled_list = [row[3] for row in shape_points]

        # Get bus positions from InfluxDB
        bus_positions = influxdb_manager.bus_positions(BUS_ID)
        logger.info(f"Retrieved {len(bus_positions)} position points from InfluxDB")
        if len(bus_positions) < 3:
            logger.error("Insufficient position points (min 3 required). Exiting.")
            sys.exit(1)

        # Extract positions
        first_position = (bus_positions[0]['longitude'], bus_positions[0]['latitude'])
        last_position = (bus_positions[-2]['longitude'], bus_positions[-2]['latitude'])
        logger.debug(f"First position: {first_position}")
        logger.debug(f"Last position: {last_position}")

        # Position correction
        logger.info("Correcting first position...")
        first_position_corrected, _, first_segment = calcs.correct_position(shape_points, first_position)
        logger.info("Correcting last position...")
        last_position_corrected, _, last_segment = calcs.correct_position(shape_points, last_position)

        # Get segment distances
        logger.info("Calculating segment distances...")
        distance_traveled_first_point_segment_point_a = mysql_manager.dist_traveled(bus_shape, first_segment[0][1], first_segment[0][0])
        distance_traveled_first_point_segment_point_b = mysql_manager.dist_traveled(bus_shape, first_segment[1][1], first_segment[1][0])
        distance_traveled_last_point_segment_point_a = mysql_manager.dist_traveled(bus_shape, last_segment[0][1], last_segment[0][0])
        distance_traveled_last_point_segment_point_b = mysql_manager.dist_traveled(bus_shape, last_segment[1][1], last_segment[1][0])
        logger.debug(f"First segment's a point distance traveled: {distance_traveled_first_point_segment_point_a}m")
        logger.debug(f"First segment's b point distance traveled: {distance_traveled_first_point_segment_point_b}m")
        logger.debug(f"Last segment's a point distance traveled: {distance_traveled_last_point_segment_point_a}m")
        logger.debug(f"Last segment's b point distance traveled: {distance_traveled_last_point_segment_point_b}m")

        # Calculate traveled distances
        logger.info("Calculating route distances...")
        relative_initial_point_distance = calcs.calculate_distance_along_route(
            first_segment[0], first_segment[1], first_position_corrected, distance_traveled_first_point_segment_point_b - distance_traveled_first_point_segment_point_a
        )
        absolute_first_point_distance = relative_initial_point_distance + distance_traveled_first_point_segment_point_a
        relative_final_point_distance = calcs.calculate_distance_along_route(
            last_segment[0], last_segment[1], last_position_corrected, distance_traveled_last_point_segment_point_b - distance_traveled_last_point_segment_point_a
        )
        absolute_last_point_distance = relative_final_point_distance + distance_traveled_last_point_segment_point_a


        logger.info(f"First point distance traveled: {absolute_first_point_distance:.2f}m | Last point distance traveled: {absolute_last_point_distance:.2f}m")


        # Calculate avg speed
        distance_traveled_in_section = abs(absolute_last_point_distance - absolute_first_point_distance)
        time_passed_in_section = bus_positions[-2]['time'] - bus_positions[0]['time']
        time_passed_in_section_secs = time_passed_in_section.total_seconds()
        logger.info(f"Time elapsed: {time_passed_in_section_secs} seconds or {time_passed_in_section_secs / 3600} h")
        speed = distance_traveled_in_section / time_passed_in_section_secs # m/s
        logger.info(f"Average speed: {speed} m/s or {speed * 3.6} km/h")

        # Predict time to achieve bus_positions[-1]
        point_to_predict = (bus_positions[-1]['longitude'], bus_positions[-1]['latitude'])
        point_to_predict_corrected, _, segment_to_predict = calcs.correct_position(shape_points, point_to_predict)

        distance_traveled_segment_to_predict_point_a = mysql_manager.dist_traveled(bus_shape, segment_to_predict[0][1], segment_to_predict[0][0])
        distance_traveled_segment_to_predict_point_b = mysql_manager.dist_traveled(bus_shape, segment_to_predict[1][1], segment_to_predict[1][0])
        distance_segment_to_predict = distance_traveled_segment_to_predict_point_b - distance_traveled_segment_to_predict_point_a

        distance_to_predict_relative = calcs.calculate_distance_along_route(
            segment_to_predict[0], segment_to_predict[1], point_to_predict_corrected, distance_segment_to_predict
        )

        absolute_point_to_predict_distance = distance_traveled_segment_to_predict_point_a + distance_to_predict_relative
        logger.info(f"Distance to predict: {absolute_point_to_predict_distance:.2f}m")

        distancia_por_recorrer = absolute_point_to_predict_distance - absolute_last_point_distance
        tiempo_predicho = distancia_por_recorrer / speed
        logger.info(f"Predicted time: {tiempo_predicho} secs or {tiempo_predicho / 60} mins")

        # Predict bus_positions[-1] at its time
        time_passed_to_next_position = bus_positions[-1]['time'] - bus_positions[-2]['time']
        time_passed_to_next_position_secs = time_passed_to_next_position.total_seconds()
        distance_traveled_to_next_position = speed * time_passed_to_next_position_secs
        absolute_distance_traveled_to_next_position = absolute_last_point_distance + distance_traveled_to_next_position
        left_distance, right_distance = calcs.find_surrounding_distances(distance_traveled_list, absolute_distance_traveled_to_next_position)
        left_point = mysql_manager.get_coordinates(bus_shape, left_distance)
        right_point = mysql_manager.get_coordinates(bus_shape, right_distance)
        latitude_predicted, longitude_predicted = interpolate_point(float(left_point[0]), float(left_point[1]), float(left_distance),
                                                                    float(right_point[0]), float(right_point[1]), float(right_distance),
                                                                    float(absolute_distance_traveled_to_next_position))

        logger.info(
            f"Predicted position {time_passed_to_next_position_secs} seconds in: ({latitude_predicted}, {longitude_predicted})")
        ###############################
        # --- Folium Map Generation ---

        # Calculate map center
        mid_lat = sum(lat for lon, lat in route) / len(route)
        mid_lon = sum(lon for lon, lat in route) / len(route)
        map_folium = folium.Map(location=[mid_lat, mid_lon], zoom_start=14)

        # Draw the route as a blue polyline
        folium.PolyLine(
            locations=[(lat, lon) for lon, lat in route],
            color='blue', weight=5, opacity=0.7, tooltip='Bus Route'
        ).add_to(map_folium)

        # Mark each bus position as a green circle
        for pos in bus_positions:
            folium.CircleMarker(
                location=(pos['latitude'], pos['longitude']),
                radius=5, color='green', fill=True, fill_opacity=0.7,
                tooltip='Bus Position'
            ).add_to(map_folium)

        # Mark corrected initial and final positions
        folium.Marker(
            location=(first_position_corrected[1], first_position_corrected[0]),
            popup='Corrected Initial Position',
            icon=folium.Icon(color='red')
        ).add_to(map_folium)

        folium.Marker(
            location=(last_position_corrected[1], last_position_corrected[0]),
            popup='Corrected Final Position',
            icon=folium.Icon(color='purple')
        ).add_to(map_folium)

        folium.Marker(
            location=(latitude_predicted, longitude_predicted),
            popup='Predicted Position',
            icon=folium.Icon(color='pink')
        ).add_to(map_folium)

        # Add animated path for bus positions
        AntPath(
            locations=[(pos['latitude'], pos['longitude']) for pos in bus_positions],
            color='green'
        ).add_to(map_folium)

        # Save the map to an HTML file
        map_folium.save('bus_route_map.html')
        logger.info("Folium map saved to bus_route_map.html")
    except Exception as e:
        logger.exception("Fatal error during processing:")
        sys.exit(1)

    finally:
        logger.info("Processing completed")

if __name__ == "__main__":
    main()
