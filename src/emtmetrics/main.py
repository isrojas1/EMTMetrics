import sys

from utils import *
import distance
import logging
import folium
from folium.plugins import AntPath

logger = logging.getLogger(__name__)


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    BUS_ID = "buses:712"
    logger.info(f"Starting processing for bus: {BUS_ID}")

    try:
        # Get bus shape
        bus_shape = get_bus_shape(BUS_ID)
        logger.info(f"Retrieved bus shape: {bus_shape}")

        if not bus_shape:
            logger.error("No bus shape found. Exiting.")
            sys.exit(1)

        # Get shape points from MySQL
        resultados_mysql = shape_points(bus_shape)
        logger.info(f"Retrieved {len(resultados_mysql)} route points from database")

        if not resultados_mysql:
            logger.error("No route points found in database. Exiting.")
            sys.exit(1)

        ruta = [(fila[1], fila[0]) for fila in resultados_mysql]  # (lon, lat)

        # Get bus positions from InfluxDB
        resultados_influx = bus_positions(BUS_ID)
        logger.info(f"Retrieved {len(resultados_influx)} position points from InfluxDB")

        if len(resultados_influx) < 3:
            logger.error("Insufficient position points (min 3 required). Exiting.")
            sys.exit(1)

        # Extract positions
        posicion_inicial = (resultados_influx[0]['longitude'], resultados_influx[0]['latitude'])
        posicion_final = (resultados_influx[-2]['longitude'], resultados_influx[-2]['latitude'])
        logger.debug(f"Initial position: {posicion_inicial}")
        logger.debug(f"Final position: {posicion_final}")

        # Position correction
        logger.info("Correcting initial position...")
        punto_corregido_inicial, _, segmento_inicial = corregir_posicion_optimizada(resultados_mysql,
                                                                                                    posicion_inicial)
        logger.info("Correcting final position...")
        punto_corregido_final, _, segmento_final = corregir_posicion_optimizada(resultados_mysql,
                                                                                              posicion_final)

        # Get segment distances
        logger.info("Calculating segment distances...")
        distancia_segmento_inicial_a = dist_traveled(bus_shape, segmento_inicial[0][1], segmento_inicial[0][0])
        distancia_segmento_inicial_b = dist_traveled(bus_shape, segmento_inicial[1][1], segmento_inicial[1][0])

        distancia_segmento_final_a = dist_traveled(bus_shape, segmento_final[0][1], segmento_final[0][0])
        distancia_segmento_final_b = dist_traveled(bus_shape, segmento_final[1][1], segmento_final[1][0])

        logger.debug(f"Segment a point distance (initial): {distancia_segmento_inicial_a}m")
        logger.debug(f"Segment b point distance (initial): {distancia_segmento_inicial_b}m")
        logger.debug(f"Segment a point distance (final): {distancia_segmento_final_a}m")
        logger.debug(f"Segment b point distance (final): {distancia_segmento_final_b}m")

        # Calculate traveled distances
        logger.info("Calculating route distances...")
        # distancia desde A
        distancia_inicial = distance.calculate_distance_along_route(
            segmento_inicial[0], segmento_inicial[1], punto_corregido_inicial, distancia_segmento_inicial_b - distancia_segmento_inicial_a
        )
        # distancia desde B
        distancia_final = distance.calculate_distance_along_route(
            segmento_final[0], segmento_final[1], punto_corregido_final, distancia_segmento_final_b - distancia_segmento_final_a
        )

        distancia_inicial_real = distancia_inicial + distancia_segmento_inicial_a
        distancia_final_real = distancia_final + distancia_segmento_final_a

        logger.info(f"Initial distance: {distancia_inicial_real:.2f}m | Final distance: {distancia_final_real:.2f}m")


        # Calculate avg speed
        distancia_recorrida = abs(distancia_final_real - distancia_inicial_real)
        tiempo_transcurrido = resultados_influx[-2]['time'] - resultados_influx[0]['time']
        tiempo_transcurrido_secs = tiempo_transcurrido.total_seconds()
        logger.info(f"Time elapsed: {tiempo_transcurrido_secs} seconds or {tiempo_transcurrido_secs / 3600} h")
        speed = distancia_recorrida / tiempo_transcurrido_secs # m/s
        logger.info(f"Average speed: {speed} m/s or {speed * 3.6} km/h")

        # Posicion a predecir dist_traveled
        posicion_a_predecir = (resultados_influx[-1]['longitude'], resultados_influx[-1]['latitude'])
        punto_corregido_a_predecir, _,  segmento_a_predecir = corregir_posicion_optimizada(resultados_mysql, posicion_a_predecir)

        distacia_segmento_a_predecir_a = dist_traveled(bus_shape, segmento_a_predecir[0][1], segmento_a_predecir[0][0])
        distacia_segmento_a_predecir_b = dist_traveled(bus_shape, segmento_a_predecir[1][1], segmento_a_predecir[1][0])
        distacia_segmento_a_predecir = distacia_segmento_a_predecir_b - distacia_segmento_a_predecir_a

        distancia_a_predecir = distance.calculate_distance_along_route(
            segmento_a_predecir[0], segmento_final[1], punto_corregido_a_predecir, distacia_segmento_a_predecir
        )

        distancia_a_predecir_real = distacia_segmento_a_predecir_a + distancia_a_predecir
        logger.info(f"Distance to predict: {distancia_a_predecir_real:.2f}m")

        distancia_por_recorrer = distancia_a_predecir_real - distancia_final_real
        tiempo_predicho = distancia_por_recorrer / speed
        logger.info(f"Predicted time: {tiempo_predicho} secs or {tiempo_predicho / 60} mins")

        # --- Folium Map Generation ---

        # Calculate map center
        mid_lat = sum(lat for lon, lat in ruta) / len(ruta)
        mid_lon = sum(lon for lon, lat in ruta) / len(ruta)
        map_folium = folium.Map(location=[mid_lat, mid_lon], zoom_start=14)

        # Draw the route as a blue polyline
        folium.PolyLine(
            locations=[(lat, lon) for lon, lat in ruta],
            color='blue', weight=5, opacity=0.7, tooltip='Bus Route'
        ).add_to(map_folium)

        # Mark each bus position as a green circle
        for pos in resultados_influx:
            folium.CircleMarker(
                location=(pos['latitude'], pos['longitude']),
                radius=5, color='green', fill=True, fill_opacity=0.7,
                tooltip='Bus Position'
            ).add_to(map_folium)

        # Mark corrected initial and final positions
        folium.Marker(
            location=(punto_corregido_inicial[1], punto_corregido_inicial[0]),
            popup='Corrected Initial Position',
            icon=folium.Icon(color='red')
        ).add_to(map_folium)

        folium.Marker(
            location=(punto_corregido_final[1], punto_corregido_final[0]),
            popup='Corrected Final Position',
            icon=folium.Icon(color='purple')
        ).add_to(map_folium)

        # Add animated path for bus positions
        AntPath(
            locations=[(pos['latitude'], pos['longitude']) for pos in resultados_influx],
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


# Para probar la correccion de punto de ruta!
def main2():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    BUS_ID = "buses:712"
    bus_shape = get_bus_shape(BUS_ID)

    logger.info(f"Bus id: {BUS_ID}")
    logger.info(f"Bus shape detectado: {bus_shape}")

    resultados_mysql = shape_points(bus_shape)
    ruta = [(fila[1], fila[0]) for fila in resultados_mysql]  # (lon, lat)

    resultados_influx = bus_positions(BUS_ID)
    posicion = resultados_influx[-1]
    posicion_autobus = (posicion['longitude'], posicion['latitude'])

    logger.info(f"Punto original: {posicion_autobus}")
    punto_corregido, distancia, segmento = corregir_posicion_optimizada(resultados_mysql, posicion_autobus)
    punto_corregido_float = (float(punto_corregido[0]), float(punto_corregido[1]))
    logger.info(f"Punto corregido: {punto_corregido_float}")
    logger.info(f"Distancia: {distancia}")
    logger.info(f"Segmento usado: {segmento}")

    # Crear mapa comparativo
    mapa = crear_mapa_comparativo(
        pos_original=posicion_autobus,
        pos_corregida=punto_corregido,
        ruta=ruta,
        segmento=segmento
    )

    relative_path = 'target/comparacion_posiciones.html'
    absolute_path = os.path.abspath(relative_path)

    # Guardar y mostrar mapa
    mapa.save(relative_path)
    logger.info(f"Mapa guardado como '{absolute_path}'")

    # (Opcional) Abrir automáticamente en navegador
    import webbrowser
    webbrowser.open(f'file://{absolute_path}')


def main3():
    bus_id = "buses:712"

    logger.info(f"Retrieving shape ID for bus: {bus_id}")
    shape_id = get_bus_shape(bus_id)

    if shape_id:
        logger.info(f"Success! Shape ID for bus {bus_id}: {shape_id}")
    else:
        logger.info(f"No shape ID found for bus {bus_id}")


if __name__ == "__main__":
    main()
