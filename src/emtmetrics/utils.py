import logging
import os
from decimal import Decimal
from typing import List
import folium
from influxdb_client import InfluxDBClient, Point
import mysql.connector
from influxdb_client.client.exceptions import InfluxDBError
from mysql.connector import Error
import numpy as np
from scipy.spatial import cKDTree

logger = logging.getLogger(__name__)


# MYSQL and INFLUX
def shape_points(shape_id: int) -> List[tuple]:
    try:
        conexion = mysql.connector.connect(
            host='localhost',
            user='root',
            password='tfgautobuses',
            database='emtdata'
        )

        if conexion.is_connected():
            logger.info("Conexión mysql exitosa.")
            cursor = conexion.cursor()
            consulta = f"SELECT shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shapes WHERE shape_id = {shape_id}"
            cursor.execute(consulta)
            return cursor.fetchall()

    except Error as e:
        logger.info(f"Ocurrió un error: {e}")

    finally:
        if 'conexion' in locals() and conexion.is_connected():
            cursor.close()
            conexion.close()
            logger.info("Conexión mysql cerrada.")


def dist_traveled(shape_id: int, shape_pt_lat: float, shape_pt_lon: float) -> int:
    try:
        conexion = mysql.connector.connect(
            host='localhost',
            user='root',
            password='tfgautobuses',
            database='emtdata'
        )

        if conexion.is_connected():
            logger.info("Conexión mysql exitosa.")
            cursor = conexion.cursor()

            # Parameterized query to prevent SQL injection
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
            return result[0] if result else 0

    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
        return 0

    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 0

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()


def bus_positions(bus_id: str) -> list:
    """
    Retrieve bus position data from InfluxDB

    Args:
        bus_id: Bus identifier (e.g., "buses:712")

    Returns:
        List of position dictionaries with keys:
        - 'time': datetime object
        - 'latitude': float
        - 'longitude': float
    """
    token = os.environ.get("INFLUXDB_TOKEN")
    if not token:
        logger.error("INFLUXDB_TOKEN environment variable not set")
        return []

    org = "opentwins"
    influx_url = "http://192.168.32.131:30716"

    try:
        client = InfluxDBClient(url=influx_url, token=token, org=org)
        query_api = client.query_api()

        # Validate bus_id format
        if not (isinstance(bus_id, str) and ":" in bus_id):
            logger.error(f"Invalid bus_id format: {bus_id}")
            return []

        # Build query with parameterized bus_id
        query = f'''
            from(bucket: "default")
                |> range(start: -2h)
                |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
                |> filter(fn: (r) => r["_field"] == "value_gps_properties_longitude" or 
                                     r["_field"] == "value_gps_properties_latitude" or 
                                     r["_field"] == "value_line_properties_direction" or 
                                     r["_field"] == "value_line_properties_code")
                |> filter(fn: (r) => r["thingId"] == "{bus_id}")
                |> map(fn: (r) => ({{ r with _value: float(v: r._value) }}))
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: true)
                |> duplicate(column: "value_line_properties_code", as: "temp_code")
                |> duplicate(column: "value_line_properties_direction", as: "temp_direction")
                |> difference(columns: ["temp_code", "temp_direction"], keepFirst: true)
                |> fill(column: "temp_code", value: 0.0)
                |> fill(column: "temp_direction", value: 0.0)
                |> map(fn: (r) => ({{
                    r with
                    changeGroup: if r.temp_code != 0.0 or r.temp_direction != 0.0 then 1 else 0
                }}))
                |> cumulativeSum(columns: ["changeGroup"])
                |> keep(columns: ["_time", "value_gps_properties_latitude", "value_gps_properties_longitude", "changeGroup"])
                |> filter(fn: (r) => r.changeGroup == 0)
                |> sort(columns: ["_time"])
        '''

        logger.info(f"Querying positions for bus: {bus_id}")
        tables = query_api.query(org=org, query=query)
        logger.info(f"Received {len(tables)} tables from InfluxDB")

        data = []
        for table in tables:
            for row in table.records:
                try:
                    data.append({
                        'time': row.values['_time'],
                        'latitude': row.values.get('value_gps_properties_latitude'),
                        'longitude': row.values.get('value_gps_properties_longitude')
                    })
                except KeyError as e:
                    logger.warning(f"Missing position data in record: {e}")

        logger.info(f"Retrieved {len(data)} position records")
        return data

    except InfluxDBError as e:
        logger.error(f"InfluxDB query failed: {e}")
        return []
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return []
    finally:
        try:
            client.close()
        except UnboundLocalError:
            pass  # Client not initialized

def get_bus_route(bus_id: str) -> dict:
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "opentwins"
    influx_url = "http://192.168.32.131:30716"
    client = InfluxDBClient(url=influx_url, token=token, org=org)
    query_api = client.query_api()

    queryLinea = (
        'from(bucket: "default")\n'
        '  |> range(start: -1d)\n'
        '  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\n'
        '  |> filter(fn: (r) => r["_field"] == "value_line_properties_code")\n'
        f'  |> filter(fn: (r) => r["thingId"] == "{bus_id}")\n'
        '  |> last()\n'
        '  |> map(fn: (r) => ({\n'
        '          valor: "lines:" + r._value\n'
        '        }))\n'
    )

    querySentido = (
        'from(bucket: "default")\n'
        '  |> range(start: -1d)\n'
        '  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\n'
        '  |> filter(fn: (r) => r["_field"] == "value_line_properties_direction")\n'
        f'  |> filter(fn: (r) => r["thingId"] == "{bus_id}")\n'
        '  |> last()\n'
        '  |> map(fn: (r) => ({\n'
        '          valor: "lines:" + r._value\n'
        '        }))\n'
    )

    # Fetch linea
    result_linea = query_api.query(org=org, query=queryLinea)
    linea = None
    for table in result_linea:
        for record in table.records:
            linea = record.values.get('valor')

    # Fetch sentido
    result_sentido = query_api.query(org=org, query=querySentido)
    sentido = None
    for table in result_sentido:
        for record in table.records:
            sentido = record.values.get('valor')

    return {'linea': linea, 'sentido': sentido}


def get_bus_shape(bus_id: str) -> int | None:
    # Get bus route information
    route_info = get_bus_route(bus_id)
    if not route_info.get('linea') or not route_info.get('sentido'):
        return None

    # Extract numeric values from prefixed strings
    try:
        linea_value = route_info['linea'].split(':')[-1]  # Extract "123" from "lines:123"
        sentido_value = route_info['sentido'].split(':')[-1]  # Extract "1" from "lines:1"
    except (IndexError, TypeError):
        return None

    try:
        # Establish database connection
        conexion = mysql.connector.connect(
            host='localhost',
            user='root',
            password='tfgautobuses',
            database='emtdata'
        )

        if conexion.is_connected():
            cursor = conexion.cursor()
            # Use parameterized query to prevent SQL injection
            query = """
                SELECT shape_id 
                FROM trips_summary 
                WHERE route_id = %s 
                AND direction_id = %s
                LIMIT 1
            """
            cursor.execute(query, (linea_value, int(sentido_value)))
            result = cursor.fetchone()
            if result and result[0] is not None:
                try:
                    return int(result[0])
                except ValueError:
                    logger.error(f"shape_id '{result[0]}' is not convertible to int.")
                    return None
            else:
                return None

    except mysql.connector.Error as e:
        logger.error(f"Database error occurred: {e}")
        return None

    finally:
        # Ensure proper resource cleanup
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()


# PROCESSING
def corregir_posicion_optimizada(ruta, posicion_autobus):
    ruta_float = []
    for punto in ruta:
        lat = punto[0]
        lon = punto[1]
        if isinstance(lat, Decimal):
            lat = float(lat)
        if isinstance(lon, Decimal):
            lon = float(lon)
        ruta_float.append((lon, lat))

    if isinstance(posicion_autobus, dict):
        lon = float(posicion_autobus['longitude'])
        lat = float(posicion_autobus['latitude'])
        pos_float = (lon, lat)
    else:
        pos_float = (float(posicion_autobus[0]), float(posicion_autobus[1]))

    tree = cKDTree(ruta_float)
    distancias, indices = tree.query(pos_float, k=2)

    segmentos = []
    for idx in indices:
        if idx > 0:
            segmentos.append((ruta_float[idx - 1], ruta_float[idx]))
        if idx < len(ruta_float) - 1:
            segmentos.append((ruta_float[idx], ruta_float[idx + 1]))

    segmentos = list(set(segmentos))

    mejor_dist = float('inf')
    mejor_punto = None
    mejor_segmento = None

    for p1, p2 in segmentos:
        p1_arr = np.array(p1)
        p2_arr = np.array(p2)
        pos_arr = np.array(pos_float)

        v = p2_arr - p1_arr
        w = pos_arr - p1_arr

        c = np.dot(w, v)
        b = np.dot(v, v)

        if b == 0:
            dist = np.linalg.norm(w)
            if dist < mejor_dist:
                mejor_dist = dist
                mejor_punto = p1
                mejor_segmento = (p1, p2)
            continue

        t = max(0, min(1, c / b))
        punto_proy = p1_arr + t * v
        dist = np.linalg.norm(pos_arr - punto_proy)

        if dist < mejor_dist:
            mejor_dist = dist
            mejor_punto = tuple(punto_proy)
            mejor_segmento = (p1, p2)

    return mejor_punto, mejor_dist, mejor_segmento


# SHOW IN MAP
def crear_mapa_comparativo(pos_original, pos_corregida, ruta, segmento=None):
    """
    Crea un mapa Folium para comparar posiciones
    :param pos_original: Tupla (lon, lat) de posición original
    :param pos_corregida: Tupla (lon, lat) de posición corregida
    :param ruta: Lista de puntos de ruta [(lon1, lat1), ...]
    :param segmento: Tupla con puntos de segmento ((lon1, lat1), (lon2, lat2))
    """
    # Convertir a formato Folium (lat, lon)
    pos_orig_folium = (pos_original[1], pos_original[0])
    pos_corr_folium = (pos_corregida[1], pos_corregida[0])

    # Convertir ruta a formato Folium
    ruta_folium = [(lat, lon) for lon, lat in ruta]

    # Centro del mapa (punto medio)
    lat_centro = (pos_orig_folium[0] + pos_corr_folium[0]) / 2
    lon_centro = (pos_orig_folium[1] + pos_corr_folium[1]) / 2

    # Crear mapa
    mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=16)

    # Añadir ruta completa
    folium.PolyLine(
        locations=ruta_folium,
        color='blue',
        weight=3,
        opacity=0.7,
        tooltip="Ruta del autobús"
    ).add_to(mapa)

    # Añadir segmento usado (si existe)
    if segmento:
        seg_folium = [
            (segmento[0][1], segmento[0][0]),  # (lat, lon) del primer punto
            (segmento[1][1], segmento[1][0])  # (lat, lon) del segundo punto
        ]
        folium.PolyLine(
            locations=seg_folium,
            color='purple',
            weight=5,
            opacity=0.9,
            tooltip="Segmento usado para proyección"
        ).add_to(mapa)

    # Añadir posición original
    folium.Marker(
        location=pos_orig_folium,
        popup="Posición original GPS",
        icon=folium.Icon(color='red', icon='exclamation-triangle')
    ).add_to(mapa)

    # Añadir posición corregida
    folium.Marker(
        location=pos_corr_folium,
        popup="Posición corregida",
        icon=folium.Icon(color='green', icon='check-circle')
    ).add_to(mapa)

    # Línea entre posición original y corregida
    folium.PolyLine(
        locations=[pos_orig_folium, pos_corr_folium],
        color='orange',
        weight=2,
        dash_array='5, 10',
        tooltip=f"Distancia: {np.linalg.norm(np.array(pos_original) - np.array(pos_corregida)):.6f} grados"
    ).add_to(mapa)

    return mapa
