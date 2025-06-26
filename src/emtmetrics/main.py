import os
from decimal import Decimal
from typing import List
import folium
from influxdb_client import InfluxDBClient, Point
import mysql.connector
from mysql.connector import Error
import numpy as np
from scipy.spatial import cKDTree


def shape_points(shape_id: int) -> List[tuple]:
    try:
        conexion = mysql.connector.connect(
            host='localhost',
            user='root',
            password='tfgautobuses',
            database='emtdata'
        )

        if conexion.is_connected():
            print("Conexión exitosa.")
            cursor = conexion.cursor()
            consulta = f"SELECT shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shapes WHERE shape_id = {shape_id}"
            cursor.execute(consulta)
            return cursor.fetchall()

    except Error as e:
        print(f"Ocurrió un error: {e}")

    finally:
        if 'conexion' in locals() and conexion.is_connected():
            cursor.close()
            conexion.close()
            print("Conexión cerrada.")


def bus_positions(bus_id: str):
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "opentwins"
    influx_url = "http://192.168.32.131:30716"
    client = InfluxDBClient(url=influx_url, token=token, org=org)
    query_api = client.query_api()

    query = (
        'from(bucket: "default")\n'
        '  |> range(start: -1h)\n'
        '  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\n'
        '  |> filter(fn: (r) => r["_field"] == "value_gps_properties_longitude" or r["_field"] == "value_gps_properties_latitude")\n'
        f'  |> filter(fn: (r) => r["thingId"] == "{bus_id}")\n'
        '  |> map(fn: (r) => ({ r with _value: float(v: r._value) }))\n'
        '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
    )

    tables = query_api.query(org=org, query=query)

    data = []
    for table in tables:
        for row in table.records:
            data.append({
                'time': row.values['_time'],
                'latitude': row.values['value_gps_properties_latitude'],
                'longitude': row.values['value_gps_properties_longitude']
            })
    return data


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


def main():
    SHAPE_ID = 43
    BUS_ID = "buses:712"

    resultados_mysql = shape_points(SHAPE_ID)
    ruta = [(fila[1], fila[0]) for fila in resultados_mysql]  # (lon, lat)

    resultados_influx = bus_positions(BUS_ID)
    posicion = resultados_influx[-1]
    posicion_autobus = (posicion['longitude'], posicion['latitude'])

    print(f"Punto original: {posicion_autobus}")
    punto_corregido, distancia, segmento = corregir_posicion_optimizada(resultados_mysql, posicion_autobus)
    punto_corregido_float = (float(punto_corregido[0]), float(punto_corregido[1]))
    print(f"Punto corregido: {punto_corregido_float}")
    print(f"Distancia: {distancia}")
    print(f"Segmento usado: {segmento}")

    # Crear mapa comparativo
    mapa = crear_mapa_comparativo(
        pos_original=posicion_autobus,
        pos_corregida=punto_corregido,
        ruta=ruta,
        segmento=segmento
    )

    # Guardar y mostrar mapa
    mapa.save('comparacion_posiciones.html')
    print("Mapa guardado como 'comparacion_posiciones.html'")

    # (Opcional) Abrir automáticamente en navegador
    import webbrowser
    webbrowser.open('comparacion_posiciones.html')


if __name__ == "__main__":
    main()
