import folium
import numpy as np


def crear_mapa_correcion_posicion(pos_original, pos_corregida, ruta, segmento=None):
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
