from utils import *

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
