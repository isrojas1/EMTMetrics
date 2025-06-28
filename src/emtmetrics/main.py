from utils import *

def main():
    BUS_ID = "buses:712"
    bus_shape = get_bus_shape(BUS_ID)

    print(f"Bus id: {BUS_ID}")
    print(f"Bus shape detectado: {bus_shape}")

    resultados_mysql = shape_points(bus_shape)
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

    relative_path = 'target/comparacion_posiciones.html'
    absolute_path = os.path.abspath(relative_path)

    # Guardar y mostrar mapa
    mapa.save(relative_path)
    print(f"Mapa guardado como '{absolute_path}'")

    # (Opcional) Abrir automáticamente en navegador
    import webbrowser
    webbrowser.open(f'file://{absolute_path}')


def main2():
    bus_id = "buses:712"

    print(f"Retrieving shape ID for bus: {bus_id}")
    shape_id = get_bus_shape(bus_id)

    if shape_id:
        print(f"Success! Shape ID for bus {bus_id}: {shape_id}")
    else:
        print(f"No shape ID found for bus {bus_id}")


if __name__ == "__main__":
    main()
