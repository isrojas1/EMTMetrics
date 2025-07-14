from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.exceptions import InfluxDBError
import os
import logging
from typing import List, Dict, Any, Optional, Tuple


class InfluxDBManager:
    def __init__(self, url: str, org: str, bucket: str = "default"):
        """
        Simplified InfluxDB manager

        :param url: InfluxDB server URL
        :param org: Organization name
        :param bucket: Bucket name (default: "default")
        """
        self.url = url
        self.org = org
        self.bucket = bucket
        self.token = os.environ.get("INFLUXDB_TOKEN")

        if not self.token:
            logging.error("INFLUXDB_TOKEN environment variable not set")
            raise ValueError("Missing INFLUXDB_TOKEN environment variable")

    def _execute_query(self, query: str) -> Any:
        """
        Execute a Flux query and return raw tables

        :param query: Flux query string
        :return: Query result tables
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
            query_api = client.query_api()
            return query_api.query(query=query, org=self.org)

    def get_stops_for_line_and_direction(self, line: str, sentido: str) -> List[Dict[str, Any]]:
        """
        Returns the list of stops (with order and coordinates) for a given line and sentido.

        Args:
            line: The line code as a string (e.g., "3.0")
            sentido: The direction as a string (e.g., "2")

        Returns:
            List of dictionaries with keys: codParada, orden, latitud, longitud
        """
        flux_query = f'''
        import "strings"
    
        linea =
          from(bucket: "{self.bucket}")
              |> range(start: -1d)
              |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
              |> filter(fn: (r) => r["_field"] =~ /^value_stops_properties_\\d+_(orden|sentido)$/)
              |> filter(fn: (r) => r["thingId"] == "lines:{line}")
              |> last()
              |> map(fn: (r) => ({{
            codLinea: r["thingId"],
                  codParada: strings.split(v: r._field, t: "_")[3],
                  tipo: strings.split(v: r._field, t: "_")[4],
                  valor: r._value,
                  _time: r._time
                }}))
              |> pivot(rowKey: ["_time", "codLinea", "codParada"], columnKey: ["tipo"], valueColumn: "valor")
              |> filter(fn: (r) => exists r.sentido and string(v: r.sentido) == "{sentido}")
              |> map(fn: (r) => ({{
            codParada: r.codParada,
                  orden: int(v: r.orden)
                }}))
              |> sort(columns: ["orden"], desc: false)
    
        paradas =
          from(bucket: "{self.bucket}")
            |> range(start: -1d)
            |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
            |> filter(fn: (r) => r["_field"] =~ /^value_stop_properties_latitud$/ or r["_field"] =~ /^value_stop_properties_longitud$/)
            |> last()
            |> map(fn: (r) => ({{
            codParada: strings.trimPrefix(v: r["thingId"], prefix: "stops:"),
                tipo: if r["_field"] =~ /^value_stop_properties_latitud$/ then "latitud" else "longitud",
                valor: r._value,
                _time: r._time
              }}))
            |> pivot(rowKey: ["_time", "codParada"], columnKey: ["tipo"], valueColumn: "valor")
    
        join(
          tables: {{linea: linea, paradas: paradas}},
          on: ["codParada"]
        )
          |> map(fn: (r) => ({{
            codParada: r.codParada,
              orden: r.orden,
              latitud: float(v: r.latitud),
              longitud: float(v: r.longitud)
            }}))
          |> sort(columns: ["orden"], desc: false)
          |> yield(name: "ParadasConCoordenadas")
            '''
        try:
            tables = self._execute_query(flux_query)
            stops = []
            for table in tables:
                for record in table.records:
                    stops.append({
                        "codParada": record.values.get("codParada"),
                        "orden": record.values.get("orden"),
                        "latitud": record.values.get("latitud"),
                        "longitud": record.values.get("longitud"),
                    })
            return stops
        except InfluxDBError as e:
            logging.error(f"Failed to fetch stops for line {line}, sentido {sentido}: {e}")
            return []
        except Exception as e:
            logging.exception("Unexpected error in get_stops_for_line_and_direction")
            return []

    def bus_positions(self, bus_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve bus position data from InfluxDB
        """
        # Validate input
        if not self._valid_bus_id(bus_id):
            return []

        try:
            # Build and execute query
            query = self._build_positions_query(bus_id)
            tables = self._execute_query(query)

            # Process results
            return self._process_positions(tables)
        except InfluxDBError as e:
            logging.error(f"Position query failed: {e}")
            return []
        except Exception as e:
            logging.exception("Unexpected error in bus_positions")
            return []

    def get_bus_route(self, bus_id: str) -> Dict[str, Optional[str]]:
        """
        Get current route information for a bus
        """
        try:
            # Get route components
            linea = self._get_last_value(bus_id, "value_line_properties_code", "linea")
            sentido = self._get_last_value(bus_id, "value_line_properties_direction", "sentido")
            return {'linea': linea, 'sentido': sentido}
        except InfluxDBError as e:
            logging.error(f"Route query failed: {e}")
            return {'linea': None, 'sentido': None}
        except Exception as e:
            logging.exception("Unexpected error in get_bus_route")
            return {'linea': None, 'sentido': None}

    def _get_last_value(self, bus_id: str, field: str, alias: str) -> Optional[str]:
        """
        Get last value for a specific field
        """
        query = self._build_last_value_query(bus_id, field)
        tables = self._execute_query(query)

        # Extract value from results
        for table in tables:
            for record in table.records:
                return record.values.get('valor')
        return None

    def _build_positions_query(self, bus_id: str) -> str:
        """Build positions query"""
        return f'''
            from(bucket: "{self.bucket}")
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

    def _build_last_value_query(self, bus_id: str, field: str) -> str:
        """Build last value query"""
        return f'''
            from(bucket: "{self.bucket}")
                |> range(start: -1d)
                |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
                |> filter(fn: (r) => r["_field"] == "{field}")
                |> filter(fn: (r) => r["thingId"] == "{bus_id}")
                |> last()
                |> map(fn: (r) => ({{
                    valor: string(v: r._value)
                }}))
        '''

    def _process_positions(self, tables) -> List[Dict[str, Any]]:
        """Process position results into dictionaries"""
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
                    logging.warning(f"Missing position data: {e}")
        return data

    def _valid_bus_id(self, bus_id: str) -> bool:
        """Validate bus ID format"""
        if not (isinstance(bus_id, str) and ":" in bus_id):
            logging.error(f"Invalid bus_id format: {bus_id}")
            return False
        return True
