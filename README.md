# EMTMetrics
Este repositorio contiene el código fuente de EMTMetrics, servicio encargado del cálculo de las predicciones y métricas, escrito en Python y usando el framework FastAPI, constituye una API REST que expone diferentes endpoints a través de los cuales se puede solicitar que haga distintos tipos de predicciones.

## Generacion de imagen
En la raíz del repositorio, ejecutar:
`docker build -t IP_NODO_CLUSTER:30002/library/emtmetrics:0.2.0 .`

## Variables de entorno

- INFLUXDB_ORGANIZATION
- INFLUXDB_TOKEN
- INFLUXDB_URL

- MYSQL_HOSTNAME
- MYSQL_USER
- MYSQL_PASSWORD
- MYSQL_DATABASE
