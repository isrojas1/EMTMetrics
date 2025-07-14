import logging
import os

from fastapi import FastAPI

from .controller.prediction_controller import router as prediction_router
from .controller.details_controller import router as details_router
from .service.prediction_service import PredictionService
from .utils.influxdb_manager import InfluxDBManager
from .utils.mysql_manager import MySQLManager

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize DB managers
influxdb_manager = InfluxDBManager(os.environ.get("INFLUXDB_URL"), os.environ.get("INFLUXDB_ORGANIZATION"),
                                   os.environ.get("INFLUXDB_TOKEN"))
mysql_manager = MySQLManager(os.environ.get("MYSQL_HOSTNAME"), os.environ.get("MYSQL_USER"),
                             os.environ.get("MYSQL_PASSWORD"), os.environ.get("MYSQL_DATABASE"))

# Initialize services
prediction_service = PredictionService(influxdb_manager, mysql_manager)

# Initialize FastAPI app
app = FastAPI(title="Bus Prediction API", description="Simple API for bus predictions", version="1.0.0")
app.include_router(prediction_router)
app.include_router(details_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
