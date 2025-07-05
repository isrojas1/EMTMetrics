from fastapi import APIRouter, HTTPException, Depends

from ..model.location_request import LocationRequest
from ..model.position_prediction_request import PositionPredictionRequest
from ..model.position_prediction_response import PositionPredictionResponse
from ..model.time_prediction_request import TimePredictionByCoordinatesRequest, TimePredictionByDistanceTraveledRequest, \
    TimePredictionByStopRequest
from ..model.time_prediction_response import TimePredictionResponse
from ..service.prediction_service import *

router = APIRouter()

def get_service():
    from ..app import service
    return service

@router.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Bus Prediction API is running"}


@router.post("/api/predictions/position", response_model=PositionPredictionResponse)
async def predict_future_position(request: PositionPredictionRequest, service=Depends(get_service)):
    """
    Predict where a bus will be in X seconds
    """
    try:
        logger.info(f"Predicting position for bus {request.bus_id} in {request.prediction_time_seconds} seconds")

        # Validate bus_id format
        if not request.bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")

        # Validate prediction time (max 1 hour)
        if request.prediction_time_seconds <= 0 or request.prediction_time_seconds > 3600:
            raise HTTPException(status_code=400, detail="Prediction time must be between 1 and 3600 seconds (1 hour)")

        result = service.calculate_predicted_position(request.bus_id, request.prediction_time_seconds)

        return PositionPredictionResponse(
            bus_id=request.bus_id,
            predicted_location=LocationRequest(
                latitude=result["latitude"],
                longitude=result["longitude"]
            ),
            distance_traveled=result["distance_traveled"],
            prediction_time_seconds=request.prediction_time_seconds,
            current_speed=result["current_speed"],
            message="Position prediction calculated successfully"
        )

    except Exception as e:
        logger.error(f"Error in position prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Position prediction failed: {str(e)}")


@router.post("/api/predictions/time", response_model=TimePredictionResponse)
async def predict_arrival_time_by_coords(request: TimePredictionByCoordinatesRequest, service=Depends(get_service)):
    """
    Predict when a bus will arrive at a target location
    """
    try:
        logger.info(f"Predicting arrival time for bus {request.bus_id}")

        # Validate bus_id format
        if not request.bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")


        # Validate prediction time limit
        result = service.calculate_predicted_arrival_by_coords(
            request.bus_id,
            request.target_location
        )

        return TimePredictionResponse(
            bus_id=request.bus_id,
            predicted_location=request.target_location,
            predicted_arrival_time="placeholder TODO",
            seconds_to_arrival=result["predicted_time_seconds"],
            current_speed=result["current_speed"],
            message="Prediction calculated successfully"
        )

    except Exception as e:
        logger.error(f"Error in time prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.post("/api/predictions/time/bydistance", response_model=TimePredictionResponse)
async def predict_arrival_time_by_distance(request: TimePredictionByDistanceTraveledRequest, service=Depends(get_service)):
    """
    Predict when a bus will arrive at a target location
    """
    try:
        logger.info(f"Predicting arrival time for bus {request.bus_id}")

        # Validate bus_id format
        if not request.bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")

        # Validate prediction time limit
        result = service.calculate_predicted_arrival_time_by_distance(
            request.bus_id,
            request.target_location
        )

        return TimePredictionResponse(
            bus_id=request.bus_id,
            predicted_location=LocationRequest(
                latitude=result["latitude"],
                longitude=result["longitude"]
            ),
            predicted_arrival_time="placeholder TODO",
            seconds_to_arrival=result["predicted_time_seconds"],
            current_speed=result["current_speed"],
            message="Prediction calculated successfully"
        )

    except Exception as e:
        logger.error(f"Error in time prediction: {e}")
        raise (HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.post("/api/predictions/time/bydistance", response_model=TimePredictionResponse))
async def predict_arrival_time_by_stop(request: TimePredictionByStopRequest, service=Depends(get_service)):
    """
    Predict when a bus will arrive at a target location
    """
    try:
        logger.info(f"Predicting arrival time for bus {request.bus_id}")

        # Validate bus_id format
        if not request.bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")

        # Validate prediction time limit
        result = service.calculate_predicted_arrival_time_by_stop(
            request.bus_id,
            request.stop_order
        )

        return TimePredictionResponse(
            bus_id=request.bus_id,
            predicted_location=LocationRequest(
                latitude=result["latitude"],
                longitude=result["longitude"]
            ),
            predicted_arrival_time="placeholder TODO",
            seconds_to_arrival=result["predicted_time_seconds"],
            current_speed=result["current_speed"],
            message="Prediction calculated successfully"
        )

    except Exception as e:
        logger.error(f"Error in time prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")