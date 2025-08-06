import logging
from typing import List, Tuple

from fastapi import APIRouter, HTTPException, Depends, Query

from ..model.bus_details_response import BusDetailsResponse
from ..model.shape_details_response import ShapeDetailsResponse, Point

router = APIRouter()

logger = logging.getLogger(__name__)


def get_service():
    from ..app import prediction_service
    return prediction_service

@router.get("/api/details/bus", response_model=BusDetailsResponse)
async def get_bus_details(
    bus_id: str = Query(..., description="The bus ID, must start with 'buses:'"),
    service=Depends(get_service)
):
    """
    Get bus line details for the given bus ID.
    """
    try:
        # Validate bus_id format
        if not bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")

        return service.get_bus_details(bus_id)

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Error in position prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Position prediction failed: {str(e)}")

@router.get("/api/details/shape", response_model=ShapeDetailsResponse)
async def get_shape_details(
    bus_id: str = Query(..., description="The bus ID, must start with 'buses:'"),
    service=Depends(get_service)
):
    """
    Get bus line details for the given bus ID.
    """
    try:
        # Validate bus_id format
        if not bus_id.startswith("buses:"):
            raise HTTPException(status_code=400, detail="Invalid bus_id format. Must start with 'buses:'")

        route_data = service.get_route_data(bus_id)

        coordinates =  route_data.route_coordinates
        points = [Point(latitude=lat, longitude=lon) for lat, lon in coordinates]
        response = ShapeDetailsResponse(points=points)

        return response

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Error in position prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Position prediction failed: {str(e)}")
