import logging

from fastapi import APIRouter, HTTPException, Depends, Query

from ..model.bus_details_response import BusDetailsResponse

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
