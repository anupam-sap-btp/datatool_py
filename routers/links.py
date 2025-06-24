from fastapi import APIRouter, HTTPException, Request, Depends, status
from models.schemas import LinkStep, LinkDetail
from database.connection import get_db
from database.links import create_db_linkstep, check_db_linkstep_exists, get_db_linkstep
from database.objects import check_db_object_phase_exists
from database.steps import check_db_step_exists
from datetime import datetime
from typing import List

# Define the router
router = APIRouter(
    prefix="/links",
    tags=["links"],
    responses={404: {"description": "Not found"}},
)


@router.post("/linkstep", response_model=str, status_code=status.HTTP_201_CREATED)  
async def add_linkstep(link: LinkStep, conn = Depends(get_db)):
    try:
        # Check if link exists
        if check_db_linkstep_exists(link, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Link already exists"
            )
        # Check if object exists
        if not check_db_object_phase_exists(link.object_id, link.phase_num, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Phase {link.phase_num} for Object ID {link.object_id} does not exist"
            )
        
        # Check if step exists
        if not check_db_step_exists(link.step_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Step with ID {link.step_id} does not exist"
            )

        # Create link
        phase_id = create_db_linkstep(link, conn)
        
        return f"Link added successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )
    
@router.get("/object/{object_id}", response_model=List[LinkDetail], status_code=status.HTTP_200_OK)  
async def add_linkstep(object_id: str, conn = Depends(get_db)):
    try:
        return get_db_linkstep(object_id, conn)
        return f"Link added successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object links: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object link"
        )