from fastapi import APIRouter, HTTPException, Request, Depends, status
from models.schemas import Object, ObjectResponse, PhaseAdd
from database.connection import get_db
from database.objects import read_db_object, check_db_object_exists, create_db_object, delete_db_object, update_db_object, \
                             read_db_object_phase, check_db_object_phase_exists, create_db_object_phase, delete_db_object_phase, update_db_object_phase
from datetime import datetime

# Define the router
router = APIRouter(
    prefix="/objects",
    tags=["objects"],
    responses={404: {"description": "Not found"}},
)

# Read object
@router.get("/{object_id}")
async def read_object(object_id: str, conn = Depends(get_db)) -> ObjectResponse:
    try:
        object_data = read_db_object(object_id, conn)
        print(object_data)
        if not object_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Object not found"
            )
        return object_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )

# Create object
@router.post("/", response_model=ObjectResponse, status_code=status.HTTP_201_CREATED)
async def create_object(object: Object, conn = Depends(get_db)):
    try:
        # Check if object exists
        if check_db_object_exists(object.object_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Object with ID {object.object_id} already exists"
            )
        
        # Create object
        object_dict = object.model_dump()
        object_id = create_db_object(object_dict, conn)
        
        return {"id" : 10, "object_id": object_id, "description": object.description, "object_type": object.object_type, "volume": object.volume, "created_at": datetime.now(), "created_by": object.created_by}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    
# Delete object
@router.delete("/{object_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_object(object_id: str, conn = Depends(get_db)) -> None:
    try:
        # Check if object exists
        if not check_db_object_exists(object_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Object with ID {object_id} does not exist"
            )

        object_data = delete_db_object(object_id, conn)
        print(object_data)
        if not object_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Object not found"
            )
        return object_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )    
    
@router.put("/{object_id}", response_model=ObjectResponse, status_code=status.HTTP_200_OK)
async def update_object(
    object_id: str, 
    object_update: Object, 
    conn = Depends(get_db)
):
    try:

        # Check if object exists
        if not check_db_object_exists(object_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Object with ID {object_id} does not exist"
            )
        # Convert Pydantic model to dict and remove None values
        update_data = object_update.model_dump(exclude_unset=True)
        print(update_data)
        
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields provided for update"
            )
        
        return update_db_object(object_id, update_data, conn)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )    
    
@router.post("/{object_id}/phase/", response_model=str, status_code=status.HTTP_201_CREATED)  
async def add_object_phase(object_id: str, phase: PhaseAdd, conn = Depends(get_db)):
    try:
        # Check if object exists
        if check_db_object_phase_exists(object_id, phase.phase_num, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Phase {phase.phase_num} for Object ID {object_id} already exists"
            )
        
        # Create phase in the object
        phase_dict = phase.model_dump()
        phase_id = create_db_object_phase(object_id, phase_dict, conn)
        
        return f"Phase {phase.phase_num} for Object ID {object_id} added successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )
    
@router.delete("/{object_id}/phase/{phase_num}", status_code=status.HTTP_204_NO_CONTENT)  
async def delete_object_phase(object_id: str, phase_num: int, conn = Depends(get_db)):
    try:
        # Check if object exists
        if not check_db_object_phase_exists(object_id, phase_num, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Phase {phase_num} for Object ID {object_id} does not exist"
            )
        
        # Delete phase from the object
        phase_data = delete_db_object_phase(object_id, phase_num, conn)
        
        print(phase_data)
        if not phase_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Phase not found"
            )
        return phase_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )    
    

@router.put("/{object_id}/phase/{phase_num}", response_model=str, status_code=status.HTTP_200_OK)  
async def update_object_phase(object_id: str, phase_num: int, phase: PhaseAdd, conn = Depends(get_db)):
    try:
        # Check if object exists
        if not check_db_object_phase_exists(object_id, phase_num, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Phase {phase_num} for Object ID {object_id} does not exist"
            )
        
        # Convert Pydantic model to dict and remove None values
        update_data = phase.model_dump(exclude_unset=True)
        print(update_data)
        
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields provided for update"
            )
        # Update phase from the object
        phase_data = update_db_object_phase(object_id, phase_num, update_data, conn)
        
        print(phase_data)
        if not phase_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Phase not found"
            )
        return f"Phase {phase.phase_num} for Object ID {object_id} updated successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )        
    
# Read object
@router.get("/{object_id}/phase/{phase_num}", response_model=PhaseAdd, status_code=status.HTTP_200_OK)
async def read_object_phase(object_id: str, phase_num: int, conn = Depends(get_db)) -> PhaseAdd:
    try:
        phase_data = read_db_object_phase(object_id, phase_num, conn)
        print(phase_data)
        if not phase_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Object not found"
            )
        return phase_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )    