from fastapi import APIRouter, HTTPException, Request, Depends, status
from models.schemas import Step, StepResponse, File, FileResponse
from database.connection import get_db
from database.steps import read_db_step, check_db_step_exists, create_db_step, delete_db_step, update_db_step, \
                           check_db_step_file_exists, create_db_step_file, delete_db_step_file
from datetime import datetime


# Define the router
router = APIRouter(
    prefix="/steps",
    tags=["steps"],
    responses={404: {"description": "Not found"}},
)

# Read step
@router.get("/{step_id}", response_model=StepResponse, status_code=status.HTTP_200_OK)
async def read_step(step_id: str, conn = Depends(get_db)) -> StepResponse:
    try:
        step_data = read_db_step(step_id, conn)
        print(step_data)
        if not step_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Step not found"
            )
        return step_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching step"
        )
    
# Create step
@router.post("/", response_model=StepResponse, status_code=status.HTTP_201_CREATED)
async def create_step(step: Step, conn = Depends(get_db)):
    try:
        # Check if step exists
        if check_db_step_exists(step.step_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Step with ID {step.step_id} already exists"
            )
        
        # Create step
        step_dict = step.model_dump()
        step_id = create_db_step(step_dict, conn)
        
        return {"step_id": step_id, "description": step.description, "step_type": step.step_type, "notebook": step.notebook, "status": step.status, "created_at": datetime.now(), "created_by": step.created_by}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    
# Delete step
@router.delete("/{step_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_step(step_id: str, conn = Depends(get_db)) -> None:
    try:
        # Check if step exists
        if not check_db_step_exists(step_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Step with ID {step_id} does not exist"
            )

        step_data = delete_db_step(step_id, conn)
        print(step_data)
        if not step_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="step not found"
            )
        return step_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching step"
        )    
    
@router.put("/{step_id}", response_model=StepResponse, status_code=status.HTTP_200_OK)
async def update_step(
    step_id: str, 
    step_update: Step, 
    conn = Depends(get_db)
):
    try:

        # Check if step exists
        if not check_db_step_exists(step_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Step with ID {step_id} does not exist"
            )
        # Convert Pydantic model to dict and remove None values
        update_data = step_update.model_dump(exclude_unset=True)
        print(update_data)
        
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields provided for update"
            )
        
        return update_db_step(step_id, update_data, conn)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    
# Create file in a step
@router.post("/{step_id}/file", response_model=str, status_code=status.HTTP_201_CREATED)
async def create_step_file(step_id: str, file: File, conn = Depends(get_db)):
    try:
        # Check if file exists
        if check_db_step_file_exists(step_id, file.file_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {file.file_id} for Step {step_id} already exists"
            )
        
        # Create step
        step_file_dict = file.model_dump()
        step_file_id = create_db_step_file(step_id, step_file_dict, conn)
        
        # return {"step_id": step_id, "description": step.description, "step_type": step.step_type, "notebook": step.notebook, "status": step.status, "created_at": datetime.now(), "created_by": step.created_by}
        return f"File {file.file_id} for Step {step_id} created successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    
# Delete file from a step
@router.delete("/{step_id}/file/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_step_file(step_id: str, file_id: str, conn = Depends(get_db)):
    try:
        # Check if file exists
        if not check_db_step_file_exists(step_id, file_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {file_id} for Step {step_id} does not exist"
            )
        
        # Delete file from step
        step_file_id = delete_db_step_file(step_id, file_id, conn)
        
        # return {"step_id": step_id, "description": step.description, "step_type": step.step_type, "notebook": step.notebook, "status": step.status, "created_at": datetime.now(), "created_by": step.created_by}
        return f"File {file_id} for Step {step_id} deleted successfully"
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )    