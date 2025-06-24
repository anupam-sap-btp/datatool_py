from fastapi import APIRouter, HTTPException, Request, Depends, status
from models.schemas import JobCreate, JobCreateResponse, JobStatusResponse, JobStatus, JobHeaderResponse, JobStepResponse, JobFullResponse
from database.connection import get_db
from database.objects import check_db_object_exists
from database.jobs import create_db_job, check_db_job_exists, update_db_job_status, check_db_job_step_active, \
                          update_db_job_step_status, update_db_job_step_notebook_status, read_db_job, read_db_job_step, read_db_jobfull
from datetime import datetime
from typing import Optional, Literal, List

# Define the router
router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=JobCreateResponse, status_code=status.HTTP_201_CREATED)  
async def create_job(job: JobCreate, conn = Depends(get_db)):
    try:
        # Check if object exists
        if not check_db_object_exists(job.object_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Link already exists"
            )

        # Create job
        job_id = create_db_job(job, conn)
        
        return {"job_id": job_id} #job_id
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )
    
@router.post("/{job_id}/{job_status}", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)  
async def control_job(job_id: int, 
                      job_status: Literal["prepare", "activate", "cancel", "moveNext", "forceComplete", "hold", "resume"], 
                      conn = Depends(get_db)):
    try:
        # Check if job exists
        if not check_db_job_exists(job_id, conn):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Job {job_id} does not exist"
            )

        # Create status update
        job_ret = update_db_job_status(job_id, job_status, conn)
        
        return job_ret
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )
    
@router.post("/{job_id}/counter/{counter}/subcounter/{subcounter}/{step_status}", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)  
async def control_job_step(job_id: int, counter: int, subcounter: int,
                      step_status: Literal["complete", "cancel", "hold", "resume"], 
                      conn = Depends(get_db)):
    try:
        # Check if job step exists and is active
        step_status_db = check_db_job_step_active(job_id, counter, subcounter, conn)
        if not step_status_db:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Job {job_id}, step {counter}-{subcounter} does not exist"
            )
        elif step_status_db[0] != 'active':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Job {job_id} step {counter}-{subcounter} is not active"
            )
        # Create status update
        job_ret = update_db_job_step_status(job_id, counter, subcounter, step_status, conn)
        
        return job_ret
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error processing step status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing step status"
        )    
    
@router.post("/notebook", status_code=status.HTTP_200_OK)
async def process_notebook(data: Request, conn = Depends(get_db)):
    try:
        # Accept any JSON payload
        payload = await data.json()
        run_id = payload.get("run" , {}).get("run_id", "")
        event = payload.get("event_type", "")

        # print(f'Run ID: {payload["run"]["run_id"]}, Event: {payload["event_type"]}')
        # update_db_job_step_notebook_status(payload["run"]["run_id"], payload["event_type"], conn)
        print(f'Run ID: {run_id}, Event: {event}')
        if run_id != "" and event != "":
            update_db_job_step_notebook_status(run_id, event, conn)
        
        return
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error Processing Webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error Processing Webhook"
        ) 
    
# Read job
@router.get("/{job_id}", response_model=JobHeaderResponse, status_code=status.HTTP_200_OK)
async def read_job(job_id: int, conn = Depends(get_db)):
    try:
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        job_data = read_db_job(job_id, conn)
        print(job_data)
        if not job_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found"
            )
        return job_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching Job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching Job"
        )    
    
@router.get("/{job_id}/counter/{counter}/subcounter/{subcounter}", response_model=JobStepResponse, status_code=status.HTTP_200_OK)
async def read_job_step(job_id: int, counter: int, subcounter: int, conn = Depends(get_db)):
    try:
        step_data = read_db_job_step(job_id, counter, subcounter, conn)
        print(step_data)
        if not step_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id}-{counter}-{subcounter} not found"
            )
        return step_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching Job Step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching Job Step"
        )  
    
@router.get("/readfull/{job_id}", response_model=JobFullResponse, status_code=status.HTTP_200_OK)
async def read_jobfull(job_id: int, conn = Depends(get_db)):
    try:
        job_data = read_db_jobfull(job_id, conn)
        # print(job_data)
        if not job_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found"
            )
        return job_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching Job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching Job"
        )      