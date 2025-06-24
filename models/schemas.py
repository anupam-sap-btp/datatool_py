from pydantic import BaseModel
from typing import Optional, Literal, List
from datetime import datetime

# Define data models using Pydantic
class Object(BaseModel):
    object_id: str
    description: Optional[str] = None
    object_type: str
    volume: Optional[int] = None
    created_by: str

class ObjectResponse(BaseModel):
    id: int  # Primary key
    object_id: str
    description: Optional[str] = None
    object_type: str
    volume: Optional[int] = None
    created_at: datetime
    created_by: str

class ObjectCreate(Object):
    id: int
    name: str
    price: float

class Item(ObjectCreate):
    in_stock: bool = True

class ObjectUpdate(Object):
    pass    

class PhaseAdd(BaseModel):
    phase_num: int
    name: str
    description: str
    status: Literal["draft", "final", "inactive"]

class Step(BaseModel):
    step_id: str
    step_type: str
    description: Optional[str] = None
    notebook: str
    status: Literal["draft", "final", "inactive"]
    created_by: str

class StepResponse(BaseModel):
    step_id: str
    step_type: str
    description: Optional[str] = None
    notebook: str
    status: Literal["draft", "final", "inactive"]
    created_at: datetime
    created_by: str

class FileColumn(BaseModel):
    column_name: str
    column_tech_name: Optional[str]
    active: bool
    description: Optional[str]
class File(BaseModel):
    file_id: str
    file_ext: Optional[Literal["csv", "xlsx"]]
    file_type: Literal["input", "output"]
    file_category: Literal["data", "mapping"]
    description: Optional[str]
    validation_req: bool
    created_at: datetime
    created_by: str
    columns: List[FileColumn]

class FileResponse(BaseModel):
    step_id: str
    file_id: str
    file_ext: Optional[Literal["csv", "xlsx"]]
    file_type: Literal["input", "output"]
    file_category: Literal["data", "mapping"]
    description: Optional[str]
    validation_req: bool
    created_at: datetime
    created_by: str
    columns: List[FileColumn]    

class LinkStep(BaseModel):
    object_id: str
    phase_num: int
    step_id: str
    step_seq: int
    created_by: str

class LinkDetail(BaseModel):
    object_id: str
    phase_num: int
    step_id: str
    step_seq: int
    phase_name: str
    notebook: Optional[str]
    step_type: str
    step_description: str

class JobCreate(BaseModel):
    description: str
    object_id: str
    created_by: str

class JobCreateResponse(BaseModel):
    job_id: int    

class JobStatusResponse(BaseModel):
    job_id: int
    job_status: str
    current_phase: Optional[int]
    current_phase_name: Optional[str]
    current_step: Optional[str]
    current_step_desc: Optional[str]

class JobStatus(BaseModel):
    job_status_action: Literal["activate", "cancel", "moveNext", "forceComplete", "hold", "resume"]

class JobHeaderResponse(BaseModel):
    job_id: int
    object_id: str
    description: str
    status: str
    status_text: Optional[str]
    current_phase: Optional[int]
    current_step: Optional[str]
    previous_phase: Optional[int]
    previous_step: Optional[str]
    created_at: datetime
    created_by: str

class JobStepResponse(BaseModel):
    job_id: int
    job_counter: int
    job_subcounter: int
    phase_num: int
    phase_name: str
    step_id: str
    step_seq: int
    status: Optional[str] = None
    status_text: Optional[str] = None
    step_type: str
    description: Optional[str] = None
    notebook: Optional[str] = None
    notebook_status: Optional[str] = None
    notebook_run_id: Optional[int] = None
    notebook_job_status: Optional[str] = None
    prereq_status: Optional[str] = None
    person_responsible: Optional[str] = None
    step_folder: Optional[str] = None
    # created_at: datetime
    created_by: Optional[str] = None

class JobFullResponse(JobHeaderResponse):
    steps: List[JobStepResponse]

class FileDataURL(BaseModel):
    folder: str    
    file: str

class FileURLs(BaseModel):
    blob: str    
    file: str
    url: str    