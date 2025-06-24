from fastapi import FastAPI, Request, HTTPException, status, APIRouter, Depends
from routers.objects import router as object_router
from routers.steps import router as step_router
from routers.links import router as link_router
from routers.jobs import router as job_router
from util.files import create_blob_folder, create_blob_url, create_blob_urls_download
from util.notebook import send_email
from models.schemas import FileDataURL, FileURLs
from typing import List
app = FastAPI(
    title="DataTool Server",
    description="DataTool Server")

app.include_router(job_router)
app.include_router(object_router)
app.include_router(step_router)
app.include_router(link_router)




@app.get("/")
def read_root():
    return "Server is running."

@app.post("/uploadurl", response_model=str, status_code=status.HTTP_200_OK)
def test(fileurl: FileDataURL):
    print(fileurl)
    res = create_blob_url(f'{fileurl.folder}/{fileurl.file}')
    print(res)
    return res

@app.post("/downloadurl", response_model=List[FileURLs], status_code=status.HTTP_200_OK)
def test(fileurl: FileDataURL):
    print(fileurl)
    res = create_blob_urls_download(fileurl.folder)
    # print(res)
    return res

@app.get("/test")
def test():
    res = create_blob_folder("test/")
    return res

@app.get("/testurl/{path}", response_model=str, status_code=status.HTTP_200_OK)
def test(path: str):
    print(path)
    res = create_blob_url(path)
    print(res)
    return res

@app.post("/testhook", status_code=status.HTTP_200_OK)
async def testhook(data: Request):
    # Accept any JSON payload
    payload = await data.json()
    print(f'Run ID: {payload["run"]["run_id"]}, Event: {payload["event_type"]}')
    # update_db_job_step_notebook_status(payload["run"]["run_id"], payload["event_type"])
    return

@app.post("/sendmail", status_code=status.HTTP_200_OK)
async def sendmail(data: str):
    send_email(data)
    return
    