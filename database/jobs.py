from psycopg2 import sql
from fastapi import HTTPException, status
from database.connection import get_db_connection
from models.schemas import JobCreate, JobStepResponse, JobHeaderResponse, JobFullResponse
from util.files import create_blob_folder
from util.notebook import run_notebook_job, send_email
from datetime import datetime
from psycopg2.extras import RealDictCursor

def create_db_job(job: JobCreate, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            sql.SQL('INSERT INTO public."Jobs" (object_id, description, status, created_by) VALUES (%s, %s, %s, %s) RETURNING job_id'),
            (job.object_id, job.description, 'created', job.created_by)
        )

        job_id = cursor.fetchone()[0]

        cursor.execute('SELECT phase_num, step_id, step_seq, phase_name, step_type, step_description, notebook \
                        FROM public."object_phase_step_v" WHERE object_id = %s', (job.object_id,))
        step_details = cursor.fetchall()

        step_id_list = list(set([row[1] for row in step_details]))
        cursor.execute(
                'SELECT step_id, file_id, file_type, file_category, description, validation_req \
                      FROM public."StepFiles" WHERE step_id = ANY(%s)',
                (step_id_list,)  
            )
        step_file_details = cursor.fetchall()

        step_data = []
        step_file_data = []
        for idx, row in enumerate(step_details):
            step_data.append((job_id, idx+1, 1, row[0], row[1], row[2], row[3], row[4], row[5], row[6], 'created', job.created_by))
            step_files = list(filter(lambda t: t[0] == row[1], step_file_details))
            for step_file in step_files:
                step_file_data.append((job_id, idx+1, 1, step_file[1], step_file[2], step_file[3], step_file[4], step_file[5], job.created_by))

        insert_query_steps = sql.SQL("""
            INSERT INTO public."JobSteps" (job_id, job_counter, job_subcounter, phase_num, step_id, step_seq, phase_name, step_type, description, notebook, status, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        cursor.executemany(insert_query_steps, step_data)


        insert_query_step_files = sql.SQL("""
            INSERT INTO public."JobStepFiles" (job_id, job_counter, job_subcounter, file_id, file_type, 
                                         file_category, description, validation_req, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        cursor.executemany(insert_query_step_files, step_file_data)
        
        conn.commit()
        return job_id
    except HTTPException:
        raise        
    except Exception as e:
        conn.rollback()
        print(f"Error creating job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating job"
        )
    finally:
        if cursor:
            cursor.close()

def check_db_job_exists(job_id: int, conn):
    """Check if a job with given ID exists"""
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT 1 FROM public."Jobs" WHERE job_id = {}').format(
            sql.Literal(job_id))
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking job existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking job existence"
        )
    finally:
        if cursor:
            cursor.close()

def check_db_job_step_active(job_id: int, counter: int, subcounter: int, conn):
    """Check if a job step is active"""
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT status FROM public."JobSteps" WHERE job_id = {} AND job_counter = {} AND job_subcounter = {}').format(
            sql.Literal(job_id), sql.Literal(counter), sql.Literal(subcounter))
        cursor.execute(check_query)
        return cursor.fetchone() 
    except Exception as e:
        print(f"Error checking job existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking job existence"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_job_status(job_id: int, job_status: str, conn):
    """Update job status after performing validations"""
    cursor = None
    try:
        cursor = conn.cursor()
        fetch_job_query = sql.SQL('SELECT status, current_phase, current_step, status_text FROM public."Jobs" WHERE job_id = {}').format(
            sql.Literal(job_id))
        
        fetch_job_steps_query = sql.SQL('SELECT job_counter, job_subcounter, phase_num, step_id, step_seq, \
                                        phase_name, step_type, status, status_text, description, notebook, notebook_job_status \
                        FROM public."JobSteps" WHERE job_id = {} ORDER BY job_counter, job_subcounter' ).format(
            sql.Literal(job_id))
        
        cursor.execute(fetch_job_query)
        job_data =  cursor.fetchone() 

        cursor.execute(fetch_job_steps_query)
        job_steps_data =  cursor.fetchall() 

        if job_data[0] == 'created' and job_status == 'activate':
            if job_steps_data[0][6] == 'automatic':
                #exception to be raised as it needs a prepare phase
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Job cannot be set to activate without a prepare phase"
                )
            else:
                create_blob_folder(f'Job-{job_id}/')
                create_blob_folder(f'Job-{job_id}/Step-{job_steps_data[0][0]}-{job_steps_data[0][1]}-{job_steps_data[0][3]}/')
                # Update status to active, update current phase and current step, create the main and step folder
                # Update fist step status to active
                update_job_status_table(job_id, 'active', job_steps_data[0][2], job_steps_data[0][3], job_data[1], job_data[2], 'Job Activated', conn)
                update_job_step_status_table(job_id, 'active', job_steps_data[0], job_steps_data[0], conn)
                return {
                        "job_id": job_id,
                        "job_status": "active",
                        "current_phase": job_steps_data[0][2],
                        "current_phase_name": job_steps_data[0][5],
                        "current_step": job_steps_data[0][3],
                        "current_step_desc": job_steps_data[0][9]
                        }
        elif job_data[0] == 'created' and job_status == 'prepare':
            if job_steps_data[0][6] == 'automatic':
                create_blob_folder(f'Job-{job_id}/')
                # Update status to prepare, do not update current phase or step, create the main folder
                return {
                        "job_id": job_id,
                        "job_status": "prepare"
                        }
            else:
                create_blob_folder(f'Job-{job_id}/')
                create_blob_folder(f'Job-{job_id}/Step-{job_steps_data[0][0]}-{job_steps_data[0][1]}-{job_steps_data[0][3]}/')
                # Update status to active, update current phase and current step, create the main and step folder
                # Update fist step status to active
                update_job_status_table(job_id, 'active', job_steps_data[0][2], job_steps_data[0][3], job_data[1], job_data[2], 'Job Activated', conn)
                update_job_step_status_table(job_id, 'active', job_steps_data[0], job_steps_data[0], conn)
                return {
                        "job_id": job_id,
                        "job_status": "active",
                        "current_phase": job_steps_data[0][2],
                        "current_phase_name": job_steps_data[0][5],
                        "current_step": job_steps_data[0][3],
                        "current_step_desc": job_steps_data[0][9]
                        }
        elif job_data[0] == 'prepare' and job_status != 'activate':
            #exception to be raised as it needs a prepare phase
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only activate is possible after prepare phase"
            )       
        else:
            update_job_query = sql.SQL('UPDATE public."Jobs" SET status = {}, current_phase = {}, current_step = {}, status_text = {} WHERE job_id = {}').format(
                sql.Literal(job_status), sql.Literal(job_data[1]), sql.Literal(job_data[2]), sql.Literal(job_data[3]), sql.Literal(job_id))
            cursor.execute(update_job_query)
            conn.commit()
            pass
    except Exception as e:
        print(f"Error checking job existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking job existence"
        )
    finally:
        if cursor:
            cursor.close()

def update_job_status_table(job_id: int, job_status: str, current_phase: int, current_step: str, previous_phase: int, previous_step: str, status_text: str, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        update_job_query = sql.SQL('UPDATE public."Jobs" SET status = {}, current_phase = {}, current_step = {}, \
                                   previous_phase = {}, previous_step = {}, status_text = {} WHERE job_id = {}').format(
            sql.Literal(job_status), sql.Literal(current_phase), sql.Literal(current_step), sql.Literal(previous_phase), \
                sql.Literal(previous_step), sql.Literal(status_text), sql.Literal(job_id))
        cursor.execute(update_job_query)
        conn.commit()
    except Exception as e:
        print(f"Error updating job status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating job status"
        )
    finally:
        if cursor:
            cursor.close()

def update_job_step_status_table(job_id: int, step_status: str, job_step_next: tuple, job_step_prev: tuple, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        run_id = None
        # In case of an automatic step with Notebook, run databricks and store the run_id
        if job_step_next[6] == 'automatic' and job_step_next[10] != '':
            run_id = run_notebook_job(job_id, job_step_next)
            # run_id = 110
        
        # TBD - Take action for last step
        # In case we are running the first step, update the first step only
        if job_step_next == job_step_prev:
            update_job_step_query = sql.SQL('UPDATE public."JobSteps" SET status = {}, notebook_run_id = {} WHERE \
                                            job_id = {} AND job_counter = {} AND job_subcounter = {}').format(
            sql.Literal(step_status), sql.Literal(run_id), sql.Literal(job_id), sql.Literal(job_step_next[0]), sql.Literal(job_step_next[1]))
            cursor.execute(update_job_step_query)
            conn.commit()
        else:
        # If we are running for steps in the middle, update the current step and the previous step    
            update_job_step_query = sql.SQL('UPDATE public."JobSteps" SET status = {}, notebook_run_id = {} WHERE \
                                            job_id = {} AND job_counter = {} AND job_subcounter = {}').format(
            sql.Literal(step_status), sql.Literal(run_id), sql.Literal(job_id), sql.Literal(job_step_next[0]), sql.Literal(job_step_next[1]))

            update_job_step_prev_query = sql.SQL('UPDATE public."JobSteps" SET status = {} WHERE \
                                            job_id = {} AND job_counter = {} AND job_subcounter = {}').format(
            sql.Literal('completed'), sql.Literal(job_id), sql.Literal(job_step_prev[0]), sql.Literal(job_step_prev[1]))
            cursor.execute(update_job_step_query)
            cursor.execute(update_job_step_prev_query)
            conn.commit()

        
    except Exception as e:
        print(f"Error updating job step status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating job step status"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_job_step_status(job_id: int, counter: int, subcounter: int, step_status: str, conn):
    # TBD - Implement logic for complete, cancel, hold, resume based on step_status
    cursor = None
    try:
        cursor = conn.cursor()
        # Get all the steps in sorted way
        fetch_job_steps_query = sql.SQL('SELECT job_counter, job_subcounter, phase_num, step_id, step_seq, \
                                        phase_name, step_type, status, status_text, description, notebook, notebook_job_status \
                        FROM public."JobSteps" WHERE job_id = {} ORDER BY job_counter, job_subcounter' ).format(
            sql.Literal(job_id))
        cursor.execute(fetch_job_steps_query)
        job_steps_data =  cursor.fetchall() 
        current_step_index = next(i for i, step in enumerate(job_steps_data) if step[0] == counter and step[1] == subcounter)
        current_step_data = job_steps_data[current_step_index]

        if len(job_steps_data) > current_step_index + 1:
            # Take action when not last step
            next_step_data = job_steps_data[current_step_index + 1]

            create_blob_folder(f'Job-{job_id}/Step-{next_step_data[0]}-{next_step_data[1]}-{next_step_data[3]}/')
            update_job_status_table(job_id, 'active', next_step_data[2], next_step_data[3], current_step_data[2], current_step_data[3], f"Step {current_step_data[3]}completed", conn)
            update_job_step_status_table(job_id, 'active', next_step_data, current_step_data, conn)
            return {
                    "job_id": job_id,
                    "job_status": "active",
                    "current_phase": next_step_data[2],
                    "current_phase_name": next_step_data[5],
                    "current_step": next_step_data[3],
                    "current_step_desc": next_step_data[9]
                    }
        else:
            # Take action when last step
            update_job_status_table(job_id, 'completed', 0, '', current_step_data[2], current_step_data[3], f"Job completed", conn)
            update_job_step_status_table(job_id, 'completed', current_step_data, current_step_data, conn)
            return {
                    "job_id": job_id,
                    "job_status": "completed",
                    "current_phase": 0,
                    "current_phase_name": "",
                    "current_step": "",
                    "current_step_desc": ""
                    }

        # TBD - Validation for automatic steps
        # TBD - Validation for manual steps
    except Exception as e:
        print(f"Error updating job step status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating job step status"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_job_step_notebook_status(run_id: int, event_type: str, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        # Get the step for the notbook
        fetch_job_steps_query = sql.SQL('SELECT job_counter, job_subcounter, phase_num, step_id, step_seq, \
                                        phase_name, step_type, status, status_text, description, notebook, notebook_job_status, job_id \
                        FROM public."JobSteps" WHERE notebook_run_id = {} ORDER BY job_counter, job_subcounter' ).format(
            sql.Literal(run_id))
        cursor.execute(fetch_job_steps_query)
        notebook_steps_data =  cursor.fetchone() 
        job_id = notebook_steps_data[12]
        job_counter = notebook_steps_data[0]
        job_subcounter = notebook_steps_data[1]
        step_current_status = notebook_steps_data[7]

        # Get the job
        fetch_job_query = sql.SQL('SELECT status, current_phase, current_step, status_text FROM public."Jobs" WHERE job_id = {}').format(
            sql.Literal(job_id))
        cursor.execute(fetch_job_query)
        job_data =  cursor.fetchone() 
        
        # On successful run, 
        #       update the status of the step, 
        #       update the current/previous phase/step of the job, 
        #       update the status of next step
        #       if it is the last step, update the job status
        notebook_status = event_type.split('_', 1)[1]
        if notebook_status == 'success':
            step_status = 'completed'
        elif notebook_status == 'failure':
            step_status = 'failed'
        else:
            step_status = step_current_status

        update_job_step_query = sql.SQL('UPDATE public."JobSteps" SET status = {}, notebook_job_status = {} WHERE \
                                        notebook_run_id = {}').format(sql.Literal(step_status), sql.Literal(notebook_status), sql.Literal(run_id))
        cursor.execute(update_job_step_query)
        conn.commit()

        if notebook_status == 'success' and step_current_status != 'completed':
            update_db_job_step_status(job_id, job_counter, job_subcounter, 'Active', conn)
            send_email(f"Step {notebook_steps_data[3]} completed successfully for job {job_id}.")
    except Exception as e:
        print(f"Error updating job step status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating job step status"
        )
    finally:
        if cursor:
            cursor.close()

def read_db_job(job_id: int, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        fetch_job_query = sql.SQL('SELECT job_id, object_id, status, current_phase, current_step, previous_phase, previous_step, description, status_text, \
                                  created_at, created_by FROM public."Jobs" WHERE job_id = {}').format(
            sql.Literal(job_id))
        cursor.execute(fetch_job_query)
        job_data =  cursor.fetchone() 
        
        if job_data is None:
            return None
        else:
            return {"job_id": job_data[0], "object_id": job_data[1], "description": job_data[7], "status": job_data[2], "status_text": job_data[8], 
                "current_phase": job_data[3], "current_step": job_data[4], "previous_phase": job_data[5], "previous_step": job_data[6], 
                "created_at": job_data[9], "created_by": job_data[10]}
    except Exception as e:
        print(f"Error fetching Job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching JOb"
        )
    finally:
        if cursor:
            cursor.close()

def read_db_job_step(job_id: int, counter: int, subcounter: int, conn):
    cursor = None
    try:
        columns = JobStepResponse.model_fields.keys()
        # cursor = conn.cursor()
        # fetch_job_step_query = sql.SQL('SELECT job_id, job_counter, job_subcounter, phase_num, step_id, step_seq, \
        #                                 phase_name, step_type, status, status_text, description, notebook, notebook_status, notebook_run_id, notebook_job_status, \
        #                                 prereq_status, person_responsible, step_folder, created_at, created_by \
        #                 FROM public."JobSteps" WHERE job_id = {} and job_counter = {} and job_subcounter = {} ORDER BY job_counter, job_subcounter' ).format(
        #     sql.Literal(job_id), sql.Literal(counter), sql.Literal(subcounter))
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        fetch_job_step_query = sql.SQL('SELECT {} \
                        FROM public."JobSteps" WHERE job_id = {} and job_counter = {} and job_subcounter = {} ORDER BY job_counter, job_subcounter' ).format(
            sql.SQL(', ').join(map(sql.Identifier, columns)), sql.Literal(job_id), sql.Literal(counter), sql.Literal(subcounter))

        cursor.execute(fetch_job_step_query)
        job_data =  cursor.fetchone() 
        print(job_data)
        return JobStepResponse.model_validate(job_data) if job_data else None
        
        # if job_data is None:
        #     return None
        # else:
        #     return {"job_id": job_data[0], "job_counter": job_data[1], "job_subcounter": job_data[2], "phase_num": job_data[3], "phase_name": job_data[6], 
        #         "step_id": job_data[4], "step_seq": job_data[5], "status": job_data[8], "status_text": job_data[9], 
        #         "step_type": job_data[7], "description": job_data[10], "notebook": job_data[11], "notebook_status": job_data[12], 
        #         "notebook_run_id": job_data[13], "notebook_job_status": job_data[14], "prereq_status": job_data[15], 
        #         "person_responsible": job_data[16], "step_folder": job_data[17], "created_at": job_data[18], "created_by": job_data[19]}
    except Exception as e:
        print(f"Error fetching Job Step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching JOb Step"
        )
    finally:
        if cursor:
            cursor.close()            

def read_db_jobfull(job_id: int, conn):
    cursor = None
    try:
        columns_header = JobHeaderResponse.model_fields.keys()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        fetch_job_query = sql.SQL('SELECT {} FROM public."Jobs" WHERE job_id = {}').format(
            sql.SQL(', ').join(map(sql.Identifier, columns_header)), sql.Literal(job_id))
        cursor.execute(fetch_job_query)
        job_data =  cursor.fetchone() 
        
        columns_steps = JobStepResponse.model_fields.keys()
        fetch_job_step_query = sql.SQL('SELECT {} \
                        FROM public."JobSteps" WHERE job_id = {} ORDER BY job_counter, job_subcounter' ).format(
            sql.SQL(', ').join(map(sql.Identifier, columns_steps)), sql.Literal(job_id))

        
        cursor.execute(fetch_job_step_query)
        job_data_steps =  cursor.fetchall() 

        return JobFullResponse( **job_data, steps=job_data_steps)
    except Exception as e:
        print(f"Error fetching Job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching JOb"
        )
    finally:
        if cursor:
            cursor.close()            