from psycopg2 import sql
from fastapi import HTTPException, status
from database.connection import get_db_connection
from models.schemas import LinkStep, LinkDetail
from datetime import datetime
from psycopg2.extras import RealDictCursor
from typing import Optional, Literal, List

def check_db_linkstep_exists(link: LinkStep, conn):
    """Check if a link exists"""
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL("""
            SELECT 1 FROM public."ObjectsPhasesSteps" WHERE object_id = {} AND phase_num = {} 
                              AND step_id = {} AND step_seq = {}
        """).format(
            sql.Literal(link.object_id),
            sql.Literal(link.phase_num),
            sql.Literal(link.step_id),
            sql.Literal(link.step_seq),
            )
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except HTTPException:
        raise    
    except Exception as e:
        print(f"Error checking object existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking object existence"
        )
    finally:
        if cursor:
            cursor.close()

def create_db_linkstep(link: LinkStep, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        query = sql.SQL("""
            INSERT INTO public."ObjectsPhasesSteps" (object_id, phase_num, step_id, step_seq, created_by)
            VALUES ({object_id}, {phase_num}, {step_id}, {step_seq}, {created_by})
        """).format(
            object_id=sql.Literal(link.object_id),
            phase_num=sql.Literal(link.phase_num),
            step_id=sql.Literal(link.step_id),
            step_seq=sql.Literal(link.step_seq),
            created_by=sql.Literal(link.created_by),
        )
        cursor.execute(query, (link.object_id, link.phase_num, link.step_id, link.step_seq))
        conn.commit()
    except HTTPException:
        raise    
    except Exception as e:
        print(f"Error creating link: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating link"
        )
    finally:
        if cursor:
            cursor.close()  


def get_db_linkstep(object_id: str, conn) -> List[LinkDetail]:
    cursor = None
    try:
        columns_det = LinkDetail.model_fields.keys()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        fetch_job_query = sql.SQL('SELECT {} FROM public."object_phase_step_v" WHERE object_id = {}').format(
            sql.SQL(', ').join(map(sql.Identifier, columns_det)), sql.Literal(object_id))
        cursor.execute(fetch_job_query)
        link_data =  cursor.fetchall() 
        

        return [LinkDetail( **record) for record in link_data]


    except Exception as e:
        print(f"Error fetching Object Links: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching Object Links"
        )
    finally:
        if cursor:
            cursor.close()            