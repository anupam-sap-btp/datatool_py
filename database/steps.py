from psycopg2 import sql
from fastapi import HTTPException, status
from database.connection import get_db_connection
from models.schemas import StepResponse
from datetime import datetime


def read_db_step(step_id: str, conn) -> StepResponse:
    cursor = None
    try:
        cursor = conn.cursor()
            
        query = sql.SQL('SELECT * FROM public."StepMaster" WHERE step_id = {}').format(
            sql.Literal(step_id))
        cursor.execute(query)
        step = cursor.fetchone()
        
        if step is None:
            raise NotFoundError(f"Step with id {step_id} not found.")
        return {
            "step_id": step[0],
            "step_type": step[5],
            "description": step[6],
            "notebook": step[1],
            "status": step[2],
            "created_at": step[3],
            "created_by": step[4]
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching step"
        )         
    finally:
        if cursor:
            cursor.close()    

def check_db_step_exists(step_id: str, conn):
    """Check if an step with given ID exists"""
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT 1 FROM public."StepMaster" WHERE step_id = {}').format(
            sql.Literal(step_id))
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking step existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking step existence"
        )
    finally:
        if cursor:
            cursor.close()

def create_db_step(step: dict, conn):
    """Create a new step in database"""
    cursor = None
    try:
        cursor = conn.cursor()
        # print(step)
        insert_query = sql.SQL("""
            INSERT INTO public."StepMaster" (step_id, step_type, description, notebook, status, created_by)
            VALUES ({step_id}, {step_type}, {description}, {notebook}, {status}, {created_by})
        """).format(
            step_id=sql.Literal(step["step_id"]),
            step_type=sql.Literal(step["step_type"]),
            description=sql.Literal(step["description"]),
            notebook=sql.Literal(step["notebook"]),
            status=sql.Literal(step["status"]),
            created_by=sql.Literal(step["created_by"]),
        )
        # print(insert_query)
        cursor.execute(insert_query)
        result = conn.commit()
        print(result)
        return step["step_id"]    
    except Exception as e:
        print(f"Error creating step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating step"
        )
    finally:
        if cursor:
            cursor.close()

def delete_db_step(step_id: str, conn):
    """Delete an step from database"""
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Delete step
        delete_query = sql.SQL('DELETE FROM public."StepMaster" WHERE step_id = {}').format(
            sql.Literal(step_id))
        cursor.execute(delete_query)
        conn.commit()

        return {"message": f"Step with ID {step_id} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting step: {e}")
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting step"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_step(step_id: str, step_data: dict, conn):
    """Update an existing step in database"""
    cursor = None
    try:
        cursor = conn.cursor()
                
        # Build dynamic update query
        set_clauses = []
        values = {}
        
        for field, value in step_data.items():
            if value is not None:  # Skip None values to allow partial updates
                set_clauses.append(sql.SQL("{} = {}").format(
                    sql.Identifier(field),
                    sql.Literal(value)
                ))
        
        if not set_clauses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid fields provided for update"
            )
        
        update_query = sql.SQL("""
            UPDATE public."StepMaster" 
            SET {set_clauses}
            WHERE step_id = {step_id}
            RETURNING step_id, step_type, description, notebook, status, created_at, created_by
        """).format(
            set_clauses=sql.SQL(', ').join(set_clauses),
            step_id=sql.Literal(step_id)
        )
        
        cursor.execute(update_query)
        updated_step = cursor.fetchone()
        conn.commit()
        
        if not updated_step:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update step"
            )
        
        return {
            "step_id": updated_step[0],
            "step_type": updated_step[1],
            "description": updated_step[2],
            "notebook": updated_step[3],
            "status": updated_step[4],
            "created_at": updated_step[5],
            "created_by": updated_step[6]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating step: {e}")
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating step"
        )
    finally:
        if cursor:
            cursor.close()

def check_db_step_file_exists(step_id: str, file_id: str, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT 1 FROM public."StepFiles" WHERE step_id = {} AND file_id = {}').format(
            sql.Literal(step_id), sql.Literal(file_id))
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking the file in a step: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking the file in a step"
        )
    finally:
        if cursor:
            cursor.close()

def create_db_step_file(step_id: str, file: dict, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        insert_query_file = sql.SQL("""
            INSERT INTO public."StepFiles" (step_id, file_id, file_ext, file_type, file_category, description, created_by)
            VALUES ({step_id}, {file_id}, {file_ext}, {file_type}, {file_category}, {description}, {created_by})
        """).format(
            step_id=sql.Literal(step_id),
            file_id=sql.Literal(file["file_id"]),
            file_ext=sql.Literal(file["file_ext"]),
            file_type=sql.Literal(file["file_type"]),
            file_category=sql.Literal(file["file_category"]),
            description=sql.Literal(file["description"]),
            created_by=sql.Literal(file["created_by"]),
        )

        cursor.execute(insert_query_file)
        column_data = []
        for col in file["columns"]:
            column_data.append((step_id, file["file_id"], col["column_name"], col["column_tech_name"], col["active"], col["description"], file["created_by"]))

        insert_query_columns = sql.SQL("""
            INSERT INTO public."StepFileColumns" (step_id, file_id, column_name, column_tech_name, active, description, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        

        cursor.executemany(insert_query_columns, column_data)

        result = conn.commit()
        print(result)
        return step_id    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error creating object phase: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating object phase"
        )
    finally:
        if cursor:
            cursor.close()

def delete_db_step_file(step_id: str, file_id: str, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Delete file from step
        delete_query_file = sql.SQL('DELETE FROM public."StepFiles" WHERE step_id = {} AND file_id = {}').format(
            sql.Literal(step_id), sql.Literal(file_id))
        cursor.execute(delete_query_file)

        # Delete file columns from step
        delete_query_cols = sql.SQL('DELETE FROM public."StepFileColumns" WHERE step_id = {} AND file_id = {}').format(
            sql.Literal(step_id), sql.Literal(file_id))
        cursor.execute(delete_query_cols)
        
        result = conn.commit()
        print(result)
        return step_id    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error creating object phase: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating object phase"
        )
    finally:
        if cursor:
            cursor.close()


class NotFoundError(Exception):
    pass