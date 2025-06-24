from psycopg2 import sql
from fastapi import HTTPException, status
from database.connection import get_db_connection
from models.schemas import ObjectResponse, PhaseAdd
from datetime import datetime


def read_db_object(object_id: str, conn) -> ObjectResponse:
    cursor = None
    try:
        cursor = conn.cursor()
            
        query = sql.SQL('SELECT * FROM public."ObjectMaster" WHERE object_id = {}').format(
            sql.Literal(object_id))
        cursor.execute(query)
        object = cursor.fetchone()
        
        if object is None:
            raise NotFoundError(f"Object with id {object_id} not found.")
        return {
            "id": object[0],
            "object_id": object[1],
            "description": object[2],
            "object_type": object[3],
            "volume": object[4],
            "created_at": object[5],
            "created_by": object[6]
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )         
    finally:
        if cursor:
            cursor.close()    

def check_db_object_exists(object_id: str, conn):
    """Check if an object with given ID exists"""
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT 1 FROM public."ObjectMaster" WHERE object_id = {}').format(
            sql.Literal(object_id))
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking object existence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking object existence"
        )
    finally:
        if cursor:
            cursor.close()

def create_db_object(object: dict, conn):
    """Create a new object in database"""
    cursor = None
    try:
        cursor = conn.cursor()
        # print(object)
        insert_query = sql.SQL("""
            INSERT INTO public."ObjectMaster" (object_id, description, object_type, volume, created_by)
            VALUES ({object_id}, {description}, {object_type}, {volume}, {created_by})
        """).format(
            object_id=sql.Literal(object["object_id"]),
            description=sql.Literal(object["description"]),
            object_type=sql.Literal(object["object_type"]),
            volume=sql.Literal(object["volume"]),
            created_by=sql.Literal(object["created_by"]),
        )
        # print(insert_query)
        cursor.execute(insert_query)
        result = conn.commit()
        print(result)
        return object["object_id"]    
    except Exception as e:
        print(f"Error creating object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating object"
        )
    finally:
        if cursor:
            cursor.close()


def delete_db_object(object_id: str, conn):
    """Delete an object from database"""
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Delete object
        delete_query = sql.SQL('DELETE FROM public."ObjectMaster" WHERE object_id = {}').format(
            sql.Literal(object_id))
        cursor.execute(delete_query)
        conn.commit()
        print('test')
        return {"message": f"Object with ID {object_id} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting object: {e}")
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting object"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_object(object_id: str, object_data: dict, conn):
    """Update an existing object in database"""
    cursor = None
    try:
        cursor = conn.cursor()
                
        # Build dynamic update query
        set_clauses = []
        values = {}
        
        for field, value in object_data.items():
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
            UPDATE public."ObjectMaster" 
            SET {set_clauses}
            WHERE object_id = {object_id}
            RETURNING id, object_id, description, object_type, volume, created_at, created_by
        """).format(
            set_clauses=sql.SQL(', ').join(set_clauses),
            object_id=sql.Literal(object_id)
        )
        
        cursor.execute(update_query)
        updated_object = cursor.fetchone()
        conn.commit()
        
        if not updated_object:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update object"
            )
        
        return {
            "id": updated_object[0],
            "object_id": updated_object[1],
            "description": updated_object[2],
            "object_type": updated_object[3],
            "volume": updated_object[4],
            "created_at": updated_object[5],
            "created_by": updated_object[6]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating object: {e}")
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating object"
        )
    finally:
        if cursor:
            cursor.close()


def check_db_object_phase_exists(object_id: str, phase_num: int, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        check_query = sql.SQL('SELECT 1 FROM public."ObjectPhases" WHERE object_id = {} AND phase_num = {}').format(
            sql.Literal(object_id), sql.Literal(phase_num))
        cursor.execute(check_query)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking the phase in an object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking the phase in an object"
        )
    finally:
        if cursor:
            cursor.close()

def create_db_object_phase(object_id: str, phase: dict, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO public."ObjectPhases" (object_id, phase_num, name, description, status)
            VALUES ({object_id}, {phase_num}, {name}, {description}, {status})
        """).format(
            object_id=sql.Literal(object_id),
            phase_num=sql.Literal(phase["phase_num"]),
            name=sql.Literal(phase["name"]),
            description=sql.Literal(phase["description"]),
            status=sql.Literal(phase["status"]),
        )

        cursor.execute(insert_query)
        result = conn.commit()
        print(result)
        return object_id    
    except Exception as e:
        print(f"Error creating object phase: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating object phase"
        )
    finally:
        if cursor:
            cursor.close()


def delete_db_object_phase(object_id: str, phase_num: int, conn):
    cursor = None
    try:
        cursor = conn.cursor()
        delete_query = sql.SQL("""
            DELETE FROM public."ObjectPhases" WHERE object_id = {} AND phase_num = {}
        """).format(
            sql.Literal(object_id), sql.Literal(phase_num)
        )

        cursor.execute(delete_query)
        conn.commit()
        
        return {"message": f"Phase {phase_num} for Object with ID {object_id} deleted successfully"}
    except Exception as e:
        print(f"Error deleting object phase: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting object phase"
        )
    finally:
        if cursor:
            cursor.close()

def update_db_object_phase(object_id: str, phase_num: int, phase_data: dict, conn):
    cursor = None
    try:
        cursor = conn.cursor()
                
        # Build dynamic update query
        set_clauses = []
        values = {}
        
        for field, value in phase_data.items():
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
            UPDATE public."ObjectPhases" 
            SET {set_clauses}
            WHERE object_id = {object_id} AND phase_num = {phase_num}
            RETURNING object_id, phase_num, name, description, status
        """).format(
            set_clauses=sql.SQL(', ').join(set_clauses),
            object_id=sql.Literal(object_id),
            phase_num=sql.Literal(phase_num)
        )
        
        cursor.execute(update_query)
        updated_phase = cursor.fetchone()
        conn.commit()
        
        if not updated_phase:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update object phase"
            )
        
        return {
            "object_id": updated_phase[0],
            "phase_num": updated_phase[1],
            "name": updated_phase[2],
            "description": updated_phase[3],
            "status": updated_phase[4]
        }
    except Exception as e:
        print(f"Error deleting object phase: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting object phase"
        )
    finally:
        if cursor:
            cursor.close()

def read_db_object_phase(object_id: str, phase_num: int, conn) -> PhaseAdd:
    cursor = None
    try:
        cursor = conn.cursor()
            
        query = sql.SQL('SELECT * FROM public."ObjectPhases" WHERE object_id = {} AND phase_num = {}').format(
            sql.Literal(object_id),
            sql.Literal(phase_num))
        cursor.execute(query)
        phase = cursor.fetchone()
        
        if phase is None:
            raise NotFoundError(f"Phase {phase_num} for Object id {object_id} not found.")
        return {

            "phase_num": object[1],
            "name": object[2],
            "description": object[3],
            "status": object[4]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching object: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching object"
        )         
    finally:
        if cursor:
            cursor.close()    

class NotFoundError(Exception):
    pass