"""
MySQL Database Configuration and Connection Service
Handles SSH tunnel and MySQL database connections for employee data integration
"""
import os
import pymysql
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from typing import Dict, List, Any, Optional
import logging
from contextlib import contextmanager
import paramiko

from app.core.settings import settings

logger = logging.getLogger(__name__)

class MySQLService:
    """MySQL database service with SSH tunnel support"""
    
    def __init__(self):
        self.ssh_host = settings.mysql_ssh_host
        self.ssh_port = settings.mysql_ssh_port
        self.ssh_username = settings.mysql_ssh_user
        self.ssh_key_path = settings.mysql_ssh_key_path
        
        self.mysql_host = settings.mysql_host
        self.mysql_port = settings.mysql_port
        self.mysql_database = settings.mysql_database
        self.mysql_username = settings.mysql_user
        self.mysql_password = settings.mysql_password
        
        self.ssh_tunnel = None
        self.connection = None
        
    def test_ssh_connection(self) -> bool:
        """Test SSH connection to remote server"""
        try:
            # Load SSH private key
            private_key = paramiko.RSAKey.from_private_key_file(self.ssh_key_path)
            
            ssh_tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=private_key,
                remote_bind_address=(self.mysql_host, self.mysql_port)
            )
            ssh_tunnel.start()
            ssh_tunnel.close()
            logger.info("SSH connection test successful")
            return True
        except Exception as e:
            logger.error(f"SSH connection test failed: {e}")
            return False
    
    def test_mysql_connection(self) -> bool:
        """Test MySQL database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("MySQL connection test successful")
                    # With DictCursor, result is a dictionary with column name as key
                    return result.get('1') == 1
        except Exception as e:
            logger.error(f"MySQL connection test failed: {e}")
            return False
    
    @contextmanager
    def get_connection(self):
        """Context manager for MySQL database connection with SSH tunnel"""
        ssh_tunnel = None
        connection = None
        
        try:
            # Load SSH private key
            private_key = paramiko.RSAKey.from_private_key_file(self.ssh_key_path)
            
            # Create SSH tunnel
            ssh_tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=private_key,
                remote_bind_address=(self.mysql_host, self.mysql_port)
            )
            ssh_tunnel.start()
            
            # Connect to MySQL through SSH tunnel
            connection = pymysql.connect(
                host='127.0.0.1',
                port=ssh_tunnel.local_bind_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            
            yield connection
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
            if ssh_tunnel:
                ssh_tunnel.close()
    
    def get_employee_tables(self) -> List[str]:
        """Get list of employee-related tables in the database"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = 'up_users'
                """, (self.mysql_database,))
                result = cursor.fetchall()
                return [row.get('table_name') or row.get('TABLE_NAME') for row in result]
    
    def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_key, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.mysql_database, table_name))
                return cursor.fetchall()
    
    def get_all_employees(self, table_name: str = None) -> List[Dict[str, Any]]:
        """Get all employees from SQL database"""
        if not table_name:
            # Try to find the employee table automatically
            tables = self.get_employee_tables()
            if not tables:
                raise ValueError("No employee tables found in database")
            table_name = tables[0]  # Use the first employee table found
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name}")
                return cursor.fetchall()
    
    def get_employee_by_code(self, employee_code: str, table_name: str = None) -> Optional[Dict[str, Any]]:
        """Get specific employee by their code"""
        if not table_name:
            tables = self.get_employee_tables()
            if not tables:
                raise ValueError("No employee tables found in database")
            table_name = tables[0]
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Try different possible column names for employee code
                possible_code_columns = ['kekaemployeecode', 'employee_code', 'emp_code', 'code']
                
                for col_name in possible_code_columns:
                    try:
                        cursor.execute(f"SELECT * FROM {table_name} WHERE {col_name} = %s", (employee_code,))
                        return cursor.fetchone()
                    except:
                        continue
                
                return None
    
    def get_permit_files(self, table_name: str = None) -> List[Dict[str, Any]]:
        """Get permit files from SQL database"""
        if not table_name:
            # Try to find permit files table
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND (table_name LIKE '%%permit%%' OR table_name LIKE '%%file%%' OR table_name = 'permits')
                    """, (self.mysql_database,))
                    result = cursor.fetchall()
                    if not result:
                        raise ValueError("No permit files tables found in database")
                    table_name = result[0].get('table_name') or result[0].get('TABLE_NAME')
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name}")
                return cursor.fetchall()
    
    def get_permit_by_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get permit by file_id (id column in permits table)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Query permits table by id
                    cursor.execute("SELECT * FROM permits WHERE id = %s", (file_id,))
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Found permit with id={file_id} in MySQL")
                    else:
                        logger.warning(f"No permit found with id={file_id} in MySQL")
                    return result
        except Exception as e:
            logger.error(f"Error querying permit by id: {e}")
            return None
    
    def get_permit_by_address(self, address: str) -> Optional[Dict[str, Any]]:
        """Get permit by address (partial match)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Query permits table by address with LIKE for partial match
                    cursor.execute("SELECT * FROM permits WHERE address LIKE %s LIMIT 1", (f"%{address}%",))
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Found permit with address matching '{address}' in MySQL: id={result.get('id')}")
                    else:
                        logger.warning(f"No permit found with address matching '{address}' in MySQL")
                    return result
        except Exception as e:
            logger.error(f"Error querying permit by address: {e}")
            return None
    
    def get_permits_by_address(self, address: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get multiple permits by address (partial match) - returns list for multiple matches"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Query permits table by address with LIKE for partial match
                    cursor.execute("SELECT * FROM permits WHERE address LIKE %s LIMIT %s", (f"%{address}%", limit))
                    results = cursor.fetchall()
                    logger.info(f"Found {len(results)} permits matching address '{address}' in MySQL")
                    return results
        except Exception as e:
            logger.error(f"Error querying permits by address: {e}")
            return []

# Global MySQL service instance
mysql_service = MySQLService()
