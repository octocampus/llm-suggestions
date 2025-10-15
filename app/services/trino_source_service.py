from typing import List, Dict, Any, Optional
from loguru import logger
from app.core.logging import logger as app_logger
from app.services.postgres_service import get_postgres_service


class TrinoDataFetchService:
    
    def __init__(self, trino_cursor):
        
        self.cursor = trino_cursor
        self.postgres_service = get_postgres_service()
    
    def get_table_sample_data(
        self, 
        source_key: str, 
        schema_name: str, 
        table_name: str, 
        limit: int = 100
    ) -> Dict[str, Any]:

        query = f"""
            SELECT * 
            FROM "{source_key}"."{schema_name}"."{table_name}"
            LIMIT {limit}
        """
        try:
            app_logger.info(f"Fetching sample data from {source_key}.{schema_name}.{table_name}")
            self.cursor.execute(query)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in self.cursor.description]
            
            # Fetch all rows
            rows = self.cursor.fetchall()
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))
            
            return {
                "source_key": source_key,
                "schema_name": schema_name,
                "table_name": table_name,
                "columns": columns,
                "row_count": len(data),
                "rows": data
            }
            
        except Exception as e:
            msg = f"Error fetching data from {source_key}.{schema_name}.{table_name}: {str(e)}"
            app_logger.error(msg)
            raise Exception(msg)
    
    def get_tables_from_discovery(
        self, 
        source_id: str, 
        schema_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:

        try:
            # Get discovery data from PostgreSQL
            discovery_results = self.postgres_service.query_discovery_data(
                schema=schema_filter if schema_filter else "",
                source_id=source_id
            )
            
            if not discovery_results:
                app_logger.warning(f"No discovery data found for source_id: {source_id}")
                return []
            
            tables_info = []
            
            # Parse discovery data to extract table information
            for record in discovery_results:
                schemas = record.get("schemas", [])
                
                for schema in schemas:
                    schema_name = schema.get("schema_name")
                    
                    # Apply schema filter if provided
                    if schema_filter and schema_name != schema_filter:
                        continue
                    
                    tables = schema.get("tables", [])
                    
                    for table in tables:
                        table_info = {
                            "source_id": source_id,
                            "schema_name": schema_name,
                            "table_name": table.get("table_name"),
                            "columns": table.get("columns", [])
                        }
                        tables_info.append(table_info)
            
            app_logger.info(f"Found {len(tables_info)} tables for source_id: {source_id}")
            return tables_info
            
        except Exception as e:
            msg = f"Error getting tables from discovery: {str(e)}"
            app_logger.error(msg)
            raise Exception(msg)
    
    def fetch_all_tables_sample_data(
        self,
        source_key: str,
        source_id: str,
        schema_filter: Optional[str] = None,
        limit_per_table: int = 100
    ) -> List[Dict[str, Any]]:

        try:
            # Get all tables from discovery
            tables_info = self.get_tables_from_discovery(source_id, schema_filter)
            
            if not tables_info:
                return []
            
            results = []
            
            for table_info in tables_info:
                try:
                    # Fetch sample data for each table
                    sample_data = self.get_table_sample_data(
                        source_key=source_key,
                        schema_name=table_info["schema_name"],
                        table_name=table_info["table_name"],
                        limit=limit_per_table
                    )
                    results.append(sample_data)
                    
                except Exception as table_error:
                    app_logger.error(
                        f"Failed to fetch data from {table_info['table_name']}: {str(table_error)}"
                    )
                    # Continue with other tables even if one fails
                    continue
            
            app_logger.info(f"Successfully fetched data from {len(results)} tables")
            return results
            
        except Exception as e:
            msg = f"Error fetching all tables sample data: {str(e)}"
            app_logger.error(msg)
            raise Exception(msg)
    
    def get_table_row_count(
        self, 
        source_key: str, 
        schema_name: str, 
        table_name: str
    ) -> int:

        query = f"""
            SELECT COUNT(*) as row_count
            FROM "{source_key}"."{schema_name}"."{table_name}"
        """
        
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            msg = f"Error getting row count from {source_key}.{schema_name}.{table_name}: {str(e)}"
            app_logger.error(msg)
            raise Exception(msg)


def create_trino_data_fetch_service(trino_cursor):
    return TrinoDataFetchService(trino_cursor)