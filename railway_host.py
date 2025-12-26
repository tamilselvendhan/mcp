import asyncio
import json
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.types import Tool, TextContent
from fastapi import FastAPI
from mcp.server.http import HttpServer
from mcp.server import Server
import os

CONNECTION_STRING = os.environ["DATABASE_URL"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)  # Send logs to stderr to avoid JSON parsing issues
    ]
)
logger = logging.getLogger(__name__)

logger.info("=" * 50)
logger.info("MCP SERVER STARTING")
logger.info(f"Python executable: {sys.executable}")
logger.info(f"Python version: {sys.version}")
logger.info(f"psycopg2 version: {psycopg2.__version__}")
logger.info("=" * 50)

# Database configuration
# CONNECTION_STRING = "dbname=mcp user=postgres password=Tamil@5793 host=localhost port=5432"
# CONNECTION_STRING = "dbname=postgres user=postgres password=SbRwfLPFrLP2SDIo host=db.nrckcdciafhjaheiolvm.supabase.co port=5432"
logger.info(f"Connection string configured (password hidden)")

app = FastAPI()
server = Server("employee-data-server")
http_server = HttpServer(server)
logger.info("MCP Server object created")

@app.post("/mcp")
async def mcp_endpoint(request: dict):
    return await http_server.handle_request(request)

def execute_query(query: str):
    """Execute SQL query and return results"""
    logger.info("--- execute_query called ---")
    logger.info(f"Query received: {query}")
    
    try:
        logger.info("Attempting PostgreSQL connection...")
        conn = psycopg2.connect(CONNECTION_STRING)
        logger.info("Connection successful!")
        
        logger.info("Executing query...")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            logger.info(f"Query executed successfully. Rows returned: {len(results)}")
            
            return {
                "success": True,
                "data": [dict(row) for row in results],
                "row_count": len(results)
            }
    except Exception as e:
        logger.error(f"ERROR in execute_query: {str(e)}")
        import traceback
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        if 'conn' in locals():
            logger.info("Closing connection")
            conn.close()

@server.list_tools()
async def list_tools():
    """Define available MCP tools"""
    logger.debug("list_tools() called")
    return [
        Tool(
            name="query_employee_data",
            description="""Query employee data using SQL. 
            
            YOU (Claude) should generate the SQL query based on the user's natural language request.
            
            Database Schema:
            - Table: public.employee
            - Columns: 
              * id (integer) - Employee ID
              * first_name (text) - First name
              * last_name (text) - Last name  
              * email (text) - Email address
              * department (text) - Department name
              * salary (numeric) - Annual salary
            
            Instructions for generating queries:
            1. Analyze the user's natural language question
            2. Generate appropriate SQL SELECT query
            3. ALWAYS use "public.employee" as table name
            4. Add LIMIT clause (max 100 rows unless user specifies)
            5. Only SELECT queries allowed (no INSERT/UPDATE/DELETE)
            
            Examples:
            
            User: "Show all engineers"
            SQL: SELECT * FROM public.employee WHERE department = 'Engineering' LIMIT 100
            
            User: "Who makes over 100k?"
            SQL: SELECT first_name, last_name, department, salary FROM public.employee WHERE salary > 100000 ORDER BY salary DESC LIMIT 100
            
            User: "Average salary by department"
            SQL: SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count FROM public.employee GROUP BY department ORDER BY avg_salary DESC
            
            User: "Top 5 earners"
            SQL: SELECT first_name, last_name, department, salary FROM public.employee ORDER BY salary DESC LIMIT 5
            
            Generate the SQL that best answers the user's question!
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL SELECT query you (Claude) generated based on the user's request"
                    }
                },
                "required": ["sql"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict):
    """Execute the tool"""
    logger.info("*** call_tool invoked ***")
    logger.info(f"Tool name: {name}")
    logger.info(f"Arguments: {arguments}")
    
    if name == "query_employee_data":
        sql = arguments["sql"].strip()
        logger.info(f"SQL query: {sql}")
        
        # Security validation
        sql_upper = sql.upper()
        
        # Only allow SELECT
        if not sql_upper.startswith('SELECT'):
            logger.warning("Query rejected: Not a SELECT statement")
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": False,
                    "error": "Only SELECT queries are allowed"
                })
            )]
        
        # Block dangerous operations
        forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        if any(keyword in sql_upper for keyword in forbidden):
            logger.warning("Query rejected: Contains forbidden operations")
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": False,
                    "error": "Query contains forbidden operations"
                })
            )]
        
        logger.info("Query passed security validation")
        
        # Execute query
        logger.info("Calling execute_query()...")
        result = execute_query(sql)
        result["executed_query"] = sql
        
        logger.debug(f"Result: {result}")
        
        return [TextContent(
            type="text",
            text=json.dumps(result, indent=2, default=str)
        )]
    
    logger.warning(f"Unknown tool: {name}")
    return [TextContent(
        type="text",
        text=json.dumps({"error": f"Unknown tool: {name}"})
    )]

async def main():
    """Run the MCP server"""
    logger.info(">>> Entering main() function <<<")
    from mcp.server.stdio import stdio_server
    
    logger.info("Starting stdio_server...")
    async with stdio_server() as (read_stream, write_stream):
        logger.info("✓ stdio_server initialized")
        logger.info("Running server.run()...")
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    logger.info("__main__ block executed")
    asyncio.run(main())