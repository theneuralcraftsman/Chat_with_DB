from flask import Flask, request, jsonify, g
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
import openai 


app = Flask(__name__)

CORS(app, origins="*")  # Enable CORS for all routes


openai.api_key = 'OPENAI_API_KEY'



# Global variable to store the database connection pool
db_pool = None

# Function to create the database connection pool
def create_db_pool(host, user, password, database, pool_size=5):
    global db_pool
    db_config = {
        "host": host,
        "user": user,
        "password": password,
        "database": database
    }
    db_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                          pool_size=pool_size,
                                                          **db_config)




def get_db_connection():
    if 'db_connection' not in g:
        g.db_connection = db_pool.get_connection()
    return g.db_connection



# Route to handle the POST request to connect to the database
@app.route('/connect_old', methods=['POST'])
def connect_to_database_old():
    try:
        data = request.json
        host = data['host']
        user = data['user']
        password = data['password']
        database = data['database']
        create_db_pool(host, user, password, database)
        return jsonify({"message": "Connected to the database successfully!"}), 200
    except Exception as e:
        return jsonify({"error": f"Error connecting to the database: {e}"}), 500




def execute_query(query):
    try:
        connection = g.db_connection
        cursor = connection.cursor()
        cursor.execute(query)
        
        if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            connection.commit()
        
        if query.strip().upper().startswith("INSERT"):
            return None, None  # No column names or data to return for INSERT queries
        else:
            column_names = [desc[0] for desc in cursor.description]  # Get column names
            data = cursor.fetchall()  # Get data
            cursor.close()
            return column_names, data
    
    except Exception as e:
        error_message = f"Error executing query: {e}"
        # You can log the error or handle it as needed
        return None, error_message



# Route to close the database connection
@app.route('/close_connection', methods=['POST'])
def close_connection():
    try:
        if 'db_connection' in g:
            db_connection = g.pop('db_connection', None)
            if db_connection is not None:
                db_connection.close()
        global db_pool
        db_pool = None
        return jsonify({"message": "Database connection closed successfully!"}), 200
    except Exception as e:
        return jsonify({"error": f"Error closing database connection: {e}"}), 500







# Function to get table names from the database
def get_table_names():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()
        table_names = [table[0] for table in tables]
        return table_names
    except Exception as e:
        raise Exception(f"Error getting table names: {e}")


# Function to get table schemas from the database
def get_table_schemas_mysql():
    table_names = get_table_names()
    table_schemas = {}
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        counter = 0  # Initialize a counter
        for table_name in table_names:
            if counter >= 10:  # Check if counter exceeds 10
                break
            cursor.execute(f"SHOW CREATE TABLE {table_name}")
            schema = cursor.fetchone()
            table_schemas[table_name] = schema[1]
            counter += 1  # Increment the counter
        cursor.close()
        return table_schemas
    except Exception as e:
        raise Exception(f"Error getting table schemas: {e}")



## Function to extract sql query
def extract_sql_query(response_text):
    # Simple approach: Extract SQL query from a known response pattern
    # This example assumes the SQL query is enclosed in a markdown code block (```sql)
    # Adjust the logic based on the actual format of your OpenAI response
    if '```sql' in response_text:
        start = response_text.find('```sql') + len('```sql\n')
        end = response_text.find('```', start)
        sql_query = response_text[start:end].strip()
        return True, sql_query
    return False, response_text  # Fallback to returning the full response if pattern not found




# Function used to convert list of table data to string so that it can be feeded to LLM
# start and end index define the range of entries we are going to include
def format_data(data, start_index, end_index):
    # Slice the data to include only items within the specified range
    data_range = data[start_index:end_index]
    
    # Get the maximum length of each column within the range
    max_lengths = [max(len(str(item)) for item in row) for row in zip(*data_range)]
    
    # Format each row within the range with proper alignment
    formatted_rows = [' '.join(f"{str(item):<{length}}" for item, length in zip(row, max_lengths)) for row in data_range]
    
    # Combine formatted rows into a string
    result_string = '\n'.join(formatted_rows)
    
    return result_string




# Function to convert retrieved data into Markdown table format
def convert_to_markdown_table(column_names, data):
    if column_names is not None and data is not None:
        # Construct the header row
        header_row = "| " + " | ".join(column_names) + " |\n"
        # Construct the separator row
        separator_row = "| " + " | ".join(["---"] * len(column_names)) + " |\n"
        # Construct the data rows
        data_rows = ""
        for row in data:
            data_rows += "| " + " | ".join(str(cell) for cell in row) + " |\n"
        # Combine all parts to form the Markdown table
        markdown_table = header_row + separator_row + data_rows
        return markdown_table
    else:
        return "No data to display."







# Providing prompt to GPT for response generation
def gen_response(request_string, user_query, table_schema, query_result):
    try:
        response = openai.ChatCompletion.create(
        model = "gpt-3.5-turbo",
        messages = [
                {"role": "system", "content": f"You are a helpful technical SQL assistant. {request_string}"},
                {"role": "system", "content": f"This is the table info and schema in this database: '{table_schema}'"},
                {"role": "user", "content": f"Use these query results to analyze: {query_result}"},
                {"role": "user", "content": f"User Query: '{user_query}'"}

            ]
        )

        return response
    except Exception as e:
        raise Exception(f"Error generating response: {e}")





# This is for testing purposes
@app.route('/gen', methods=['POST'])
# Providing prompt to GPT for response generation
def gen():
    try:
        # Get the user query from the request data
        user_input = request.json.get('query')


        response = openai.ChatCompletion.create(
        model = "gpt-3.5-turbo",
        messages = [
                {"role": "system", "content": f"You are a helpful bot"},
                {"role": "user", "content": f"User Query: '{user_input}'"}

            ]
        )
        
        return jsonify({"message": f"{response.choices[0].message['content'].strip()}"}), 200
    except Exception as e:
        raise Exception(f"Error generating response: {e}")







# Route to handle the POST request to generate a response based on a user query
@app.route('/generate_response', methods=['POST'])
def generate_response():
    try:
        # Get the user query from the request data
        user_input = request.json.get('query')
        show_result_summary = True

        # Ensure a query was provided
        if not user_input:
            return jsonify({"error": "No query provided"}), 400

            
        table_schemas = get_table_schemas_mysql()

        table_schemas_str = ""
        count = 0
        for table_name, schema in table_schemas.items():
            count+=1
            table_schemas_str += f"Table: {count}. {table_name}\nSchema: {schema}\n\n"
        table_schemas_str += f"There are total of {count} tables in database" 
        

        db_type = "MySQL"

        #return jsonify({"message": f"{table_schemas_str}"}), 200

        # Get the response from first phase query (To get SQL query which needs to be processed)
        sql_query = gen_response(f"Translate this English sentence to {db_type} query. Do not ask users to provide SQL query. If you're unsure of the table or any such inputs dont give any SQL query just ask the user to provide that information. Also if there are multiple tables with similar names prompt user to choose the table from which user wants answers also display the similar table names. But do try to follow the history to know the table",user_input, table_schemas_str,"")
            
        # Check if there is SQL query present and the SQL query 
        isSQL, sql_query = extract_sql_query(sql_query.choices[0].message['content'].strip())
        #return jsonify({"message": f"{sql_query}"}), 200
        # If SQL query is present we process it to fetch values 
        if isSQL:
            columns, data = execute_query(sql_query)

            #return jsonify({"message": f"{data}"}), 200      
            # Formats the sql query output into string format so that it can be feeded into LLM
            # It takes two more parameters with data i.e. start index and end index
            # This is used to limit the amount of results to be included 
            results_string = format_data(data, 0, 10)
            #print(results_string)
            #jsonify({"message": f"{results_string}"}), 200

            # Converts the query output to markdown table to display in message
            markdown_table = convert_to_markdown_table(columns, data)

            # Show the query result summarization using GPT is check
            if show_result_summary:
                output = gen_response("According to the SQL response answer user query do not return SQL query only describe the query output. Use the table schema info to answer questions if asked questions related to that information. If there are table values first describe a little then display values in list order. If table information is missing respond it's empty or can't be fetched:",user_input, table_schemas_str, results_string)
                output = output.choices[0].message['content'].strip()

                #output = f"""SQL Query:\n```sql\n{sql_query}\n```\n\nAnswer:\n\n{output}\n\n\nTabular: \n{markdown_table}"""
                return jsonify({"message": f"{output}\n", "code":f"\n```sql\n{sql_query}\n```", "table":f"\n{markdown_table}"}), 200
            else:
                #output = f"""SQL Query:\n```sql\n{sql_query}\n```\n\n\nTabular: \n{markdown_table}"""
                return jsonify({"message": f"", "code":f"\n```sql\n{sql_query}\n```", "table":f"\n{markdown_table}"}), 200

        # Else show the non sql response 
        else:
            output = f"""{sql_query}""" 
            #output = f"""SQL Query:\n```sql\n{sql_query}\n```\n\nAnswer:\n\n{output}\n\n\nTabular: \n{markdown_table}"""
            return jsonify({"message": f"{output}"}), 200
    except Exception as e:
        return jsonify({"error": f"Error executing query: {e}"}), 500





@app.route('/')
def hello_world():
	return jsonify({"message":"""*Hello* World *italic* **bold**\n\n""",
                 "code":"""\n\n```sql
SELECT * FROM employees
WHERE department = 'Sales'
ORDER BY last_name ASC;
```""",
                 "table":"""
\n\n| Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 | Column 7 | Column 8 | Column 9 | Column 10 |
|----------|----------|----------|----------|----------|----------|----------|----------|----------|-----------|
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |"""})




@app.route('/check', methods=['POST'])
def get_username():
    try:
        # Get JSON data from the request
        data = request.get_json()

        # Extract the user_name from the JSON data
        user_name = data.get('user_name')

        if user_name:
            # Return the user_name as username in JSON response
            return jsonify({"message":"""*Hello* World *italic* **bold**\n\n""",
                            "code":"""\n\n```sql
SELECT * FROM employees
WHERE department = 'Sales'
ORDER BY last_name ASC;
```""",
                 "table":"""
\n\n| Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 | Column 7 | Column 8 | Column 9 | Column 10 |
|----------|----------|----------|----------|----------|----------|----------|----------|----------|-----------|
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   | Data 10   |"""})
        else:
            # Handle the case where user_name is not provided
            return jsonify({"error": "user_name not provided"}), 400

    except Exception as e:
        return jsonify({"error": "Exception", "message": str(e)}), 500
















if __name__ == "__main__":
    app.run()
