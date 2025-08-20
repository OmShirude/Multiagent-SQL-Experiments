from openai import OpenAI
import psycopg2

# OpenAI API Key
client = OpenAI(api_key="my_api_key") # add your openai api key here 

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="database_name", # change this
    user="postgres",
    password="password", # change this
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Get Database Schema
def get_database_schema(cursor):
    cursor.execute("""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public';
    """)
    schema = cursor.fetchall()
    schema_description = "\n".join(
        [f"Table: {row[0]}, Column: {row[1]}, Type: {row[2]}" for row in schema]
    )
    return schema_description

# Generate SQL Query
def generate_query(user_input, schema_description):
    prompt = f"""
    You are a SQL expert. Based on the database schema provided, generate a PostgreSQL query for the user's natural language input. 
    Only return the SQL query and nothing else.
    Also if the user's natural language input is not relevant to the database, then request the user to provide the input relevant to the database. 

    Database Schema:
    {schema_description}

    User Query: {user_input}
    """
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "You are a helpful assistant."},
                  {"role": "user", "content": prompt}],
        temperature=0
    )
    query = response.choices[0].message.content.strip()

    # Remove surrounding text (if any)
    query = query.strip()
    if query.startswith("```sql"):
        query = query.split("```sql")[1].split("```")[0].strip()
    return query

# Execute Query
def execute_query(cursor, query):
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        return f"Error executing query: {e}"

# Main Function
if __name__ == "__main__":
    schema_description = get_database_schema(cursor)
    user_input = input("Enter your query in natural language: ")
    sql_query = generate_query(user_input, schema_description)
    print("Generated SQL Query:", sql_query)
    
    results = execute_query(cursor, sql_query)
    print("Query Results:", results)

# Close connection
cursor.close()
conn.close()
