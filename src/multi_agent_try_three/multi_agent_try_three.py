from openai import OpenAI
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

client = OpenAI(api_key="my_api_key") # add your openai api key here

DATABASE_URL = "postgresql+psycopg2://postgres:password@localhost:5432/database_name" # add your credentials here
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def get_detailed_schema():
    inspector = inspect(engine)
    schema = ""
    for table_name in inspector.get_table_names():
        schema += f"Table: {table_name}\nColumns:\n"
        for column in inspector.get_columns(table_name):
            column_info = f"- {column['name']} ({column['type']})"
            if column.get("primary_key"):
                column_info += " [Primary Key]"
            schema += column_info + "\n"
    return schema.strip()

def agent1_normalize_query(user_input, schema_description):
    prompt = f"""
    You are an advanced AI that preprocesses user queries for a structured database. 

    **Your Objectives:**
    1. **Normalize user input**  
       - Convert queries into structured, database-friendly format.  
       - Maintain **case sensitivity** for names, table fields, and entity names.  
       - Replace **synonyms with database terms** (e.g., "flicks" → "films").  
       - Ensure **first names, last names, and other data match their correct format** (e.g., "PENELOPE GUINESS" → "Penelope Guiness").  

    2. **Interpret queries using general knowledge**  
       - If a user asks about a concept **not directly in the database**, infer the best way to retrieve it.  
       - **Example:**  
         - "Which European countries are in the database?" → Use **knowledge of world geography** to filter countries by continent.  
         - "Which actors won an award?" → The database may not store awards, but you can find **popular actors with high movie counts**.  
         - "What are the most rented movies?" → Check films with the highest **rental count**.

    3. **Ensure clarity for the SQL generator (Agent2)**  
       - Convert vague queries into precise ones.
       - Identify missing details and infer them intelligently.

    **Database Schema (Reference for Normalization):**  
    {schema_description}

    🔹 **User Query:** {user_input}  

    🔹 **Provide the normalized query (without extra explanation).**
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return response.choices[0].message.content.strip()


def agent2_generate_sql(user_input, schema_description):
    prompt = f"""
    You are a SQL expert. Based on the detailed database schema provided, generate a PostgreSQL query for the user's natural language input. 
    Only return the SQL query and nothing else.

    **Database Schema:**
    {schema_description}

    **User Query:** {user_input}
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )

    query = response.choices[0].message.content.strip()

    # Extract SQL query if wrapped in markdown
    if "```sql" in query:
        query = query.split("```sql")[1].split("```")[0].strip()
    
    # Ensure the response is a valid SQL statement before execution
    allowed_commands = ["SELECT", "INSERT", "UPDATE", "DELETE"]
    if not any(query.strip().upper().startswith(cmd) for cmd in allowed_commands):
        raise ValueError("Generated response is not a valid SQL query.")

    return query

def agent3_format_results(query_result):
    prompt = f"""
    You are an AI that formats SQL query results into a user-friendly response.
    Convert the following SQL result into a readable text summary.
    
    **SQL Query Result:**
    {query_result}
    
    **User-Friendly Summary:**
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return response.choices[0].message.content.strip()


def main():
    print("Welcome! Type your query in natural language. Type 'exit' to quit.")
    schema_description = get_detailed_schema()
    while True:
        user_input = input("\nEnter your query: ").strip()
        if user_input.lower() in ["exit", "bye", "quit"]:
            print("Goodbye!")
            break
        try:
            # Step 1: Normalize User Query (Agent 1)
            simplified_query = agent1_normalize_query(user_input, schema_description)

            # Step 2: Generate SQL Query (Agent 2)
            sql_query = agent2_generate_sql(simplified_query, schema_description)

            # Print the SQL Query
            print(f"\n🔹 **Generated SQL Query:**\n{sql_query}")

            # Step 3: Execute SQL Query and Fetch Results
            result = session.execute(text(sql_query)).fetchall()

            # Step 4: Format the Results for User (Agent 3)
            formatted_result = agent3_format_results(result)

            # Print Final Output
            print(f"\n🔹 **Formatted Output:**\n{formatted_result}")

        except Exception as e:
            session.rollback()
            print(f"\n❌ **Error:** {e}")

if __name__ == "__main__":
    main()
