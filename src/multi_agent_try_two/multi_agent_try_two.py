from openai import OpenAI
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

# Set up OpenAI API Key
client = OpenAI(api_key="my_api_key") # add your openai api key here 

# Set up the database connection using SQLAlchemy
DATABASE_URL = "postgresql+psycopg2://postgres:password@localhost:5432/database_name" # add your credentials here
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def get_detailed_schema():
    inspector = inspect(engine)
    schema = ""
    
    # Get all table names
    for table_name in inspector.get_table_names():
        schema += f"Table: {table_name}\n"
        schema += "Columns:\n"
        
        # Get columns for each table
        for column in inspector.get_columns(table_name):
            column_info = f"- {column['name']} ({column['type']})"
            if column.get("primary_key"):
                column_info += " [Primary Key]"
            if column.get("foreign_keys"):
                fk = list(column["foreign_keys"])[0]
                column_info += f" [Foreign Key to {fk.target_fullname}]"
            schema += column_info + "\n"

            # Get sample data
            query = text(f"SELECT * FROM {table_name} LIMIT 1;")
            with engine.connect() as connection:
                result = connection.execute(query)
                for row in result:
                    schema += f"Sample Data: {row}\n"
        
        # Get foreign keys
        fkeys = inspector.get_foreign_keys(table_name)
        if fkeys:
            schema += "Foreign Keys:\n"
            for fk in fkeys:
                schema += f"- {fk['constrained_columns']} references {fk['referred_table']}({fk['referred_columns']})\n"
        
        # Add indexes
        indexes = inspector.get_indexes(table_name)
        if indexes:
            schema += "Indexes:\n"
            for index in indexes:
                schema += f"- {index['name']} (Columns: {index['column_names']})\n"
        
        schema += "\n"
    return schema.strip()

def preprocess_user_input(user_input, schema_description):
    """First agent that processes and customizes the user input"""
    prompt = f"""
    You are an expert in understanding database queries. Your task is to:
    1. Analyze the user's natural language input
    2. Understand the database schema provided
    3. Reformulate the query to be more specific and database-friendly
    4. Include relevant table names and column names from the schema
    5. Make the query more precise and unambiguous
    
    Database Schema:
    {schema_description}

    User's Original Query: {user_input}

    Provide a reformulated, database-specific version of the query that includes relevant table and column names.
    Only return the reformulated query, nothing else.
    """
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Simplify and transform the user query into a database-friendly natural language format by using database schema.For Example- User Input: What is the address with the shortest street name? Output: Retrieve the address where the length of the 'address' column (street name) is the shortest from the 'address' table."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3
    )
    
    return response.choices[0].message.content.strip()

def generate_sql_query(preprocessed_input, schema_description):
    """Second agent that converts the preprocessed input into SQL"""
    prompt = f"""
    You are a SQL expert. Based on the detailed database schema and preprocessed query, generate a PostgreSQL query.
    Only return the SQL query, nothing else.
    If the query is not relevant to the database schema, request a relevant query instead.

    Database Schema:
    {schema_description}

    Preprocessed Query: {preprocessed_input}
    """
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": """You are an expert postgres SQL query generator.You need to provide a SQL query based on the preprocessed query and database schema.For Example- Preprocessed Query:Retrieve the address where the length of the 'address' column (street name) is the shortest from the 'address' table. 
             Output:SELECT address
                    FROM address
                    ORDER BY LENGTH(address) ASC
                    LIMIT 1;"""},
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )
    
    query = response.choices[0].message.content.strip()
    
    # Clean up any markdown formatting
    if query.startswith("```sql"):
        query = query.split("```sql")[1].split("```")[0].strip()
    elif query.startswith("```"):
        query = query.split("```")[1].split("```")[0].strip()
        
    return query

def main():
    print("Welcome to the Try_Two!")
    print("Ask your questions in natural language. Type 'exit' or 'bye' to quit.")
    
    # Get schema description
    schema_description = get_detailed_schema()

    
    while True:
        user_input = input("\nEnter your query in natural language: ").strip()
        
        if user_input.lower() in ["exit", "bye", "quit"]:
            print("Goodbye!")
            break
            
        try:
            # Stage 1: Preprocess the user input
            # print("\nPreprocessing query...")
            preprocessed_query = preprocess_user_input(user_input, schema_description)
            # print(f"Preprocessed Query: {preprocessed_query}")
            print("\n===== Preprocessed Query =====")
            print(preprocessed_query)
            print("==============================\n")

            # Stage 2: Generate SQL
            print("\nGenerating SQL...")
            sql_query = generate_sql_query(preprocessed_query, schema_description)
            print(f"Generated SQL Query:\n{sql_query}")
            
            # Execute the query
            result = session.execute(text(sql_query)).fetchall()
            
            # Display results
            if result:
                print("\nQuery Results:")
                for row in result:
                    print(row)
            else:
                print("\nNo results found for the query.")
                
        except Exception as e:
            print(f"\nError: {e}")

if __name__ == "__main__":
    main()
