import os
import sqlite3
import streamlit as st
from openai import OpenAI
import pandas as pd

st.title('🔍 Query Your Data')

# Assuming you've set your OPENAI_API_KEY in your environment variables,
# otherwise set it directly with openai.api_key = "your-api-key"
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

# List all SQLite databases in the /databases/ directory
databases = [db for db in os.listdir('databases/') if db.endswith('.db')]

# Create a dropdown menu in the sidebar for database selection
selected_db = st.sidebar.selectbox('Select a database:', databases)

def get_schema(db_path):
    """Extract the schema (DDL) from an SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
    schema = "\n".join([row[0] for row in cursor.fetchall()])
    cursor.close()
    conn.close()
    return schema

def adjust_sql_for_sqlite(sql_query):
    """Adjusts an SQL query to be more compatible with SQLite."""
    adjusted_query = sql_query.replace("ILIKE", "LIKE")
    adjusted_query = adjusted_query.replace("TRUE", "1").replace("FALSE", "0")
    # Add more adjustments here as needed
    return adjusted_query

def generate_response(input_text, selected_db):
    db_schema = get_schema(f'databases/{selected_db}')
    
    prompt = f"""
### Task
Generate a SQL query to answer [QUESTION]{input_text}[/QUESTION]

### Database Schema
The query will run on a database with the following schema:
{db_schema}

### Answer
Given the database schema, here is the SQL query that [QUESTION]{input_text}[/QUESTION]
[SQL]

"""
    
    response = client.chat.completions.create(
        model="sql",  # Adjust according to your model
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.5,
        stream=True,
    )
    spinner = st.spinner("Generating SQL query...")

    expander = st.expander("SQL Query", expanded=True)
    with expander:
        stream = st.write_stream(response)
    sql_query = stream

    # Adjust the SQL query for SQLite compatibility
    adjusted_sql_query = adjust_sql_for_sqlite(sql_query)
    
    # Execute the adjusted SQL query and return the result
    conn = sqlite3.connect(f'databases/{selected_db}')
    cursor = conn.cursor()
    try:
        cursor.execute(adjusted_sql_query)  # Use the adjusted SQL query here
        rows = cursor.fetchall()
        
        # Convert the query results into a DataFrame for display below the expander
        if rows:
            columns = [description[0] for description in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.write(df)  # Display the DataFrame below the expander
        else:
            st.info("Query executed successfully, but no data was returned.")
    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
    finally:
        cursor.close()
        conn.close()


with st.form('my_form'):
    text = st.text_area('Enter text:', 'Who won the most gold medals in 2012?')
    submitted = st.form_submit_button('Submit')
    if submitted:
        generate_response(text, selected_db)
