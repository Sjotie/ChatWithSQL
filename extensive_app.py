import os
import sqlite3
import streamlit as st
from openai import OpenAI
import pandas as pd

st.title('🔍 Query Your Data')

# Initialize or load the selected database in session state
if 'selected_db' not in st.session_state:
    st.session_state.selected_db = None

# Assuming you've set your OPENAI_API_KEY in your environment variables,
# otherwise set it directly with openai.api_key = "your-api-key"
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

databases = [db for db in os.listdir('databases/') if db.endswith('.db')]

# Update session state upon database selection
def update_selected_db():
    st.session_state.selected_db = st.session_state.selected_db
    
st.session_state.selected_db = st.sidebar.selectbox('Select a database:', databases, on_change=update_selected_db)


if st.session_state.selected_db in databases:
    default_index = databases.index(st.session_state.selected_db)
else:
    default_index = 0  # Fallback to the first database if not found or not set


def get_schema(db_path):
    """Extract the schema (DDL) from an SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    schema = ""
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall()]
        schema += f"\nTable: {table}\nColumns: {', '.join(columns)}\n"
    cursor.close()
    conn.close()
    return schema

# Function to display table samples
def display_table_samples(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    samples = {}
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")  # Adjust LIMIT for more or fewer rows
        rows = cursor.fetchall()
        if rows:
            columns = [description[0] for description in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            samples[table] = df
        else:
            samples[table] = pd.DataFrame()
    
    cursor.close()
    conn.close()
    return samples


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

# Layout adjustment with st.columns
col1, col2 = st.columns([1, 1])  # Adjust the width ratio as needed

with col1:
    st.subheader('Database Tables')
    if st.session_state.selected_db:
        # Display tables and samples immediately upon database selection
        samples = display_table_samples(f'databases/{st.session_state.selected_db}')
        for table, df in samples.items():
            st.markdown(f"#### Sample from {table}")
            st.dataframe(df)

with col2:
    # Place your SQL generation and other interactive elements here
    with st.form('my_form'):
        text = st.text_area('Enter text:', 'Who won the most gold medals in 2012?')
        submitted = st.form_submit_button('Submit')
        if submitted and st.session_state.selected_db:
            generate_response(text, st.session_state.selected_db)
