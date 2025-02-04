from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import sqlite3
import pandas as pd
import google.generativeai as genai
from fastapi.middleware.cors import CORSMiddleware

# Initialize FastAPI app
app = FastAPI()

# Configure the Google Generative AI
GOOGLE_API_KEY = "AIzaSyDlyyh_V98h8EPQNxQtkTyvIOykKVKhKKk"
genai.configure(api_key=GOOGLE_API_KEY)




# Allow all origins or specify the frontend URL
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this for your frontend's domain, or keep "*" for all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)



prompt=[
    """

You are an expert in converting English questions to SQL query!
database name "GMS" and the following columns - (DocDate, FYears, CardName, GroupName, Principal, Item, Quantity, Sales_Value, Cost, GP, Gp_Ratio, ProductSegment, Product), and without special characters or the word SQL:
also the SQL code should not have "```" in the beginning or end and the word SQL should not appear in the output.
remove all special characters also " \  | "

Example 1: How many total records have been placed?
SELECT COUNT(*) FROM GMS;

Example 2: Show all records placed by Amson Vaccine & Pharma Pvt Ltd
SELECT * FROM GMS WHERE CardName = 'Amson Vaccine & Pharma Pvt Ltd';

Example 3: How many records have the Item VIDAS TSH 60 TESTS?
SELECT COUNT(*) FROM GMS WHERE Item = 'VIDAS TSH 60 TESTS';

Example 4: Retrieve all records where the quantity is more than 40.
SELECT * FROM GMS WHERE Quantity > 40;

Example 5: Find all records placed in the first half H1 of Fiscal Year 2024 2025.
SELECT * FROM GMS WHERE FYears = '2024 2025' AND Period = 'H1';

Example 6: List the total Sales Value for records in the second half H2 of Fiscal Year 2023 2024.
SELECT SUM(Sales_Value) FROM GMS WHERE FYears = '2023 2024' AND Period = 'H2';

Example 7: Show all records with the Item API STAPH 25STRIPS 25MEDIA.
SELECT * FROM GMS WHERE Item = 'API STAPH 25STRIPS 25MEDIA';

Example 8: Retrieve all records for products in the ID AST Manual product segment.
SELECT * FROM GMS WHERE ProductSegment = 'ID AST Manual';

Example 9: List the InvDocNum of all records for the Group Name Private.
SELECT InvDocNum FROM GMS WHERE GroupName = 'Private';

Example 10: Show the total Sales Value for each Card Name.
SELECT CardName, SUM(Sales_Value) FROM GMS GROUP BY CardName;

Example 11: Find all records in the North Zone.
SELECT * FROM GMS WHERE Zone = 'North';

Example 12: List all records in the Biomerieux Division product division.
SELECT * FROM GMS WHERE Principal = 'Biomerieux Division';

Example 13: Retrieve all records where the Sales Value is greater than 90000.
SELECT * FROM GMS WHERE Sales_Value > 90000;

Example 14: Show all records for the period H1 2023 2024.
SELECT * FROM GMS WHERE Period = 'H1 2023 2024';

Example 15: List all records placed in Fiscal Year 2024 2025 for the Group Name Private.
SELECT * FROM GMS WHERE FYears = '2024 2025' AND GroupName = 'Private';

Example 16: How many records have been placed by customers in the South Zone?
SELECT COUNT(*) FROM GMS WHERE Zone = 'South';

Example 17: Show all records that have the same Item VIDAS TSH 60 TESTS and were placed in Fiscal Year 2023 2024.
SELECT * FROM GMS WHERE Item = 'VIDAS TSH 60 TESTS' AND FYears = '2023 2024';

Example 18: Find the total Sales Value for each half H1 H2 in Fiscal Year 2024 2025.
SELECT Period, SUM(Sales_Value) FROM GMS WHERE FYears = '2024 2025' GROUP BY Period;

Example 19: Retrieve the records where the Principal is Biomerieux.
SELECT * FROM GMS WHERE Principal = 'Biomerieux';

Example 20: Show the highest Sales Value in Fiscal Year 2024 2025.
SELECT MAX(Sales_Value) FROM GMS WHERE FYears = '2024 2025';

Example 21: List all records for the North Zone.
SELECT * FROM GMS WHERE Zone = 'North';

Example 22: Find the average quantity for all records in Fiscal Year 2023 2024.
SELECT AVG(Quantity) FROM GMS WHERE FYears = '2023 2024';

Example 23: Retrieve Card Name and total Sales Value for records placed in the first half H1 of any Fiscal Year.
SELECT CardName, SUM(Sales_Value) FROM GMS WHERE Period LIKE 'H1%' GROUP BY CardName;

Example 24: Show all records with Sales Value greater than 50000.
SELECT * FROM GMS WHERE Sales_Value > 50000;

Example 25: List records placed by Noon Foundation Rwp in Fiscal Year 2024 2025.
SELECT * FROM GMS WHERE CardName = 'Noon Foundation Rwp' AND FYears = '2024 2025';

Example 26: How many records have been placed with a quantity less than 30?
SELECT COUNT(*) FROM GMS WHERE Quantity < 30;

Example 27: Find all records with a related equipment note starting with 410416.
SELECT * FROM GMS WHERE Related Equipment LIKE '410416%';

Example 28: Show the total Sales Value for all records with Card Name Patients Welfare Foundation Khi.
SELECT SUM(Sales_Value) FROM GMS WHERE CardName = 'Patients Welfare Foundation Khi';

Example 29: List all records with a Sales Value greater than 90000.
SELECT * FROM GMS WHERE Sales_Value > 90000;

Example 30: Retrieve InvDocNum for records placed by customers from the North Zone and the Product Group is ID AST API.
SELECT InvDocNum FROM GMS WHERE Zone = 'North' AND ProductGroup = 'ID AST API';

Example 31: Show the total Sales Value for records placed this month.
SELECT SUM(Sales_Value) FROM GMS WHERE STRFTIME('%m', DocDate) = STRFTIME('%m', 'now');

Example 32: Display all items for Orgentec Diagnostics.
SELECT Item, CardName FROM GMS WHERE CardName = 'Orgentec Diagnostics';

SELECT
  Principal,
  SUM(Sales_Value) AS TotalSalesValue
FROM GMS
WHERE
  STRFTIME('%m', DocDate) = STRFTIME('%m', 'now')
GROUP BY
  Principal;


remove "``````"
from the start and ending of the query 

 """
]



# Function to interact with Generative AI
def get_gemini_response(question: str, prompt: list):
    model = genai.GenerativeModel("gemini-pro")
    response = model.generate_content([prompt[0], question])
    return response.text.strip("```\n").strip()

# Function to execute SQL query on SQLite
def read_sql_query(sql: str, db: str):
    try:
        conn = sqlite3.connect(db)
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Request model for running a SQL query
class SQLQuery(BaseModel):
    question: str

@app.get("/")
def root():
    return {"message": "Welcome to the GMS SQL Query API!"}

@app.get("/test")
def query_to_sql(question: str):
    """
    Converts a natural language question to SQL and executes it.
    """
    sql_query = get_gemini_response(question, prompt)
    if not sql_query:
        raise HTTPException(status_code=400, detail="Failed to generate SQL query.")
    
    result = read_sql_query(sql_query, "GMSLIVE.db")
    print(result)
    return {"query": sql_query, "result": result.to_dict(orient="records")}






@app.post("/run-query/")
def execute_query(sql: str = Query(..., description="The SQL query to execute")):
    """
    Executes a given SQL query and returns the results.
    """
    result = read_sql_query(sql, "GMSLIVE.db")
    return {"result": result.to_dict(orient="records")}
