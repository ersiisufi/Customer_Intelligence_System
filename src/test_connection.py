from src.databse import load_query_to_df, get_db_connection

# Testing our Master View from Phase 1
test_query = "SELECT * FROM v_sales_summary LIMIT 10;"
engine = get_db_connection()

try:
    data = load_query_to_df(test_query, engine)
    print("Connection Successful!")
    print(data.head())
    print("\nData Types Summary:")
    print(data.dtypes)
except Exception as e:
    print("Connection Failed.")
    print(f"Error: {e}")
    