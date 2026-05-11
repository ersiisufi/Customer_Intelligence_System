import pandas as pd
from datetime import timedelta
from src.databse import load_query_to_df, get_db_connection

engine = get_db_connection()

def get_rfm_data():
    """Extracts data and calculates RFM metrics."""
    
    # 1. Extraction
    query = "SELECT * FROM v_sales_summary;"
    df = load_query_to_df(query, engine=engine)

    # Ensure date is actually a datetime object
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

    # 2. Setup Reference Date (The "Today" of the dataset)
    # Since the Olist data is historical, we use the day after the last purchase
    snapshot_date = df['order_purchase_timestamp'].max() + timedelta(days=1)

    # 3. Calculation 
    rfm = df.groupby('customer_unique_id').agg({
        'order_purchase_timestamp': lambda x: (snapshot_date - x.max()).days, # Recency
        'order_id': 'count',                                               # Frequency
        'total_transaction_value': 'sum'                                   # Monetary
    })

    # Rename columns for clarity
    rfm.rename(columns={
        'order_purchase_timestamp': 'recency',
        'order_id': 'frequency',
        'total_transaction_value': 'monetary'
    }, inplace=True)

    # 4. Scoring (R-F-M scores from 1 to 5)
    # Note: For Recency, a LOWER number is BETTER (more recent), so we label 5 to 1
    rfm['R'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    
    # For Frequency and Monetary, HIGHER is BETTER. 
    # PROBLEM: Frequency is mostly 1s, so qcut will fail because it can't create 5 unique bins.
    # FIX: We use 'rank(method="first")' to break ties.
    rfm['F'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    rfm['M'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
    
    # Combine into one string for easy segmenting
    rfm['rfm_score'] = rfm['R'].astype(str) + rfm['F'].astype(str) + rfm['M'].astype(str)
    rfm['segment'] = rfm.apply(assign_segment, axis=1)

    return rfm.reset_index()


def assign_segment(df):
    """Categorizes customers based on their RFM scores."""
    
    # Define the mapping logic
    # We focus primarily on R and F for the segment names
    r = df['R']
    f = df['F']
    
    if r >= 4 and f >= 4:
        return 'Champions'
    elif r >= 3 and f >= 3:
        return 'Loyal Customers'
    elif r >= 4 and f <= 2:
        return 'Recent & Promising'
    elif r <= 2 and f >= 4:
        return 'Cant Lose Them'
    elif r <= 2 and f <= 2:
        return 'Hibernating / Lost'
    else:
        return 'About to Sleep'


if __name__ == "__main__":
# Test the processor
    print("Calculating RFM segments...")
    rfm_df = get_rfm_data()
    print(rfm_df.head())
    print(rfm_df.describe())
    # print(rfm_df[rfm_df['rfm_score']=='555'].count())  # Check top segment
    print(f"\nTotal unique customers segmented: {len(rfm_df)}")