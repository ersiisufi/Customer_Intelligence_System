import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from src.processor import get_rfm_data
from src.databse import get_db_connection

def perform_clustering(rfm, n_clusters=4):
    """Performs KMeans clustering on RFM data."""
    
    # Select only the R, F, M columns for clustering
    rfm_data = rfm[['recency', 'frequency', 'monetary']]
    
    # Standardize the data
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(rfm_data)
    
    # Fit KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, init='k-means++')
    rfm['cluster'] = kmeans.fit_predict(rfm_scaled)
    
    return rfm, kmeans

def label_clusters(rfm_df):
    """
    Manually maps cluster numbers to business names based on 
    Centroid Analysis results.
    """
    # Map based on specific findings:
    cluster_map = {
        2: 'VIP / Whales (High Spend)',
        3: 'Loyalists (High Frequency)',
        0: 'Standard / Mass Market',
        1: 'At Risk / Low Engagement'
    }
    
    rfm_df['business_segment'] = rfm_df['cluster'].map(cluster_map)
    return rfm_df


def plot_3d_interactive(df):
    fig = px.scatter_3d(df, x='recency', y='frequency', z='monetary',
                        color='business_segment', # Using the human names we made!
                        title='Customer Intelligence Clusters',
                        opacity=0.6,
                        log_z=True) # This fixes the monetary scale visually
    fig.show()


def export_to_db(df, table_name='fact_customer_intelligence'):
    try:
        
        engine = get_db_connection()
        print(f" Exporting {len(df)} rows to '{table_name}'...")

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(" Export Successful! You can now query this in DBeaver.")
    except Exception as e:
        print(f" Export Failed: {e})")


if __name__ == "__main__":
    print("Fetching data and training K-Means...")
    rfm_results = get_rfm_data()
    clustered_df, model = perform_clustering(rfm_results)
    
    print("\nCluster Distribution:")
    print(clustered_df['cluster'].value_counts())

    analysis = clustered_df.groupby('cluster').agg({
    'recency': ['mean', 'min', 'max'],
    'frequency': ['mean', 'min', 'max'],
    'monetary': ['mean', 'min', 'max', 'count']
    }).round(2)

    final_df = label_clusters(clustered_df)
    print(final_df[['customer_unique_id', 'business_segment']].head())

    print("\n--- Cluster Characterization ---")
    print(analysis)

    export_to_db(final_df)

    # plot_3d_interactive(final_df)


