#!/usr/bin/env python
import os
import sys
import psycopg2
from psycopg2 import extras
import pandas as pd
from datetime import datetime
import warnings

# Disable Prophet print outputs and logging warnings
warnings.filterwarnings("ignore")
import logging
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

from prophet import Prophet

def get_db_connection():
    conn_str = os.getenv("PG_CONN_STR", "postgresql://postgres:postgres@localhost:5432/gitpulse")
    return psycopg2.connect(conn_str)

def fetch_daily_metrics():
    print("Fetching daily repository metrics from marts...")
    try:
        conn = get_db_connection()
        query = """
            select
                repo_id,
                repo_name,
                event_date as ds,
                cast(total_events as double precision) as y
            from marts.fct_daily_repo_metrics
            order by repo_id, event_date
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return None

def detect_anomalies(df):
    if df is None or df.empty:
        print("No repository metrics found. Anomaly detection skipped.")
        return []

    anomalies = []
    unique_repos = df['repo_id'].unique()
    print(f"Running anomaly detection for {len(unique_repos)} repositories...")

    for repo_id in unique_repos:
        repo_df = df[df['repo_id'] == repo_id].copy()
        repo_name = repo_df['repo_name'].iloc[0]
        
        # Format ds column as datetime
        repo_df['ds'] = pd.to_datetime(repo_df['ds'])
        
        n_points = len(repo_df)
        
        if n_points >= 3:
            try:
                # Initialize and fit Prophet model with minimal seasonalities for short local data
                m = Prophet(
                    interval_width=0.95,
                    yearly_seasonality=False,
                    weekly_seasonality=False,
                    daily_seasonality=False
                )
                m.fit(repo_df[['ds', 'y']])
                
                # Predict on training data
                forecast = m.predict(repo_df[['ds']])
                
                # Join forecast bounds with actual values
                results = repo_df.merge(
                    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], 
                    on='ds'
                )
                
                # Flag anomalies
                for _, row in results.iterrows():
                    actual = row['y']
                    upper = row['yhat_upper']
                    lower = row['yhat_lower']
                    
                    is_anomaly = actual > upper or actual < lower
                    if is_anomaly:
                        # Score indicates how far outside the uncertainty bounds the value lies
                        span = (upper - lower) if (upper - lower) > 0 else 1.0
                        score = (actual - upper) / span if actual > upper else (lower - actual) / span
                        
                        anomalies.append({
                            'repo_id': int(repo_id),
                            'repo_name': repo_name,
                            'ds': row['ds'].strftime('%Y-%m-%d'),
                            'y': float(actual),
                            'yhat_lower': float(lower),
                            'yhat_upper': float(upper),
                            'anomaly_score': float(score)
                        })
            except Exception as e:
                print(f"Error running Prophet for repo {repo_name} ({repo_id}): {e}")
                # Fallback to standard-deviation-based detection on error
                run_fallback_detection(repo_id, repo_name, repo_df, anomalies)
        else:
            # Fallback for cold-start (less than 3 days of historical data)
            run_fallback_detection(repo_id, repo_name, repo_df, anomalies)

    print(f"Anomaly detection complete. Detected {len(anomalies)} anomalous activity events.")
    return anomalies

def run_fallback_detection(repo_id, repo_name, repo_df, anomalies_list):
    """
    Standard deviation / threshold-based anomaly detection fallback for cold starts
    """
    if len(repo_df) == 0:
        return
        
    avg_y = repo_df['y'].mean()
    std_y = repo_df['y'].std()
    
    # If standard deviation is 0 or NaN, set to a default value
    if pd.isna(std_y) or std_y == 0:
        std_y = avg_y * 0.2 if avg_y > 0 else 1.0
        
    # Anomaly threshold: 1.5 standard deviations above the mean, or at least a minimal raw score of 5
    for _, row in repo_df.iterrows():
        actual = row['y']
        upper_bound = max(avg_y + 1.5 * std_y, 5.0)
        lower_bound = max(avg_y - 1.5 * std_y, 0.0)
        
        if actual > upper_bound:
            score = (actual - upper_bound) / std_y
            anomalies_list.append({
                'repo_id': int(repo_id),
                'repo_name': repo_name,
                'ds': row['ds'].strftime('%Y-%m-%d'),
                'y': float(actual),
                'yhat_lower': float(lower_bound),
                'yhat_upper': float(upper_bound),
                'anomaly_score': float(score)
            })

def save_anomalies(anomalies):
    if not anomalies:
        print("No anomalies to save.")
        return True

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Format anomalies for bulk insert
        tuples = [
            (
                a['repo_id'], 
                a['repo_name'], 
                a['ds'], 
                a['y'], 
                a['yhat_lower'], 
                a['yhat_upper'], 
                a['anomaly_score']
            ) 
            for a in anomalies
        ]
        
        # Truncate anomalies for this run or append (we append and can de-duplicate downstream if needed, or truncate for a fresh run list)
        # Let's truncate and insert for the daily active list
        cursor.execute("TRUNCATE TABLE analytics.detected_anomalies;")
        
        insert_query = """
            INSERT INTO analytics.detected_anomalies (repo_id, repo_name, ds, y, yhat_lower, yhat_upper, anomaly_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        extras.execute_batch(cursor, insert_query, tuples, page_size=1000)
        conn.commit()
        print(f"Successfully saved {len(anomalies)} anomalies to analytics.detected_anomalies.")
        return True
    except Exception as e:
        print(f"Error saving anomalies: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    df = fetch_daily_metrics()
    if df is None:
        sys.exit(1)
        
    anomalies = detect_anomalies(df)
    success = save_anomalies(anomalies)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
