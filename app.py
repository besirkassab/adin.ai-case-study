import os
import json
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from database import get_engine
from flask import Flask, request, Response

app = Flask(__name__)

def create_queries_for_campaign_card(engine, campaign_id, start_date, end_date):
    query_campaign_exist = "SELECT campaign_name FROM tbl_daily_campaigns WHERE campaign_id = :campaign_id"
    query_campaigns = """
    SELECT * FROM tbl_daily_campaigns
    WHERE date >= :start_date AND date <= :end_date
    """
    
    query_scores = """
    SELECT * FROM tbl_daily_scores
    WHERE date >= :start_date AND date <= :end_date
    """
    
    with engine.connect() as conn:
        campaign_exist_df = pd.read_sql(text(query_campaign_exist), conn, params={'campaign_id': campaign_id})

        if not campaign_exist_df.empty:
            query_campaigns += " AND campaign_id = :campaign_id"
            query_scores += " AND campaign_id = :campaign_id"
            
            campaigns_df = pd.read_sql(text(query_campaigns), conn, params={'start_date': start_date, 'end_date': end_date, 'campaign_id': campaign_id})
            scores_df = pd.read_sql(text(query_scores), conn, params={'start_date': start_date, 'end_date': end_date, 'campaign_id': campaign_id})
        else:
            campaigns_df = pd.read_sql(text(query_campaigns), conn, params={'start_date': start_date, 'end_date': end_date})
            scores_df = pd.read_sql(text(query_scores), conn, params={'start_date': start_date, 'end_date': end_date})

    return campaigns_df, scores_df

def create_queries_for_campaign_table(engine):
    query_campaign_table = """SELECT t1.date, t1.campaign_id, t1.campaign_name, t2.effectiveness, t2.media, t2.creative
        FROM tbl_daily_campaigns AS t1
        LEFT JOIN tbl_daily_scores AS t2 ON t1.campaign_id = t2.campaign_id

        UNION

        SELECT t1.date, t1.campaign_id, t1.campaign_name, t2.effectiveness, t2.media, t2.creative
        FROM tbl_daily_campaigns AS t1
        RIGHT JOIN tbl_daily_scores AS t2 ON t1.campaign_id = t2.campaign_id;
    """
    
    
    with engine.connect() as conn:
        campaign_tables_df = pd.read_sql(text(query_campaign_table), conn)
    
    return campaign_tables_df

@app.route('/')
def get_campaigns():
    # Retrieve query parameters
    campaign_id = request.args.get('campaign_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # Establish database connection
    engine = get_engine()

    # Define and Execute SQL queries, then load them into DF
    campaigns_df, scores_df = create_queries_for_campaign_card(engine=engine, campaign_id=campaign_id, start_date=start_date, end_date=end_date)

    # Ensure 'date' columns are datetime objects
    campaigns_df['date'] = pd.to_datetime(campaigns_df['date'])
    scores_df['date'] = pd.to_datetime(scores_df['date'])

    # Merge dataframes on campaign_id and date
    merged_df = pd.merge(campaigns_df, scores_df, on=['campaign_id', 'date'])

    # Calculate the total days in the date range
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_date_dt - start_date_dt).days

    # Aggregate metrics
    if campaign_id:
        campaign_name = merged_df['campaign_name_x'].iloc[0]
        impressions = int(merged_df['impressions'].sum())
        clicks = int(merged_df['clicks'].sum())
        views = int(merged_df['views'].sum())
    else:
        campaign_name = 'All'
        impressions = int(merged_df['impressions'].sum())
        clicks = int(merged_df['clicks'].sum())
        views = int(merged_df['views'].sum())

    # Get daily trends and convert to required format
    daily_impressions = merged_df.groupby('date')['impressions'].sum().reset_index()
    daily_impressions_dict = {row['date'].strftime('%Y-%m-%d'): int(row['impressions']) for index, row in daily_impressions.iterrows()}
    
    daily_cpm = merged_df.groupby('date')['cpm'].mean().reset_index()
    daily_cpm_dict = {row['date'].strftime('%Y-%m-%d'): float(round(row['cpm'], 2)) for index, row in daily_cpm.iterrows()}

    # Get campaign summary table
    campaign_cards_df = create_queries_for_campaign_table(engine=engine)
    campaign_summary = campaign_cards_df.groupby('campaign_id').agg({
        'date': ['min', 'max'],
        'campaign_name': 'first',
        'effectiveness': 'mean',
        'media': 'mean',
        'creative': 'mean'
    }).reset_index()
    print(campaign_summary.to_string())
    campaign_summary.columns = ['campaign_id', 'start_date', 'end_date', 'campaign_name', 'effectiveness', 'media', 'creative']
    campaign_summary['start_date'] = campaign_summary['start_date'].astype(str)
    campaign_summary['end_date'] = campaign_summary['end_date'].astype(str)

    # Convert all data to native Python types
    response = {
        "campaignCard": {
            "campaignName": campaign_name,
            "range": f"{start_date_dt.strftime('%d %b')} - {end_date_dt.strftime('%d %b')}",
            "days": total_days
        },
        "performanceMetrics": {
            "currentMetrics": {
                "impressions": impressions,
                "clicks": clicks,
                "views": views
            }
        },
        "volumeUnitCostTrend": {
            "impressionsCpm": {
                "impression": daily_impressions_dict,
                "cpm": daily_cpm_dict
            }
        },
        "campaignTable": {
            "start_date": campaign_summary['start_date'].tolist(),
            "end_date": campaign_summary['end_date'].tolist(),
            "adin_id": campaign_summary['campaign_id'].tolist(),
            "campaign": campaign_summary['campaign_name'].tolist(),
            "effectiveness": campaign_summary['effectiveness'].astype(int).tolist(),
            "media": campaign_summary['media'].astype(int).tolist(),
            "creative": campaign_summary['creative'].astype(int).tolist()
        }
    }

    # Serialize the response dictionary into a JSON string
    json_response = json.dumps(response)

    # Create a Flask Response object with the JSON string and specify the content type as application/json
    return Response(json_response, status=200, mimetype='application/json')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)

