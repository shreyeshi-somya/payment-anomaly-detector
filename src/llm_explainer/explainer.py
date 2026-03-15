import anthropic

client = anthropic.Anthropic()

def explain_anomaly(anomaly_date, metric, daily_zscore, drilldown_df):
    dimensions = drilldown_df[['level', 'dimension_value', 'dimension_zscore']].to_string(index=False)
    
    prompt = f"""You are a payments analyst. An anomaly was detected in our payment metrics.

    Date: {anomaly_date}
    Metric: {metric}
    Z-score: {daily_zscore:.2f} (values above 3 are anomalous)

    Breakdown by dimension (highest z-scores indicate where the anomaly is concentrated):
    {dimensions}

    Based on this data, write a brief hypothesis explaining what might have caused this anomaly. Be specific about which dimensions are most affected."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.content[0].text