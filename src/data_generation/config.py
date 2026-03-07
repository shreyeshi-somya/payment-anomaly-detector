SCHEMA = {
    "transaction_id": "string",
    "timestamp": "datetime",
    "amount": "float",
    "payment_method": ["card", "ach", "wire"],
    "card_network": ["visa", "mastercard", "amex", "discover", None],  # None for non-card
    "region": ["west", "midwest", "northeast", "south"],
    "merchant_category": ["retail", "saas", "food_bev", "electronics", "services"],
    "merchant_size": ["smb", "mid_market", "enterprise"],
    "processing_latency_ms": "int",
    "status": ["success", "declined", "failed"],
    "decline_reason": ["insufficient_funds", "fraud_suspected", "card_expired", "network_error", None]
}

DATE_RANGE = {
    "start": "2024-01-01",
    "end": "2025-12-31"
}

TOTAL_RECORDS = 5_000_000