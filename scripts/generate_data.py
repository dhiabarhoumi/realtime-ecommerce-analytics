#!/usr/bin/env python3
"""Generate synthetic e-commerce clickstream data for replay."""

import gzip
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List

import pandas as pd

# Set deterministic seeds
random.seed(42)

# Configuration
NUM_USERS = 50000
NUM_SESSIONS = 100000
NUM_EVENTS = 1000000
START_DATE = datetime(2023, 9, 1)
END_DATE = datetime(2023, 11, 30)

# Product catalog
PRODUCTS = [
    {"id": f"sku_{i:05d}", "price": round(random.uniform(9.99, 299.99), 2)}
    for i in range(1, 1001)
]

# UTM campaigns
UTM_CAMPAIGNS = [
    {"source": "google", "medium": "cpc", "campaign": "spring23"},
    {"source": "facebook", "medium": "social", "campaign": "spring23"},
    {"source": "email", "medium": "email", "campaign": "newsletter_q3"},
    {"source": "direct", "medium": "none", "campaign": "none"},
    {"source": "organic", "medium": "organic", "campaign": "none"},
]

# Countries and cities
GEO_DATA = [
    {"country": "US", "city": "New York"},
    {"country": "US", "city": "Los Angeles"},
    {"country": "DE", "city": "Berlin"},
    {"country": "DE", "city": "Munich"},
    {"country": "FR", "city": "Paris"},
    {"country": "UK", "city": "London"},
    {"country": "CA", "city": "Toronto"},
    {"country": "AU", "city": "Sydney"},
]

# User agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)",
    "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0",
]


def generate_session() -> dict:
    """Generate a user session with realistic behavior."""
    user_id = f"u_{random.randint(1, NUM_USERS)}"
    session_id = f"s_{uuid.uuid4().hex[:16]}"
    
    # Session start time
    session_start = START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )
    
    # Session characteristics
    utm = random.choice(UTM_CAMPAIGNS)
    geo = random.choice(GEO_DATA)
    device = {
        "ua": random.choice(USER_AGENTS),
        "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
        "mobile": random.choice([True, False])
    }
    
    # Session funnel progression
    events = []
    current_time = session_start
    
    # Always start with page view
    landing_page = random.choice(["/", "/products", "/category/electronics", "/category/clothing"])
    events.append(create_event(
        user_id, session_id, current_time, "page_view", landing_page, utm, device, geo
    ))
    
    # Simulate browsing behavior
    num_page_views = random.randint(1, 8)
    for _ in range(num_page_views):
        current_time += timedelta(seconds=random.randint(5, 120))
        
        if random.random() < 0.6:  # 60% chance to view a product
            product = random.choice(PRODUCTS)
            page = f"/product/{product['id']}"
            events.append(create_event(
                user_id, session_id, current_time, "page_view", page, utm, device, geo, product
            ))
            
            # 25% chance to add to cart after viewing product
            if random.random() < 0.25:
                current_time += timedelta(seconds=random.randint(10, 60))
                events.append(create_event(
                    user_id, session_id, current_time, "add_to_cart", page, utm, device, geo, product
                ))
                
                # 40% chance to purchase after adding to cart
                if random.random() < 0.4:
                    current_time += timedelta(seconds=random.randint(30, 300))
                    events.append(create_event(
                        user_id, session_id, current_time, "purchase", page, utm, device, geo, product
                    ))
        else:
            # Regular page view
            page = random.choice(["/about", "/contact", "/blog", "/category/home", "/search"])
            events.append(create_event(
                user_id, session_id, current_time, "page_view", page, utm, device, geo
            ))
    
    return events


def create_event(user_id: str, session_id: str, timestamp: datetime, 
                event_type: str, page: str, utm: dict, device: dict, geo: dict, 
                product: dict = None) -> dict:
    """Create a single event."""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": timestamp.isoformat() + "Z",
        "user_id": user_id,
        "session_id": session_id,
        "page": page,
        "product_id": product["id"] if product else None,
        "price": product["price"] if product and event_type in ["add_to_cart", "purchase"] else None,
        "currency": "USD",
        "qty": 1 if product and event_type in ["add_to_cart", "purchase"] else None,
        "referrer": random.choice([None, "https://google.com", "https://facebook.com", "https://twitter.com"]),
        "utm": utm,
        "device": device,
        "geo": geo
    }
    return event


def main():
    """Generate and save synthetic data."""
    print("Generating synthetic e-commerce events...")
    
    all_events = []
    for i in range(NUM_SESSIONS):
        if i % 10000 == 0:
            print(f"Generated {i} sessions...")
        
        session_events = generate_session()
        all_events.extend(session_events)
    
    print(f"Generated {len(all_events)} total events")
    
    # Sort by timestamp
    all_events.sort(key=lambda x: x["event_ts"])
    
    # Convert to DataFrame and save
    df = pd.DataFrame(all_events)
    
    # Save as compressed CSV
    output_file = "data/events.csv.gz"
    df.to_csv(output_file, index=False, compression="gzip")
    
    print(f"Saved {len(df)} events to {output_file}")
    print(f"File size: {pd.os.path.getsize(output_file) / (1024*1024):.1f} MB")
    
    # Print sample statistics
    print(f"\nEvent type distribution:")
    print(df["event_type"].value_counts())
    
    print(f"\nDate range: {df['event_ts'].min()} to {df['event_ts'].max()}")


if __name__ == "__main__":
    main()

# Updated: 2025-10-04 19:46:26
# Added during commit replay
