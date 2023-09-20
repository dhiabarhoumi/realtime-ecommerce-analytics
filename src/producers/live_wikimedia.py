#!/usr/bin/env python3
"""Live producer that converts Wikimedia RecentChange events to e-commerce schema."""

import json
import random
import uuid
from datetime import datetime
from typing import Dict, Optional

import requests
from kafka import KafkaProducer
from sseclient import SSEClient


class LiveWikimediaProducer:
    def __init__(self, brokers: str = "localhost:9092", topic: str = "events_raw"):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks='all',
            retries=3,
        )
        self.topic = topic
        self.events_sent = 0
        
        # Mapping for realistic e-commerce simulation
        self.page_types = ["/product/", "/category/", "/", "/search", "/cart", "/checkout"]
        self.products = [f"sku_{i:05d}" for i in range(1, 1001)]
        self.currencies = ["USD", "EUR", "GBP", "CAD"]
        
    def wikimedia_to_ecommerce(self, wiki_event: Dict) -> Optional[Dict]:
        """Convert Wikimedia event to e-commerce clickstream event."""
        try:
            # Extract basic info
            timestamp = wiki_event.get('meta', {}).get('dt', datetime.utcnow().isoformat() + 'Z')
            user = wiki_event.get('user', 'anonymous')
            
            # Generate synthetic e-commerce data based on wiki activity
            event_type = self._determine_event_type(wiki_event)
            page = self._generate_page(wiki_event)
            user_id = f"u_{hash(user) % 100000}"
            session_id = f"s_{uuid.uuid4().hex[:16]}"
            
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_ts": timestamp,
                "user_id": user_id,
                "session_id": session_id,
                "page": page,
                "product_id": None,
                "price": None,
                "currency": random.choice(self.currencies),
                "qty": None,
                "referrer": "https://en.wikipedia.org",
                "utm": {
                    "source": "wikipedia",
                    "medium": "referral",
                    "campaign": "live_demo"
                },
                "device": {
                    "ua": "Mozilla/5.0 (Live Demo)",
                    "os": "Unknown",
                    "mobile": random.choice([True, False])
                },
                "geo": {
                    "country": wiki_event.get('meta', {}).get('domain', 'en.wikipedia.org')[:2].upper(),
                    "city": "Unknown"
                }
            }
            
            # Add product details for product/cart/purchase events
            if event_type in ["add_to_cart", "purchase"] or "/product/" in page:
                product_id = random.choice(self.products)
                price = round(random.uniform(9.99, 299.99), 2)
                
                event.update({
                    "product_id": product_id,
                    "price": price if event_type in ["add_to_cart", "purchase"] else None,
                    "qty": 1 if event_type in ["add_to_cart", "purchase"] else None
                })
                
                if "/product/" not in page:
                    event["page"] = f"/product/{product_id}"
            
            return event
            
        except Exception as e:
            print(f"Error converting event: {e}")
            return None
    
    def _determine_event_type(self, wiki_event: Dict) -> str:
        """Determine e-commerce event type based on wiki activity."""
        event_type = wiki_event.get('type', 'edit')
        
        # Map wiki events to e-commerce events with realistic probabilities
        if event_type == 'edit':
            return random.choices(
                ["page_view", "add_to_cart", "purchase"],
                weights=[0.7, 0.2, 0.1]
            )[0]
        elif event_type == 'new':
            return random.choices(
                ["page_view", "add_to_cart"],
                weights=[0.8, 0.2]
            )[0]
        else:
            return "page_view"
    
    def _generate_page(self, wiki_event: Dict) -> str:
        """Generate realistic e-commerce page based on wiki activity."""
        title = wiki_event.get('title', '')
        
        # Map wiki categories to e-commerce pages
        if any(word in title.lower() for word in ['category', 'list', 'index']):
            return random.choice(["/category/electronics", "/category/clothing", "/category/home"])
        elif any(word in title.lower() for word in ['search', 'special']):
            return "/search"
        elif len(title) > 50:  # Long titles -> product pages
            return f"/product/{random.choice(self.products)}"
        else:
            return random.choice(self.page_types)
    
    def start_streaming(self):
        """Start streaming from Wikimedia RecentChange."""
        url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        
        print(f"Starting live stream from {url}")
        print(f"Sending to Kafka topic: {self.topic}")
        
        try:
            for event in SSEClient(url):
                if event.data:
                    try:
                        wiki_data = json.loads(event.data)
                        ecommerce_event = self.wikimedia_to_ecommerce(wiki_data)
                        
                        if ecommerce_event:
                            # Send to Kafka
                            partition_key = ecommerce_event.get('session_id', '')
                            
                            self.producer.send(
                                self.topic,
                                value=ecommerce_event,
                                key=partition_key
                            )
                            
                            self.events_sent += 1
                            
                            if self.events_sent % 100 == 0:
                                print(f"Sent {self.events_sent} events (last: {ecommerce_event['event_type']})")
                                
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"Error processing event: {e}")
                        continue
                        
        except KeyboardInterrupt:
            print(f"\\nStopped after sending {self.events_sent} events")
        finally:
            self.producer.flush()
            self.producer.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Stream live Wikimedia events as e-commerce data")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka brokers")
    parser.add_argument("--topic", default="events_raw", help="Kafka topic")
    
    args = parser.parse_args()
    
    producer = LiveWikimediaProducer(brokers=args.brokers, topic=args.topic)
    producer.start_streaming()


if __name__ == "__main__":
    main()

# Updated: 2025-10-04 19:46:28
# Added during commit replay
