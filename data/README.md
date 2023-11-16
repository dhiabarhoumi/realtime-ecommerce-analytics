# Synthetic E-commerce Events Dataset

This file contains synthetic e-commerce clickstream events generated for the real-time analytics pipeline.

**File**: `events.csv.gz`
**Records**: ~1,000,000 events
**Time Range**: September 2023 - November 2023
**Schema**: See `src/schemas/event.py`

## Generation Parameters:
- **Users**: 50,000 unique users
- **Sessions**: 100,000 sessions
- **Products**: 1,000 SKUs
- **Event Distribution**: 70% page_view, 20% add_to_cart, 10% purchase
- **Conversion Rate**: ~3.5% (realistic e-commerce benchmark)

## Columns:
- event_id, event_type, event_ts
- user_id, session_id, page
- product_id, price, currency, qty
- referrer, utm (source/medium/campaign)
- device (ua/os/mobile), geo (country/city)

To regenerate this dataset:
```bash
python scripts/generate_data.py
```

*This file is compressed to ~15MB and contains deterministic synthetic data for reproducible testing.*