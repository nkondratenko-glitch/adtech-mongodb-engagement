# HW 3: User Engagement Tracking with MongoDB

This homework extends the normalized MySQL schema from Homework 1 with a MongoDB document model for user engagement history.

## Why MongoDB here

Relational tables are efficient for normalized master data and transactional consistency, but they become less convenient for highly nested, user-centric histories such as:
- ad impressions per user
- clicks nested inside impressions
- multi-session engagement across devices
- per-user behavioral lookups for targeting

MongoDB is a better fit for this access pattern because a single user document can contain the user profile, impression history, click events, and derived sessions in one place.

## MongoDB document model

Collection: `user_engagement`

Each document stores one user and their embedded engagement history:

```json
{
  "_id": 12345,
  "demographics": {
    "age": 29,
    "gender": "Female",
    "location_id": 2,
    "location": "Canada",
    "interests": ["Technology", "Sports"]
  },
  "impressions": [
    {
      "impression_id": 1000001,
      "impression_time": "2024-10-11T10:13:00Z",
      "device_type": "mobile",
      "cost": 0.0342,
      "campaign": {
        "campaign_id": 14,
        "campaign_name": "Campaign_14",
        "advertiser_id": 3,
        "advertiser_name": "Advertiser_3",
        "category": "Technology"
      },
      "clicked": true,
      "clicks": [
        {
          "click_id": 700001,
          "click_time": "2024-10-11T10:13:20Z",
          "cpc_amount": 0.52
        }
      ]
    }
  ],
  "sessions": [
    {
      "session_id": "12345_1",
      "session_start": "2024-10-11T10:13:00Z",
      "session_end": "2024-10-11T10:39:00Z",
      "device_type": "mobile",
      "impression_count": 4,
      "click_count": 1,
      "events": [
        {
          "impression_id": 1000001,
          "campaign_id": 14,
          "campaign_name": "Campaign_14",
          "category": "Technology",
          "impression_time": "2024-10-11T10:13:00Z",
          "clicked": true,
          "clicks": [
            {
              "click_id": 700001,
              "click_time": "2024-10-11T10:13:20Z",
              "cpc_amount": 0.52
            }
          ]
        }
      ]
    }
  ],
  "stats": {
    "total_impressions": 4,
    "total_clicks": 1
  },
  "updated_at": "2024-10-11T11:00:00Z"
}
```

## Indexes

Create these indexes in MongoDB for efficient lookups:

```javascript
db.user_engagement.createIndex({ "demographics.location": 1 });
db.user_engagement.createIndex({ "impressions.campaign.advertiser_id": 1, "impressions.impression_time": -1 });
db.user_engagement.createIndex({ "impressions.campaign.campaign_id": 1, "impressions.impression_time": -1 });
db.user_engagement.createIndex({ "sessions.session_start": -1 });
db.user_engagement.createIndex({ "impressions.clicked": 1, "impressions.campaign.category": 1 });
```

## Deliverables included

- `mongo/queries.js` – MongoDB queries for all required tasks
- `scripts/load_user_engagement_to_mongo.py` – loads data from MySQL into MongoDB
- `scripts/run_mongo_analytics.py` – executes MongoDB queries and exports JSON/CSV results
- `docker-compose.mongo.yml` – local MongoDB + mongo-express setup
- `scripts/05_start_mongo.sh` – starts MongoDB containers
- `scripts/06_load_mongo.sh` – loads documents into MongoDB
- `scripts/07_run_mongo_queries.sh` – runs analytics export
- `requirements_hw3.txt` – Python dependencies
- `docs/hw3_schema_notes.md` – schema and design notes

## Notes about the loader

The loader expects the normalized MySQL schema from Homework 1:
- `users`
- `locations`
- `interests`
- `user_interests`
- `campaigns`
- `advertisers`
- `campaign_target_interests`
- `impressions`
- `clicks`

If `impressions` and `clicks` contain no rows, user documents will still be loaded with demographics and empty interaction arrays.

## How to run

1. Start MongoDB:

```bash
docker compose -f docker-compose.mongo.yml up -d
```

2. Install Python dependencies:

```bash
pip install -r requirements_hw3.txt
```

3. Load documents from MySQL into MongoDB:

```bash
python scripts/load_user_engagement_to_mongo.py
```

4. Run analytics and export results:

```bash
python scripts/run_mongo_analytics.py
```

Results are saved in the `mongo_reports/` folder.

## Screenshots

Add your screenshots to:
- `docs/hw3_queries_demo.png`
