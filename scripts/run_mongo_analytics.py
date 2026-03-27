import csv
import os
from pathlib import Path

from bson import json_util
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MONGO_DB = os.getenv("MONGO_DB", "adtech_mongo")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "user_engagement")
DEMO_USER_ID = int(os.getenv("DEMO_USER_ID", "1"))
DEMO_ADVERTISER_ID = int(os.getenv("DEMO_ADVERTISER_ID", "1"))

OUTPUT_DIR = Path("mongo_reports")
OUTPUT_DIR.mkdir(exist_ok=True)


def write_csv(path: Path, rows):
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["no_data"])
        return

    if isinstance(rows[0], dict):
        fieldnames = sorted({k for row in rows for k in row.keys()})
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)


def q1_all_interactions(collection, user_id: int):
    doc = collection.find_one(
        {"_id": user_id},
        {"_id": 1, "demographics": 1, "impressions": 1, "stats": 1}
    )
    return doc or {}


def q2_last_5_sessions(collection, user_id: int):
    pipeline = [
        {"$match": {"_id": user_id}},
        {
            "$project": {
                "_id": 1,
                "demographics": 1,
                "last_5_sessions": {"$slice": ["$sessions", 5]}
            }
        }
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else {}


def q3_clicks_per_hour_per_campaign(collection, advertiser_id: int):
    pipeline = [
        {"$unwind": "$impressions"},
        {"$match": {"impressions.campaign.advertiser_id": advertiser_id}},
        {"$unwind": "$impressions.clicks"},
        {
            "$match": {
                "impressions.clicks.click_time": {
                    "$gte": {"$dateSubtract": {"startDate": "$$NOW", "unit": "hour", "amount": 24}}
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "campaign_id": "$impressions.campaign.campaign_id",
                    "campaign_name": "$impressions.campaign.campaign_name",
                    "hour": {
                        "$dateTrunc": {
                            "date": "$impressions.clicks.click_time",
                            "unit": "hour"
                        }
                    }
                },
                "click_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "campaign_id": "$_id.campaign_id",
                "campaign_name": "$_id.campaign_name",
                "hour": "$_id.hour",
                "click_count": 1
            }
        },
        {"$sort": {"hour": 1, "campaign_id": 1}}
    ]
    return list(collection.aggregate(pipeline))


def q4_ad_fatigue(collection):
    pipeline = [
        {"$unwind": "$impressions"},
        {
            "$group": {
                "_id": {
                    "user_id": "$_id",
                    "campaign_id": "$impressions.campaign.campaign_id",
                    "campaign_name": "$impressions.campaign.campaign_name"
                },
                "impression_count": {"$sum": 1},
                "click_count": {
                    "$sum": {
                        "$cond": [{"$eq": ["$impressions.clicked", True]}, 1, 0]
                    }
                }
            }
        },
        {
            "$match": {
                "impression_count": {"$gte": 5},
                "click_count": 0
            }
        },
        {
            "$project": {
                "_id": 0,
                "user_id": "$_id.user_id",
                "campaign_id": "$_id.campaign_id",
                "campaign_name": "$_id.campaign_name",
                "impression_count": 1,
                "click_count": 1
            }
        },
        {"$sort": {"impression_count": -1, "user_id": 1}}
    ]
    return list(collection.aggregate(pipeline))


def q5_top_categories(collection, user_id: int):
    pipeline = [
        {"$match": {"_id": user_id}},
        {"$unwind": "$impressions"},
        {"$match": {"impressions.clicked": True}},
        {
            "$group": {
                "_id": "$impressions.campaign.category",
                "click_count": {"$sum": {"$size": "$impressions.clicks"}}
            }
        },
        {"$sort": {"click_count": -1, "_id": 1}},
        {"$limit": 3},
        {
            "$project": {
                "_id": 0,
                "category": "$_id",
                "click_count": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def main():
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    results = {
        "q1_all_interactions_for_user": q1_all_interactions(collection, DEMO_USER_ID),
        "q2_last_5_sessions_for_user": q2_last_5_sessions(collection, DEMO_USER_ID),
        "q3_clicks_per_hour_per_campaign_last_24h_for_advertiser": q3_clicks_per_hour_per_campaign(collection, DEMO_ADVERTISER_ID),
        "q4_ad_fatigue_users": q4_ad_fatigue(collection),
        "q5_top_3_categories_for_user": q5_top_categories(collection, DEMO_USER_ID),
    }

    with (OUTPUT_DIR / "mongo_analytics_report.json").open("w", encoding="utf-8") as f:
        f.write(json_util.dumps(results, indent=2, ensure_ascii=False))

    write_csv(OUTPUT_DIR / "q3_clicks_per_hour_per_campaign.csv", results["q3_clicks_per_hour_per_campaign_last_24h_for_advertiser"])
    write_csv(OUTPUT_DIR / "q4_ad_fatigue_users.csv", results["q4_ad_fatigue_users"])
    write_csv(OUTPUT_DIR / "q5_top_3_categories_for_user.csv", results["q5_top_3_categories_for_user"])

    print("MongoDB analytics exported to mongo_reports/")

    client.close()


if __name__ == "__main__":
    main()
