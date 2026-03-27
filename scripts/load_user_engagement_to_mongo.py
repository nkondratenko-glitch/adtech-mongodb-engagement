import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

import mysql.connector
from pymongo import MongoClient

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "adtech"),
}

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MONGO_DB = os.getenv("MONGO_DB", "adtech_mongo")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "user_engagement")

SESSION_GAP_MINUTES = int(os.getenv("SESSION_GAP_MINUTES", "30"))


def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def build_user_maps(cur) -> Dict[int, Dict[str, Any]]:
    cur.execute("""
        SELECT u.user_id, u.age, u.gender, u.location_id, l.location_name
        FROM users u
        LEFT JOIN locations l ON l.location_id = u.location_id
    """)
    users = {}
    for user_id, age, gender, location_id, location_name in cur.fetchall():
        users[user_id] = {
            "_id": int(user_id),
            "demographics": {
                "age": age,
                "gender": gender,
                "location_id": location_id,
                "location": location_name,
                "interests": [],
            },
            "impressions": [],
            "sessions": [],
            "stats": {
                "total_impressions": 0,
                "total_clicks": 0,
            },
        }

    cur.execute("""
        SELECT ui.user_id, i.interest_name
        FROM user_interests ui
        JOIN interests i ON i.interest_id = ui.interest_id
        ORDER BY ui.user_id
    """)
    for user_id, interest_name in cur.fetchall():
        if user_id in users:
            users[user_id]["demographics"]["interests"].append(interest_name)

    return users


def build_campaign_map(cur) -> Dict[int, Dict[str, Any]]:
    cur.execute("""
        SELECT c.campaign_id, c.campaign_name, c.advertiser_id, a.advertiser_name
        FROM campaigns c
        JOIN advertisers a ON a.advertiser_id = c.advertiser_id
    """)
    campaigns = {}
    for campaign_id, campaign_name, advertiser_id, advertiser_name in cur.fetchall():
        campaigns[campaign_id] = {
            "campaign_id": int(campaign_id),
            "campaign_name": campaign_name,
            "advertiser_id": int(advertiser_id),
            "advertiser_name": advertiser_name,
            "category": "general",
        }

    cur.execute("""
        SELECT cti.campaign_id, i.interest_name
        FROM campaign_target_interests cti
        JOIN interests i ON i.interest_id = cti.interest_id
        ORDER BY cti.campaign_id, cti.interest_id
    """)
    for campaign_id, interest_name in cur.fetchall():
        if campaign_id in campaigns and campaigns[campaign_id]["category"] == "general":
            campaigns[campaign_id]["category"] = interest_name

    return campaigns


def has_column(cur, table_name: str, column_name: str) -> bool:
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
    """, (table_name, column_name))
    return cur.fetchone()[0] > 0


def fetch_impression_click_rows(cur):
    device_type_exists = has_column(cur, "impressions", "device_type")

    if device_type_exists:
        query = """
            SELECT
                i.impression_id,
                i.campaign_id,
                i.user_id,
                i.impression_time,
                i.impression_cost,
                i.device_type,
                cl.click_id,
                cl.click_time,
                cl.cpc_amount
            FROM impressions i
            LEFT JOIN clicks cl ON cl.impression_id = i.impression_id
            ORDER BY i.user_id, i.impression_time, i.impression_id, cl.click_time
        """
    else:
        query = """
            SELECT
                i.impression_id,
                i.campaign_id,
                i.user_id,
                i.impression_time,
                i.impression_cost,
                'unknown' AS device_type,
                cl.click_id,
                cl.click_time,
                cl.cpc_amount
            FROM impressions i
            LEFT JOIN clicks cl ON cl.impression_id = i.impression_id
            ORDER BY i.user_id, i.impression_time, i.impression_id, cl.click_time
        """

    cur.execute(query)
    return cur.fetchall()


def build_user_documents(users, campaigns, rows):
    impression_map = {}
    user_impressions = defaultdict(list)

    for row in rows:
        (
            impression_id,
            campaign_id,
            user_id,
            impression_time,
            impression_cost,
            device_type,
            click_id,
            click_time,
            cpc_amount,
        ) = row

        if user_id not in users:
            continue

        if impression_id not in impression_map:
            campaign = campaigns.get(campaign_id, {
                "campaign_id": campaign_id,
                "campaign_name": f"Campaign_{campaign_id}",
                "advertiser_id": None,
                "advertiser_name": None,
                "category": "general",
            })

            impression_doc = {
                "impression_id": int(impression_id),
                "impression_time": impression_time,
                "device_type": device_type or "unknown",
                "cost": float(impression_cost or 0),
                "campaign": campaign,
                "clicked": False,
                "clicks": [],
            }
            impression_map[impression_id] = impression_doc
            user_impressions[user_id].append(impression_doc)

        if click_id is not None:
            impression_map[impression_id]["clicked"] = True
            impression_map[impression_id]["clicks"].append({
                "click_id": int(click_id),
                "click_time": click_time,
                "cpc_amount": float(cpc_amount or 0),
            })

    for user_id, user_doc in users.items():
        impressions = sorted(
            user_impressions.get(user_id, []),
            key=lambda x: x["impression_time"] or datetime.min
        )

        user_doc["impressions"] = impressions
        user_doc["stats"]["total_impressions"] = len(impressions)
        user_doc["stats"]["total_clicks"] = sum(len(imp["clicks"]) for imp in impressions)
        user_doc["sessions"] = build_sessions(user_id, impressions)

    return list(users.values())


def build_sessions(user_id: int, impressions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sessions: List[Dict[str, Any]] = []
    if not impressions:
        return sessions

    gap = timedelta(minutes=SESSION_GAP_MINUTES)
    current = []

    def finalize_session(session_impressions: List[Dict[str, Any]], session_number: int):
        end_candidates = []
        for imp in session_impressions:
            end_candidates.append(imp["impression_time"])
            for clk in imp["clicks"]:
                end_candidates.append(clk["click_time"])
        end_time = max(end_candidates)
        events = []
        click_count = 0
        for imp in session_impressions:
            click_count += len(imp["clicks"])
            events.append({
                "impression_id": imp["impression_id"],
                "campaign_id": imp["campaign"]["campaign_id"],
                "campaign_name": imp["campaign"]["campaign_name"],
                "category": imp["campaign"].get("category", "general"),
                "impression_time": imp["impression_time"],
                "clicked": imp["clicked"],
                "clicks": imp["clicks"],
            })
        return {
            "session_id": f"{user_id}_{session_number}",
            "session_start": session_impressions[0]["impression_time"],
            "session_end": end_time,
            "device_type": session_impressions[0]["device_type"],
            "impression_count": len(session_impressions),
            "click_count": click_count,
            "events": events,
        }

    session_number = 1
    current.append(impressions[0])

    for prev, curr in zip(impressions, impressions[1:]):
        prev_time = prev["impression_time"]
        curr_time = curr["impression_time"]
        same_device = prev["device_type"] == curr["device_type"]

        if curr_time - prev_time <= gap and same_device:
            current.append(curr)
        else:
            sessions.append(finalize_session(current, session_number))
            session_number += 1
            current = [curr]

    if current:
        sessions.append(finalize_session(current, session_number))

    sessions.sort(key=lambda s: s["session_start"], reverse=True)
    return sessions


def create_indexes(collection):
    collection.create_index("demographics.location")
    collection.create_index([("impressions.campaign.advertiser_id", 1), ("impressions.impression_time", -1)])
    collection.create_index([("impressions.campaign.campaign_id", 1), ("impressions.impression_time", -1)])
    collection.create_index([("sessions.session_start", -1)])
    collection.create_index([("impressions.clicked", 1), ("impressions.campaign.category", 1)])


def main():
    mysql_conn = get_mysql_connection()
    cur = mysql_conn.cursor()

    users = build_user_maps(cur)
    campaigns = build_campaign_map(cur)
    rows = fetch_impression_click_rows(cur)
    documents = build_user_documents(users, campaigns, rows)

    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    collection.drop()
    if documents:
        collection.insert_many(documents, ordered=False)
    create_indexes(collection)

    print(f"Loaded {len(documents)} user documents into MongoDB collection '{MONGO_COLLECTION}'.")

    cur.close()
    mysql_conn.close()
    mongo.close()


if __name__ == "__main__":
    main()
