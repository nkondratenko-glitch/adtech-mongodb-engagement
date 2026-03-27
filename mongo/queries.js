// Homework 3 MongoDB queries
// Collection: user_engagement

use adtech_mongo;

// Q1. Retrieve all ad interactions for a specific user, including impressions and clicks
db.user_engagement.find(
  { _id: 12345 },
  {
    _id: 1,
    demographics: 1,
    impressions: 1,
    stats: 1
  }
);

// Q2. Retrieve a user's last 5 ad sessions with timestamps and click behavior
// Assumes sessions are stored in descending order by session_start.
db.user_engagement.aggregate([
  { $match: { _id: 12345 } },
  {
    $project: {
      _id: 1,
      demographics: 1,
      last_5_sessions: { $slice: ["$sessions", 5] }
    }
  }
]);

// Q3. Number of ad clicks per hour per campaign in the last 24 hours for a specified advertiser
db.user_engagement.aggregate([
  { $unwind: "$impressions" },
  { $match: { "impressions.campaign.advertiser_id": 3 } },
  { $unwind: "$impressions.clicks" },
  {
    $match: {
      "impressions.clicks.click_time": {
        $gte: new Date(Date.now() - 24 * 60 * 60 * 1000)
      }
    }
  },
  {
    $group: {
      _id: {
        campaign_id: "$impressions.campaign.campaign_id",
        campaign_name: "$impressions.campaign.campaign_name",
        hour: {
          $dateTrunc: {
            date: "$impressions.clicks.click_time",
            unit: "hour"
          }
        }
      },
      click_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      campaign_id: "$_id.campaign_id",
      campaign_name: "$_id.campaign_name",
      hour: "$_id.hour",
      click_count: 1
    }
  },
  { $sort: { hour: 1, campaign_id: 1 } }
]);

// Q4. Find users who have seen the same ad 5+ times but never clicked
db.user_engagement.aggregate([
  { $unwind: "$impressions" },
  {
    $group: {
      _id: {
        user_id: "$_id",
        campaign_id: "$impressions.campaign.campaign_id",
        campaign_name: "$impressions.campaign.campaign_name"
      },
      impression_count: { $sum: 1 },
      click_count: {
        $sum: {
          $cond: [{ $eq: ["$impressions.clicked", true] }, 1, 0]
        }
      }
    }
  },
  {
    $match: {
      impression_count: { $gte: 5 },
      click_count: 0
    }
  },
  {
    $project: {
      _id: 0,
      user_id: "$_id.user_id",
      campaign_id: "$_id.campaign_id",
      campaign_name: "$_id.campaign_name",
      impression_count: 1,
      click_count: 1
    }
  },
  { $sort: { impression_count: -1, user_id: 1 } }
]);

// Q5. Retrieve a user's top 3 most engaged ad categories based on past clicks
db.user_engagement.aggregate([
  { $match: { _id: 12345 } },
  { $unwind: "$impressions" },
  { $match: { "impressions.clicked": true } },
  {
    $group: {
      _id: "$impressions.campaign.category",
      click_count: { $sum: { $size: "$impressions.clicks" } }
    }
  },
  { $sort: { click_count: -1, _id: 1 } },
  { $limit: 3 },
  {
    $project: {
      _id: 0,
      category: "$_id",
      click_count: 1
    }
  }
]);

// Recommended indexes
db.user_engagement.createIndex({ "demographics.location": 1 });
db.user_engagement.createIndex({ "impressions.campaign.advertiser_id": 1, "impressions.impression_time": -1 });
db.user_engagement.createIndex({ "impressions.campaign.campaign_id": 1, "impressions.impression_time": -1 });
db.user_engagement.createIndex({ "sessions.session_start": -1 });
db.user_engagement.createIndex({ "impressions.clicked": 1, "impressions.campaign.category": 1 });
