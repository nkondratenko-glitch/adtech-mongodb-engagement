# Homework 3 MongoDB schema notes

## Collection: user_engagement

### Why one document per user
This model is optimized for user-centric queries such as "show me all interactions for user X", "show the last sessions", and "show top clicked categories". A single document avoids multiple joins and makes real-time targeting lookups faster.

### Why impressions are embedded
Impressions belong naturally to a user timeline and are almost always queried in the context of that user. Embedding impression history gives fast retrieval of all user interactions in one read.

### Why clicks are nested under impressions
A click is the result of a specific impression. Nesting clicks inside the corresponding impression preserves attribution and matches the business meaning of "this click happened because of that impression".

### Why sessions are stored separately from raw impressions
Sessions are derived behavioral groupings used in product analytics and personalization. Precomputing and embedding them avoids repeated sessionization logic during read time.

### Why campaign metadata is duplicated inside impressions
MongoDB favors read performance over full normalization. Repeating campaign name, advertiser name, and category inside the impression makes analytical and real-time queries simpler and faster.

### Trade-off
This design intentionally duplicates some campaign metadata inside user documents. That is acceptable here because the workload is dominated by reads and behavioral analysis rather than high-frequency campaign master-data updates.
