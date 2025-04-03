# Migrating KillrVideo from Cassandra 3.x to 4.0

This guide outlines the process of migrating the KillrVideo application schema from Cassandra 3.x to 4.0, highlighting the key features introduced in Cassandra 4.0 that can improve your application's data model. 

## Table of Contents

1. [Introduction](#introduction)
2. [Key Cassandra 4.0 Features](#key-cassandra-40-features)
3. [Migration Process Overview](#migration-process-overview)
4. [Schema Enhancements](#schema-enhancements)
   - [Automatic Timestamps](#automatic-timestamps)
   - [Materialized Views](#materialized-views)
   - [Enhanced Security](#enhanced-security)
   - [Collection Improvements](#collection-improvements)
5. [Query Pattern Improvements](#query-pattern-improvements)
6. [Complete Migration Example](#complete-migration-example)
7. [Performance Considerations](#performance-considerations)
8. [Looking Ahead to Cassandra 5.0](#looking-ahead-to-cassandra-50)

## Introduction

Cassandra 4.0 introduced several improvements to data modeling capabilities while maintaining backward compatibility with 3.x schemas. This guide focuses on incremental enhancements you can make to leverage these new features without requiring a complete schema redesign.

The KillrVideo application serves as our example—a video sharing platform with users, videos, comments, ratings, and recommendations.

## Key Cassandra 4.0 Features

Cassandra 4.0 introduced several features that can enhance your data models:

1. **Current Time Functions**:
   - `currentTimestamp()`: Returns the current timestamp
   - `currentDate()`: Returns the current date
   - `currentTime()`: Returns the current time
   - `currentTimeUUID()`: Returns a TimeUUID for the current time

2. **Enhanced CQL Operations**:
   - Arithmetic operations between timestamp/date and duration values
   - Support for arithmetic operations on numeric types
   - Support for selecting Map values and Set elements in SELECT queries

3. **Materialized Views Improvements**:
   - Initial build parallelization
   - Configurable concurrent builder threads
   - More robust maintenance

4. **Security Enhancements**:
   - Role-based authentication with datacenter restrictions
   - Audit logging of database activity

5. **Other Improvements**:
   - Network Topology Strategy auto-expanding replication
   - Server-side DESCRIBE statements support
   - Better performance with the new SSTable format

## Migration Process Overview

The migration from Cassandra 3.x to 4.0 can be approached incrementally:

1. **Upgrade Infrastructure**: Upgrade Cassandra clusters to version 4.0
2. **Enhance Schemas**: Apply 4.0 features to existing schemas 
3. **Update Queries**: Modify application queries to leverage new capabilities
4. **Optimize Performance**: Fine-tune for Cassandra 4.0's performance characteristics

This guide focuses on step 2: enhancing your schema to take advantage of Cassandra 4.0 features.

## Schema Enhancements

### Automatic Timestamps

One of the most immediately useful features in Cassandra 4.0 is automatic timestamp generation.

#### Before (Cassandra 3.x):
```sql
CREATE TABLE killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp,
    email text,
    firstname text,
    lastname text
);

-- Application must generate timestamp:
INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname)
VALUES (uuid(), toTimestamp(now()), 'user@example.com', 'John', 'Doe');
```

#### After (Cassandra 4.0):
```sql
CREATE TABLE killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp DEFAULT currentTimestamp(),
    email text,
    firstname text,
    lastname text
);

-- Timestamp is automatically generated:
INSERT INTO killrvideo.users (userid, email, firstname, lastname)
VALUES (uuid(), 'user@example.com', 'John', 'Doe');
```

#### Migration Steps:
1. Alter existing tables to add DEFAULT clauses:
   ```sql
   ALTER TABLE killrvideo.users 
   ALTER created_date SET DEFAULT currentTimestamp();
   ```

2. Update application code to remove manual timestamp generation where applicable

#### Benefits:
- More consistent timestamps across your application
- Simplified application code
- Reduced client-server coordination for time-based operations

### Improved Batch Operations

While Cassandra 4.0 includes improvements to materialized views, they are still considered experimental and not recommended for production use. Instead, let's focus on the improvements to batch operations which make the traditional denormalized approach more reliable.

#### Before (Cassandra 3.x):
```sql
-- Original table
CREATE TABLE killrvideo.comments (
    videoid uuid,
    commentid timeuuid,
    comment text,
    userid uuid,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Denormalized table for by-user queries
CREATE TABLE killrvideo.comments_by_user (
    userid uuid,
    commentid timeuuid,
    comment text,
    videoid uuid,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Application writes to both tables in a batch
BEGIN BATCH
  INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
  VALUES (?, ?, ?, ?);
  
  INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
  VALUES (?, ?, ?, ?);
APPLY BATCH;
```

#### After (Cassandra 4.0):
```sql
-- Original table with auto-generated TimeUUID
CREATE TABLE killrvideo.comments (
    videoid uuid,
    commentid timeuuid DEFAULT currentTimeUUID(),
    comment text,
    userid uuid,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Denormalized table, unchanged but with improved batch reliability
CREATE TABLE killrvideo.comments_by_user (
    userid uuid,
    commentid timeuuid,
    comment text,
    videoid uuid,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Application uses more reliable batches with auto-generated TimeUUID
BEGIN BATCH
  INSERT INTO killrvideo.comments (videoid, comment, userid)
  VALUES (?, ?, ?);
  
  INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
  VALUES (?, (SELECT commentid FROM killrvideo.comments WHERE videoid = ? LIMIT 1), ?, ?);
APPLY BATCH;
```

#### Migration Steps:
1. Update the comments table to use auto-generated TimeUUIDs:
   ```sql
   ALTER TABLE killrvideo.comments 
   ALTER commentid SET DEFAULT currentTimeUUID();
   ```

2. Update application code to leverage the auto-generated TimeUUIDs

3. Continue using both tables with improved batch reliability

#### Benefits:
- Automatic TimeUUID generation reduces application complexity
- Improved batch reliability in Cassandra 4.0
- Continues to use proven denormalization patterns
- No experimental features in production

### Enhanced Security

Cassandra 4.0 introduces improved role-based access controls and audit logging.

#### Before (Cassandra 3.x):
```sql
-- Basic role
CREATE ROLE app_user WITH PASSWORD = 'password' AND LOGIN = true;

-- No datacenter restrictions available
```

#### After (Cassandra 4.0):
```sql
-- Role with datacenter restrictions
CREATE ROLE app_user 
WITH PASSWORD = 'password' 
AND LOGIN = true 
AND ACCESS TO DATACENTERS {'dc1', 'dc2'};

-- Enable audit logging in cassandra.yaml
```

#### Migration Steps:
1. Update roles with datacenter restrictions:
   ```sql
   ALTER ROLE app_user WITH ACCESS TO DATACENTERS {'dc1', 'dc2'};
   ```

2. Configure audit logging in cassandra.yaml:
   ```yaml
   audit_logging_options:
     enabled: true
     logger: BinAuditLogger
     # Additional audit configuration
   ```

#### Benefits:
- Improved security with datacenter-level access control
- Comprehensive audit trails for compliance
- Better handling of multi-datacenter deployments

### Collection Improvements

Cassandra 4.0 enhances the way you can interact with collections in queries.

#### Before (Cassandra 3.x):
```sql
-- Check if a tag exists with CONTAINS
SELECT * FROM killrvideo.videos 
WHERE videoid = ? AND tags CONTAINS 'comedy';
```

#### After (Cassandra 4.0):
```sql
-- Check if a tag exists with CONTAINS
SELECT * FROM killrvideo.videos 
WHERE videoid = ? AND tags CONTAINS 'comedy';

-- Access specific collection elements directly
SELECT videoid, tags['comedy'] FROM killrvideo.videos 
WHERE videoid = ?;
```

#### Migration Steps:
1. No schema changes required, just update queries to leverage new capabilities

#### Benefits:
- More powerful collection queries
- Reduced client-side processing
- More efficient data access patterns

## Query Pattern Improvements

Cassandra 4.0's arithmetic operations enable more sophisticated queries:

### Date Arithmetic

```sql
-- Find videos added in the last 30 days
SELECT * FROM killrvideo.videos 
WHERE added_date > currentTimestamp() - 30d;

-- Calculate how many days ago a video was added
SELECT videoid, name, 
       toDate(currentTimestamp()) - toDate(added_date) AS days_since_added 
FROM killrvideo.videos;
```

### Timestamp Manipulation

```sql
-- Find videos expiring soon (if using TTL)
SELECT * FROM killrvideo.videos 
WHERE TTL(description) < 7d;

-- Get activities on a specific date
SELECT * FROM killrvideo.user_activity 
WHERE userid = ? AND day = toDate('2023-06-15');
```

## Complete Migration Example

Let's walk through migrating a key table from the KillrVideo schema:

### Original Schema (3.x)

```sql
CREATE TABLE killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp,
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,
    userid uuid
);

CREATE TABLE killrvideo.latest_videos (
    yyyymmdd text,
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    userid uuid,
    PRIMARY KEY (yyyymmdd, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);
```

### Enhanced Schema (4.0)

```sql
-- Enhanced videos table
CREATE TABLE killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp DEFAULT currentTimestamp(),
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,
    userid uuid
);

-- Using date function for proper date partitioning
CREATE TABLE killrvideo.latest_videos (
    day date DEFAULT currentDate(),
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    userid uuid,
    PRIMARY KEY (day, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

-- Note: Although Cassandra 4.0 includes improvements to materialized views,
-- they are still considered experimental and not recommended for production use.
-- Continue using denormalized tables (latest_videos) for this access pattern.
```

### Migration Steps

1. Add default timestamps to existing table:
   ```sql
   ALTER TABLE killrvideo.videos 
   ALTER added_date SET DEFAULT currentTimestamp();
   ```

2. Create new latest_videos table with date data type:
   ```sql
   CREATE TABLE killrvideo.latest_videos_new (
       day date DEFAULT currentDate(),
       added_date timestamp,
       videoid uuid,
       name text,
       preview_image_location text,
       userid uuid,
       PRIMARY KEY (day, added_date, videoid)
   ) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);
   ```

3. Migrate data from old to new table:
   ```sql
   -- For each row in latest_videos
   INSERT INTO killrvideo.latest_videos_new (
       day, added_date, videoid, name, preview_image_location, userid
   ) VALUES (
       toDate(fromText(?)), -- Convert yyyymmdd to date
       ?, ?, ?, ?, ?
   );
   ```

4. Switch to new table:
   ```sql
   ALTER TABLE killrvideo.latest_videos RENAME TO latest_videos_old;
   ALTER TABLE killrvideo.latest_videos_new RENAME TO latest_videos;
   ```

5. Continue using the denormalized latest_videos table:
   ```sql
   -- Note: Although materialized views exist in Cassandra 4.0,
   -- they are still experimental and not recommended for production use
   ```

6. Update application to use new queries

## Performance Considerations

When migrating to Cassandra 4.0, consider these performance aspects:

1. **Denormalized Tables**: Continue using denormalized tables for high-volume access patterns. Materialized views, while improved in 4.0, are still considered experimental.

2. **Secondary Indexes**: Improved in 4.0 but still have limitations for high-cardinality columns. Denormalized tables or separate lookup tables are still recommended for high-volume queries.

3. **Default Functions**: Using DEFAULT clauses adds minimal overhead but simplifies application code.

4. **Batch Operations**: Batch reliability is improved in 4.0, but batches should still be used sparingly and only for related data that needs atomic updates.

5. **SSTable Format**: Cassandra 4.0 uses a new SSTable format with better performance. Running a full compaction after upgrading helps optimize this.

## Looking Ahead to Cassandra 5.0

While this guide focuses on Cassandra 4.0, it's worth noting that Cassandra 5.0 will introduce several game-changing features:

1. **Storage-Attached Indexes (SAI)**: A superior alternative to secondary indexes that can replace many denormalized tables

2. **Vector Types**: For AI/ML-powered recommendations and similarity search

3. **Data Masking**: For enhanced security and privacy

4. **Enhanced Mathematical and Collection Functions**: For more powerful in-database processing

If you're planning a longer-term migration strategy, consider implementing the Cassandra 4.0 changes in this guide as a stepping stone toward a future Cassandra 5.0 migration.