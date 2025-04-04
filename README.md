# KillrVideo Database Schemas

Cassandra database schemas and search integration for the KillrVideo reference application.

## Key Components

![](https://img.shields.io/badge/Cassandra-3.x-5C4D7D?logo=apache-cassandra)
![](https://img.shields.io/badge/Cassandra-5.x-5C4D7D?logo=apache-cassandra)
![](https://img.shields.io/badge/License-Apache_2.0-2599AF)

### Schema Versions
- `schema-v3.cql` - Base schema for Cassandra 3.x
- `schema-v4.cql` - Enhanced schema with Cassandra 4.0 features
- `schema-v5.cql` - Latest schema with Cassandra 5.0 improvements

### Example Resources
- Sample datasets and queries for v4/v5
- Search integration (DSE Solr config/schema)
- Graph schema for recommendations (DSE only)
- Migration guides (3.x → 4.0 → 5.0)

### Directory Overview
```
killrvideo-database/
├── examples/          # Sample datasets and queries
├── graph/             # Graph database schema
├── migrating/         # Version migration guides
├── search/            # Search configuration
└── schema-v*.cql      # Versioned schema definitions
```

## Quick Start
1. Load schema: `cqlsh -f schema-v5.cql`
2. Import sample data: `cqlsh -f examples/schema-v5-data-examples.cql`

> **Note:** Requires [CQLSH](https://cassandra.apache.org/doc/latest/cassandra/tools/cqlsh.html) to be installed and configured for your Cassandra cluster.

[Apache License 2.0](LICENSE)
