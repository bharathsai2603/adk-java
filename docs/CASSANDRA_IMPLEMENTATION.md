# Cassandra Implementation

**Author:** Sandeep Belgavi
**Date:** November 5, 2025

This document has been merged into the main memory documentation.
Please refer to [Implementing and Using Memory in ADK](./memory.md) for details on the Cassandra-backed memory store.

---

## Table Definitions

### 1. Sessions Table

```cql
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT,
    app_name TEXT,
    user_id TEXT,
    state TEXT,
    event_data TEXT,
    last_update_time BIGINT,
    PRIMARY KEY ((app_name, user_id), id)
);
```

**Description:**
Stores session-level metadata for each user and app combination, including session state and the most recent update timestamp in epoch.

---

### 2. Events Table

```cql
CREATE TABLE IF NOT EXISTS events (
    id TEXT,
    session_id TEXT,
    author TEXT,
    actions_state_delta TEXT,
    actions_artifact_delta TEXT,
    actions_requested_auth_configs TEXT,
    actions_transfer_to_agent TEXT,
    content_role TEXT,
    timestamp BIGINT,
    invocation_id TEXT,
    created_at BIGINT,
    PRIMARY KEY ((session_id), id)
);
```

**Description:**
Maintains normalized event data linked to sessions, capturing authorship, timestamps, and action details.

---

### 3. Event Content Parts Table

```cql
CREATE TABLE IF NOT EXISTS event_content_parts (
    event_id TEXT,
    session_id TEXT,
    part_type TEXT,
    text_content TEXT,
    function_call_id TEXT,
    function_call_name TEXT,
    function_call_args TEXT,
    function_response_id TEXT,
    function_response_name TEXT,
    function_response_data TEXT,
    created_at BIGINT,
    PRIMARY KEY ((session_id), event_id)
);
```

**Description:**
Stores the detailed content for each event, including function calls and responses, enabling normalized content retrieval.

---

### 4. Artifacts Table

```cql
CREATE TABLE IF NOT EXISTS artifacts (
    app_name TEXT,
    user_id TEXT,
    session_id TEXT,
    filename TEXT,
    version INT,
    artifact_data BLOB,
    PRIMARY KEY ((app_name, user_id, session_id), filename, version)
);
```

**Description:**
Manages versioned binary artifacts (e.g., generated files or outputs) linked to specific sessions and users.

---

### 5. RAE (Retrieval-Augmented Experience) Data Table

```cql
CREATE TABLE IF NOT EXISTS rae_data (
    agent_name TEXT,
    user_id TEXT,
    turn_id TIMEUUID,
    data TEXT,
    embedding VECTOR<FLOAT, 3072>,
    PRIMARY KEY ((agent_name, user_id), turn_id)
);
```

**Description:**
Stores retrieval-augmented experience (RAG/memory) entries for agents, including vector embeddings for semantic similarity search.

---

### 6. Vector Similarity Index

```cql
CREATE CUSTOM INDEX IF NOT EXISTS rae_data_embedding_idx
ON rae_data (embedding)
USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
WITH OPTIONS = {'similarity_function': 'COSINE'};
```

**Description:**
Creates a **Storage-Attached Index (SAI)** for vector similarity search on the `embedding` column in the `rae_data` table using **COSINE similarity**.

