# Specification: cqlsh COPY Command Fallback Mode

**Version:** 1.0
**Date:** December 4, 2025
**Author:** Patrick
**Status:** Draft

---

## 1. Executive Summary

This specification proposes adding a fallback mechanism to cqlsh's COPY command that allows data import/export operations to succeed when direct connections to replica nodes are not possible. This addresses a fundamental incompatibility between cqlsh COPY's architecture and modern cloud/containerized Cassandra deployments.

---

## 2. Problem Statement

### 2.1 Current Behavior

The cqlsh COPY command uses a multiprocessing architecture designed for optimal performance on traditional Cassandra deployments:

1. **Token-aware routing**: COPY queries `system.peers` to discover all cluster nodes
2. **Direct connections**: Worker processes connect directly to replica nodes for each token range
3. **Parallel execution**: Multiple workers connect to different nodes simultaneously

```
┌─────────────┐     ┌──────────────┐
│   cqlsh     │────▶│  Node 1      │  (token range 0-1000)
│   COPY      │────▶│  Node 2      │  (token range 1001-2000)
│   Process   │────▶│  Node 3      │  (token range 2001-3000)
└─────────────┘     └──────────────┘
```

### 2.2 Failure Scenarios

This architecture fails in environments where replica node IPs are not directly routable:

| Environment | Why Direct Connection Fails |
|-------------|----------------------------|
| **DataStax Astra** | Nodes behind SNI proxy; `system.peers` returns internal IPs (10.0.0.x) |
| **Kubernetes** | Network policies may restrict pod-to-pod traffic |
| **Service Mesh (Istio)** | Traffic must route through sidecar proxies |
| **Cloud VPC** | Private subnets not accessible from client network |
| **SSH Tunnels** | Only tunnel endpoint is accessible |
| **Load Balancers** | Single entry point, backend nodes hidden |

### 2.3 Current Error Behavior

When COPY cannot connect to replica nodes, users see cryptic errors:

```
Failed to connect to all replicas [UUID('98a2cd7e-fa1b-3950-942e-42af495c4248'), ...]
TypeError - getaddrinfo() argument 1 must be string or None
```

Or silent timeouts:
```
No records inserted in 90 seconds, aborting
```

### 2.4 Impact

- **Astra users**: COPY command completely unusable
- **Kubernetes users**: Unpredictable failures depending on network policy
- **All users**: No clear error message explaining the root cause

---

## 3. Proposed Solution

### 3.1 Overview

Add a **fallback mode** that routes all COPY traffic through the original control connection host when direct replica connections fail or are disabled.

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   cqlsh     │────▶│  Proxy/LB    │────▶│  Node 1,2,3  │
│   COPY      │     │  (single IP) │     │  (internal)  │
│   Process   │     └──────────────┘     └──────────────┘
└─────────────┘
      │
      └── All workers use same endpoint
```

### 3.2 Behavior Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `auto` (default) | Try direct connections, fallback on failure | General use |
| `direct` | Direct connections only, fail if unreachable | Traditional clusters |
| `proxy` | Always use control connection host | Cloud/K8s/restricted networks |

---

## 4. Technical Design

### 4.1 New COPY Option

Add a new `CONNECTIONMODE` option to COPY command:

```sql
COPY keyspace.table FROM 'file.csv' WITH CONNECTIONMODE='auto';
COPY keyspace.table FROM 'file.csv' WITH CONNECTIONMODE='proxy';
COPY keyspace.table FROM 'file.csv' WITH CONNECTIONMODE='direct';
```

**Default:** `auto`

### 4.2 Configuration File Support

Allow setting default in `~/.cqlshrc`:

```ini
[copy]
connectionmode = proxy

[copy-from]
connectionmode = auto

[copy-to]
connectionmode = proxy
```

### 4.3 Code Changes

#### 4.3.1 File: `cqlshlib/copyutil.py`

**Location:** `CopyTask.__init__()` (around line 500)

```python
# Add new option parsing
self.connection_mode = options.copy.get('connectionmode', 'auto').lower()
if self.connection_mode not in ('auto', 'direct', 'proxy'):
    raise ValueError(f"Invalid CONNECTIONMODE: {self.connection_mode}. "
                     f"Must be 'auto', 'direct', or 'proxy'")
```

**Location:** `ExportTask.get_ranges()` (around line 760)

```python
def make_range_data(replicas=None):
    hosts = []
    if self.connection_mode != 'proxy' and replicas:
        for r in replicas:
            if r.is_up is not False and r.datacenter == local_dc:
                hosts.append(r.host_id)
    if not hosts:
        # Fallback to control connection host
        hosts.append(hostname)
    return {'hosts': tuple(hosts), 'attempts': 0, 'rows': 0, 'workerno': -1}
```

**Location:** `ExportTask.get_session()` (around line 1706)

```python
def get_session(self, hosts, token_range):
    """
    Returns a session connected to one of the hosts.
    In proxy mode or on connection failure, falls back to control host.
    """
    for host in hosts:
        if host in self.hosts_to_sessions:
            return self.hosts_to_sessions[host]

    # Try to connect to first available host
    last_error = None
    hosts_to_try = hosts if self.connection_mode != 'proxy' else [self.hostname]

    for host in hosts_to_try:
        try:
            session = self._create_session(host)
            self.hosts_to_sessions[host] = session
            return session
        except Exception as e:
            last_error = e
            if self.connection_mode == 'direct':
                raise  # Don't fallback in direct mode
            printdebugmsg(f"Cannot connect to {host}: {e}")
            continue

    # Fallback to control connection host (auto mode)
    if self.connection_mode == 'auto' and self.hostname not in hosts_to_try:
        printdebugmsg(f"Falling back to control host: {self.hostname}")
        try:
            session = self._create_session(self.hostname)
            # Cache under original host key to avoid repeated fallback attempts
            for host in hosts:
                self.hosts_to_sessions[host] = session
            return session
        except Exception as e:
            last_error = e

    raise last_error or RuntimeError("No available hosts for connection")

def _create_session(self, host):
    """Create a new session to the specified host."""
    endpoint = host
    if isinstance(host, UUID) and self.parent_cluster:
        for h in self.parent_cluster.metadata.all_hosts():
            if h.host_id == host:
                endpoint = h.endpoint if isinstance(h.endpoint, SniEndPoint) else h.endpoint.address
                break

    new_cluster = cluster_factory(
        endpoint,
        whitelist_lbp=endpoint,
        port=self.port,
        cql_version=self.cql_version,
        protocol_version=self.protocol_version,
        auth_provider=self.auth_provider,
        ssl_options=ssl_settings(endpoint, self.config_file) if self.ssl else None,
        compression=None,
        control_connection_timeout=self.connect_timeout,
        connect_timeout=self.connect_timeout,
        idle_heartbeat_interval=0,
        execution_profiles=self._make_execution_profiles(),
        cloud=self.parent_cluster.cloud if self.parent_cluster else None)

    return ExportSession(new_cluster, self)
```

**Location:** `ImportProcess.session` property (around line 2435)

Apply similar fallback logic for import operations.

#### 4.3.2 File: `cqlshlib/cqlsh.py`

**Location:** COPY option definitions (around line 1730)

Add to documentation string:
```python
CONNECTIONMODE='auto'   - connection strategy: 'auto' (try direct, fallback to proxy),
                          'direct' (fail if replicas unreachable),
                          'proxy' (always use control connection)
```

#### 4.3.3 File: `cqlshlib/copyutil.py` - Options parsing

**Location:** `CopyOptions` class

```python
COPY_COMMON_OPTIONS = {
    # ... existing options ...
    'connectionmode': 'auto',
}
```

### 4.4 Cloud Detection (Optional Enhancement)

Auto-detect cloud environments and default to proxy mode:

```python
def _detect_connection_mode(self):
    """Auto-detect appropriate connection mode."""
    if self.connection_mode != 'auto':
        return self.connection_mode

    # Check if connected via cloud bundle
    if self.parent_cluster and self.parent_cluster.cloud:
        printdebugmsg("Cloud cluster detected, using proxy mode")
        return 'proxy'

    # Check if control host is SNI endpoint
    if isinstance(self.host.endpoint, SniEndPoint):
        printdebugmsg("SNI endpoint detected, using proxy mode")
        return 'proxy'

    return 'auto'
```

---

## 5. User Experience

### 5.1 Improved Error Messages

When connection mode is `auto` and fallback occurs:

```
Warning: Cannot connect directly to replica nodes.
Falling back to proxy mode through control host (slower but functional).
Hint: Use CONNECTIONMODE='proxy' to suppress this warning.

Starting copy of ks.table with columns [id, name, data].
Processed 10000 rows; Rate: 2500 rows/s; Avg: 2500 rows/s
```

When connection mode is `direct` and connection fails:

```
Error: Cannot connect to replica node 10.0.0.2 (unreachable).
This may occur in cloud or containerized environments where node IPs are not directly accessible.
Hint: Try CONNECTIONMODE='auto' or CONNECTIONMODE='proxy' to route through the control connection.
```

### 5.2 Performance Implications Warning

When proxy mode is active:

```
Note: Running in proxy mode. All data will be routed through a single connection.
This may be slower than direct mode for large datasets.
For best performance with accessible nodes, use CONNECTIONMODE='direct'.
```

---

## 6. Backward Compatibility

### 6.1 Default Behavior

- Default mode is `auto`, which attempts direct connections first
- Existing scripts with accessible nodes will see no change
- Scripts in restricted environments will now succeed instead of failing

### 6.2 Explicit Direct Mode

Users who require the old behavior can use:
```sql
COPY ks.table FROM 'file.csv' WITH CONNECTIONMODE='direct';
```

### 6.3 Configuration Migration

No migration required. Existing `.cqlshrc` files continue to work.

---

## 7. Testing Requirements

### 7.1 Unit Tests

| Test Case | Description |
|-----------|-------------|
| `test_connection_mode_parsing` | Verify option parsing for all valid modes |
| `test_invalid_connection_mode` | Verify error on invalid mode value |
| `test_proxy_mode_single_host` | Verify all workers use control host in proxy mode |
| `test_auto_mode_fallback` | Verify fallback triggers on connection failure |
| `test_direct_mode_no_fallback` | Verify direct mode fails without fallback |

### 7.2 Integration Tests

| Test Case | Description |
|-----------|-------------|
| `test_copy_from_astra` | COPY FROM with Astra cloud database |
| `test_copy_to_astra` | COPY TO with Astra cloud database |
| `test_copy_k8s_restricted` | COPY with Kubernetes network policy blocking pod IPs |
| `test_copy_ssh_tunnel` | COPY through SSH tunnel to remote cluster |
| `test_copy_large_dataset_proxy` | Performance test with proxy mode on large dataset |

### 7.3 Performance Benchmarks

Compare throughput for:
- Direct mode (baseline)
- Proxy mode (single connection)
- Auto mode with fallback

Expected: Proxy mode ~30-50% slower for large datasets due to single connection bottleneck.

---

## 8. Security Considerations

### 8.1 No New Attack Surface

- Proxy mode uses existing authenticated connection
- No new ports or protocols introduced
- All existing SSL/TLS settings apply

### 8.2 Audit Logging

When fallback occurs, log for debugging:
```
[INFO] COPY connection fallback: replica 10.0.0.2 unreachable, using control host proxy.example.com
```

### 8.3 Network Policy Compliance

Proxy mode is actually MORE secure in some environments:
- All traffic goes through approved ingress point
- No need to open firewall rules to all node IPs
- Compatible with zero-trust network architectures

---

## 9. Documentation Updates

### 9.1 cqlsh Documentation

Add section to COPY command documentation:

```
CONNECTION MODES

The CONNECTIONMODE option controls how COPY connects to cluster nodes:

  auto   - (Default) Attempts direct connections to replica nodes for optimal
           performance. Falls back to routing through the control connection
           if direct connections fail.

  direct - Requires direct connections to replica nodes. Fails if nodes are
           unreachable. Use this for traditional on-premise deployments where
           all nodes are directly accessible.

  proxy  - Routes all traffic through the control connection (the host cqlsh
           initially connected to). Use this for cloud databases, Kubernetes,
           or any environment where node IPs are not directly accessible.

Examples:

  -- Auto mode (recommended for most users)
  COPY ks.table FROM 'data.csv' WITH HEADER=TRUE;

  -- Force proxy mode for cloud database
  COPY ks.table FROM 'data.csv' WITH HEADER=TRUE AND CONNECTIONMODE='proxy';

  -- Force direct mode for on-premise cluster
  COPY ks.table FROM 'data.csv' WITH HEADER=TRUE AND CONNECTIONMODE='direct';
```

### 9.2 Troubleshooting Guide

Add troubleshooting entry:

```
COPY command fails with "Cannot connect to replica" or timeout

Symptoms:
  - "Failed to connect to all replicas [UUID(...)]"
  - "No records inserted in 90 seconds, aborting"
  - Works with INSERT but fails with COPY

Cause:
  COPY attempts to connect directly to cluster nodes, which may not be
  accessible in cloud or containerized environments.

Solution:
  Use proxy connection mode:

  COPY ks.table FROM 'data.csv' WITH CONNECTIONMODE='proxy';

  Or set as default in ~/.cqlshrc:

  [copy]
  connectionmode = proxy
```

---

## 10. Implementation Plan

### Phase 1: Core Implementation
1. Add `CONNECTIONMODE` option parsing
2. Implement proxy mode (always use control host)
3. Add basic fallback in auto mode
4. Update help text and error messages

### Phase 2: Enhanced Auto-Detection
1. Detect cloud clusters automatically
2. Detect SNI endpoints
3. Add performance warnings

### Phase 3: Testing & Documentation
1. Unit tests for all modes
2. Integration tests with Astra/K8s
3. Performance benchmarks
4. Documentation updates

---

## 11. Open Questions

1. **Should NUMPROCESSES be automatically set to 1 in proxy mode?**
   - Pro: Multiple processes to same host may not help
   - Con: Could still benefit from parallel CSV parsing

2. **Should we add a global cqlsh flag `--proxy-mode`?**
   - Pro: Easier than setting option on every COPY
   - Con: `.cqlshrc` already provides this

3. **Should cloud detection be opt-out rather than opt-in?**
   - Pro: Better UX for Astra users
   - Con: Unexpected behavior change for edge cases

---

## 12. References

- Apache Cassandra JIRA: [Link TBD]
- cqlsh source: `pylib/cqlshlib/copyutil.py`
- DataStax Astra architecture: https://docs.datastax.com/en/astra/
- CASSANDRA-19118: Vector COPY support (related but separate issue)

---

## Appendix A: Full Diff Preview

See attached patch file: `cqlsh-copy-fallback.patch`

## Appendix B: Test Environment Setup

### Astra Test Database
```bash
astra db create copy-test --region us-east1
astra db cqlsh copy-test
```

### Kubernetes Test (kind)
```bash
kind create cluster
helm install cassandra bitnami/cassandra
kubectl apply -f network-policy-block-pods.yaml
```
