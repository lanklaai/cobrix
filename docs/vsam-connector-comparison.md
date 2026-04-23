# VSAM_Connector Comparison Note

## Purpose

This note compares the `VSAM_Connector` subproject with the VSAM support added to Cobrix `spark-cobol`.

`VSAM_Connector` is a separate project that exposes VSAM data through:

- a read-only JDBC driver
- an optional Apache Avatica bridge
- multiple transport backends (`JZOS`, `ZOSCONNECT`, `FTP`, `LOCAL`)

Cobrix, by contrast, adds VSAM access directly to the Spark SQL `cobol` data source using `format("cobol")` and `vsam://...` paths.

## High-Level Difference

### VSAM_Connector

- SQL/JDBC-first design
- Intended for JDBC tools and remote SQL clients
- Includes an Avatica bridge so Spark can access VSAM through generic JDBC
- Decodes records outside Spark, then returns rows over JDBC

### Cobrix VSAM support

- Spark data-source-first design
- Keeps the existing Cobrix usage model: `format("cobol")` and `USING cobol`
- Pushdown happens inside the Spark relation
- Copybook parsing and decoding stay in the native Cobrix read path

## Dataset Type Handling in VSAM_Connector

## KSDS

`VSAM_Connector` does implement basic KSDS support.

- The model treats KSDS as a "keyed" dataset.
- SQL pushdown is based on the copybook key field.
- `ZOSCONNECT` backend supports direct single-record key lookup.
- `JZOS` backend supports cursor positioning with `ZFile.locate()`.

Evidence:

- `FetchHint` is built only for keyed tables in [VSAM_Connector/src/main/java/com/vsam/jdbc/sql/VsamSelectExecutor.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/sql/VsamSelectExecutor.java:186)
- keyed vs non-keyed dataset modelling is in [VSAM_Connector/src/main/java/com/vsam/jdbc/metadata/VsamTableDefinition.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/metadata/VsamTableDefinition.java:38)
- JZOS locate logic is in [VSAM_Connector/src/main/java/com/vsam/jdbc/backend/jzos/JzosBackend.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/backend/jzos/JzosBackend.java:125)
- z/OS Connect key lookup is in [VSAM_Connector/src/main/java/com/vsam/jdbc/backend/zosconnect/ZosConnectBackend.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/backend/zosconnect/ZosConnectBackend.java:178)

### KSDS limitation

The docs claim key-range support for JZOS, but the SQL planner only extracts simple key equality.

- The docs say `BETWEEN` / `>=` / `<=` can trigger JZOS range positioning.
- The actual SQL execution layer only builds `FetchHint.keyLow/keyHigh` from a simple equality predicate on the key field.

That means:

- direct key equality lookup is implemented
- backend-side range logic is partially present
- SQL-driven range pushdown does not appear fully implemented

Relevant files:

- claimed range behavior: [VSAM_Connector/docs/sql-reference.md](/source/cobrix/VSAM_Connector/docs/sql-reference.md:185)
- actual planner behavior: [VSAM_Connector/src/main/java/com/vsam/jdbc/sql/VsamSelectExecutor.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/sql/VsamSelectExecutor.java:201)

## ESDS

`VSAM_Connector` supports ESDS only as a non-keyed sequential scan.

- No ESDS-specific metadata column exists.
- No `_vsam_rba`-style access path exists.
- No ESDS position predicate pushdown was found.

The docs explicitly describe ESDS as sequential only for the JZOS backend.

Relevant files:

- [VSAM_Connector/docs/backends.md](/source/cobrix/VSAM_Connector/docs/backends.md:138)
- [VSAM_Connector/src/main/java/com/vsam/jdbc/backend/jzos/JzosBackend.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/backend/jzos/JzosBackend.java:54)

## RRDS

`VSAM_Connector` does not implement RRDS as a distinct access mode.

- RRDS is grouped together with ESDS as "not keyed".
- No RRN metadata column exists.
- No RRDS-specific predicate pushdown exists.
- No separate RRDS locate path exists in the backends.

The docs mention RRDS, but only as sequential scan only.

Relevant files:

- [VSAM_Connector/docs/backends.md](/source/cobrix/VSAM_Connector/docs/backends.md:139)
- [VSAM_Connector/src/main/java/com/vsam/jdbc/metadata/VsamTableDefinition.java](/source/cobrix/VSAM_Connector/src/main/java/com/vsam/jdbc/metadata/VsamTableDefinition.java:43)

## LDS

No LDS support was found.

- No dataset model includes LDS as a separate organization.
- No backend logic refers to LDS access semantics.
- No docs claim LDS support.

Based on the current code structure, LDS is not supported.

## Cobrix vs VSAM_Connector

## What VSAM_Connector does that Cobrix does not

- provides a read-only JDBC driver
- provides `INFORMATION_SCHEMA` metadata tables for JDBC tools
- provides an Avatica bridge so non-z/OS clients can access live VSAM over HTTP/JDBC
- supports multiple backends beyond direct JZOS:
  - `ZOSCONNECT`
  - `FTP`
  - `LOCAL`
  - `JZOS`

## What Cobrix does that VSAM_Connector does not

- integrates directly into Spark as a native `cobol` data source
- keeps the normal Cobrix Spark API and SQL DDL model
- distinguishes KSDS, ESDS, and RRDS explicitly in the option model
- adds Spark-visible metadata columns for:
  - `_vsam_rba`
  - `_vsam_rrn`
- supports Spark-side residual filter handling after pushdown planning

## Dataset-Type Comparison Summary

| Capability | VSAM_Connector | Cobrix VSAM |
|---|---|---|
| KSDS | Yes, basic keyed support | Yes |
| KSDS equality pushdown | Yes | Yes |
| KSDS range pushdown | Partially claimed, not clearly completed in planner | Yes, planned and wired, though adapter still needs refinement |
| ESDS support | Sequential only | Yes |
| ESDS RBA pushdown | No | Yes |
| RRDS support | Sequential only, not distinct | Yes |
| RRDS RRN pushdown | No | Yes |
| LDS support | No | No |
| JDBC / BI tooling | Yes | No |
| Native Spark `format("cobol")` use | No | Yes |

## Practical Conclusion

`VSAM_Connector` is useful if the goal is:

- JDBC access
- Avatica access
- DBeaver / BI tool integration
- remote SQL access from non-z/OS clients

It is not a full VSAM organization-aware implementation in the same sense as the Cobrix VSAM reader work. Its effective model is:

- KSDS as keyed
- ESDS and RRDS as generic non-keyed sequential datasets
- no LDS

So if the question is whether `VSAM_Connector` already solved KSDS, ESDS, RRDS, and LDS comprehensively, the answer is no. It provides:

- a meaningful KSDS path
- a generic sequential path for ESDS/RRDS
- no LDS support
