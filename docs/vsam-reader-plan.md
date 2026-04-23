# VSAM Reader for Cobrix `cobol` Source

## Goal

Add VSAM read support to the existing Spark SQL `cobol` data source by introducing a `vsam://` path scheme and a JZOS-backed byte/record access layer for z/OS executors.

Example target usage:

```sql
CREATE TABLE customer_vsam
USING cobol
OPTIONS (
  path 'vsam://HLQ.APP.CUSTOMER',
  copybook 'file:///u/me/customer.cpy',
  record_format 'F',
  vsam_organization 'ksds',
  vsam_key_column 'CUSTOMER-ID'
);
```

## Status

### Done

- Added `vsam://<cataloged-dsn>` path handling to the existing `cobol` source.
- Added option parsing and validation for:
  - `vsam_organization = ksds|esds|rrds`
  - `vsam_key_column = <copybook field>` for `ksds`
- Rejected unsupported or invalid configurations:
  - `vsam://` without `vsam_organization`
  - `ksds` without `vsam_key_column`
  - `vsam_key_column` on `esds` or `rrds`
  - `lds`
  - mixed VSAM and file-system paths in one read
- Added a VSAM source abstraction in `spark-cobol`.
- Added a JZOS-backed VSAM implementation using `ZFile`.
- Refactored `CobolRelation` from `TableScan` to `PrunedFilteredScan`.
- Split scan planning between file-system backends and VSAM backends.
- Implemented one Spark partition per VSAM dataset in v1.
- Added pushdown planning for:
  - `KSDS` key predicates on `vsam_key_column`
  - `ESDS` predicates on `_vsam_rba`
  - `RRDS` predicates on `_vsam_rrn`
- Added residual filter evaluation after VSAM pushdown so unsupported predicates still behave correctly.
- Added schema metadata fields:
  - `_vsam_rba: Long` for `esds`
  - `_vsam_rrn: Long` for `rrds`
- Added the JZOS dependency and excluded it from the shaded assembly.
- Added README documentation for VSAM usage and runtime requirements.
- Added unit coverage for option parsing, validation, pushdown planning, and basic backend-independent relation behavior with a fake VSAM source.

### Partially done

- `KSDS` pushdown supports equality, `IN`, and range planning, but the JZOS range reader currently compares the raw record prefix against the upper bound. This should be tightened to compare the actual encoded key field bytes from the returned record.
- `RRDS` position access is implemented through the same numeric locate path as `ESDS`, but it still needs confirmation against real z/OS RRDS behavior.
- `ESDS` sequential scans decode records correctly, but `_vsam_rba` is only populated for direct-position reads in the current implementation. Full sequential RBA population still needs proper support.

### Still to do

- Add gated z/OS integration tests outside normal CI for:
  - `spark.read.format("cobol").load("vsam://...")`
  - `CREATE TABLE ... USING cobol`
  - SQL `WHERE` predicates on KSDS key, `_vsam_rba`, and `_vsam_rrn`
- Run validation on an actual z/OS Spark runtime with matching JZOS libraries.
- Decide whether to keep VSAM limited to `record_format = F` in v1 or broaden support later.
- Consider adding richer backend-independent tests for:
  - KSDS range iteration correctness
  - sequential metadata population
  - RRDS semantics against realistic fixtures

## Notes

- Current scope is read-only.
- `LDS` remains out of scope.
- The shaded Cobrix bundle does not include JZOS; the runtime environment must provide a compatible JZOS installation.
- In this development environment, `test-compile` passes and the non-Spark VSAM suites pass, but Spark-backed tests are not fully reliable because the local Java 17 + Spark test runtime has module/logging compatibility issues unrelated to VSAM logic.
