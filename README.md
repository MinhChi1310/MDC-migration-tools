# Logistics.DbMerger

A .NET 8 console application for merging the **MdcProd** (Source) SQL Server database into **AdcProd** (Target). Built for the GOSEI Logistics platform to consolidate two separate operational databases into one.

## Purpose

MDC and ADC are two separate Logistics platform instances that share the same schema structure but have diverged over time (different table names, extra columns, different tenant IDs, different user IDs). This tool automates:

1. **Schema Sync** — Creates missing tables in ADC and adds missing columns to existing ones. Generates high-fidelity `CREATE TABLE` scripts including Primary Keys, Default Constraints, Computed Columns, Check Constraints, and Non-Clustered Indexes.
2. **Object Sync** — Migrates missing Stored Procedures, Views, and Functions from MDC to ADC.
3. **Data Sync** — Copies row data with smart transformations:
   - **Tenant ID remapping** — Resolves or creates the tenant in ADC and rewrites `TenantId` columns across all tables.
   - **User ID remapping** — Matches users by `UserName` and rewrites audit fields (`CreatorUserId`, `LastModifierUserId`, `CreatedBy`, `ModifiedBy`, etc.).
   - **Tiered migration order** — Processes tables in 8 dependency tiers (reference → core → transactional → audit) to preserve FK integrity.
   - **Fuzzy table matching** — Handles singular/plural naming differences (e.g. `IndirectClockEvents` → `IndirectClockEvent`).
   - **Explicit table mappings** — Configurable name overrides for tables that don't follow conventions.
   - **Column injection** — Inserts default values for ADC-only columns (e.g. `Contact.OldSAPID`, `Contact.PartTimeFlex`).
4. **Validation** — Post-migration row count verification, FK integrity checks, and business logic queries.
5. **Rollback** — Generates per-step SQL rollback scripts (`DROP TABLE`, `DROP COLUMN`) for safe undo.

## Architecture

| File | Responsibility |
|---|---|
| `Program.cs` | Entry point, interactive menu, tenant/user resolution, migration orchestration |
| `SchemaSync.cs` | Table creation, column evolution, dynamic `CREATE TABLE` script generation |
| `DataMigrator.cs` | `SqlBulkCopy`-based data transfer with in-flight `TenantId` and `UserId` transformation |
| `ObjectSync.cs` | Stored Procedure / View / Function migration |
| `MigrationConfig.cs` | Defines the 8-tier table processing order (185 tables) |
| `Validator.cs` | Post-migration integrity checks (row counts, FK violations, business rules) |
| `RollbackLogger.cs` | Generates timestamped SQL rollback scripts |
| `appsettings.json` | Connection strings and runtime settings |

## Prerequisites

- .NET 8.0 SDK
- Network access to both MDC and ADC SQL Server instances
- SQL login with `CREATE TABLE`, `INSERT`, `ALTER`, and `SELECT` permissions on both databases

## Configuration

Edit `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "SourceMdc": "Server=MDC_SERVER;Database=MdcProd;User Id=...;Password=...;TrustServerCertificate=True;",
    "TargetAdc": "Server=ADC_SERVER;Database=AdcProd;User Id=...;Password=...;TrustServerCertificate=True;"
  },
  "Settings": {
    "BatchSize": 5000,
    "DryRun": true
  }
}
```

> **DryRun = true** prints what the tool *would* do without modifying either database. Always start with a dry run.

## Usage

### Interactive Mode (Recommended)

```powershell
cd Logistics.DbMerger
dotnet run
```

The menu will display:

```
=== Logistics DB Merger Tool ===
[Main Menu]
1. Sync Schema (Tables & Columns)
2. Sync Objects (Procedures, Views, Functions)
3. Sync Data (Smart Merge & Tenant Filter)
4. Validate / Verify
5. Rollback Last Action
6. Exit
```

**Recommended execution order:** 1 → 2 → 3 → 4. Option 3 will prompt for a tenant name to filter by (or press Enter for all).

### Automated Mode

```powershell
dotnet run -- --tenant="Kewdale"
```

Runs the full migration pipeline for the specified tenant without the interactive menu.

## Key Design Decisions

- **Identity preservation**: `SqlBulkCopy` uses `KeepIdentity` so source PKs are preserved in the target.
- **Buffered transform**: When `TenantId` or `UserId` transformation is needed, data is loaded into a `DataTable` for in-memory mutation before bulk insert. This trades memory for correctness.
- **Streaming mode**: When no transformation is required, data streams directly from `SqlDataReader` to `SqlBulkCopy` for optimal performance.
- **Safe-by-default**: New columns added via `ALTER TABLE` are always `NULL` to avoid breaking existing data.

## Dependencies

| Package | Purpose |
|---|---|
| `Microsoft.Data.SqlClient` | SQL Server connectivity |
| `Dapper` | Lightweight ORM for metadata queries |
| `Microsoft.Extensions.Configuration` | `appsettings.json` loading |

## Extending the Tool

- **Add new table mappings**: Edit `ExplicitTableMappings` dictionary in `Program.cs`.
- **Change migration order**: Edit the tier lists in `MigrationConfig.cs`.
- **Add column injection rules**: See the `Contact` table handling in `DataMigrator.cs` (line ~73).
- **Add validation queries**: Extend `Validator.cs` with new business logic checks.
