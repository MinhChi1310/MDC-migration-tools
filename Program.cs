using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Logistics DB Merger Tool ===");

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfiguration config = builder.Build();

            string sourceConn = config.GetConnectionString("SourceMdc");
            string targetConn = config.GetConnectionString("TargetAdc");
            bool dryRun = config.GetValue<bool>("Settings:DryRun");
            int batchSize = config.GetValue<int>("Settings:BatchSize");

            if (string.IsNullOrEmpty(sourceConn) || string.IsNullOrEmpty(targetConn) || sourceConn.Contains("YOUR_"))
            {
                Console.WriteLine("[Error] Please configure valid connection strings in appsettings.json");
                return;
            }

            // Command Line Args for automation (bypass menu if detected)
            if (args.Length > 0 && args.Any(a => a.StartsWith("--tenant") || a.StartsWith("--mode")))
            {
                // Fallback to old automated behavior or implement --mode=schema etc.
                // For now, let's keep it simple: if args exist, run Step 3 (Data) assuming Schema is done?
                // Or just standard run. Let's redirect to standard full run if --tenant present.
                Console.WriteLine("[Auto-Run] Arguments detected. Running full migration...");
                await RunFullMigration(sourceConn, targetConn, batchSize, dryRun, args);
                return;
            }

            while (true)
            {
                Console.WriteLine("\n[Main Menu]");
                Console.WriteLine("1. Sync Schema (Tables & Columns)");
                Console.WriteLine("2. Sync Objects (Procedures, Views, Functions)");
                Console.WriteLine("3. Sync Data (Smart Merge & Tenant Filter)");
                Console.WriteLine("4. Validate / Verify");
                Console.WriteLine("5. Rollback Last Action");
                Console.WriteLine("6. Exit");
                Console.WriteLine("7. List tables only in MDC (console + file) and create structure in ADC");
                Console.WriteLine("8. Clear migration data (delete rows in ADC based on IdMapping)");
                Console.Write("Select an option: ");
                
                var key = Console.ReadLine();
                try
                {
                    switch (key)
                    {
                        case "1":
                            await RunSchemaSync(sourceConn, targetConn, dryRun);
                            break;
                        case "2":
                            await RunObjectSync(sourceConn, targetConn, dryRun);
                            break;
                        case "3":
                            await RunDataSync(sourceConn, targetConn, batchSize, dryRun);
                            break;
                        case "4":
                            await RunValidation(sourceConn, targetConn);
                            break;
                        case "5":
                            await RunRollback(targetConn);
                            break;
                        case "6":
                            return;
                        case "7":
                            await RunListTablesOnlyInMdcAndCreateStructureAsync(sourceConn, targetConn, dryRun);
                            break;
                        case "8":
                            await RunClearMigrationDataAsync(targetConn);
                            break;
                        default:
                            Console.WriteLine("Invalid selection.");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Fatal Error] {ex.Message}");
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }

        static async Task RunSchemaSync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Step 1] Schema Sync");
            RollbackLogger.Initialize("schema");
            var schemaSync = new SchemaSync(sourceConn, targetConn);
            
            Console.WriteLine("Checking missing tables...");
            var missingTables = await schemaSync.GetMissingTablesAsync();
            Console.WriteLine($"Found {missingTables.Count} missing tables.");

            if (!dryRun)
            {
                foreach (var table in missingTables)
                {
                    // Skip if explicitly mapped (we map to existing table instead of creating new one with source name)
                    if (ExplicitTableMappings.ContainsKey(table))
                    {
                        Console.WriteLine($"[Schema] Skipping creation of '{table}' (Explicitly mapped to '{ExplicitTableMappings[table]}')");
                        continue;
                    }
                    await schemaSync.SyncTableAsync(table);
                }
            }
            else
            {
                Console.WriteLine("[DryRun] Skipping table creation.");
            }

            // Sync Missing Columns for EXISTING Common Tables
            Console.WriteLine("\nChecking for missing columns in common tables...");
            var sourceTables = await schemaSync.GetExistingSourceTablesAsync();
            var targetTables = await schemaSync.GetExistingTargetTablesAsync();
            
            // Identify common tables (by exact name)
            var commonTables = sourceTables.Intersect(targetTables, StringComparer.OrdinalIgnoreCase).ToList();
            Console.WriteLine($"Found {commonTables.Count} common tables. Scanning columns...");

            foreach (var table in commonTables)
            {
                if (table == "sysdiagrams" || table == "Tenants" || table.StartsWith("__")) continue;

                // This adds missing columns if any
                await schemaSync.SyncTableSchemaAsync(table, table, dryRun);
            }

            // Sync Missing Columns for EXPLICIT MAPPED Tables (that are technically "missing" via commonTables logic)
            Console.WriteLine("Checking explicit mappings...");
            foreach (var kvp in ExplicitTableMappings)
            {
                var sourceTable = kvp.Key;
                var targetTable = kvp.Value;

                if (sourceTables.Contains(sourceTable, StringComparer.OrdinalIgnoreCase) && 
                    targetTables.Contains(targetTable, StringComparer.OrdinalIgnoreCase))
                {
                     Console.WriteLine($"[Mapping] Checking {sourceTable} -> {targetTable}...");
                     await schemaSync.SyncTableSchemaAsync(sourceTable, targetTable, dryRun);
                }
            }
        }

        static async Task RunObjectSync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Step 2] Object Sync");
            RollbackLogger.Initialize("objects");
            var objectSync = new ObjectSync(sourceConn, targetConn);
            await objectSync.SyncObjectsAsync(dryRun);
        }

        static async Task RunDataSync(string sourceConn, string targetConn, int batchSize, bool dryRun)
        {
            Console.WriteLine("\n--> [Step 3] Data Sync");
            // Data Step doesn't necessarily create objects to rollback (except fuzzy match columns?)
            // Fuzzy match columns use RollbackLogger internally.
            // So we should init context "data_schema" perhaps?
            RollbackLogger.Initialize("data_schema");

            // Interactive Tenant Prompt
            Console.Write("\n[Input] Enter Tenant Name to filter by (or press Enter for ALL): ");
            // ... (rest of logic)
            string tenantName = Console.ReadLine()?.Trim();
            int? sourceTenantId = null;
            int? targetTenantId = null;

            using (var source = new SqlConnection(sourceConn))
            using (var target = new SqlConnection(targetConn))
            {
                if (!string.IsNullOrEmpty(tenantName))
                {
                    // 1. Resolve Source Tenant
                    sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (sourceTenantId == null)
                        sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                    
                    if (sourceTenantId == null)
                    {
                        Console.WriteLine($"[Error] Source Tenant '{tenantName}' not found.");
                        return;
                    }
                    Console.WriteLine($"[TenantFilter] Resolved Source ID: {sourceTenantId}");

                    // 2. Resolve/Create Target Tenant
                    // Check if exists by Name (Assume Name unique)
                    var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (existingTargetId == null)
                        existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (existingTargetId != null)
                    {
                        targetTenantId = existingTargetId;
                        Console.WriteLine($"[TenantMap] Found existing Target Tenant '{tenantName}' (ID: {targetTenantId}). Merging into it.");
                    }
                    else
                    {
                        if (!dryRun)
                        {
                            // Create New Tenant in Target to generate new ID
                            Console.WriteLine($"[TenantMap] Creating new Tenant '{tenantName}' in Target...");
                            
                            // Fetch full row from source to clone properties? 
                            // Or minimal insert? Using minimal for safety, or we should Clone.
                            // Better to CLONE the tenant row from Source.
                            var sourceTenantRow = await source.QuerySingleAsync<dynamic>("SELECT * FROM Tenants WHERE Id = @Id", new { Id = sourceTenantId });
                            
                            // Generate INSERT sql dynamically based on dictionary?
                            // Simplified: Insert Name, TenancyName, IsActive, etc.
                            // We need to exclude 'Id' if Identity.
                            var props = (IDictionary<string, object>)sourceTenantRow;
                            var cols = props.Keys.Where(k => k != "Id").ToList();
                            var vals = cols.Select(k => "@" + k).ToList();
                            string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                            
                            targetTenantId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                            Console.WriteLine($"[TenantMap] Created Target Tenant. New ID: {targetTenantId}");
                        }
                        else
                        {
                            Console.WriteLine($"[DryRun] Would Create Tenant '{tenantName}' in Target.");
                            targetTenantId = sourceTenantId; // Mock for dry run
                        }
                    }
                    
                    if (targetTenantId == null && dryRun) targetTenantId = sourceTenantId;
                }
            }

            var schemaSync = new SchemaSync(sourceConn, targetConn);
            var migrator = new DataMigrator(sourceConn, targetConn, batchSize);

            // Bước 0: Tables only in MDC – ưu tiên đọc từ file (output/mdc_only_tables.txt), không có thì gọi GetTablesOnlyInMdcAsync
            var fromFile = await Helper.ReadTableListFromNumberedFileAsync(Helper.MdcOnlyTablesFilePath);
            HashSet<string> tablesOnlyInMdc;
            if (fromFile.Count > 0)
            {
                tablesOnlyInMdc = fromFile.ToHashSet(StringComparer.OrdinalIgnoreCase);
                Console.WriteLine($"[DataSync] Tables only in MDC: {tablesOnlyInMdc.Count} (from file {Helper.MdcOnlyTablesFilePath})");
            }
            else
            {
                tablesOnlyInMdc = (await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, null, writeToFile: false))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                Console.WriteLine($"[DataSync] Tables only in MDC: {tablesOnlyInMdc.Count}");
            }
            
            // 2b. Smart User Sync (Before generic tables)
            // Need to build User Map for Audit columns
            var userMapping = new Dictionary<long, long>();
            // Only sync users if we are proceeding with data
            await SyncUsersAsync(sourceConn, targetConn, userMapping, sourceTenantId, targetTenantId, dryRun);

            // Caching target tables is good for fuzzy matching.
            var existingAdcTables = (await schemaSync.GetExistingTargetTablesAsync()).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var sourceTables = (await schemaSync.GetExistingSourceTablesAsync())
                .Where(t => !TableSkipRules.ShouldSkipTable(t))
                .ToList();

            // Match Fuzzy / Explicit
            // We need to iterate specifically in ORDER defined by MigrationConfig
            // Any table NOT in MigrationConfig will be processed AFTER.
            
            var orderedTables = new List<string>();
            var sourceTableSet = new HashSet<string>(sourceTables, StringComparer.OrdinalIgnoreCase);

            // 1. Add Ordered Tables if they exist in Source
            foreach(var t in MigrationConfig.TableOrder)
            {
                if (sourceTableSet.Contains(t))
                {
                    orderedTables.Add(t);
                    sourceTableSet.Remove(t); // Handled
                }
            }
            
            // 2. Add Remaining Tables (that were not in the config list)
            foreach(var t in sourceTableSet)
            {
                orderedTables.Add(t);
            }
            
            Console.WriteLine($"[DataSync] Tables to Migrate (Ordered): {orderedTables.Count}");

            if (!dryRun)
            {
                using var sourceConnection = new SqlConnection(sourceConn);
                using var targetConnection = new SqlConnection(targetConn);
                await sourceConnection.OpenAsync();
                await targetConnection.OpenAsync();
                await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(targetConnection);
                await FkConstraintHelper.DisableAllFkAsync(targetConnection);
                var migrationBatch = Guid.NewGuid().ToString("N");
                var pkInfoCache = new Dictionary<string, PkColumnInfo?>(StringComparer.OrdinalIgnoreCase);

                foreach (var table in orderedTables)
                {
                    if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                    var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                    string targetTable = table;

                    if (ExplicitTableMappings.ContainsKey(table))
                    {
                        string mappedTarget = ExplicitTableMappings[table];
                        targetTable = mappedTarget;
                        if (existingAdcTables.Contains(mappedTarget, StringComparer.OrdinalIgnoreCase))
                        {
                            targetTable = mappedTarget;
                            isNew = false;
                            Console.WriteLine($"[SmartMerge] Applied explicit mapping: {table} (MDC) -> {targetTable} (ADC)");
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                        }
                        else
                            Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                    }
                    else if (isNew)
                    {
                        string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                        if (bestMatch != null)
                        {
                            targetTable = bestMatch;
                            isNew = false;
                            Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                        }
                    }

                    if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                        await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                    if (!pkInfoCache.TryGetValue(targetTable, out var pkInfo))
                    {
                        pkInfo = await DataMigrator.GetPkColumnInfoAsync(targetConnection, targetTable);
                        pkInfoCache[targetTable] = pkInfo;
                    }

                    bool skipGlobalSinglePk = false;
                    if (pkInfo != null && pkInfo.PkColumnCount == 1 && !await DataMigrator.TableHasTenantIdColumnAsync(targetConnection, targetTable))
                    {
                        // Bảng không có TenantId, PK 1 cột: chỉ seed một lần (tenant chạy đầu tiên), các tenant sau skip.
                        string? mappingTable = pkInfo.DataType switch
                        {
                            "int" => "IdMappingInt",
                            "bigint" => "IdMappingBigInt",
                            "uniqueidentifier" => "IdMappingGuid",
                            _ => null
                        };

                        if (mappingTable != null)
                        {
                            var existingMappings = await targetConnection.ExecuteScalarAsync<int>(
                                $"SELECT COUNT(1) FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName",
                                new { TableName = targetTable, ColumnName = pkInfo.ColumnName });
                            skipGlobalSinglePk = existingMappings > 0;
                        }
                        else
                        {
                            var tableEsc = targetTable.Replace("]", "]]");
                            var existingRows = await targetConnection.ExecuteScalarAsync<int>(
                                $"SELECT COUNT(1) FROM [dbo].[{tableEsc}]");
                            skipGlobalSinglePk = existingRows > 0;
                        }

                        if (skipGlobalSinglePk)
                        {
                            Console.WriteLine($"[DataSync] Skipping global single-PK table '{targetTable}' (no TenantId, already seeded).");
                            continue;
                        }
                    }

                    if (tablesOnlyInMdc.Contains(targetTable))
                    {
                        await migrator.MigrateTableAsync(table, isNewTable: false, targetTableName: targetTable, sourceTenantId: sourceTenantId, targetTenantId: targetTenantId, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                        if (pkInfo != null && (pkInfo.DataType == "int" || pkInfo.DataType == "bigint" || pkInfo.DataType == "uniqueidentifier"))
                        {
                            var mappingTable = pkInfo.DataType == "int" ? "IdMappingInt" : pkInfo.DataType == "bigint" ? "IdMappingBigInt" : "IdMappingGuid";
                            var targetWhere = (targetTenantId.HasValue && await DataMigrator.TableHasTenantIdColumnAsync(targetConnection, targetTable)) ? $" WHERE TenantId = {targetTenantId.Value}" : "";
                            var pkColEsc = pkInfo.ColumnName.Replace("]", "]]");
                            var tableEsc = targetTable.Replace("]", "]]");
                            var bulkSql = $@"
INSERT INTO [dbo].[{mappingTable}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, [{pkColEsc}], [{pkColEsc}], @Batch, @TenantId FROM [dbo].[{tableEsc}]{targetWhere}";
                            var inserted = await targetConnection.ExecuteAsync(bulkSql,
                                new { TableName = targetTable, ColumnName = pkInfo.ColumnName, Batch = migrationBatch, TenantId = (int?)targetTenantId },
                                commandTimeout: 600);
                            if (inserted > 0) Console.WriteLine($"   -> IdMapping (MDC-only, bulk): {inserted} row(s) -> [dbo].[{mappingTable}]");
                        }
                    }
                    else
                    {
                        if (pkInfo == null)
                        {
                            await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable, sourceTenantId: sourceTenantId, targetTenantId: targetTenantId, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                        }
                        else if (pkInfo.PkColumnCount == 1 && pkInfo.DataType != "int" && pkInfo.DataType != "bigint" && pkInfo.DataType != "uniqueidentifier")
                        {
                            await migrator.MigrateTableNaturalPkAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, sourceTenantId, targetTenantId, userMapping);
                        }
                        else if (pkInfo.PkColumnCount > 1)
                        {
                            var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                            var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                            await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable, pkColumnNames, fkColumns, sourceTenantId, targetTenantId);
                        }
                        else
                        {
                            var whereClause = (sourceTenantId.HasValue && await DataMigrator.TableHasTenantIdColumnAsync(sourceConnection, table)) ? $" WHERE TenantId = {sourceTenantId.Value}" : "";
                            await migrator.CreateStagingTableAsync(sourceConnection, targetConnection, table, targetTable, pkInfo);
                            await migrator.InsertTableWithIdMappingAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, migrationBatch, targetTenantId, whereClause, sourceTenantId, targetTenantId, userMapping);
                        }
                    }
                }

                await FkConstraintHelper.UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, targetTenantId);
            }
            else
            {
                foreach (var table in orderedTables)
                {
                    if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                    var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                    string targetTable = table;

                    if (ExplicitTableMappings.ContainsKey(table))
                    {
                        string mappedTarget = ExplicitTableMappings[table];
                        targetTable = mappedTarget;
                        if (existingAdcTables.Contains(mappedTarget, StringComparer.OrdinalIgnoreCase))
                        {
                            targetTable = mappedTarget;
                            isNew = false;
                            Console.WriteLine($"[SmartMerge] Applied explicit mapping: {table} (MDC) -> {targetTable} (ADC)");
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                        }
                        else
                            Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                    }
                    else if (isNew)
                    {
                        string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                        if (bestMatch != null)
                        {
                            targetTable = bestMatch;
                            isNew = false;
                            Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                        }
                    }

                    if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                        await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                    Console.WriteLine($"[DryRun] Would migrate {table} -> {targetTable} (Tenant: {sourceTenantId} -> {targetTenantId})");
                }
            }
        }

        static async Task RunListTablesOnlyInMdcAndCreateStructureAsync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Option 7] Tables only in MDC: list (console + file) and create structure in ADC");
            var path = Helper.MdcOnlyTablesFilePath;
            var list = await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, path, writeToFile: true);
            Console.WriteLine($"Tables only in MDC (after skip rules): {list.Count}");
            Console.WriteLine($"Output file: {Path.GetFullPath(path)}");
            for (int i = 0; i < list.Count; i++)
                Console.WriteLine($"  {i + 1}. {list[i]}");
            if (list.Count == 0)
            {
                Console.WriteLine("Nothing to create.");
                return;
            }
            if (dryRun)
            {
                Console.WriteLine("[DryRun] Would create table structure in ADC for the above.");
                return;
            }
            var schemaSync = new SchemaSync(sourceConn, targetConn);
            foreach (var table in list)
            {
                if (ExplicitTableMappings.ContainsKey(table))
                {
                    Console.WriteLine($"[Skip] {table} (explicitly mapped to existing table)");
                    continue;
                }
                await schemaSync.SyncTableAsync(table);
                Console.WriteLine($"  Created [dbo].[{table}]");
            }
            Console.WriteLine("Done.");
        }

        /// <summary>
        /// Xóa data migration trên ADC dựa vào IdMapping (chỉ xóa các dòng có NewId trong IdMapping).
        /// Tận dụng index (TableName, ColumnName) INCLUDE (NewId) trên bảng IdMapping.
        /// </summary>
        static async Task RunClearMigrationDataAsync(string targetConnStr)
        {
            Console.WriteLine("\n--> [Option 8] Clear migration data (delete rows in ADC based on IdMapping)");
            Console.Write("[Optional] MigrationBatch (Enter = all batches, or paste batch ID): ");
            var batchInput = Console.ReadLine()?.Trim();
            Console.Write("[Optional] TenantId (Enter = all tenants, or number): ");
            var tenantInput = Console.ReadLine()?.Trim();
            int? filterTenantId = null;
            if (!string.IsNullOrEmpty(tenantInput) && int.TryParse(tenantInput, out int tid))
                filterTenantId = tid;

            await using var conn = new SqlConnection(targetConnStr);
            await conn.OpenAsync();
            await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(conn);
            await FkConstraintHelper.DisableAllFkAsync(conn);

            string? filterBatch = string.IsNullOrEmpty(batchInput) ? null : batchInput;
            var totalDeleted = 0;

            totalDeleted = await ProcessIdMappingTableAsync(conn, "IdMappingInt", filterBatch, filterTenantId);
            totalDeleted += await ProcessIdMappingTableAsync(conn, "IdMappingBigInt", filterBatch, filterTenantId);
            totalDeleted += await ProcessIdMappingTableAsync(conn, "IdMappingGuid", filterBatch, filterTenantId);

            await FkConstraintHelper.EnableAllFkAsync(conn);
            Console.WriteLine($"\n[Option 8] Done. Total rows deleted from data tables: {totalDeleted}.");
        }

        /// <summary>
        /// For one IdMapping table: get distinct (TableName, ColumnName), delete from data tables by NewId (in batches), then delete from IdMapping.
        /// </summary>
        static async Task<int> ProcessIdMappingTableAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId)
        {
            var tableList = await GetDistinctTableColumnFromIdMappingAsync(conn, mappingTable, filterBatch, filterTenantId);
            if (tableList.Count == 0) return 0;

            var dboTables = (await conn.QueryAsync<string>("SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            const int deleteBatchSize = 50000;
            int totalDeleted = 0;
            foreach (var (tableName, columnName) in tableList)
            {
                if (!dboTables.Contains(tableName))
                {
                    Console.WriteLine($"  [Skip] Table not found: {tableName}");
                    continue;
                }

                var tableEsc = tableName.Replace("]", "]]");
                var colEsc = columnName.Replace("]", "]]");

                var subWhere = " TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) subWhere += " AND MigrationBatch = @Batch";
                if (filterTenantId.HasValue) subWhere += " AND TenantId = @TenantId";

                var prm = new { TableName = tableName, ColumnName = columnName, Batch = filterBatch, TenantId = filterTenantId, BatchSize = deleteBatchSize };
                var deleteDataSql = $@"DELETE TOP (@BatchSize) FROM [dbo].[{tableEsc}] WHERE [{colEsc}] IN (SELECT NewId FROM [dbo].[{mappingTable}] WHERE{subWhere})";

                int tableDeleted = 0;
                int deleted;
                do
                {
                    deleted = await conn.ExecuteAsync(deleteDataSql, prm, commandTimeout: 600);
                    tableDeleted += deleted;
                } while (deleted == deleteBatchSize);

                if (tableDeleted > 0)
                {
                    Console.WriteLine($"  Deleted {tableDeleted} row(s) from [dbo].[{tableName}]");
                    totalDeleted += tableDeleted;
                }

                var deleteMappingSql = $@"DELETE FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) deleteMappingSql += " AND MigrationBatch = @Batch";
                if (filterTenantId.HasValue) deleteMappingSql += " AND TenantId = @TenantId";
                await conn.ExecuteAsync(deleteMappingSql, prm, commandTimeout: 600);
            }
            return totalDeleted;
        }

        static async Task<List<(string TableName, string ColumnName)>> GetDistinctTableColumnFromIdMappingAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId)
        {
            var sql = $"SELECT DISTINCT TableName, ColumnName FROM [dbo].[{mappingTable}] WHERE 1=1";
            if (!string.IsNullOrEmpty(filterBatch)) sql += " AND MigrationBatch = @Batch";
            if (filterTenantId.HasValue) sql += " AND TenantId = @TenantId";
            var rows = await conn.QueryAsync<(string TableName, string ColumnName)>(sql, new { Batch = filterBatch, TenantId = filterTenantId });
            return rows.ToList();
        }

        // Define Explicit Mappings (Source -> Target)
        private static readonly Dictionary<string, string> ExplicitTableMappings = new(StringComparer.OrdinalIgnoreCase)
        {
            { "ActualActivityConfiguration", "ActualActivityConfigurations" },
            { "IndirectClockEvent", "IndirectClockEvent" }
        };

        /// <summary>Tables that exist only in MDC with different structure from ADC; create new table with same name in ADC, do not fuzzy-match to singular/plural.</summary>
        private static readonly HashSet<string> NoFuzzyMatchTables = new(StringComparer.OrdinalIgnoreCase)
        {
            "IndirectClockEvents"
        };

        static async Task RunValidation(string sourceConn, string targetConn)
        {
            Console.WriteLine("\n--> [Step 4] Validation");
            
            // Tenant prompt again? Or rely on global args if we stored them?
            // Validation usually targets context.
            // Let's ask for Tenant ID to match Data Sync scope.
            Console.Write("\n[Input] Enter Tenant Name for Validation (or press Enter for ALL): ");
            string tenantName = Console.ReadLine()?.Trim();
            int? tenantId = null;

            if (!string.IsNullOrEmpty(tenantName))
            {
               using var source = new SqlConnection(sourceConn); // Resolve from Source to get ID
               // Reuse resolution logic... (Should refactor into helper but duplicating for speed)
               try 
               {
                   tenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                   if (tenantId == null) tenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                   
                   Console.WriteLine($"[Validator] Validating for TenantID: {tenantId}");
               }
               catch(Exception ex) { Console.WriteLine($"Error resolving tenant: {ex.Message}"); return; }
            }

            var validator = new Validator(sourceConn, targetConn);
            await validator.RunValidationAsync(tenantId);
        }

        static async Task RunRollback(string targetConn)
        {
            var file = RollbackLogger.GetCurrentFilePath();
            if (string.IsNullOrEmpty(file)) file = "rollback_generic.sql"; 
            
            Console.WriteLine($"\nCurrent Rollback File: {file}");
            Console.WriteLine("Enter filename to execute (or press Enter for current):");
            string input = Console.ReadLine();
            string targetFile = string.IsNullOrWhiteSpace(input) ? file : input;
            
            if (!File.Exists(targetFile)) 
            {
                Console.WriteLine("[Error] File not found.");
                return;
            }

            Console.WriteLine($"Executing rollback script: {targetFile}...");
            string script = File.ReadAllText(targetFile);
            
            using var conn = new SqlConnection(targetConn);
            // Split by GO if necessary, but usually simple commands work.
            // Dapper Execute allows multiple statements.
            try
            {
                await conn.ExecuteAsync(script);
                Console.WriteLine("[Success] Rollback executed.");
            }
            catch(Exception ex) 
            {
                Console.WriteLine($"[Error] {ex.Message}");
            }
        }

        static async Task RunFullMigration(string sourceConn, string targetConn, int batchSize, bool dryRun, string[] args)
        {
             // Legacy wrapper for auto-run
             await RunSchemaSync(sourceConn, targetConn, dryRun);
             await RunObjectSync(sourceConn, targetConn, dryRun);
             await RunDataSync(sourceConn, targetConn, batchSize, dryRun);
        }


        static async Task SyncUsersAsync(string sourceConnStr, string targetConnStr, Dictionary<long, long> userMapping, int? sourceTenantId, int? targetTenantId, bool dryRun)
        {
            Console.WriteLine("\n[Users] Starting Smart User Sync...");
            
            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);
            
            // 1. Fetch Source Users
            // Include TenantId to support (TenantId, UserName) matching
            string sourceSql = "SELECT Id, UserName, EmailAddress, TenantId FROM Users";
            if (sourceTenantId.HasValue) sourceSql += " WHERE TenantId = @TenantId";
            var sourceUsers = await source.QueryAsync<dynamic>(sourceSql, new { TenantId = sourceTenantId });
            
            // 2. Fetch Target Users (to find matches)
            // Include TenantId and key by (TenantId, UserName) to avoid duplicates in 'all tenant' mode
            string targetSql = "SELECT Id, UserName, TenantId FROM Users";
            if (targetTenantId.HasValue) targetSql += " WHERE TenantId = @TenantId";
            var targetRows = await target.QueryAsync<dynamic>(targetSql, new { TenantId = targetTenantId });
            var targetUsers = targetRows.ToDictionary(
                k =>
                {
                    int? tid = (int?)k.TenantId;
                    string name = (string)k.UserName;
                    return (tid?.ToString() ?? "null") + "|" + name;
                },
                v => (long)v.Id,
                StringComparer.OrdinalIgnoreCase); // Dictionary<(TenantId, UserName), Id>

            Console.WriteLine($"[Users] Found {sourceUsers.Count()} Source Users, {targetUsers.Count} Target Users.");

            // 3. Process Each Source User
            foreach (var sUser in sourceUsers)
            {
                string userName = sUser.UserName;
                long sourceId = sUser.Id;
                long targetId = 0;

                int? keyTenantId = targetTenantId ?? (int?)sUser.TenantId;
                string dictKey = (keyTenantId?.ToString() ?? "null") + "|" + userName;

                if (targetUsers.ContainsKey(dictKey))
                {
                    // Match found!
                    targetId = targetUsers[dictKey];
                    // Console.WriteLine($"   [Match] {dictKey} ({sourceId} -> {targetId})"); 
                }
                else
                {
                    // New User - Insert
                    if (!dryRun)
                    {
                        // We need to fetch FULL row to insert
                        // Excluding Id to let Identity generate it
                        var fullUser = await source.QuerySingleAsync<dynamic>("SELECT * FROM Users WHERE Id = @Id", new { Id = sourceId });
                        var props = (IDictionary<string, object>)fullUser;
                        var cols = props.Keys.Where(k => k != "Id").ToList();
                        
                        // Handle TenantId transformation in Insert
                        if (sourceTenantId != targetTenantId && props.ContainsKey("TenantId"))
                        {
                            props["TenantId"] = targetTenantId; 
                        }
                        
                        var vals = cols.Select(k => "@" + k).ToList();
                        string insertSql = $"INSERT INTO Users ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as bigint);";
                        
                        targetId = await target.ExecuteScalarAsync<long>(insertSql, (object)props);
                        Console.WriteLine($"   [Insert] Created User {userName} (NewId: {targetId})");
                    }
                    else
                    {
                        Console.WriteLine($"   [DryRun] Would Insert User {userName}");
                        targetId = sourceId; // Mock
                    }
                }
                
                // Add to Map
                if (!userMapping.ContainsKey(sourceId))
                {
                    userMapping.Add(sourceId, targetId);
                }
            }
            Console.WriteLine($"[Users] User Mapping Built: {userMapping.Count} entries.");
        }

        static string GetBestFuzzyMatch(string sourceTable, HashSet<string> targetTables)
        {
            if (NoFuzzyMatchTables.Contains(sourceTable))
                return null;
            if (sourceTable.EndsWith("s") && targetTables.Contains(sourceTable.Substring(0, sourceTable.Length - 1)))
            {
                return sourceTable.Substring(0, sourceTable.Length - 1);
            }
            if (!sourceTable.EndsWith("s") && targetTables.Contains(sourceTable + "s"))
            {
                 return sourceTable + "s";
            }
            return null;
        }

    }
}
