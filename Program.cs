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
            // var targetTables = await schemaSync.GetExistingTargetTablesAsync(); // we fetch later per table or cache? 
            // Caching target tables is good for fuzzy matching.
            var existingAdcTables = (await schemaSync.GetExistingTargetTablesAsync()).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var sourceTables = await schemaSync.GetExistingSourceTablesAsync();

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
            
            foreach (var table in orderedTables)
            {
                if (table == "sysdiagrams" || table == "Tenants" || table.StartsWith("__")) continue; // Skip systems

                var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                string targetTable = table;
                
                // 1. Check Explicit Mapping
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
                     {
                          Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                     }
                }
                
                // 2. Check Fuzzy Match (only if New and not explicitly mapped)
                else if (isNew)
                     {
                     string bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                     if (bestMatch != null)
                     {
                         targetTable = bestMatch;
                         isNew = false; 
                         Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                         // Sync Schema for Fuzzy Match
                         await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                     }
                     // other fuzzy rules...
                }
                
                // 3. Sync Schema for Exact Matches (Column Evolution)
                if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                {
                     await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                    }

                // 4. Data Migration
                if (!dryRun)
                {
                   await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable, sourceTenantId: sourceTenantId, targetTenantId: targetTenantId);
                }
                else
                {
                   Console.WriteLine($"[DryRun] Would migrate {table} -> {targetTable} (Tenant: {sourceTenantId} -> {targetTenantId})");
                }
            }
        }

        // Define Explicit Mappings (Source -> Target)
        private static readonly Dictionary<string, string> ExplicitTableMappings = new(StringComparer.OrdinalIgnoreCase)
        {
            { "ActualActivityConfiguration", "ActualActivityConfigurations" },
            { "IndirectClockEvents", "IndirectClockEvent" }
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


        static string GetBestFuzzyMatch(string sourceTable, HashSet<string> targetTables)
        {
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
