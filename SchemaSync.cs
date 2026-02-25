using Microsoft.Data.SqlClient;
using Dapper;
using System.Text;
using System.IO;
using System.Linq;

namespace Logistics.DbMerger
{
    public class SchemaSync
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        public SchemaSync(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        public async Task<List<string>> GetMissingTablesAsync()
        {
            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceTables = await source.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
            var targetTables = await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");

            var missing = sourceTables.Except(targetTables, StringComparer.OrdinalIgnoreCase).ToList();
            return missing;
        }

        public async Task SyncTableAsync(string tableName)
        {
            var createScript = await GenerateCreateScriptAsync(tableName);
            using var target = new SqlConnection(_targetConnStr);
            
            // Split by GO batch separator
            var batches = System.Text.RegularExpressions.Regex.Split(createScript, @"^\s*GO\s*$", System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            foreach (var batch in batches)
            {
                if (!string.IsNullOrWhiteSpace(batch))
                    await target.ExecuteAsync(batch);
            }

            Console.WriteLine($"[Schema] Created table {tableName} (with Constraints)");
            RollbackLogger.LogTableCreation(tableName);
        }

        private async Task<string> GenerateCreateScriptAsync(string tableName)
        {
            using var source = new SqlConnection(_sourceConnStr);
            
            // 1. Fetch Columns with extended properties (Identity, Computed)
            // Join with sys.computed_columns to get definition if computed
            var columns = await source.QueryAsync<dynamic>(@"
                SELECT 
                    c.name AS ColumnName,
                    t.name AS DataType,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.column_id,
                    cc.definition as ComputedDefinition
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                WHERE c.object_id = OBJECT_ID(@TableName)
                ORDER BY c.column_id", new { TableName = tableName });

            // 2. Fetch Default Constraints
            var defaults = await source.QueryAsync<dynamic>(@"
                SELECT 
                    name AS ConstraintName,
                    definition AS DefaultValue,
                    parent_column_id
                FROM sys.default_constraints
                WHERE parent_object_id = OBJECT_ID(@TableName)", new { TableName = tableName });
            var defaultDict = defaults.ToDictionary(k => (int)k.parent_column_id, v => (string)v.DefaultValue);

            // 3. Fetch Check Constraints
            var checks = await source.QueryAsync<dynamic>(@"
                SELECT definition AS CheckDefinition, name AS CheckName
                FROM sys.check_constraints
                WHERE parent_object_id = OBJECT_ID(@TableName)", new { TableName = tableName });

            // 4. Fetch Indexes (PK and Non-Clustered)
            var indexes = await source.QueryAsync<dynamic>(@"
                SELECT 
                    i.index_id,
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    i.is_primary_key,
                    i.is_unique,
                    i.is_padded,
                    i.ignore_dup_key,
                    i.allow_row_locks,
                    i.allow_page_locks,
                    i.fill_factor,
                    s.no_recompute,
                    i.filter_definition
                FROM sys.indexes i
                LEFT JOIN sys.stats s ON i.object_id = s.object_id AND i.index_id = s.stats_id
                WHERE i.object_id = OBJECT_ID(@TableName) 
                  AND i.type_desc <> 'HEAP'
                ORDER BY i.is_primary_key DESC, i.name", new { TableName = tableName });

            // Fetch Index Columns for ALL indexes
            var indexCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    ic.index_id,
                    c.name AS ColumnName,
                    ic.is_descending_key,
                    ic.is_included_column
                FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                WHERE ic.object_id = OBJECT_ID(@TableName)
                ORDER BY ic.index_id, ic.key_ordinal", new { TableName = tableName });
            
            var indexColLookup = indexCols.GroupBy(x => (int)x.index_id).ToDictionary(g => g.Key, g => g.ToList());

            var pkInfo = indexes.FirstOrDefault(i => i.is_primary_key == true);

            var sb = new StringBuilder();
            sb.AppendLine("SET ANSI_NULLS ON");
            sb.AppendLine("GO");
            sb.AppendLine("SET QUOTED_IDENTIFIER ON");
            sb.AppendLine("GO");
            sb.AppendLine($"CREATE TABLE [dbo].[{tableName}](");

            var colList = columns.ToList();
            bool hasLob = false;

            for (int i = 0; i < colList.Count; i++)
            {
                var col = colList[i];
                
                // Computed Column
                if (col.is_computed)
                {
                     sb.Append($"	[{col.ColumnName}] AS ({col.ComputedDefinition})");
                }
                else
                {
                    string typeDef = $"[{col.DataType}]";
                    string typeLower = ((string)col.DataType).ToLower();

                    if (typeLower == "nvarchar" || typeLower == "varchar" || typeLower == "char" || typeLower == "nchar" || typeLower == "varbinary" || typeLower == "binary")
                    {
                        string len = col.max_length == -1 ? "max" : (typeLower.StartsWith("n") ? col.max_length / 2 : col.max_length).ToString();
                        typeDef += $"({len})";
                        if (len == "max") hasLob = true;
                    }
                    else if (typeLower == "decimal" || typeLower == "numeric")
                    {
                        typeDef += $"({col.precision}, {col.scale})";
                    }
                    else if (typeLower == "text" || typeLower == "ntext" || typeLower == "image" || typeLower == "xml")
                    {
                        hasLob = true;
                    }

                    string identity = col.is_identity == true ? " IDENTITY(1,1)" : "";
                    string nullable = col.is_nullable == true ? " NULL" : " NOT NULL";
                    
                    sb.Append($"	[{col.ColumnName}] {typeDef}{identity}{nullable}");
                    
                    // Default Constraint
                    if (defaultDict.ContainsKey((int)col.column_id))
                    {
                        string defName = $"DF_{tableName}_{col.ColumnName}"; 
                        sb.Append($" CONSTRAINT [{defName}] DEFAULT {defaultDict[(int)col.column_id]}");
                    }
                }
                
                // Comma Logic
                if (i < colList.Count - 1 || pkInfo != null || checks.Any()) 
                    sb.AppendLine(",");
                else
                    sb.AppendLine("");
            }

            // Append PK with Options
            if (pkInfo != null)
            {
                sb.AppendLine($" CONSTRAINT [{pkInfo.IndexName}] PRIMARY KEY {pkInfo.IndexType} ");
                sb.AppendLine("(");
                
                var pkCols = indexColLookup.ContainsKey((int)pkInfo.index_id) ? indexColLookup[(int)pkInfo.index_id] : new List<dynamic>();
                
                for(int k=0; k<pkCols.Count; k++)
                {
                     var c = pkCols[k];
                     sb.Append($"	[{c.ColumnName}] {(c.is_descending_key ? "DESC" : "ASC")}");
                     if (k < pkCols.Count - 1) sb.Append(",");
                     sb.AppendLine();
                }
                
                // Construct Options
                var opts = new List<string>();
                opts.Add($"PAD_INDEX = {(pkInfo.is_padded == true ? "ON" : "OFF")}");
                opts.Add($"STATISTICS_NORECOMPUTE = {(pkInfo.no_recompute == true ? "ON" : "OFF")}");
                opts.Add($"IGNORE_DUP_KEY = {(pkInfo.ignore_dup_key == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_ROW_LOCKS = {(pkInfo.allow_row_locks == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_PAGE_LOCKS = {(pkInfo.allow_page_locks == true ? "ON" : "OFF")}");
                if (pkInfo.fill_factor != null && pkInfo.fill_factor > 0)
                {
                    opts.Add($"FILLFACTOR = {pkInfo.fill_factor}");
                }
                
                sb.Append($")WITH ({string.Join(", ", opts)}) ON [PRIMARY]");
                
                if (checks.Any()) sb.AppendLine(","); else sb.AppendLine("");
            }

            // Append Check Constraints
            var checkList = checks.ToList();
            for(int c=0; c < checkList.Count; c++)
            {
                var chk = checkList[c];
                sb.Append($"	CONSTRAINT [{chk.CheckName}] CHECK {chk.CheckDefinition}");
                if (c < checkList.Count - 1) sb.AppendLine(","); else sb.AppendLine("");
            }

            sb.Append(") ON [PRIMARY]");
            if (hasLob) sb.Append(" TEXTIMAGE_ON [PRIMARY]"); 
            
            sb.AppendLine("");
            sb.AppendLine("GO");
            
            // Append Non-Clustered Indexes
            foreach(var idx in indexes)
            {
                if (idx.is_primary_key == true) continue;
                
                string unique = idx.is_unique == true ? "UNIQUE " : "";
                string type = idx.IndexType; // NONCLUSTERED
                sb.AppendLine($"CREATE {unique}{type} INDEX [{idx.IndexName}] ON [dbo].[{tableName}]");
                sb.Append("(");
                
                var cols = indexColLookup.ContainsKey((int)idx.index_id) ? indexColLookup[(int)idx.index_id] : new List<dynamic>();
                var keyCols = cols.Where(x => x.is_included_column == false).ToList();
                var incCols = cols.Where(x => x.is_included_column == true).ToList();
                
                for(int k=0; k<keyCols.Count; k++)
                {
                    var c = keyCols[k];
                    sb.Append($"[{c.ColumnName}] {(c.is_descending_key ? "DESC" : "ASC")}");
                    if (k < keyCols.Count - 1) sb.Append(", ");
                }
                sb.Append(")");
                
                if (incCols.Any())
                {
                    sb.AppendLine();
                    sb.Append("INCLUDE (");
                    for(int k=0; k<incCols.Count; k++)
                    {
                        sb.Append($"[{incCols[k].ColumnName}]");
                        if (k < incCols.Count - 1) sb.Append(", ");
                    }
                    sb.Append(")");
                }
                
                if (!string.IsNullOrEmpty(idx.filter_definition))
                {
                     sb.AppendLine();
                     sb.Append($"WHERE {idx.filter_definition}");
                }
                
                sb.AppendLine(" WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]");
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        public async Task<List<string>> GetExistingTargetTablesAsync()
        {
             using var target = new SqlConnection(_targetConnStr);
             return (await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
        }

        public async Task<List<string>> GetExistingSourceTablesAsync()
        {
             using var source = new SqlConnection(_sourceConnStr);
             return (await source.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
        }

        public async Task<List<dynamic>> GetRequiredColumnsAsync(string tableName)
        {
            using var source = new SqlConnection(_sourceConnStr);
            var cols = await source.QueryAsync<dynamic>(@"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @Name", new { Name = tableName });
            return cols.ToList();
        }

        public async Task AlterTableAsync(string tableName, List<dynamic> newColumns)
        {
            if (!newColumns.Any()) return;

            using var target = new SqlConnection(_targetConnStr);
            foreach (var col in newColumns)
            {
                string typeDef = $"{col.DATA_TYPE}";
                 if (col.DATA_TYPE == "nvarchar" || col.DATA_TYPE == "varchar" || col.DATA_TYPE == "char" || col.DATA_TYPE == "nchar" || col.DATA_TYPE == "varbinary")
                {
                    string len = col.CHARACTER_MAXIMUM_LENGTH == -1 ? "MAX" : col.CHARACTER_MAXIMUM_LENGTH.ToString();
                    typeDef += $"({len})";
                }
                else if (col.DATA_TYPE == "decimal" || col.DATA_TYPE == "numeric")
                {
                     typeDef += $"({col.NUMERIC_PRECISION}, {col.NUMERIC_SCALE})";
                }
                
                // Allow NULL for new columns to be safe for existing data? 
                // Or follow source? If source says NOT NULL, we must provide default.
                // For simplified merge, we'll force NULL usually, OR trust the source but we might fail if table has rows.
                // Safer: ADD COLUMN ... NULL
                string definition = $"ALTER TABLE [{tableName}] ADD [{col.COLUMN_NAME}] {typeDef} NULL"; 
                
                Console.WriteLine($"[Schema] Altering {tableName}: Adding {col.COLUMN_NAME}");
                await target.ExecuteAsync(definition);
                
                // Log Rollback (Drop Column)
                // Note: RollbackLogger needs update or we just use raw SQL string
                // Dropping a column is: ALTER TABLE x DROP COLUMN y
                // We'll append manually for now or add helper.
                string rollback = $"IF EXISTS(SELECT * FROM sys.columns WHERE Name = N'{col.COLUMN_NAME}' AND Object_ID = Object_ID(N'dbo.{tableName}')) ALTER TABLE dbo.{tableName} DROP COLUMN [{col.COLUMN_NAME}];\n";
                using (var sw = File.AppendText($"rollback_{DateTime.Now:yyyyMMdd_HHmmss}.sql")) // This creates a NEW file every time? No, need consistent filename.
                {
                    // RollbackLogger static path usage is better.
                    // We need to expose a bespoke logging method.
                }
                RollbackLogger.LogCustomScript(rollback);
            }
        }

        public async Task SyncTableSchemaAsync(string sourceTable, string targetTable, bool dryRun = false)
        {
            var sourceCols = await GetRequiredColumnsAsync(sourceTable);
            
            using var target = new SqlConnection(_targetConnStr);
            var targetCols = (await target.QueryAsync<string>("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @Name", new { Name = targetTable })).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var missingCols = sourceCols.Where(c => !targetCols.Contains((string)c.COLUMN_NAME)).ToList();
            
            if (missingCols.Any())
            {
                if (dryRun)
                {
                     foreach(var col in missingCols) 
                        Console.WriteLine($"[DryRun] Would Add Column: {targetTable}.{col.COLUMN_NAME}");
                }
                else
                {
                    await AlterTableAsync(targetTable, missingCols);
                }
            }
            /* Verbose logging
            else
            {
                // Console.WriteLine($"[Schema] No new columns to add for {targetTable}");
            }
            */
        }

        public async Task SyncAllConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Console.WriteLine("\n=== [Post-Table] Constraint & Partition Sync ===");
            await SyncDefaultConstraintsAsync(tableMappings, dryRun);
            await SyncCheckConstraintsAsync(tableMappings, dryRun);
            await SyncIndexesAsync(tableMappings, dryRun);
            await SyncForeignKeysAsync(tableMappings, dryRun);
            await SyncPartitionsAsync(dryRun);
            Console.WriteLine("=== [Post-Table] Constraint Sync Complete ===\n");
        }

        public async Task SyncForeignKeysAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Console.WriteLine("\n[Constraints] Syncing Foreign Keys...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceFKs = await source.QueryAsync<dynamic>(@"
                SELECT 
                    fk.object_id AS FKObjectId,
                    fk.name AS FKName,
                    OBJECT_NAME(fk.parent_object_id) AS ParentTable,
                    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
                    fk.delete_referential_action_desc AS DeleteAction,
                    fk.update_referential_action_desc AS UpdateAction
                FROM sys.foreign_keys fk
                ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name");

            var sourceFKCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    fkc.constraint_object_id AS FKObjectId,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ParentColumn,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ReferencedColumn
                FROM sys.foreign_key_columns fkc
                ORDER BY fkc.constraint_object_id, fkc.constraint_column_id");

            var fkColLookup = sourceFKCols.GroupBy(x => (int)x.FKObjectId)
                .ToDictionary(g => g.Key, g => g.ToList());

            var targetFKNames = (await target.QueryAsync<string>("SELECT name FROM sys.foreign_keys"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var fk in sourceFKs)
            {
                string fkName = fk.FKName;
                string parentTable = fk.ParentTable;
                string referencedTable = fk.ReferencedTable;
                int fkObjectId = fk.FKObjectId;

                string targetParent = tableMappings.TryGetValue(parentTable, out var mp) ? mp : parentTable;
                string targetRef = tableMappings.TryGetValue(referencedTable, out var mr) ? mr : referencedTable;

                if (!targetTables.Contains(targetParent) || !targetTables.Contains(targetRef))
                { skipped++; continue; }

                if (targetFKNames.Contains(fkName))
                    continue;

                if (!fkColLookup.TryGetValue(fkObjectId, out var cols) || !cols.Any())
                { skipped++; continue; }

                bool valid = true;
                if (targetColsByTable.TryGetValue(targetParent, out var pCols) &&
                    targetColsByTable.TryGetValue(targetRef, out var rCols))
                {
                    foreach (var c in cols)
                    {
                        if (!pCols.Contains((string)c.ParentColumn) || !rCols.Contains((string)c.ReferencedColumn))
                        { valid = false; break; }
                    }
                }
                else { valid = false; }

                if (!valid)
                {
                    Console.WriteLine($"[FK] Skipping {fkName}: columns missing in target");
                    skipped++;
                    continue;
                }

                var parentColList = string.Join(", ", cols.Select(c => $"[{(string)c.ParentColumn}]"));
                var refColList = string.Join(", ", cols.Select(c => $"[{(string)c.ReferencedColumn}]"));
                string deleteAction = ((string)fk.DeleteAction).Replace("_", " ");
                string updateAction = ((string)fk.UpdateAction).Replace("_", " ");

                string sql = $"ALTER TABLE [dbo].[{targetParent}] ADD CONSTRAINT [{fkName}] " +
                    $"FOREIGN KEY ({parentColList}) REFERENCES [dbo].[{targetRef}] ({refColList}) " +
                    $"ON DELETE {deleteAction} ON UPDATE {updateAction}";

                if (dryRun)
                {
                    Console.WriteLine($"[DryRun] Would create FK: {fkName} ({targetParent} -> {targetRef})");
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Console.WriteLine($"[FK] Created: {fkName} ({targetParent} -> {targetRef})");
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID('{fkName}', 'F') IS NOT NULL ALTER TABLE [dbo].[{targetParent}] DROP CONSTRAINT [{fkName}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[FK] Failed {fkName}: {ex.Message}");
                        failed++;
                    }
                }
            }

            Console.WriteLine($"[FK] Summary: {created} created, {skipped} skipped, {failed} failed");
        }

        public async Task SyncIndexesAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Console.WriteLine("\n[Constraints] Syncing Indexes & Unique Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceIndexes = await source.QueryAsync<dynamic>(@"
                SELECT 
                    i.object_id,
                    i.index_id,
                    i.name AS IndexName,
                    OBJECT_NAME(i.object_id) AS TableName,
                    i.type_desc AS IndexType,
                    i.is_unique,
                    i.is_unique_constraint,
                    i.is_primary_key,
                    i.filter_definition,
                    i.is_padded,
                    i.ignore_dup_key,
                    i.allow_row_locks,
                    i.allow_page_locks,
                    i.fill_factor,
                    s.no_recompute
                FROM sys.indexes i
                LEFT JOIN sys.stats s ON i.object_id = s.object_id AND i.index_id = s.stats_id
                WHERE i.is_primary_key = 0
                  AND i.type_desc <> 'HEAP'
                  AND i.name IS NOT NULL
                ORDER BY OBJECT_NAME(i.object_id), i.name");

            var sourceIdxCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    ic.object_id,
                    ic.index_id,
                    c.name AS ColumnName,
                    ic.is_descending_key,
                    ic.is_included_column
                FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE i.is_primary_key = 0 AND i.type_desc <> 'HEAP' AND i.name IS NOT NULL
                ORDER BY ic.object_id, ic.index_id, ic.key_ordinal");

            var idxColLookup = sourceIdxCols
                .GroupBy(x => $"{(int)x.object_id}_{(int)x.index_id}")
                .ToDictionary(g => g.Key, g => g.ToList());

            var targetIndexNames = (await target.QueryAsync<string>(
                "SELECT name FROM sys.indexes WHERE name IS NOT NULL"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var idx in sourceIndexes)
            {
                string indexName = idx.IndexName;
                string sourceTable = idx.TableName;
                int objectId = idx.object_id;
                int indexId = idx.index_id;
                string lookupKey = $"{objectId}_{indexId}";

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetIndexNames.Contains(indexName))
                    continue;

                if (!idxColLookup.TryGetValue(lookupKey, out var cols) || !cols.Any())
                { skipped++; continue; }

                if (!targetColsByTable.TryGetValue(targetTable, out var tCols))
                { skipped++; continue; }

                bool valid = cols.All(c => tCols.Contains((string)c.ColumnName));
                if (!valid)
                { skipped++; continue; }

                var keyCols = cols.Where(x => x.is_included_column == false).ToList();
                var incCols = cols.Where(x => x.is_included_column == true).ToList();

                string sql;
                bool isUniqueConstraint = idx.is_unique_constraint == true;

                if (isUniqueConstraint)
                {
                    var keyColStr = string.Join(", ", keyCols.Select(c => $"[{(string)c.ColumnName}]"));
                    sql = $"ALTER TABLE [dbo].[{targetTable}] ADD CONSTRAINT [{indexName}] UNIQUE ({keyColStr})";
                }
                else
                {
                    string unique = idx.is_unique == true ? "UNIQUE " : "";
                    string type = idx.IndexType;
                    var keyColStr = string.Join(", ", keyCols.Select(c =>
                        $"[{(string)c.ColumnName}] {((bool)c.is_descending_key ? "DESC" : "ASC")}"));

                    sql = $"CREATE {unique}{type} INDEX [{indexName}] ON [dbo].[{targetTable}] ({keyColStr})";

                    if (incCols.Any())
                    {
                        var incColStr = string.Join(", ", incCols.Select(c => $"[{(string)c.ColumnName}]"));
                        sql += $" INCLUDE ({incColStr})";
                    }

                    if (!string.IsNullOrEmpty((string)idx.filter_definition))
                        sql += $" WHERE {idx.filter_definition}";

                    string padIndex = idx.is_padded == true ? "ON" : "OFF";
                    string noRecompute = idx.no_recompute == true ? "ON" : "OFF";
                    string ignoreDupKey = idx.ignore_dup_key == true ? "ON" : "OFF";
                    string rowLocks = idx.allow_row_locks == true ? "ON" : "OFF";
                    string pageLocks = idx.allow_page_locks == true ? "ON" : "OFF";

                    sql += $" WITH (PAD_INDEX = {padIndex}, STATISTICS_NORECOMPUTE = {noRecompute}, " +
                        $"IGNORE_DUP_KEY = {ignoreDupKey}, ALLOW_ROW_LOCKS = {rowLocks}, ALLOW_PAGE_LOCKS = {pageLocks})";
                }

                if (dryRun)
                {
                    Console.WriteLine($"[DryRun] Would create {(isUniqueConstraint ? "unique constraint" : "index")}: {indexName} on {targetTable}");
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Console.WriteLine($"[Index] Created: {indexName} on {targetTable}");
                        if (isUniqueConstraint)
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = '{indexName}' AND type = 'UQ') ALTER TABLE [dbo].[{targetTable}] DROP CONSTRAINT [{indexName}];\n");
                        else
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{indexName}') DROP INDEX [{indexName}] ON [dbo].[{targetTable}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Index] Failed {indexName}: {ex.Message}");
                        failed++;
                    }
                }
            }

            Console.WriteLine($"[Index] Summary: {created} created, {skipped} skipped, {failed} failed");
        }

        public async Task SyncCheckConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Console.WriteLine("\n[Constraints] Syncing Check Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceChecks = await source.QueryAsync<dynamic>(@"
                SELECT 
                    cc.name AS ConstraintName,
                    OBJECT_NAME(cc.parent_object_id) AS TableName,
                    cc.definition AS CheckDefinition
                FROM sys.check_constraints cc
                ORDER BY OBJECT_NAME(cc.parent_object_id), cc.name");

            var targetCheckNames = (await target.QueryAsync<string>("SELECT name FROM sys.check_constraints"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var chk in sourceChecks)
            {
                string name = chk.ConstraintName;
                string sourceTable = chk.TableName;
                string definition = chk.CheckDefinition;

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetCheckNames.Contains(name))
                    continue;

                string sql = $"ALTER TABLE [dbo].[{targetTable}] ADD CONSTRAINT [{name}] CHECK {definition}";

                if (dryRun)
                {
                    Console.WriteLine($"[DryRun] Would create check: {name} on {targetTable}");
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Console.WriteLine($"[Check] Created: {name} on {targetTable}");
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID('{name}', 'C') IS NOT NULL ALTER TABLE [dbo].[{targetTable}] DROP CONSTRAINT [{name}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Check] Failed {name}: {ex.Message}");
                        failed++;
                    }
                }
            }

            Console.WriteLine($"[Check] Summary: {created} created, {skipped} skipped, {failed} failed");
        }

        public async Task SyncDefaultConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Console.WriteLine("\n[Constraints] Syncing Default Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceDefaults = await source.QueryAsync<dynamic>(@"
                SELECT 
                    dc.name AS ConstraintName,
                    OBJECT_NAME(dc.parent_object_id) AS TableName,
                    COL_NAME(dc.parent_object_id, dc.parent_column_id) AS ColumnName,
                    dc.definition AS DefaultValue
                FROM sys.default_constraints dc
                ORDER BY OBJECT_NAME(dc.parent_object_id), dc.name");

            var targetDefaults = await target.QueryAsync<dynamic>(@"
                SELECT 
                    dc.name AS ConstraintName,
                    OBJECT_NAME(dc.parent_object_id) AS TableName,
                    COL_NAME(dc.parent_object_id, dc.parent_column_id) AS ColumnName
                FROM sys.default_constraints dc");

            var targetDefaultNames = targetDefaults.Select(d => (string)d.ConstraintName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var targetDefaultsByCol = targetDefaults
                .Select(d => $"{((string)d.TableName).ToLowerInvariant()}.{((string)d.ColumnName).ToLowerInvariant()}")
                .ToHashSet();

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var df in sourceDefaults)
            {
                string name = df.ConstraintName;
                string sourceTable = df.TableName;
                string colName = df.ColumnName;
                string defaultVal = df.DefaultValue;

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetDefaultNames.Contains(name))
                    continue;

                // Skip if column already has a default (may have been created with a different name)
                string tableColKey = $"{targetTable.ToLowerInvariant()}.{colName.ToLowerInvariant()}";
                if (targetDefaultsByCol.Contains(tableColKey))
                    continue;

                if (!targetColsByTable.TryGetValue(targetTable, out var cols) || !cols.Contains(colName))
                { skipped++; continue; }

                string sql = $"ALTER TABLE [dbo].[{targetTable}] ADD CONSTRAINT [{name}] DEFAULT {defaultVal} FOR [{colName}]";

                if (dryRun)
                {
                    Console.WriteLine($"[DryRun] Would create default: {name} on {targetTable}.{colName}");
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Console.WriteLine($"[Default] Created: {name} on {targetTable}.{colName}");
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID('{name}', 'D') IS NOT NULL ALTER TABLE [dbo].[{targetTable}] DROP CONSTRAINT [{name}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Default] Failed {name}: {ex.Message}");
                        failed++;
                    }
                }
            }

            Console.WriteLine($"[Default] Summary: {created} created, {skipped} skipped, {failed} failed");
        }

        public async Task SyncPartitionsAsync(bool dryRun)
        {
            Console.WriteLine("\n[Constraints] Checking Partitions...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourcePFs = await source.QueryAsync<dynamic>(@"
                SELECT 
                    pf.function_id,
                    pf.name AS FunctionName,
                    pf.type_desc AS FunctionType,
                    pf.boundary_value_on_right,
                    t.name AS ParameterType,
                    pp.max_length,
                    pp.precision,
                    pp.scale
                FROM sys.partition_functions pf
                INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
                INNER JOIN sys.types t ON pp.system_type_id = t.system_type_id AND pp.user_type_id = t.user_type_id");

            if (!sourcePFs.Any())
            {
                Console.WriteLine("[Partitions] No partition functions in source.");
            }
            else
            {
                var targetPFNames = (await target.QueryAsync<string>("SELECT name FROM sys.partition_functions"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                int pfCreated = 0, pfFailed = 0;

                foreach (var pf in sourcePFs)
                {
                    string pfName = pf.FunctionName;

                    if (targetPFNames.Contains(pfName))
                    {
                        Console.WriteLine($"[Partitions] Function '{pfName}' already exists.");
                        continue;
                    }

                    var boundaries = await source.QueryAsync<dynamic>(@"
                        SELECT boundary_id, value
                        FROM sys.partition_range_values
                        WHERE function_id = @FunctionId
                        ORDER BY boundary_id", new { FunctionId = (int)pf.function_id });

                    string paramType = pf.ParameterType;
                    string typeDef = paramType;
                    string typeLower = paramType.ToLower();
                    if (typeLower == "nvarchar" || typeLower == "varchar" || typeLower == "char" || typeLower == "nchar" || typeLower == "varbinary")
                    {
                        string len = pf.max_length == -1 ? "MAX" : (typeLower.StartsWith("n") ? pf.max_length / 2 : pf.max_length).ToString();
                        typeDef += $"({len})";
                    }
                    else if (typeLower == "decimal" || typeLower == "numeric")
                    {
                        typeDef += $"({pf.precision}, {pf.scale})";
                    }

                    string range = pf.boundary_value_on_right == true ? "RIGHT" : "LEFT";
                    var boundaryValues = boundaries.Select(b =>
                    {
                        object val = b.value;
                        if (val is string s) return $"N'{s}'";
                        if (val is DateTime dt) return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
                        return val?.ToString() ?? "NULL";
                    });
                    string valuesStr = string.Join(", ", boundaryValues);

                    string createPF = $"CREATE PARTITION FUNCTION [{pfName}] ({typeDef}) AS RANGE {range} FOR VALUES ({valuesStr})";

                    if (dryRun)
                    {
                        Console.WriteLine($"[DryRun] Would create partition function: {pfName}");
                        pfCreated++;
                    }
                    else
                    {
                        try
                        {
                            await target.ExecuteAsync(createPF);
                            Console.WriteLine($"[Partitions] Created function: {pfName}");
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}') DROP PARTITION FUNCTION [{pfName}];\n");
                            pfCreated++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Partitions] Failed function {pfName}: {ex.Message}");
                            pfFailed++;
                        }
                    }
                }

                Console.WriteLine($"[Partitions] Functions: {pfCreated} created, {pfFailed} failed");
            }

            var sourceSchemes = await source.QueryAsync<dynamic>(@"
                SELECT 
                    ps.name AS SchemeName,
                    pf.name AS FunctionName,
                    ds.name AS FileGroupName
                FROM sys.partition_schemes ps
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
                INNER JOIN sys.data_spaces ds ON dds.data_space_id = ds.data_space_id
                ORDER BY ps.name, dds.destination_id");

            if (!sourceSchemes.Any())
            {
                Console.WriteLine("[Partitions] No partition schemes in source.");
            }
            else
            {
                var targetSchemeNames = (await target.QueryAsync<string>("SELECT name FROM sys.partition_schemes"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var targetFileGroups = (await target.QueryAsync<string>(
                    "SELECT name FROM sys.data_spaces WHERE type = 'FG'"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var schemeGroups = sourceSchemes.GroupBy(s => (string)s.SchemeName);
                int psCreated = 0, psFailed = 0;

                foreach (var group in schemeGroups)
                {
                    string schemeName = group.Key;

                    if (targetSchemeNames.Contains(schemeName))
                    {
                        Console.WriteLine($"[Partitions] Scheme '{schemeName}' already exists.");
                        continue;
                    }

                    string funcName = group.First().FunctionName;

                    var fileGroups = group.Select(g =>
                    {
                        string fg = (string)g.FileGroupName;
                        if (!targetFileGroups.Contains(fg))
                        {
                            Console.WriteLine($"[Partitions] Filegroup '{fg}' missing in target, mapping to [PRIMARY]");
                            return "[PRIMARY]";
                        }
                        return $"[{fg}]";
                    });

                    string fgList = string.Join(", ", fileGroups);
                    string createPS = $"CREATE PARTITION SCHEME [{schemeName}] AS PARTITION [{funcName}] TO ({fgList})";

                    if (dryRun)
                    {
                        Console.WriteLine($"[DryRun] Would create partition scheme: {schemeName}");
                        psCreated++;
                    }
                    else
                    {
                        try
                        {
                            await target.ExecuteAsync(createPS);
                            Console.WriteLine($"[Partitions] Created scheme: {schemeName}");
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{schemeName}') DROP PARTITION SCHEME [{schemeName}];\n");
                            psCreated++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Partitions] Failed scheme {schemeName}: {ex.Message}");
                            psFailed++;
                        }
                    }
                }

                Console.WriteLine($"[Partitions] Schemes: {psCreated} created, {psFailed} failed");
            }

            var partitionedTables = await source.QueryAsync<dynamic>(@"
                SELECT 
                    OBJECT_NAME(p.object_id) AS TableName,
                    ps.name AS SchemeName,
                    pf.name AS FunctionName,
                    COUNT(DISTINCT p.partition_number) AS PartitionCount
                FROM sys.partitions p
                INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                WHERE i.index_id <= 1
                GROUP BY p.object_id, ps.name, pf.name
                ORDER BY OBJECT_NAME(p.object_id)");

            if (partitionedTables.Any())
            {
                Console.WriteLine("\n[Partitions] Partitioned Tables in Source:");
                foreach (var pt in partitionedTables)
                {
                    Console.WriteLine($"   {pt.TableName} -> Scheme: {pt.SchemeName}, Function: {pt.FunctionName}, Partitions: {pt.PartitionCount}");
                }
                Console.WriteLine("[Partitions] Note: Table-level partition assignment requires index rebuild and should be reviewed manually.");
            }
        }
    }
}
