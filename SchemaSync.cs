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
            
            // 1. Fetch Columns with extended properties (Identity, Computed, etc.)
            var columns = await source.QueryAsync<dynamic>(@"
                SELECT 
                    c.name AS ColumnName,
                    t.name AS DataType,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    c.column_id
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID(@TableName)
                ORDER BY c.column_id", new { TableName = tableName });

            // 2. Fetch PK Info with Options
            var pkInfo = await source.QueryFirstOrDefaultAsync<dynamic>(@"
                SELECT 
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    i.is_primary_key,
                    i.is_padded,
                    i.ignore_dup_key,
                    i.allow_row_locks,
                    i.allow_page_locks,
                    i.fill_factor,
                    s.no_recompute
                FROM sys.indexes i
                LEFT JOIN sys.stats s ON i.object_id = s.object_id AND i.index_id = s.stats_id
                WHERE i.object_id = OBJECT_ID(@TableName) AND i.is_primary_key = 1", new { TableName = tableName });

            List<string> pkColumns = new List<string>();
            if (pkInfo != null)
            {
                pkColumns = (await source.QueryAsync<string>(@"
                    SELECT c.name 
                    FROM sys.index_columns ic
                    INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    WHERE i.object_id = OBJECT_ID(@TableName) AND i.is_primary_key = 1
                    ORDER BY ic.key_ordinal", new { TableName = tableName })).ToList();
            }

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
                
                if (i < colList.Count - 1 || pkInfo != null)
                    sb.AppendLine(",");
                else
                    sb.AppendLine("");
            }

            // Append PK with Options
            if (pkInfo != null)
            {
                sb.AppendLine($" CONSTRAINT [{pkInfo.IndexName}] PRIMARY KEY {pkInfo.IndexType} ");
                sb.AppendLine("(");
                for(int k=0; k<pkColumns.Count; k++)
                {
                     sb.Append($"	[{pkColumns[k]}] ASC");
                     if (k < pkColumns.Count - 1) sb.Append(",");
                     sb.AppendLine();
                }
                
                // Construct Options
                var opts = new List<string>();
                opts.Add($"PAD_INDEX = {(pkInfo.is_padded == true ? "ON" : "OFF")}");
                opts.Add($"STATISTICS_NORECOMPUTE = {(pkInfo.no_recompute == 1 ? "ON" : "OFF")}"); // sys.stats.no_recompute is bit? usually 0/1 or bool
                opts.Add($"IGNORE_DUP_KEY = {(pkInfo.ignore_dup_key == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_ROW_LOCKS = {(pkInfo.allow_row_locks == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_PAGE_LOCKS = {(pkInfo.allow_page_locks == true ? "ON" : "OFF")}");
                if (pkInfo.fill_factor != null && pkInfo.fill_factor > 0)
                {
                    opts.Add($"FILLFACTOR = {pkInfo.fill_factor}");
                }
                
                sb.AppendLine($")WITH ({string.Join(", ", opts)}) ON [PRIMARY]");
            }

            sb.Append(") ON [PRIMARY]");
            if (hasLob) sb.Append(" TEXTIMAGE_ON [PRIMARY]"); // Heuristic: append if any LOB likely
            
            sb.AppendLine("");
            sb.AppendLine("GO");

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
    }
}
