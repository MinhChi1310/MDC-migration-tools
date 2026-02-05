using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    public class DataMigrator
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;
        private readonly int _batchSize;

        public DataMigrator(string sourceConnStr, string targetConnStr, int batchSize = 5000)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
            _batchSize = batchSize;
        }



        private async Task<bool> HasIdentityColumnAsync(SqlConnection conn, string tableName)
        {
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM sys.identity_columns 
                WHERE object_id = OBJECT_ID(@TableName)", new { TableName = tableName });
            return count > 0;
        }



        private async Task<bool> HasTenantIdColumnAsync(SqlConnection conn, string tableName)
        {
             var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName AND COLUMN_NAME = 'TenantId'", new { TableName = tableName });
            return count > 0;
        }

        public async Task MigrateTableAsync(string sourceTableName, bool isNewTable, string targetTableName = null, int? sourceTenantId = null, int? targetTenantId = null)
        {
            string destTable = targetTableName ?? sourceTableName;
            Console.WriteLine($"[Data] Migrating {sourceTableName} -> {destTable}...");
            
            using var sourceConn = new SqlConnection(_sourceConnStr);
            using var targetConn = new SqlConnection(_targetConnStr);
            await sourceConn.OpenAsync();
            await targetConn.OpenAsync();

            bool hasIdentity = await HasIdentityColumnAsync(targetConn, destTable);
            
            // Check Tenant Filter eligibility
            bool hasTenantId = await HasTenantIdColumnAsync(sourceConn, sourceTableName); 
            string whereClause = "";

            if (sourceTenantId.HasValue && hasTenantId)
            {
                 whereClause = $" WHERE TenantId = {sourceTenantId.Value}";
                 Console.WriteLine($"   -> Filtering by TenantId = {sourceTenantId.Value}");
            }
            else if (sourceTenantId.HasValue && !hasTenantId)
            {
                 Console.WriteLine("   -> Table has no TenantId. Migrating ALL rows (Global/System Table).");
            }

            // Construct Query with Custom Injections
            string selectSql = $"SELECT *";
            
            // Custom Columns for Contact
            if (sourceTableName.Equals("Contact", StringComparison.OrdinalIgnoreCase))
            {
                selectSql += ", NULL as OldSAPID, 0 as PartTimeFlex, NULL as FobNumber, 0 as ExcludeOvertime, NULL as ExcludeOvertimeComment, 0 as VisaRestriction, NULL as VisaEndDate, NULL as MaximumHoursByFortnight, NULL as RosterProfile, 0 as ExcludeFromNotifications";
            }

            selectSql += $" FROM [{sourceTableName}]{whereClause}";

            using var cmd = new SqlCommand(selectSql, sourceConn);
            cmd.CommandTimeout = 600;
            
            using var reader = await cmd.ExecuteReaderAsync();

            using var bulkCopy = new SqlBulkCopy(targetConn, 
                hasIdentity ? SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock : SqlBulkCopyOptions.TableLock, 
                null);
            
            bulkCopy.DestinationTableName = destTable;
            bulkCopy.BatchSize = _batchSize;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.NotifyAfter = 1000;
            bulkCopy.SqlRowsCopied += (sender, e) => Console.Write(".");

            // Column Mapping
            // We need to fetch Target Schema to safely map columns
            // Simple approach: Map all Reader columns that exist in Target.
            var targetSchemaCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var targetSchema = await targetConn.GetSchemaAsync("Columns", new[] { null, null, destTable });
            foreach(System.Data.DataRow row in targetSchema.Rows) 
                targetSchemaCols.Add(row["COLUMN_NAME"].ToString());

            // Check if we need to transform TenantId
            bool transformTenantId = (sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId);
            
            if (transformTenantId)
            {
                Console.WriteLine($"   -> Transforming TenantId: {sourceTenantId} -> {targetTenantId}");
                
                // For transformation, we usually need to buffer if we can't do it in SQL.
                // SQL Transform: SELECT ..., @NewTenantId as TenantId ...
                // This is MUCH faster than DataTable loop.
                // Let's rewrite the query if transforming!
                
                // We need to SELECT all columns EXCEPT TenantId, and inject new TenantId.
                // Harder to do dynamic SELECT * EXCEPT.
                // Fallback: Read into DataTable and update. (Slower but reliable for simple logic)
                // Warning: Large tables (VolumeDetail) might choke.
                // Optimization: Can we assume TenantId is a column?
                // Yes.
                
                // Let's use DataTable for now, but watch performance. User didn't mandate streaming.
                var dt = new DataTable();
                dt.Load(reader);
                
                if (dt.Columns.Contains("TenantId"))
                {
                    foreach(DataRow row in dt.Rows)
                    {
                        row["TenantId"] = targetTenantId.Value;
                    }
                }
                
                // Map columns
                foreach(DataColumn col in dt.Columns)
                {
                    if (targetSchemaCols.Contains(col.ColumnName))
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }
                
                if (dt.Rows.Count > 0)
                {
                    await bulkCopy.WriteToServerAsync(dt);
                    Console.WriteLine($"\n[Data] Completed {sourceTableName} (Transformed {dt.Rows.Count} rows)");
                }
            }
            else
            {
                // Streaming Mode (No ID Transform)
                 for (int i = 0; i < reader.FieldCount; i++)
                 {
                     string colName = reader.GetName(i);
                     if (targetSchemaCols.Contains(colName))
                        bulkCopy.ColumnMappings.Add(colName, colName);
                 }

                try
                {
                    await bulkCopy.WriteToServerAsync(reader);
                    Console.WriteLine($"\n[Data] Completed {sourceTableName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n[Error] Failed to migrate {sourceTableName}: {ex.Message}");
                }
            }
        }
    }
}
