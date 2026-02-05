using Dapper;
using Microsoft.Data.SqlClient;

namespace Logistics.DbMerger
{
    public class ObjectSync
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        public ObjectSync(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        public async Task SyncObjectsAsync(bool dryRun)
        {
            // Types: P (SQL_STORED_PROCEDURE), V (VIEW), FN (SQL_SCALAR_FUNCTION), TF (SQL_TABLE_VALUED_FUNCTION), IF (SQL_INLINE_TABLE_VALUED_FUNCTION)
            var types = new[] { "P", "V", "FN", "TF", "IF" };

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            foreach (var type in types)
            {
                var typeName = GetTypeName(type);
                Console.WriteLine($"\n[Objects] Checking {typeName}...");

                var sourceObjects = await source.QueryAsync<string>(
                    "SELECT name FROM sys.objects WHERE type = @Type AND is_ms_shipped = 0", new { Type = type });
                
                var targetObjects = await target.QueryAsync<string>(
                    "SELECT name FROM sys.objects WHERE type = @Type AND is_ms_shipped = 0", new { Type = type });

                var missing = sourceObjects.Except(targetObjects, StringComparer.OrdinalIgnoreCase).ToList();
                Console.WriteLine($"Found {missing.Count} missing {typeName}.");

                foreach (var objName in missing)
                {
                    if (dryRun)
                    {
                        Console.WriteLine($"[DryRun] Would create {typeName}: {objName}");
                        continue;
                    }

                    try
                    {
                        var definition = await source.QueryFirstOrDefaultAsync<string>(
                            "SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID(@Name)", new { Name = objName });

                        if (!string.IsNullOrEmpty(definition))
                        {
                            // Definition usually contains "CREATE PROCEDURE [Name]..."
                            // It might contain "Schema.Name".
                            // Simple execute usually works for straight copies.
                            await target.ExecuteAsync(definition);
                            Console.WriteLine($"[Success] Created {objName}");
                            RollbackLogger.LogObjectCreation(objName, typeName);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Error] Failed to create {objName}: {ex.Message}");
                    }
                }
            }
        }

        private string GetTypeName(string code) => code switch
        {
            "P" => "Stored Procedures",
            "V" => "Views",
            "FN" => "Scalar Functions",
            "TF" => "Table Functions",
            "IF" => "Inline Functions",
            _ => code
        };
    }
}
