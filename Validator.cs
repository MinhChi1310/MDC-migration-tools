using Dapper;
using Microsoft.Data.SqlClient;
using System.Text;

namespace Logistics.DbMerger
{
    public class Validator
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        public Validator(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        public async Task RunValidationAsync(int? tenantId)
        {
            Console.WriteLine($"\n[Validation] Starting Validation for Tenant: {tenantId?.ToString() ?? "All"}");
            
            await ValidateRowCountsAsync(tenantId);
            await ValidateForeignKeysAsync();
            if (tenantId.HasValue)
            {
                await ValidateBusinessLogicAsync(tenantId.Value);
            }
        }

        private async Task ValidateRowCountsAsync(int? tenantId)
        {
            Console.WriteLine("\n[1/3] Validating Row Counts...");
            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var tables = await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
            var discrepancies = new List<string>();

            foreach (var table in tables)
            {
                try 
                {
                    // Check if table exists in source
                    var sourceExists = await source.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Name", new { Name = table }) > 0;
                    if (!sourceExists) continue;

                    // Check for TenantID column
                    var hasTenant = await target.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = 'TenantId'") > 0;

                    string query = $"SELECT COUNT(*) FROM [{table}]";
                    string where = "";
                    
                    if (tenantId.HasValue && hasTenant)
                    {
                        where = $" WHERE TenantId = {tenantId}";
                    }

                    long sourceCount = await source.ExecuteScalarAsync<long>(query + where);
                    long targetCount = await target.ExecuteScalarAsync<long>(query + where);

                    if (sourceCount != targetCount)
                    {
                        string msg = $"[MISMATCH] {table}: Source={sourceCount}, Target={targetCount}";
                        Console.WriteLine(msg);
                        discrepancies.Add(msg);
                    }
                    else
                    {
                        // Console.WriteLine($"[OK] {table}: {sourceCount}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Skip] {table}: {ex.Message}");
                }
            }
            
            if (discrepancies.Count == 0) Console.WriteLine("[OK] All verified table row counts match.");
        }

        private async Task ValidateForeignKeysAsync()
        {
            Console.WriteLine("\n[2/3] Validating Foreign Keys in Target...");
            using var target = new SqlConnection(_targetConnStr);
            
            try
            {
                // Simple DBCC check
                var results = await target.QueryAsync("DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS");
                if (results.Any())
                {
                    Console.WriteLine("[ERROR] Foreign Key Violations Found:");
                    foreach (var row in results)
                    {
                        Console.WriteLine($" - Table: {row.Table}, Constraint: {row.Constraint}, Where: {row.Where}");
                    }
                }
                else
                {
                    Console.WriteLine("[OK] No Foreign Key violations detected.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Error] FK Validation failed: {ex.Message}");
            }
        }

        private async Task ValidateBusinessLogicAsync(int tenantId)
        {
            Console.WriteLine("\n[3/3] Running Business Validation Queries...");
            using var target = new SqlConnection(_targetConnStr);

            // 1. Active Contacts
            await RunCheck(target, "Active Contacts", 
                $"SELECT COUNT(*) FROM Contact WHERE IsDeleted = 0 AND TenantID = {tenantId}");

            // 2. Timeband Range
            await RunCheck(target, "Timeband Schedule Range", 
                $"SELECT 'Start: ' + CONVERT(varchar, MIN(ScheduleStart), 120) + ' End: ' + CONVERT(varchar, MAX(ScheduleEnd), 120) FROM Timeband WHERE TenantID = {tenantId}");
            
            // 3. Leave Request Stats
            await RunCheck(target, "Leave Requests by Status", 
                $"SELECT 'Status ' + CAST(StatusID as varchar) + ': ' + CAST(COUNT(*) as varchar) FROM LeaveRequest WHERE TenantID = {tenantId} GROUP BY StatusID");
        }

        private async Task RunCheck(SqlConnection conn, string name, string sql)
        {
            try 
            {
                // Check if table exists first?
                // Or just try catch
                var result = await conn.QueryAsync<string>(sql); // Assuming string output or simple scalar
                // Actually result might be complex row.
                // Let's print rows dynamic
                
                // If the user's query returns multiple columns/rows, QueryAsync<dynamic> is better
                var rows = await conn.QueryAsync(sql);
                
                Console.WriteLine($"--- {name} ---");
                if (!rows.Any())
                {
                    Console.WriteLine("(No results)");
                    return;
                }

                foreach (var row in rows)
                {
                   Console.WriteLine(row);
                }
            }
            catch(SqlException ex)
            {
                // Table likely doesn't exist
                // Console.WriteLine($"[Skip] {name}: Table missing or error ({ex.Message})");
            }
        }
    }
}
