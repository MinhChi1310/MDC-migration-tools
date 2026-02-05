using System.IO;

namespace Logistics.DbMerger
{
    public static class RollbackLogger
    {
        private static string _filePath;

        // Default legacy init
        static RollbackLogger()
        {
            Initialize("general");
        }

        public static void Initialize(string context)
        {
            _filePath = $"rollback_{context}_{DateTime.Now:yyyyMMdd_HHmmss}.sql";
            // Ensure we don't overwrite if called multiple times rapidly? Unique timestamp usually handles it.
            // Or we append if same context?
            if (!File.Exists(_filePath))
            {
                File.AppendAllText(_filePath, $"-- Rollback Script ({context}) Generated at {DateTime.Now}\n\n");
            }
        }
        
        public static string GetCurrentFilePath() => _filePath;

        public static void LogTableCreation(string tableName)
        {
            var sql = $"IF OBJECT_ID('dbo.{tableName}', 'U') IS NOT NULL DROP TABLE dbo.{tableName};\n";
            File.AppendAllText(_filePath, sql);
            Console.WriteLine($"[Rollback] Added DROP TABLE for {tableName}");
        }

        public static void LogObjectCreation(string objectName, string typeDesc)
        {
            // typeDesc e.g. "PROCEDURE", "VIEW", "FUNCTION"
            // We need to map generic object types if needed, but 'DROP PROCEDURE' works 
            // provided we know the type.
            
            // Heuristic mappings based on inputs or we accept the SQL verb directly
            string dropVerb = typeDesc switch
            {
                "Stored Procedures" => "PROCEDURE",
                "Views" => "VIEW",
                "Scalar Functions" => "FUNCTION",
                "Table Functions" => "FUNCTION",
                "Inline Functions" => "FUNCTION",
                _ => "PROCEDURE" // Fallback
            };

            var sql = $"IF OBJECT_ID('dbo.{objectName}') IS NOT NULL DROP {dropVerb} dbo.{objectName};\n";
            File.AppendAllText(_filePath, sql);
            Console.WriteLine($"[Rollback] Added DROP {dropVerb} for {objectName}");
        }

        public static void LogCustomScript(string script)
        {
            File.AppendAllText(_filePath, script);
        }
    }
}
