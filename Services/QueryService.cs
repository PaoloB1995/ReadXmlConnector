using System.IO;
using System.Reflection;

namespace ReadXmlConnector.Services
{
    public static class QueryService
    {
        private static readonly Assembly Assembly = Assembly.GetExecutingAssembly();

        private static string GetQuery(string queryPath)
        {
            var resourceName = $"ReadXmlConnector.Queries.{queryPath.Replace('/', '.').Replace('\\', '.')}";

            using (var stream = Assembly.GetManifestResourceStream(resourceName) ?? throw new FileNotFoundException($"Query non trovata: {queryPath}"))
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        public static string SelectWorkOrders = "SELECT DISTINCT s.WorkOrder\r\nFROM SCM_PROJECT_COMPONENT p WITH(NOLOCK)\r\nINNER JOIN SCM_B2B_SERIAL_NUMBER s WITH(NOLOCK)\r\nON (p.COMPONENT_SERIAL_NUMBER = s.SerialNumber)\r\nWHERE p.PROJECT_ID = @pattern_name";

        // => GetQuery("GetWorkOrders.sql");
        public static string UpdateMovements = "UPDATE movimenti\r\nSET quantita = @quantity, coeff_sfrido = @scrapPerc, um_sfrido=0, custom_1 = @remnantPerc\r\nWHERE codice_ordine IN (@orders) AND codice_fase = 10";

            //=> GetQuery("UpdateMovimenti.sql");

        //public static class ProjectComponents
        //{
        //    public static string GetComponentSerialNumber => GetQuery("ProjectComponents/GetComponentSerialNumber.sql");
        //}
    }
}
