using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace ReadXmlConnector.Services
{
    public static class QueryService
    {

        public static string SelectWorkOrders = "SELECT DISTINCT WorkOrder FROM SCM_B2B_SERIAL_NUMBER WITH(NOLOCK) where SerialNumber = @serial";

        public static string UpdateMovements = "UPDATE movimenti\r\nSET quantita = @quantity, coeff_sfrido = @scrapPerc, um_sfrido=0, custom_1 = @remnantPerc\r\nWHERE codice_ordine IN (@orders) AND codice_fase = 10";

        public static string InsertAssociation = "INSERT INTO SCM_PROJECT_COMPONENT(PROJECT_ID, COMPONENT_SERIAL_NUMBER, FLAG01) VALUES(@project, @serial, 0)";

        public static string UpdateAssociation = "UPDATE SCM_PROJECT_COMPONENT SET PROJECT_ID = @project WHERE COMPONENT_SERIAL_NUMBER = @serial";

        public static string CheckAssociation = "select top(1) COMPONENT_SERIAL_NUMBER from SCM_PROJECT_COMPONENT where COMPONENT_SERIAL_NUMBER = @serial";

    }
}
