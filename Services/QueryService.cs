using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace ReadXmlConnector.Services
{
    public static class QueryService
    {

        public static string SelectWorkOrders = "SELECT top(1) WorkOrder FROM SCM_SERIAL_NUMBER WITH(NOLOCK) where SerialNumber = @serial";

        public static string InsertConsumption = "INSERT INTO SCM_CONSUMPTION(WorkOrder, Operation, MaterialCode, RemnantCode, ConsumptionGood, ConsumptionScrap, ConsumptionRemnant, ProgramName, CrDate) VALUES(@order, 10, @material, @remnantcode, @good, @scrap, @remnant, @prgName, SYSDATETIME())";

        public static string InsertAssociation = "INSERT INTO SCM_PROJECT_COMPONENT(PROJECT_ID, COMPONENT_SERIAL_NUMBER, FLAG01) VALUES(@project, @serial, 0)";

        public static string UpdateAssociation = "UPDATE SCM_PROJECT_COMPONENT SET PROJECT_ID = @project WHERE COMPONENT_SERIAL_NUMBER = @serial";

        public static string CheckAssociation = "select top(1) COMPONENT_SERIAL_NUMBER from SCM_PROJECT_COMPONENT where COMPONENT_SERIAL_NUMBER = @serial and PROJECT_ID = @project";

    }
}
