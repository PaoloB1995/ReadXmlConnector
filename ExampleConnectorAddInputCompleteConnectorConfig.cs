using SedApta.NotificationEngine.Interfaces.Connector;
using SedApta.NotificationEngine.Interfaces.Utils.ConfigAttributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReadXmlConnector
{
    public class ExampleConnectorAddInputCompleteConnectorConfig : AbstractAddInputCompleteConnectorConfig
    {
        [Property("FileNameFilter", "File name filter")]
        public string FileNameFilter { get; set; }
    }
}
