using SedApta.NotificationEngine.Interfaces.Connector;
using SedApta.NotificationEngine.Interfaces.Utils.ConfigAttributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReadXmlConnector
{
    public class ExampleConnectorAddInputStartConnectorConfig : AbstractAddInputStartConnectorConfig
    {
        [Property("FileFullPath", "Full path of generated file")]
        public string FileFullPath { get; set; }
    }
}
