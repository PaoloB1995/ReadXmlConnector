using SedApta.NotificationEngine.Interfaces.Connector;
using SedApta.NotificationEngine.Interfaces.Utils.ConfigAttributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReadXmlConnector
{
    /*
     * GLOBAL CONFIGURATION
     * 
     * This configuration class contains global properties that will be displayied as global connector properties in NE configurator.
     * All these properties are configured once for each connector instance.
     * 
     * This class must have default constructor and each property must have Property attribute.
     */
    public class ExampleConnectorConfiguration : AbstractConnectorConfig
    {
        [Property("Path to monitor", "Monitor all files in this path (remember trailing slash)")]
        public string PathToMonitor { get; set; }

        [Property("Connection String", "Connection String to the database")]
        public string ConnectionString { get; set; }

        [Property("Output Path", "Folder in which file is moved after the process")]
        public string OutputPath { get; set; }
    }
}
