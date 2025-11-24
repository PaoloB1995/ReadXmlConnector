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
     * INPUT CONNECTOR CONFIGURATION
     * 
     * This configuration class contains input properties that will be displayied as connector properties when this
     * connector is used as input connector in operations.
     * All these properties are configured once for each operation.
     * 
     * This class must have default constructor and each property must have Property attribute.
     */
    public class ExampleConnectorInputConfiguration : AbstractInputConnectorConfig
    {
        [Property("FileNameFilter", "File name filter")]
        public string FileNameFilter { get; set; }

    }
}
