using SedApta.NotificationEngine.Interfaces;
using SedApta.NotificationEngine.Interfaces.Connector;
using SedApta.NotificationEngine.Interfaces.Connector.Delegate;
using SedApta.NotificationEngine.Interfaces.Data;
using SedApta.NotificationEngine.Interfaces.Message;
using SedApta.NotificationEngine.Interfaces.Utils.ConnectorAttributes;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ReadXmlConnector.Services;
using System.Linq;

namespace ReadXmlConnector
{
    /*
     * CONNECTOR CLASS
     * 
     * This class contains the connector code.
     * The connector class must extends one of these classes, depends on the type of connector:
     *   AbstractInputConnector: for input only connectors,
     *   AbstractOutputConnector: for output only connectors,
     *   AbstractBothConnector: for input/output connectors
     * and must define a constructor with following signature:
     *   public ExampleConnector(<ConnectorConfigType> configuration, GlobalConfiguration globalConfiguration)
     * 
     * After this, the connector class must implement a couple of methods:
     *   InternalStartListening: invoked on startup for starting the connector when used as input connector,
     *   InternalSendMessage: send a message when used as output connector.
     */
    [Connector("Read Xml Connector")]
    public class ReadXmlConnector : AbstractConnector<ExampleConnectorConfiguration, ExampleConnectorInputConfiguration, ExampleConnectorAddInputStartConnectorConfig, ExampleConnectorAddInputCompleteConnectorConfig, ExampleConnectorOutputConfiguration>,
            IInputConnector<ExampleConnectorInputConfiguration>, IOutputConnector<ExampleConnectorOutputConfiguration>,
            IAdditionalInputConnector<ExampleConnectorAddInputStartConnectorConfig, ExampleConnectorAddInputCompleteConnectorConfig>
    {
        private static readonly ILogger logger = LoggerFactory.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private Dictionary<string, List<StartListenerInput<ExampleConnectorInputConfiguration>>> watchedFiles = new Dictionary<string, List<StartListenerInput<ExampleConnectorInputConfiguration>>>();
        private FileSystemWatcher watchFolder = null;

        public ReadXmlConnector(ExampleConnectorConfiguration configuration, GlobalConfiguration globalConfiguration) : base(configuration, globalConfiguration) { }

        public override void Dispose()
        {
            logger.Trace("ExampleConnector " + configuration.InstanceName + "stopped listening for events.");
            if (watchFolder != null) watchFolder.Dispose();
        }

        /*
         * This method will be invoked only once at service startup, and it will receive as input parameter a list of all input configuration classes, 
         * one for each operation defined in the NE configurator web interface. 
         * In order to raise an event to the internal NE engine, this method must invoke following callback
         *   MessageProcessor.GetProcessorInstance(<OperationId>).ProcessMessage(...)
         * where the operationId is the is of the operation associated to the input configuration.
         */
        public void StartListening(List<StartListenerInput<ExampleConnectorInputConfiguration>> startListenerInput)
        {
            watchedFiles.Add("#~ANY~#", new List<StartListenerInput<ExampleConnectorInputConfiguration>>());

            foreach (StartListenerInput<ExampleConnectorInputConfiguration> item in startListenerInput)
            {
                ExampleConnectorInputConfiguration eventConfiguration = item.InputConfiguration;
                if (string.IsNullOrWhiteSpace(eventConfiguration.FileNameFilter))
                {
                    //Add this operation to special ANY key
                    watchedFiles["#~ANY~#"].Add(item);
                }
                else
                {
                    if (!watchedFiles.ContainsKey(eventConfiguration.FileNameFilter))
                        watchedFiles.Add(eventConfiguration.FileNameFilter, new List<StartListenerInput<ExampleConnectorInputConfiguration>>());

                    watchedFiles[eventConfiguration.FileNameFilter].Add(item);
                }
            }
            StartActivityMonitoring(configuration.PathToMonitor);
        }

        /*
         * InternalSendMessage will be invoked once for each message to sent as output message.
         */
        public bool SendMessage(ExampleConnectorOutputConfiguration outputConfiguration, MessageTypeData OutputMessage, NEMessageEnvelope message)
        {
            File.WriteAllText(outputConfiguration.FileFullPath, message.ToString());
            return true;
        }
        public bool AdditionalInput(ExampleConnectorAddInputStartConnectorConfig startConfig, MessageTypeData startMessage, ExampleConnectorAddInputCompleteConnectorConfig completeConfig, MessageTypeData completeMessage, NEMessageEnvelope message, AdditionalInputCallback callback)
        {
            File.WriteAllText(startConfig.FileFullPath, startMessage.ToString());
            FileSystemWatcher watchAddInputFolder = null;
            callback.Disposing = () =>
            {
                watchAddInputFolder.Dispose();
            };

            watchAddInputFolder = new FileSystemWatcher(configuration.PathToMonitor, completeConfig.FileNameFilter);

            // Hook the triggers(events) to our handler (eventRaised)
            //watchAddInputFolder.Error += new ErrorEventHandler(watchAddInputFolder_Error);
            watchAddInputFolder.Created += new FileSystemEventHandler((object sender, System.IO.FileSystemEventArgs e) =>
            {
                string fullPath = e.FullPath;
                logger.Trace("Received file to process: " + e.FullPath);
                Thread.Sleep(1000);
                string content = null;

                try
                {
                    using (StreamReader sr = new StreamReader(fullPath))
                    {
                        content = sr.ReadToEnd();
                    }
                }
                catch (Exception ex)
                {
                    logger.Error("Error reading file " + fullPath, ex);
                    return;
                }
                callback.ProcessMessage(content);
            });
            //Enable monitoring
            try
            {
                watchAddInputFolder.EnableRaisingEvents = true;
                logger.Info("Started monitoring folder " + configuration.PathToMonitor + " for files " + completeConfig.FileNameFilter);
            }
            catch (ArgumentException)
            {
                logger.Error("Error during start of monitoring of folder " + configuration.PathToMonitor + ". This folder won't be monitored.");
            }

            return true;
        }

        private void StartActivityMonitoring(string pathToMonitor)
        {
            watchFolder = new FileSystemWatcher(pathToMonitor, "*.*");

            // Hook the triggers(events) to our handler (eventRaised)
            //watchFolder.Error += new ErrorEventHandler(watchFolder_Error);
            watchFolder.Created += new FileSystemEventHandler(EventRaised);
            watchFolder.Renamed += new RenamedEventHandler(EventRaised);

            //Enable monitoring
            try
            {
                watchFolder.EnableRaisingEvents = true;
                logger.Info("Started monitoring folder " + pathToMonitor);
            }
            catch (ArgumentException)
            {
                logger.Error("Error during start of monitoring of folder " + pathToMonitor + ". This folder won't be monitored.");
            }
        }

        private async void EventRaised(object sender, System.IO.FileSystemEventArgs e)
        {
            string xmlFilePath = e.FullPath;

            XDocument xmlDoc = XDocument.Load(xmlFilePath);

            // --- 1. Recupero tutti i pattern ---
            var patterns = xmlDoc.Root?.Element("Patterns")?.Elements("Pattern").ToList();

            if (patterns == null || patterns.Count == 0)
            {
                Console.WriteLine("Nessun pattern trovato nel file XML");
                return;
            }

            Console.WriteLine($"Trovati {patterns.Count} pattern.");

            foreach (var pattern in patterns)
            {
                string name = pattern.Attribute("Name")?.Value ?? "";
                decimal area = ParseDecimal(pattern.Attribute("Area")?.Value);
                decimal partsPerc = ParseDecimal(pattern.Attribute("PartsPerc")?.Value);
                decimal scrapPerc = ParseDecimal(pattern.Attribute("ScrapPerc")?.Value);
                decimal remnantPerc = ParseDecimal(pattern.Attribute("RemnantPerc")?.Value);

                Console.WriteLine($"\n--- Elaboro pattern: {name} ---");
                Console.WriteLine($"Area={area}, Parts%={partsPerc}, Scrap%={scrapPerc}, Remnant%={remnantPerc}");


                // --- 2. Ricavo ordini dal DB per il pattern ---
                List<string> workOrders = await ProcessDatabaseOperations(configuration.ConnectionString, name);

                if (workOrders.Count == 0)
                {
                    Console.WriteLine($"Nessun ordine trovato per il pattern {name}");
                    continue;
                }

                // --- 3. Calcoli ---
                decimal areaPerOrder = area / workOrders.Count;
                decimal partsArea = areaPerOrder * (partsPerc / 100);
                decimal scrapArea = areaPerOrder * (scrapPerc / 100);
                decimal remnantArea = areaPerOrder * (remnantPerc / 100);
                decimal quantity = partsArea + scrapArea + remnantArea;

                Console.WriteLine($"Ordini trovati: {workOrders.Count}, area per ordine={areaPerOrder}");
                Console.WriteLine($"PartsArea={partsArea}, ScrapArea={scrapArea}, RemnantArea={remnantArea}");

                // --- 4. Aggiorna la tabella movimenti ---
                await UpdateDatabaseOperations("Server=DELSQL02\\delsql;Database=demo-sbonucelliscm-nicim_scm-dev;TrustServerCertificate=true;User Id=user_demo-sbonucelliscm-nicim_scm-dev;Password=123Stella$;", workOrders, quantity, scrapArea, remnantArea);

                Console.WriteLine($"Aggiornamento completato per il pattern {name}");

                try
                {
                    string outputFolder = configuration.OutputPath;
                    Directory.CreateDirectory(outputFolder); // si assicura che la cartella esista

                    string destinationFilePath = Path.Combine(outputFolder, Path.GetFileName(xmlFilePath));

                    // Se il file esiste già nella cartella di destinazione, aggiunge un timestamp per evitare conflitti
                    if (File.Exists(destinationFilePath))
                    {
                        string fileNameWithoutExt = Path.GetFileNameWithoutExtension(xmlFilePath);
                        string ext = Path.GetExtension(xmlFilePath);
                        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                        destinationFilePath = Path.Combine(outputFolder, $"{fileNameWithoutExt}_{timestamp}{ext}");
                    }

                    File.Move(xmlFilePath, destinationFilePath);
                    Console.WriteLine($"File spostato in: {destinationFilePath}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Errore durante lo spostamento del file: {ex.Message}");
                }
            }

            Console.WriteLine("\nElaborazione completata con successo!");

        }

        // --- FUNZIONI DI SUPPORTO ---

        static decimal ParseDecimal(string value)
            => decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result) ? result : 0m;

        /**
         * Seleziona gli ordini associati a un pattern
         */
        static async Task<List<string>> ProcessDatabaseOperations(string connectionString, string patternName)
        {
            List<string> result = new List<string>();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {

                    await connection.OpenAsync();

                    string commandText = QueryService.SelectWorkOrders.Replace("@pattern_name", $"'{patternName}'");

                    using (SqlCommand getWOs = new SqlCommand(commandText, connection))
                    {

                        getWOs.CommandType = CommandType.Text;

                        Console.WriteLine($"Eseguo query WorkOrders per pattern: {patternName}");
                        SqlDataReader reader = await getWOs.ExecuteReaderAsync();

                        while (reader.Read())
                        {
                            string workOrder = reader["WorkOrder"]?.ToString() ?? "";
                            if (!string.IsNullOrEmpty(workOrder))
                                result.Add(workOrder);
                        }
                    }


                }



                return result;
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Errore DB (ProcessDatabaseOperations): {ex.Message}");
                throw;
            }
        }

        /**
         * Aggiorna la tabella movimenti con i valori calcolati
         */
        static async Task UpdateDatabaseOperations(
            string connectionString,
            List<string> workOrders,
            decimal partsArea,
            decimal scrapArea,
            decimal remnantArea)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    string ordini = string.Join(",", workOrders.ConvertAll(wo => $"'{wo}'"));
                    string commandText = QueryService.UpdateMovements.Replace("@orders", ordini);

                    using (SqlCommand updateMovimenti = new SqlCommand(commandText, connection))
                    {
                        updateMovimenti.CommandType = CommandType.Text;

                        updateMovimenti.Parameters.Add("quantity", SqlDbType.Decimal).Value = partsArea;
                        updateMovimenti.Parameters.Add("scrapPerc", SqlDbType.Decimal).Value = scrapArea;
                        updateMovimenti.Parameters.Add("remnantPerc", SqlDbType.Decimal).Value = remnantArea;

                        int rowsAffected = await updateMovimenti.ExecuteNonQueryAsync();
                        Console.WriteLine($"Aggiornati {rowsAffected} record nella tabella movimenti per gli ordini: {ordini}");
                    }

                }

            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Errore DB (UpdateDatabaseOperations): {ex.Message}");
                throw;
            }
        }
    }
}