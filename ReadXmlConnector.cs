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
using Sedapta.Configuration.Client;
using Sedapta.Configuration.Primitives.Models;

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
            logger.Info("Started monitoring folder ");

            string xmlFilePath = e.FullPath;

            XDocument xmlDoc = XDocument.Load(xmlFilePath);

            // Recupero i dati dalla General Statistic

            var general = xmlDoc
                .Root
                .Element("GeneralStatistics")
                .Element("GeneralStatistic");

            // Converte gli attributi in decimal
            decimal area = ParseDecimal((string)general.Attribute("Area"));
            decimal partsPerc = ParseDecimal((string)general.Attribute("PartsPerc"));
            decimal scrapPerc = ParseDecimal((string)general.Attribute("ScrapPerc"));
            decimal remnantPerc = ParseDecimal((string)general.Attribute("RemnantPerc"));

            List<string> distinctOrders = new List<string>();

            // --- 1. Recupero tutti i pattern ---
            var patterns = xmlDoc.Root?.Element("Patterns")?.Elements("Pattern").ToList();

            if (patterns == null || patterns.Count == 0)
            {
                logger.Debug("Nessun pattern trovato nel file XML");
                return;
            }

            logger.Debug($"Trovati {patterns.Count} pattern.");

            var product = "NeConnector";
            var instance = "DEFAULT";

            var cs = ConfigurationFactory.Create(product, instance);
            var databaseMOM = cs.GetPropertyByUniqueName<Database>("supplychain.core.nicimdatabase.config+NicimDatabase", instance);


            foreach (var pattern in patterns)
            {
                string name = pattern.Attribute("Name")?.Value ?? "";

                // Ciclo su tutte le Part dentro questo Pattern
                var patternParts = pattern.Element("Parts")?.Elements("Part");

                if (patternParts != null)
                {
                    foreach (var part in patternParts)
                    {
                        string serial = part.Attribute("Code")?.Value ?? "";

                        logger.Debug($"Seriale {serial} trovato.");

                        bool res = await AddAssociation(configuration.ConnectionString, serial, name);

                        List<string> Orders = await RetrieveOrders(configuration.ConnectionString, serial);

                        // 3️⃣ Aggiungi alla lista globale solo i valori distinti
                        foreach (var wo in Orders)
                        {
                            if (!distinctOrders.Contains(wo))
                                distinctOrders.Add(wo);
                        }

                    }
                }

            }


            if (distinctOrders.Count == 0)
            {
                logger.Debug($"Nessun ordine trovato per i seriali processati");
            }
            else
            {
                // --- 3. Calcoli ---
                decimal areaPerOrder = area / distinctOrders.Count;
                decimal partsArea = areaPerOrder * (partsPerc / 100);
                decimal scrapArea = areaPerOrder * (scrapPerc / 100);
                decimal remnantArea = areaPerOrder * (remnantPerc / 100);
                decimal quantity = partsArea + scrapArea + remnantArea;

                logger.Debug($"Ordini trovati: {distinctOrders.Count}, area per ordine={areaPerOrder}");
                logger.Debug($"PartsArea={partsArea}, ScrapArea={scrapArea}, RemnantArea={remnantArea}");

                // --- 4. Aggiorna la tabella movimenti ---
                await UpdateDatabaseOperations(configuration.ConnectionString, distinctOrders, quantity, scrapArea, remnantArea);

            }

            try
            {
                string outputFolder = configuration.OutputPath;
                System.IO.Directory.CreateDirectory(outputFolder); // si assicura che la cartella esista

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
                logger.Debug($"File spostato in: {destinationFilePath}");
            }
            catch (Exception ex)
            {
                logger.Error($"Errore durante lo spostamento del file: {ex.Message}");
            }
            
             logger.Debug("\nElaborazione completata con successo!");
        }

        // --- FUNZIONI DI SUPPORTO ---

        static decimal ParseDecimal(string value)
            => decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result) ? result : 0m;

        /**
         * Seleziona gli ordini associati a un pattern
         */
        static async Task<List<string>> RetrieveOrders(string connectionString, string serial)
        {
            List<string> result = new List<string>();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {

                    await connection.OpenAsync();

                    string commandText = QueryService.SelectWorkOrders.Replace("@serial", $"'{serial}'");

                    using (SqlCommand getWOs = new SqlCommand(commandText, connection))
                    {

                        getWOs.CommandType = CommandType.Text;

                        logger.Debug($"Eseguo query WorkOrders per seruale: {serial}");
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
                logger.Error($"Errore DB (ProcessDatabaseOperations): {ex.Message}");
                throw;
            }
        }


        static async Task<bool> AddAssociation(string connectionString, string serial, string prjName)
        {
            List<string> result = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    using (SqlCommand checkAssociation = new SqlCommand(QueryService.CheckAssociation, connection))
                    {
                        checkAssociation.CommandType = CommandType.Text;

                        // Aggiungi il parametro @serial
                        checkAssociation.Parameters.Add("@serial", SqlDbType.VarChar).Value = serial;

                        // Esegui la query (di solito ExecuteScalar va benissimo)
                        object queryResult = await checkAssociation.ExecuteScalarAsync();

                        if (queryResult == null || queryResult == DBNull.Value)
                        {
                            // ------------------------------------------------------
                            // 2️⃣ NON ESISTE → esegui INSERT
                            // ------------------------------------------------------
                            logger.Debug($"Nessun record per {serial}. Eseguo INSERT...");

                            using (SqlCommand insertCmd = new SqlCommand(QueryService.InsertAssociation, connection))
                            {
                                insertCmd.CommandType = CommandType.Text;

                                insertCmd.Parameters.Add("@project", SqlDbType.VarChar).Value = prjName;
                                insertCmd.Parameters.Add("@serial", SqlDbType.VarChar).Value = serial;

                                int insertResult = await insertCmd.ExecuteNonQueryAsync();

                                if (insertResult > 0)
                                    result.Add($"INSERTED: {serial}");
                                else
                                    result.Add($"INSERT_FAILED: {serial}");
                            }

                            return true;
                        }
                        else
                        {
                            // ------------------------------------------------------
                            // 3️⃣ ESISTE → esegui UPDATE
                            // ------------------------------------------------------
                            logger.Debug($"Record già esistente per {serial}. Eseguo UPDATE...");

                            using (SqlCommand updateCmd = new SqlCommand(QueryService.UpdateAssociation, connection))
                            {
                                updateCmd.CommandType = CommandType.Text;

                                updateCmd.Parameters.Add("@project", SqlDbType.VarChar).Value = prjName;
                                updateCmd.Parameters.Add("@serial", SqlDbType.VarChar).Value = serial;

                                int updateResult = await updateCmd.ExecuteNonQueryAsync();

                                if (updateResult > 0)
                                    result.Add($"UPDATED: {serial}");
                                else
                                    result.Add($"UPDATE_FAILED: {serial}");
                            }
                            return false;
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                logger.Error($"Errore DB (AddAssociation): {ex.Message}");
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
                        logger.Debug($"Aggiornati {rowsAffected} record nella tabella movimenti per gli ordini: {ordini}");
                    }

                }

            }
            catch (SqlException ex)
            {
                logger.Error($"Errore DB (UpdateDatabaseOperations): {ex.Message}");
                throw;
            }
        }
    }
}