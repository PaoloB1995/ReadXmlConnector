using ReadXmlConnector.Services;
using Sedapta.Configuration.Client;
using Sedapta.Configuration.Primitives.Models;
using SedApta.NotificationEngine.Interfaces;
using SedApta.NotificationEngine.Interfaces.Connector;
using SedApta.NotificationEngine.Interfaces.Connector.Delegate;
using SedApta.NotificationEngine.Interfaces.Data;
using SedApta.NotificationEngine.Interfaces.Message;
using SedApta.NotificationEngine.Interfaces.Utils.ConnectorAttributes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

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
        private List<StartListenerInput<ExampleConnectorInputConfiguration>> _startListenerInput;
        private FileSystemWatcher watchFolder = null;
        private NetworkConnection networkConnection = null;
        Timer scmMachineScanTimer;

        public ReadXmlConnector(ExampleConnectorConfiguration configuration, GlobalConfiguration globalConfiguration) : base(configuration, globalConfiguration) { }

        public override void Dispose()
        {
            logger.Trace("ExampleConnector " + configuration.InstanceName + "stopped listening for events.");

            scmMachineScanTimer?.Dispose();
            scmMachineScanTimer = null;

            if (watchFolder != null)
            {
                watchFolder.Dispose();
                watchFolder = null;
            }

            if (networkConnection != null)
            {
                networkConnection.Dispose();
                networkConnection = null;
            }
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

            // Avvia subito il primo tentativo, poi ogni N minuti se non connesso
            scmMachineScanTimer = new Timer(TryConnectAndMonitor, null, TimeSpan.Zero, TimeSpan.FromMinutes(configuration.MintutesRetry));

        }


        private void TryConnectAndMonitor(object state)
        {
            // Se il watcher è già attivo, non fare nulla
            if (watchFolder != null && watchFolder.EnableRaisingEvents)
            {
                logger.Trace("Monitoring già attivo, skip.");
                return;
            }

            logger.Info("Tentativo di connessione alla risorsa di rete...");

            // Pulisce eventuale connessione precedente fallita
            if (networkConnection != null)
            {
                networkConnection.Dispose();
                networkConnection = null;
            }

            bool connected = ConnectToNetworkFolder();

            if (connected)
            {
                logger.Info($"Connessione riuscita. Avvio monitoring della cartella: {configuration.PathToMonitor}");
                StartActivityMonitoring(configuration.PathToMonitor);

                // Stoppa il timer: non serve più riprovare
                scmMachineScanTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            }
            else
            {
                logger.Warn($"Connessione fallita. Riprovo tra {configuration.MintutesRetry} minuti... (cartella: {configuration.PathToMonitor})");

                // Se il watcher era attivo ma la connessione è caduta, lo disabilita
                if (watchFolder != null)
                {
                    watchFolder.EnableRaisingEvents = false;
                    watchFolder.Dispose();
                    watchFolder = null;
                }
            }
        }

        private bool ConnectToNetworkFolder()
        {
            try
            {
                // Verifica se il path è una risorsa di rete (inizia con \\)
                if (configuration.PathToMonitor.StartsWith(@"\\"))
                {
                    // Assumo che la configurazione contenga username e password
                    // Puoi modificare questi campi in base alla tua classe ExampleConnectorConfiguration
                    string username = "scm"; // Aggiungi questa proprietà alla configurazione
                    string password = "scm"; // Aggiungi questa proprietà alla configurazione
                    string domain = ".";     // Opzionale: aggiungi questa proprietà  

                    // Crea le credenziali
                    NetworkCredential credentials;
                    credentials = new NetworkCredential(username, password, domain);

                    // Estrae il percorso di rete (es: \\server\share)
                    string networkPath = ExtractNetworkPath(configuration.PathToMonitor);

                    logger.Info($"Connessione alla risorsa di rete: {networkPath}");

                    // Stabilisce la connessione
                    networkConnection = new NetworkConnection(networkPath, credentials);

                    logger.Info("Connessione alla risorsa di rete stabilita con successo.");
                }
                else
                {
                    logger.Info("Il percorso monitorato è locale, nessuna autenticazione di rete necessaria.");
                }
                return true;
            }
            catch (Exception ex)
            {
                logger.Error($"Errore durante la connessione alla risorsa di rete: {ex.Message}", ex);
                networkConnection = null;
                return false;
            }
        }

        private string ExtractNetworkPath(string fullPath)
        {
            // Estrae il percorso di rete base (es: da \\server\share\folder\subfolder a \\server\share)
            if (!fullPath.StartsWith(@"\\"))
                return fullPath;

            string[] parts = fullPath.TrimStart('\\').Split('\\');
            if (parts.Length >= 2)
            {
                return $@"\\{parts[0]}\{parts[1]}";
            }

            return fullPath;
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

        // Metodo helper per caricare XML con retry logic
        private async Task<XDocument> LoadXmlWithRetry(string xmlFilePath, int maxRetries = 3, int delayMs = 1000)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                try
                {
                    return XDocument.Load(xmlFilePath);
                }
                catch (IOException ex) when (i < maxRetries - 1)
                {
                    logger.Warn($"File {xmlFilePath} temporaneamente bloccato. Tentativo {i + 1}/{maxRetries}. Errore: {ex.Message}");
                    await Task.Delay(delayMs);
                }
                catch (UnauthorizedAccessException ex) when (i < maxRetries - 1)
                {
                    logger.Warn($"Accesso negato al file {xmlFilePath}. Tentativo {i + 1}/{maxRetries}. Errore: {ex.Message}");
                    await Task.Delay(delayMs);
                }
            }

            // Ultimo tentativo senza catch - se fallisce, l'eccezione viene propagata
            return XDocument.Load(xmlFilePath);
        }


        private void DebugFileAccess(string filePath)
        {
            try
            {
                logger.Debug($"=== DEBUG FILE ACCESS per {filePath} ===");

                // Verifica esistenza
                logger.Debug($"File esiste: {File.Exists(filePath)}");

                // Verifica attributi
                if (File.Exists(filePath))
                {
                    FileAttributes attrs = File.GetAttributes(filePath);
                    logger.Debug($"Attributi: {attrs}");

                    FileInfo fi = new FileInfo(filePath);
                    logger.Debug($"Dimensione: {fi.Length} bytes");
                    logger.Debug($"Creato: {fi.CreationTime}");
                    logger.Debug($"IsReadOnly: {fi.IsReadOnly}");
                }

                // Verifica identità corrente
                logger.Debug($"Utente corrente: {System.Security.Principal.WindowsIdentity.GetCurrent().Name}");

                // ===== RETRY OPEN READ =====
                const int maxAttempts = 3;
                int attempt = 0;
                bool success = false;

                while (attempt < maxAttempts && !success)
                {
                    attempt++;

                    try
                    {
                        using (FileStream fs = File.OpenRead(filePath))
                        {
                            logger.Debug($"Permesso di lettura: OK (tentativo {attempt})");
                            success = true;
                        }
                    }
                    catch (IOException ioEx)
                    {
                        logger.Warn($"Tentativo {attempt} fallito: file in uso. {ioEx.Message}");

                        if (attempt >= maxAttempts)
                            throw;

                        Thread.Sleep(1000); // 1 secondo di attesa prima di riprovare
                    }
                }

                logger.Debug("=== FINE DEBUG ===");
            }
            catch (Exception ex)
            {
                logger.Error($"Debug failed: {ex.Message}", ex);
            }
        }

        public class OrderItem
        {
            public string Order { get; set; }
            public decimal Quantity { get; set; }
        }

        private async void EventRaised(object sender, System.IO.FileSystemEventArgs e)
        {

            try
            {

                logger.Info("Started monitoring folder ");

                string xmlFilePath = e.FullPath;

                // DEBUG: verifica permessi
                DebugFileAccess(xmlFilePath);

                XDocument xmlDoc = null;

                try
                {
                    // Carica il file XML con retry logic per gestire file locking
                    xmlDoc = await LoadXmlWithRetry(xmlFilePath);
                }
                catch (Exception ex)
                {
                    logger.Error($"Errore durante il caricamento del file {xmlFilePath}: {ex.Message}", ex);
                    return;
                }

                //XDocument xmlDoc = XDocument.Load(xmlFilePath);

                // Recupero i dati dalla General Statistic

                var general = xmlDoc
                    .Root
                    .Element("GeneralStatistics")
                    .Element("GeneralStatistic");

                // Converte gli attributi in decimal
                //decimal area = ParseDecimal((string)general.Attribute("Area"));
                //decimal partsPerc = ParseDecimal((string)general.Attribute("PartsPerc"));
                //decimal scrapPerc = ParseDecimal((string)general.Attribute("ScrapPerc"));
                //decimal remnantPerc = ParseDecimal((string)general.Attribute("RemnantPerc"));

                //List<string> distinctOrders = new List<string>();

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
                    string materialcode = pattern.Attribute("MaterialCode")?.Value ?? "";
                    string sheetcode = pattern.Attribute("SheetCode")?.Value ?? "";
                    decimal area = ParseDecimal((string)pattern.Attribute("Area"));
                    decimal partsPerc = ParseDecimal((string)pattern.Attribute("PartsPerc"));
                    decimal scrapPerc = ParseDecimal((string)pattern.Attribute("ScrapPerc"));
                    decimal remnantPerc = ParseDecimal((string)pattern.Attribute("RemnantPerc"));
                    decimal totalQuantity = 0;

                    // Ciclo su tutte le Part dentro questo Pattern
                    var patternParts = pattern.Element("Parts")?.Elements("Part");

                    if (patternParts != null)
                    {
                        List<OrderItem> orderItems = new List<OrderItem>();

                        foreach (var part in patternParts)
                        {
                            string serial = part.Attribute("Code")?.Value ?? "";
                            decimal quantity = ParseDecimal((string)part.Attribute("Quantity"));

                            totalQuantity += quantity;

                            logger.Debug($"Seriale {serial} trovato.");

                            bool res = await AddAssociation(configuration.ConnectionString, serial, name);

                            List<string> Orders = await RetrieveOrders(configuration.ConnectionString, serial);

                            foreach (var order in Orders)
                            {
                                orderItems.Add(new OrderItem
                                {
                                    Order = order,
                                    Quantity = quantity
                                });
                            }

                        }

                        if (orderItems.Count == 0)
                        {
                            logger.Debug($"Nessun ordine trovato per i seriali processati");
                        }
                        else
                        {
                            // --- 3. Calcoli ---
                            decimal areaPerOrder = area / totalQuantity;
                            decimal partsArea = areaPerOrder * (partsPerc / 100);
                            decimal scrapArea = areaPerOrder * (scrapPerc / 100);
                            decimal remnantArea = areaPerOrder * (remnantPerc / 100);

                            logger.Debug($"Ordini trovati: {orderItems.Count}, area per ordine={areaPerOrder}");
                            logger.Debug($"PartsArea={partsArea}, ScrapArea={scrapArea}, RemnantArea={remnantArea}");

                            // --- 4. Aggiorna la tabella movimenti ---
                            await UpdateDatabaseOperations(configuration.ConnectionString, orderItems, partsArea, scrapArea, remnantArea, name, materialcode, sheetcode);

                        }

                    }

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
            catch (Exception ex)
            {
                logger.Error($"Errore nel processamento del file");
                return;
            }
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

                        logger.Debug($"Eseguo query WorkOrders per seriale: {serial}");
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
        static async Task 
            UpdateDatabaseOperations(
            string connectionString,
            List<OrderItem> workOrders,
            decimal partsArea,
            decimal scrapArea,
            decimal remnantArea,
            string name,
            string material,
            string sheetcode)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Raggruppa gli ordini e conta le occorrenze
                    var groupedOrders = workOrders
                        .GroupBy(o => o.Order)
                        .Select(g => new
                        {
                            Order = g.Key,
                            TotalQuantity = g.Sum(x => x.Quantity)
                        });

                    string commandText = QueryService.InsertConsumption;


                    foreach (var order in groupedOrders)
                    {
                        using (SqlCommand cmd = new SqlCommand(commandText, connection))
                        {
                            cmd.CommandType = CommandType.Text;

                            cmd.Parameters.Add("@order", SqlDbType.VarChar).Value = order.Order;
                            cmd.Parameters.Add("@material", SqlDbType.VarChar).Value = material;
                            cmd.Parameters.Add("@remnantcode", SqlDbType.VarChar).Value = sheetcode != material ? sheetcode : material;
                            cmd.Parameters.Add("@good", SqlDbType.Decimal).Value = partsArea * order.TotalQuantity;
                            cmd.Parameters.Add("@scrap", SqlDbType.Decimal).Value = scrapArea * order.TotalQuantity;
                            cmd.Parameters.Add("@remnant", SqlDbType.Decimal).Value = remnantArea * order.TotalQuantity;
                            cmd.Parameters.Add("@prgName", SqlDbType.VarChar).Value = name;

                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    logger.Debug($"Inseriti {groupedOrders.Count()} record nella tabella SCM_CONSUMPTION");

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