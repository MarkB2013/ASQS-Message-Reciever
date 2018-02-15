using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SQSMessageReceiver
{
    public partial class Form1 : Form
    {
        //Location to save downloaded files
        static DirectoryInfo sqsMessages = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.SQS_Messages_Folder);
        //Location to save exception logs
        static DirectoryInfo sessionLogs = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Session_Log_Folder);
        //Public access key for Amazon SQS queue
        static string accessKey = Properties.Settings.Default.accessPKey;
        //Private access key for Amazon SQS queue
        static string secretKey = Properties.Settings.Default.accessSKey;
        //URL for the Amazon SQS queue
        static string queueUrl = Properties.Settings.Default.accessURL;
        //Directory for svaed file archive
        static DirectoryInfo dexArchive = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Dex_Archive);
        //Directory for dex tray for seperate process
        static DirectoryInfo dexTray = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Dex_Tray);
        //Directory for machine database
        static DirectoryInfo machineDB = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Machine_DB);
        //Directory of machine information spreadsheet
        static DirectoryInfo excelSheet = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Excel_File);
        //Directory for machine archive
        static DirectoryInfo machineArchive = new DirectoryInfo(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Machine_Archive);
        //String list to store exception for saving
        List<string> errorLog = new List<string>();
        //String list to store received message data
        List<string> receivedMessages = new List<string>();

        public Form1()
        {
            InitializeComponent();

            Console.WriteLine("Listening for messages... ");

            //Loop forever
            while (true)
            {
                List<string> files = new List<string>();

                try
                {
                    //Method to receive messages from Amazon SQS queue
                    GetMessages();
                }
                catch(Exception totalFail)
                {
                    //Capture exceptions thrown by GetMessages method and save to exception log folder
                    File.WriteAllText(sessionLogs + "Master_Fail_" + DateTime.Now.ToString("dd-MMM-yy_HH-mm-ss") + ".txt", totalFail.ToString());
                }

                foreach(FileInfo file in sqsMessages.GetFiles("*.txt"))
                {
                    files.Add(file.Name);
                }

                if(files.Count > 0)
                {
                    ProcessMessages();
                }
            }                            
        }
        
        //Method to receive messages from Amazon SQS queue
        public void GetMessages()
        {
            //Credentials for queue
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            //Start session with queue
            var client = new AmazonSQSClient(credentials, RegionEndpoint.EUWest1);
            //Request messages with following settings
            var request = new ReceiveMessageRequest
            {
                //Request all messages available
                AttributeNames = new List<string>() { "All" },
                //Queue URL for messages
                QueueUrl = queueUrl,
                //Visibility of messages to other services timeout
                VisibilityTimeout = (int)TimeSpan.FromMinutes(1).TotalSeconds,
                //Retry period
                WaitTimeSeconds = (int)TimeSpan.FromSeconds(20).TotalSeconds,
            };
            //Receive messages
            var response = client.ReceiveMessage(request);
            //If message has been received
            if (response.Messages.Count > 0)
            {
                //Foreach loop for each message received from queue
                foreach (var message in response.Messages)
                {
                    //Console.WriteLine(response.Messages.Count);

                    //Display message ID in console
                    Console.WriteLine("");
                    Console.WriteLine("*** New Message ***");
                    Console.WriteLine("");
                    Console.WriteLine("New message ID '" + message.MessageId + "':");

                    //Split message body contents for processing
                    string[] messageContents = message.Body.Split('\\');

                    //Uncomment to show message body in console
                    //Console.WriteLine("  Body: " + message.Body);

                    //Show receipt handle in console
                    //Console.WriteLine("  Receipt handle: " + message.ReceiptHandle);

                    //Uncomment to show extra info in console
                    //Console.WriteLine("  MD5 of body: " + message.MD5OfBody);
                    //Console.WriteLine("  MD5 of message attributes: " +
                    //message.MD5OfMessageAttributes);
                    //Console.WriteLine("  Attributes:");

                    //List to store desired information from message body
                    List<string> dexData = new List<string>();

                    //Uncomment to add message ID's to list for processing outsude GetMessages method
                    //receivedMessages.Add(message.MessageId + ".txt");

                    //Foreach loop to seperate desired data from message format
                    foreach (string bodyLine in messageContents)
                    {
                        //Data that is desired is on lines that start with n, undesired data is on lines that start with r
                        if (bodyLine.StartsWith("n"))
                        {
                            //Add desired data to list
                            dexData.Add(bodyLine.Substring(1));
                        }
                    }
                    
                    //Seperate message data to retrieve identification data
                    string[] messageBodyData = message.Body.Split(',');

                    //Search for strings that contain indentification variables
                    string machineNumberElement = Array.Find(messageBodyData,
                                            element => element.Contains("OperatorIdentifier"));
                    string recordDateAsStringElement = Array.Find(messageBodyData,
                                            element => element.Contains("MachineDate"));
                    string recordOriginElement = Array.Find(messageBodyData,
                                            element => element.Contains("Origin"));
                    string deviceIDElement = Array.Find(messageBodyData,
                                            element => element.Contains("HWSerial"));

                    //Seperate identifier from data
                    string[] machineElementSplit = machineNumberElement.Split(':');
                    string[] recordDateAsStringElementSplit = recordDateAsStringElement.Split(':', '_');
                    string[] recordOriginElementSplit = recordOriginElement.Split(':');
                    string[] deviceIDElementSplit = deviceIDElement.Split(':');

                    //Retreive variable values from seperated data
                    string machineNumber = machineElementSplit[1].Trim('"');
                    string recordDateAsString = recordDateAsStringElementSplit[1].Trim('"');
                    string recordOrigin = recordOriginElementSplit[1].Trim('"');
                    string deviceID = deviceIDElementSplit[1].Trim('"');

                    //Parse machine record date from message data
                    DateTime recordDate = DateTime.ParseExact(recordDateAsString, "yyyyMMdd HHmmss", CultureInfo.InvariantCulture);

                    //Display identification variables in console for logging perposes
                    Console.WriteLine("######\\\\\\\\\\\\\\//////////////######");
                    Console.WriteLine("Machine Number: " + machineNumber);
                    Console.WriteLine("Record Date: " + recordDate.ToString("dd/MMM/yyyy HH:mm:ss"));
                    Console.WriteLine("Origin: " + recordOrigin);
                    Console.WriteLine("PHYSID: " + deviceID);
                    Console.WriteLine("######//////////////\\\\\\\\\\\\\\######");
                    Console.WriteLine("");

                    //Write desired data to new file
                    File.WriteAllLines(sqsMessages + deviceID + "_" + recordDate.ToString("yyyyMMdd_HHmmss") + "_" + recordOrigin + ".txt", dexData);
                    //Confirm in console that operation was successful
                    Console.WriteLine("Message saved successfully");
                    //Remove message from Amazon SQS queue to avoid duplication
                    client.DeleteMessage(request.QueueUrl, message.ReceiptHandle);

                    //Uncomment to display message attributes in console
                    //foreach (var attr in message.Attributes)
                    //{
                        //Console.WriteLine("    " + attr.Key + ": " + attr.Value);
                    //}
                }               
            }
            else
            {
                //Announce no messages received in console at current time
                Console.WriteLine("No messages received: " + DateTime.Now.ToString());
            }
        }

        public void ProcessMessages()
        {
            //Bool variables for checking if excel file exists and is recent
            bool checkExcel = false;
            bool checkExcelDate = false;

            //Check if machine input excel file exists and check if file was updated in the last 12 hours
            if (File.Exists(excelSheet.ToString()))
            {
                checkExcel = true;

                DateTime ExcelDate = File.GetLastWriteTimeUtc(excelSheet.ToString());
                TimeSpan ExcelAge = DateTime.Now - ExcelDate;

                if (ExcelAge.TotalHours < 36)
                {
                    checkExcelDate = true;
                }
                else
                {
                    errorLog.Add("Excel sheet needs to be updated!");
                }
            }
            else
            {
                errorLog.Add("Excel sheet not found. Location:" + excelSheet.ToString());
            }

            if (checkExcel == true && checkExcelDate == true)
            {

                //String list for file names of all messages in save folder
                List<string> fileNames = new List<string>();

                //Create array and fill with each machine record from machine information excel sheet
                string[] excelData = File.ReadAllLines(Properties.Settings.Default.Database_Path + Properties.Settings.Default.Excel_File);

                //Foreach text file found in Amazon SQS message save folder, add filename to filenames list
                foreach (FileInfo file in sqsMessages.GetFiles("*.txt"))
                {
                    fileNames.Add(file.Name);
                }

                //Machine process counter for keeping track of progress
                int processCount = 0;
                //Total machine count
                int excelLineCount = excelData.Length;
                //String for storing message file data for storing in DAT file in machine archive
                string databaseData = "";

                foreach (string machineEntry in excelData)
                {
                    //Count of lines processed giving indication of progress in console
                    processCount++;

                    //Console.WriteLine(processCount + " out of " + excelLineCount + " machines processed");

                    //Split the excel line into individual variables and add to an array. Excel data is delimited with a comma
                    string[] entryData = machineEntry.Split(',');

                    //Machine information variable declaration
                    string machinenumber = "";
                    string machinelocation = "";
                    string telemetrydevice = "";
                    string telemetryNumber = "";
                    string drivername = "";
                    string routenumber = "";
                    string machinecapacity = "";
                    string machinemodel = "";
                    string machinetype = "";
                    string machinesector = "";

                    //Set variables to line data
                    telemetryNumber = entryData[14].Trim('"');
                    machinenumber = entryData[0].Trim('"');
                    machinelocation = entryData[2].Trim('"');
                    string RouteNumberFull = entryData[17].Trim('"');
                    string[] DriverName = entryData[18].Trim('"').Split('(');
                    telemetrydevice = entryData[8].Trim('"');
                    string[] RouteNumberData = RouteNumberFull.Split(' ');
                    drivername = DriverName[0].Trim('"');
                    machinecapacity = entryData[10].Trim('"');
                    machinemodel = entryData[19].Trim('"');
                    machinetype = entryData[20].Trim('"');
                    machinesector = entryData[21].Trim('"');

                    //Dex file meter variable declarations
                    string Meters = "";
                    string CoinMechSerials = "";
                    string CoinMechMeters = "";
                    string TubeMeters = "";
                    string DispenseMeters = "";
                    string DiscountMeters = "";
                    string OverPayMeters = "";
                    string CashFillMeters = "";
                    string TubeContentsValue = "";
                    string CashlessSales = "";

                    //Foreach file name in filenames list
                    foreach (string fileName in fileNames)
                    {
                        try
                        {
                            //Seperate data in filename
                            string[] fileNameData = fileName.Split('_', '.');
                            //Get current files unique PHYSID for matching with machine records
                            string deviceID = fileNameData[0];
                            //If PHYSID matches the current machine excel sheet entry
                            if (deviceID == telemetryNumber)
                            {
                                //String for full filepath
                                string filePath = sqsMessages + fileName;
                                //Record origin can be automatic or refill(machine triggered or manually triggered by driver/mechanic)
                                string recordOrigin = fileNameData[3];
                                //Parse record date from filename
                                DateTime recordDateTime = DateTime.ParseExact(fileNameData[1] + fileNameData[2], "yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                                //Read file contents and add each line to array
                                string[] fileData = File.ReadAllLines(filePath);

                                try
                                {
                                    //Route number data formatting as it contains irrelevant extra information
                                    routenumber = RouteNumberData[1] + " " + RouteNumberData[2];
                                }
                                catch
                                {

                                }

                                //Origin identifier for indicating the source of the record
                                string originIdentifier = "";

                                //Origin can be Automatic, REFILL or CASH_REFILL but its only neccessary to indentify if its Automatic or other 
                                if (recordOrigin == "AUTOMATIC")
                                {
                                    originIdentifier = "A1";
                                }
                                else
                                {
                                    originIdentifier = "F1";
                                }

                                try
                                {
                                    //Route number data formatting as it contains irrelevant extra information
                                    routenumber = RouteNumberData[1] + " " + RouteNumberData[2];
                                }
                                catch
                                {
                                    //Catch block empty as route numbers can change and it will be evident in reports if this causes a problem
                                }

                                //Dex file meter variables list
                                List<string> ProductMeters = new List<string>();

                                //Dex file data list
                                List<string> MachineData = new List<string>();

                                //Using Array.Find to find the meter readings in the dex file based on the reading tag
                                Meters = Array.Find(fileData,
                                        element => element.StartsWith("VA1", StringComparison.Ordinal));

                                CoinMechSerials = Array.Find(fileData,
                                    element => element.StartsWith("CA1", StringComparison.Ordinal));

                                CoinMechMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA2", StringComparison.Ordinal));

                                TubeMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA3", StringComparison.Ordinal));

                                DispenseMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA4", StringComparison.Ordinal));

                                DiscountMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA7", StringComparison.Ordinal));

                                OverPayMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA8", StringComparison.Ordinal));

                                CashFillMeters = Array.Find(fileData,
                                    element => element.StartsWith("CA10", StringComparison.Ordinal));

                                TubeContentsValue = Array.Find(fileData,
                                    element => element.StartsWith("CA15", StringComparison.Ordinal));

                                CashlessSales = Array.Find(fileData,
                                    element => element.StartsWith("DA2", StringComparison.Ordinal));

                                //This variable is declared before the foreach loop of the lines in the dex file because of the in structure of the dex file the meters are stored on seperate lines to there product tags
                                //The PA1 line contains the product tag so this variable catches that tag for when the loop continues to the next line containing the meter reading it can add that tag to the captured meter variable
                                string PA1 = "";


                                //Foreach loop through each line in the dex file
                                foreach (string Line in fileData)
                                {
                                    //Variable for matching product tag with associated meter
                                    string NewProduct = "";

                                    if (Line.StartsWith("PA1"))
                                    {
                                        //If the line starts with PA1 that indicates the line is a product tag

                                        PA1 = Line;
                                    }
                                    else if (Line.StartsWith("PA2"))
                                    {
                                        //If the line starts with PA2 that indicates its the product meter of the previously captured product tag
                                        //Add the tag and meter reading together in a variable delimited with a hyphen

                                        NewProduct = PA1 + "-" + Line;
                                        //Add product tag/meter to the products data list
                                        ProductMeters.Add(NewProduct);
                                    }
                                }

                                //Meter variable declaration
                                string TotalVends = "0";
                                string TotalCash = "0";
                                string ResetVends = "0";
                                string ResetCash = "0";

                                //If the dex file meter entry existed
                                if (Meters != null)
                                {
                                    //Split the line by its delimiter
                                    string[] MeterLine = Meters.Split('*');

                                    //Set variable's data from readings
                                    TotalVends = MeterLine[1];
                                    TotalCash = MeterLine[2];
                                    ResetVends = MeterLine[3];
                                    ResetCash = MeterLine[4];
                                }

                                //Coin mech variables
                                string CoinMechSerialNumber = "0";
                                string CoinMechModel = "0";
                                string CoinMechSoftwareVersion = "0";

                                if (CoinMechSerials != null)
                                {
                                    string[] CoinMechSerialLine = CoinMechSerials.Split('*');
                                    CoinMechSerialNumber = CoinMechSerialLine[1];
                                    CoinMechModel = CoinMechSerialLine[2];
                                    CoinMechSoftwareVersion = CoinMechSerialLine[3];
                                }

                                //Coin mech meter variables
                                string CoinMechTotalCash = "0";
                                string CoinMechTotalVends = "0";
                                string CoinMechResetCash = "0";
                                string CoinMechResetVends = "0";

                                if (CoinMechMeters != null)
                                {
                                    string[] CoinMechMeterLine = CoinMechMeters.Split('*');
                                    CoinMechTotalCash = CoinMechMeterLine[1];
                                    CoinMechTotalVends = CoinMechMeterLine[2];
                                    CoinMechResetCash = CoinMechMeterLine[3];
                                    CoinMechResetVends = CoinMechMeterLine[4];
                                }

                                //Cash in variables
                                string CashIn = "0";
                                string ToCashBoxReset = "0";
                                string CashToTubesReset = "0";
                                string ToCashBoxInit = "0";
                                string CashToTubesInit = "0";

                                if (TubeMeters != null)
                                {
                                    string[] TubeMeterLine = TubeMeters.Split('*');
                                    CashIn = TubeMeterLine[1];
                                    ToCashBoxReset = TubeMeterLine[2];
                                    CashToTubesReset = TubeMeterLine[3];
                                    ToCashBoxInit = TubeMeterLine[4];
                                    CashToTubesInit = TubeMeterLine[5];
                                }

                                //Cash out vairbales
                                string CashDispensedReset = "0";
                                string CashManualDispenseReset = "0";
                                string CashDispensedInit = "0";
                                string CashManualDispenseInit = "0";

                                if (DispenseMeters != null)
                                {
                                    string[] DispenseMeterLine = DispenseMeters.Split('*');
                                    CashDispensedReset = DispenseMeterLine[1];
                                    CashManualDispenseReset = DispenseMeterLine[2];
                                    CashDispensedInit = DispenseMeterLine[3];
                                    CashManualDispenseInit = DispenseMeterLine[4];
                                }

                                //Discount variables
                                string DiscountsValueReset = "0";
                                string DiscountsValueInit = "0";

                                if (DiscountMeters != null)
                                {
                                    string[] DiscountMeterLine = DiscountMeters.Split('*');
                                    DiscountsValueReset = DiscountMeterLine[1];
                                    DiscountsValueInit = DiscountMeterLine[2];
                                }

                                //Overpay variables
                                string OverPayValueReset = "0";
                                string OverPayValueInit = "0";

                                if (OverPayMeters != null)
                                {
                                    string[] OverPayMeterLine = OverPayMeters.Split('*');
                                    OverPayValueReset = OverPayMeterLine[1];
                                    OverPayValueInit = OverPayMeterLine[2];
                                }

                                //Cash in from driver vairables
                                string CashFillValueReset = "0";
                                string CashFillValueInit = "0";

                                if (CashFillMeters != null)
                                {
                                    string[] CashFillMeterLine = CashFillMeters.Split('*');
                                    CashFillValueReset = CashFillMeterLine[1];
                                    CashFillValueInit = CashFillMeterLine[2];
                                }

                                //Value of coins in coin mech variable
                                string TubeValue = "0";

                                if (TubeContentsValue != null)
                                {
                                    string[] TubeContentsLine = TubeContentsValue.Split('*');
                                    TubeValue = TubeContentsLine[1];
                                }

                                //Cashless meter variables
                                string CashlessCashMeter = "";
                                string CashlessVendMeter = "";

                                if (CashlessSales != null)
                                {
                                    string[] CashlessLine = CashlessSales.Split('*');
                                    CashlessCashMeter = CashlessLine[1];
                                    CashlessVendMeter = CashlessLine[2];
                                }

                                //Combine all product variables to string deleimited with a comma
                                string ProductList = string.Join(",", ProductMeters.ToArray());

                                //Combine all variables into one string to become a single line entry
                                databaseData = originIdentifier + "-" + recordDateTime.ToString("ddMMyyyy,HHmmss")
                                    + "*A2-" + machinenumber + "*A3-" + machinelocation + "*A4-" + telemetrydevice + "*A5-"
                                    + telemetryNumber + "*A6-" + drivername + "*A7-" + routenumber + "*A8-" + machinecapacity + "*A9-"
                                    + machinemodel + "*A10-" + machinetype + "*A11-" + machinesector + "*A12-" + TotalVends
                                    + "*A13-" + TotalCash + "*A14-" + ResetVends + "*A15-" + ResetCash + "*A16-" + CoinMechSerialNumber
                                    + "*A17-" + CoinMechModel + "*A18-" + CoinMechSoftwareVersion + "*A19-" + CoinMechTotalCash
                                    + "*A20-" + CoinMechTotalVends + "*A21-" + CoinMechResetCash + "*A22-" + CoinMechResetVends
                                    + "*A23-" + CashIn + "*A24-" + ToCashBoxReset + "*A25-" + CashToTubesReset + "*A26-" + ToCashBoxInit
                                    + "*A27-" + CashToTubesInit + "*A28-" + CashDispensedReset + "*A29-" + CashManualDispenseReset
                                    + "*A30-" + CashDispensedInit + "*A31-" + CashManualDispenseInit + "*A32-" + DiscountsValueReset
                                    + "*A33-" + DiscountsValueInit + "*A34-" + OverPayValueReset + "*A35-" + OverPayValueInit
                                    + "*A36-" + CashFillValueReset + "*A37-" + CashFillValueInit + "*A38-" + TubeValue
                                    + "*A39-" + CashlessCashMeter + "*A40-" + CashlessVendMeter
                                    + "*A41_" + ProductList;

                                //Check if current months machine file exists if not creates it and adds the data to the file
                                if (File.Exists(machineArchive + machinenumber + "-" + recordDateTime.ToString("MMM-yy") + ".dat"))
                                {
                                    string[] machineData = File.ReadAllLines(machineArchive + machinenumber + "-" + recordDateTime.ToString("MMM-yy") + ".dat");

                                    foreach (string machineLine in machineData)
                                    {
                                        MachineData.Add(machineLine);
                                    }

                                    if (databaseData != "")
                                    {
                                        MachineData.Add(databaseData);
                                    }

                                    File.WriteAllLines(machineArchive + machinenumber + "-" + recordDateTime.ToString("MMM-yy") + ".dat", MachineData);
                                }
                                else
                                {
                                    if (databaseData != "")
                                    {
                                        MachineData.Add(databaseData);
                                    }

                                    File.WriteAllLines(machineArchive + machinenumber + "-" + recordDateTime.ToString("MMM-yy") + ".dat", MachineData);
                                }
                            }
                        }//Catch exceptions and add to error log list
                        catch (Exception fileError)
                        {
                            errorLog.Add(fileError.ToString());
                        }
                    }
                }

                //Move saved message to archive and clear saved message directory to avoid duplication
                foreach (string file in fileNames)
                {
                    string[] NameData = file.Split('_', '.');
                    DateTime DexTime = DateTime.ParseExact(NameData[1] + NameData[2], "yyyyMMddHHmmss", CultureInfo.InstalledUICulture);

                    File.Copy(sqsMessages + file, dexArchive + NameData[0] + "_" + NameData[1] + "_" + NameData[2] + "_" + NameData[3] + ".dex", true);
                    File.Copy(sqsMessages + file, machineDB + NameData[0] + ".dex", true);
                    File.SetCreationTime(machineDB + NameData[0] + ".dex", DexTime);

                    try
                    {
                        File.Move(sqsMessages + file, dexTray + NameData[0] + "_" + NameData[1] + "_" + NameData[2] + "_" + NameData[3] + ".dex");
                    }
                    catch
                    {
                        errorLog.Add("File could not be moved: " + sqsMessages + file);
                    }
                }
                //If an axception has been thrown
                if (errorLog.Count > 0)
                {
                    //Write all exceptions to log file
                    File.WriteAllLines(sessionLogs + "Error Log " + DateTime.Now.ToString("dd-MMM-yy_HH-mm-ss") + ".txt", errorLog);
                }
            }
            else
            {
                MessageBox.Show("Excel sheet is not accessable or is out of date");
            }

            Console.WriteLine("Database Synchronized.");
            Console.WriteLine("**********************");
        }
    }
}
