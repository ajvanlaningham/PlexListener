using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace PlexListener
{
    internal class Program
    {
        private static IConfiguration _configuration;
        private static ServiceBusClient _serviceBusClient;
        private static ServiceBusProcessor _processor;
        private static BlobContainerClient _hotContainerClient;
        private static ServiceBusSender _successSender;
        private static ServiceBusSender _errorSender;

        static async Task Main(string[] args)
        {
            InitializeConfiguration();

            InitializeAzureClients();

            await SetupServiceBusProcessor();

            Console.WriteLine("Listener is running. Press any key to exit...");
            Console.ReadKey();

            await _processor.DisposeAsync();
            await _serviceBusClient.DisposeAsync();
        }

        private static void InitializeConfiguration()
        {
            _configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();
        }

        private static void InitializeAzureClients()
        {
            string sbConnectionString = _configuration["AzureServiceBus:ConnectionString"];
            _serviceBusClient = new ServiceBusClient(sbConnectionString);

            string successQueueName = _configuration["AzureServiceBus:SendQueueName"];
            string errorQueueName = _configuration["AzureServiceBus:ErrorQueueName"];
            _successSender = _serviceBusClient.CreateSender(successQueueName);
            _errorSender = _serviceBusClient.CreateSender(errorQueueName);

            string blobConnectionString = _configuration["AzureBlobStorage:ConnectionString"];
            string hotContainerName = _configuration["AzureBlobStorage:HotContainerName"];
            _hotContainerClient = new BlobContainerClient(blobConnectionString, hotContainerName);
            _hotContainerClient.CreateIfNotExists();

            var mediaMappings = _configuration.GetSection("MediaMappings")
                .Get<Dictionary<string, string>>();
            foreach (var mapping in mediaMappings)
            {
                Directory.CreateDirectory(mapping.Value);
            }
        }

        private static async Task SetupServiceBusProcessor()
        {
            string listenQueueName = _configuration["AzureServiceBus:ListenQueueName"];
            _processor = _serviceBusClient.CreateProcessor(listenQueueName, new ServiceBusProcessorOptions());

            _processor.ProcessMessageAsync += ProcessMessageHandler;
            _processor.ProcessErrorAsync += ProcessErrorHandler;

            await _processor.StartProcessingAsync();
        }

        private static async Task ProcessMessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received message: {body}");

            bool isSuccess = true;

            try
            {
                FolderNode rootFolder = JsonSerializer.Deserialize<FolderNode>(body, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                if (rootFolder == null)
                {
                    Console.WriteLine("Failed to deserialize the folder structure.");
                    isSuccess = false;
                }
                else
                {
                    await DownloadFolderAsync(rootFolder, _hotContainerClient);
                }

                if (isSuccess)
                {
                    await SendMessageAsync(_successSender, $"Successfully processed message: {args.Message.MessageId}");
                    Console.WriteLine("All files downloaded successfully.");
                }
                else
                {
                    await SendMessageAsync(_errorSender, $"Failed to process message: {args.Message.MessageId}");
                    Console.WriteLine("There were issues processing the message.");
                }

                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                isSuccess = false;

                await SendMessageAsync(_errorSender, $"Exception processing message {args.Message.MessageId}: {ex.Message}");
            }
        }

        private static Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Error in the Service Bus processor: {args.Exception.Message}");
            return Task.CompletedTask;
        }

        private static async Task DownloadFolderAsync(FolderNode folder, BlobContainerClient blobClient, string currentBlobPath = "")
        {
            string updatedBlobPath = string.IsNullOrEmpty(currentBlobPath) ? folder.Name : $"{currentBlobPath}/{folder.Name}";

            string mediaType = DetermineMediaType(updatedBlobPath);

            if (string.IsNullOrEmpty(mediaType))
            {
                Console.WriteLine($"Unknown media type for folder: {updatedBlobPath}. Skipping...");
                return;
            }

            string downloadDirectory = _configuration[$"MediaMappings:{mediaType}"];
            if (string.IsNullOrEmpty(downloadDirectory))
            {
                Console.WriteLine($"No download directory mapped for media type: {mediaType}. Skipping...");
                return;
            }

            string localFolderPath = Path.Combine(downloadDirectory, updatedBlobPath.Replace(mediaType, "").Trim('/'));
            Directory.CreateDirectory(localFolderPath);

            foreach (var file in folder.Files)
            {
                string blobName = $"{updatedBlobPath}/{file.Name}";
                string downloadPath = Path.Combine(localFolderPath, file.Name);

                bool downloadSuccess = await DownloadFileAsync(blobClient, blobName, downloadPath);
                if (!downloadSuccess)
                {
                    throw new Exception($"Failed to download file: {blobName}");
                }
            }

            foreach (var subfolder in folder.Subfolders)
            {
                await DownloadFolderAsync(subfolder, blobClient, updatedBlobPath);
            }
        }

        private static string DetermineMediaType(string folderPath)
        {


            var segments = folderPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (segments.Length < 2)
            {
                return null; // Undefined media type
            }

            string mediaType = segments[1]; // Second segment
            return mediaType;
        }

        private static async Task<bool> DownloadFileAsync(BlobContainerClient blobClient, string blobName, string downloadPath)
        {
            BlobClient blob = blobClient.GetBlobClient(blobName);

            if (!await blob.ExistsAsync())
            {
                Console.WriteLine($"Blob {blobName} does not exist. Skipping download.");
                return false;
            }

            try
            {
                Console.WriteLine($"Downloading {blobName} to {downloadPath}...");
                await blob.DownloadToAsync(downloadPath);
                Console.WriteLine($"Downloaded {blobName} successfully.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to download {blobName}: {ex.Message}");
                return false;
            }
        }

        private static async Task SendMessageAsync(ServiceBusSender sender, string messageContent)
        {
            try
            {
                ServiceBusMessage message = new ServiceBusMessage(messageContent);
                await sender.SendMessageAsync(message);
                Console.WriteLine($"Sent message to queue: {sender.EntityPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to send message to queue {sender.EntityPath}: {ex.Message}");
            }
        }
    }
}