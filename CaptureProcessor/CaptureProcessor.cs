using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.CaptureProcessor
{
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Avro;
    using Avro.Generic;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage;
    public class CaptureProcessor
    {
        IEventProcessor processor;
        public IEventProcessor Processor { get { return processor; } }

        PartitionContext context;
        public PartitionContext Context { get { return context; } }

        EventHubsDetails eventHubsDetails;
        bool useStartFile = false;
        string startString;
        public CaptureProcessor(IEventProcessor processor, PartitionContext context, EventHubsDetails eventHubsDetails)
        {
            this.context = context;
            this.processor = processor;
            this.eventHubsDetails = eventHubsDetails;
            if(eventHubsDetails.StartingAt != null && eventHubsDetails.StartingAt != DateTime.MinValue)
            {
                useStartFile = true;
                startString = GetStartString(context.PartitionId);
            }
        }
        /*
         *             //if (!string.IsNullOrEmpty(eventHubsDetails.StartingAtFile))
         *             
        public CaptureProcessor(EventProcessorHost host, string eventHubName, string consumerGroup, int partitionId)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;
            var ctor = typeof(PartitionContext).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];
            context = (PartitionContext)ctor.Invoke(new object[] { host, partitionId.ToString(), eventHubName, consumerGroup, token });
        }
        public Task RegisterCaptureProcessor<T>() where T : IEventProcessor
        {
            processor = Activator.CreateInstance(typeof(T)) as IEventProcessor;
            return Task.CompletedTask;
        }
        */
        string CleanString(string part)
        {
            return part.Replace("{", "").Replace("}", "");
        }

        //Default name pattern
        //{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}
        string FormatStorageString(string partitionId)
        {
            string result = "";
            result = eventHubsDetails.CaptureFileNameFormat.Replace("{Namespace}", eventHubsDetails.NamespaceName);
            result = result.Replace("{EventHub}", eventHubsDetails.EventHubName);
            result = result.Replace("{PartitionId}", partitionId);
            result = result.Replace("{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}", "");
            
            return result;
        }

        string GetStartString(string partitionId)
        {
            string result = "";
            result = eventHubsDetails.CaptureFileNameFormat.Replace("{Namespace}", eventHubsDetails.NamespaceName);
            result = result.Replace("{EventHub}", eventHubsDetails.EventHubName);
            result = result.Replace("{PartitionId}", partitionId);
            result = result.Replace("{Year}", eventHubsDetails.StartingAt.Value.Year.ToString());
            result = result.Replace("{Month}", eventHubsDetails.StartingAt.Value.Month.ToString("D2"));
            result = result.Replace("{Day}", eventHubsDetails.StartingAt.Value.Day.ToString("D2"));
            result = result.Replace("{Hour}", eventHubsDetails.StartingAt.Value.Hour.ToString("D2"));
            result = result.Replace("{Minute}", eventHubsDetails.StartingAt.Value.Minute.ToString("D2"));
            result = result.Replace("{Second}", eventHubsDetails.StartingAt.Value.Second.ToString("D2"));
            return result;
        }
        public async Task StartPump()
        {
            //start the pump on storage account or 
            CloudStorageAccount storageAccount;
            if (CloudStorageAccount.TryParse(eventHubsDetails.StorageAccountConnectionString, out storageAccount))
            {
                var cloudBlobClient = storageAccount.CreateCloudBlobClient();
                var cloudBlobContainer = cloudBlobClient.GetContainerReference(eventHubsDetails.CaptureContainer);
                string containerUri = cloudBlobContainer.Uri.ToString() + "/";
                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    BlobRequestOptions bro = new BlobRequestOptions();
                    string format = FormatStorageString(context.PartitionId);
                    var results = cloudBlobContainer.ListBlobsSegmentedAsync(format,
                        true, BlobListingDetails.None, null, blobContinuationToken, bro, null).Result;
                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = results.ContinuationToken;
                    foreach (IListBlobItem item in results.Results)
                    {
                        if(useStartFile)
                        {
                            if (string.Compare(containerUri + startString, item.Uri.ToString()) > 0)
                            {
                                //do something?
                                continue;
                            }
                        }

                        Console.WriteLine(item.Uri);
                        ICloudBlob blob = await cloudBlobClient.GetBlobReferenceFromServerAsync(item.Uri);
                        using (Stream stream = blob.OpenReadAsync(null, bro, null).Result)
                        {
                            await processor.ProcessEventsAsync(context, ReadFile(stream));
                        }

                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.
            }

            /*
            //file local store
            foreach (var file in Directory.EnumerateFiles(filePath, searchPattern))
            {
                //load each file
                using (var inStream = File.OpenRead(file))
                {
                    processor.ProcessEventsAsync(context, ReadFile(inStream));
                }
            }
            */
            //return Task.CompletedTask;
        }

        IEnumerable<EventData> ReadFile(Stream stream)
        {
            var t = typeof(EventData);
            var sysPropType = typeof(Microsoft.Azure.EventHubs.EventData.SystemPropertiesCollection);

            var reader = Avro.File.DataFileReader<GenericRecord>.OpenReader(stream);
            Dictionary<string, List<object>> dictionary = new Dictionary<string, List<object>>();
            while (reader.HasNext())
            {
                EventData result = null;
                Object body;
                var data = reader.Next();
                if (data.TryGetValue("Body", out body))
                {
                    result = new EventData(body as byte[]);
                    t.GetProperty("SystemProperties").SetValue(result, Activator.CreateInstance(sysPropType, true));
                }
                Object userProperties;
                if (data.TryGetValue("Properties", out userProperties))
                {
                    Dictionary<String, Object> properties = userProperties as Dictionary<String, Object>;
                    foreach (var property in properties)
                    {
                        result.Properties.Add(property.Key, property.Value);
                    }
                }
                Object sysProperties;
                if (data.TryGetValue("SystemProperties", out sysProperties))
                {
                    Dictionary<String, Object> properties = sysProperties as Dictionary<String, Object>;
                    foreach (var property in properties)
                    {
                        result.SystemProperties[property.Key] = property.Value;
                    }
                }
                IEnumerator<Avro.Field> enu = data.Schema.GetEnumerator();
                while (enu.MoveNext())
                {
                    if (enu.Current.Name == "Body" || enu.Current.Name == "SystemProperties" || enu.Current.Name == "Properties")
                        continue;
                    //all we should have left are System Properties
                    Object prop;
                    if (data.TryGetValue(enu.Current.Name, out prop))
                    {
                        result.SystemProperties[enu.Current.Name] = prop;
                    }
                }
                yield return result;
            }
        }
    }

}
