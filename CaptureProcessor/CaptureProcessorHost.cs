using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.CaptureProcessor
{
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Avro;
    using Avro.Generic;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage;

    /*
     * TODO
     * EventProcessorOptions - specifically max batch size
     * More flexible captureFileNameFormat - this now only supports default
     * Saving which files you have already read or which you should start with
     */
    public class CaptureProcessorHost
    {
        public CaptureProcessorHost(string namespaceName, string eventHubName, int partitionCount,
            string storageAccountConnectionString, string captureContainer, string captureFileNameFormat) : this(
                namespaceName, eventHubName, partitionCount, storageAccountConnectionString, captureContainer, captureFileNameFormat, null)
        {

        }
        public CaptureProcessorHost(string namespaceName, string eventHubName, int partitionCount,
            string storageAccountConnectionString, string captureContainer, string captureFileNameFormat, DateTime? startingAt)
        {
            eventHubsDetails = new EventHubsDetails(namespaceName, eventHubName, partitionCount,
            storageAccountConnectionString, captureContainer, captureFileNameFormat, startingAt);
        }
        EventHubsDetails eventHubsDetails;

        string consumerGroup = "$Capture";

        const string defaultCaptureFileNameFormat = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}";
        List<CaptureProcessor> processors = new List<CaptureProcessor>();
        public Task RegisterCaptureProcessorAsync<T>() where T : IEventProcessor
        {

            //you don't need real credentials, but the format must be real. 
            EventProcessorHost host = new EventProcessorHost(eventHubsDetails.EventHubName, consumerGroup,
                "Endpoint=sb://servicebus.windows.net/;SharedAccessKeyName=NOT_A_REAL_KEY;SharedAccessKey=NOT_A_REAL_KEY=",
                 eventHubsDetails.StorageAccountConnectionString,
                 "lease");
            for(int i=0;i< eventHubsDetails.PartitionCount; i++)
            {
                IEventProcessor processor;
                PartitionContext context;
                processor = Activator.CreateInstance(typeof(T)) as IEventProcessor;
                CancellationTokenSource source = new CancellationTokenSource();
                CancellationToken token = source.Token;

                var ctor = typeof(PartitionContext).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];
                context = (PartitionContext)ctor.Invoke(new object[] { host, i.ToString(), eventHubsDetails.EventHubName, consumerGroup, token });

                CaptureProcessor processorInstance = new CaptureProcessor(processor, context, eventHubsDetails);
                processors.Add(processorInstance);
            }
            processors.ForEach(x => x.StartPump());
            return Task.CompletedTask;
        }

    }
}
