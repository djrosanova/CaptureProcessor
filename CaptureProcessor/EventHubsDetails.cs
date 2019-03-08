using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.EventHubs.CaptureProcessor
{
    public class EventHubsDetails
    {
        public EventHubsDetails(string namespaceName, string eventHubName, int partitionCount,
            string storageAccountConnectionString, string captureContainer, string captureFileNameFormat, DateTime? startingAt)
        {
            EventHubName = eventHubName;
            NamespaceName = namespaceName;
            PartitionCount = partitionCount;
            StorageAccountConnectionString = storageAccountConnectionString;
            CaptureContainer = captureContainer;
            CaptureFileNameFormat = captureFileNameFormat;
            StartingAt = startingAt;
        }

        public string NamespaceName { get; }
        public string EventHubName { get; }
        public int PartitionCount { get; }
        public string StorageAccountConnectionString { get; }
        public string CaptureContainer { get; }
        public string CaptureFileNameFormat { get; }
        public DateTime? StartingAt { get; }
    }
}
