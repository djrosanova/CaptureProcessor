using System;
using System.Collections.Generic;
using System.Text;

namespace ConsoleAppTest
{
    using Microsoft.Azure.EventHubs.CaptureProcessor;

    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    public class MyProcessor : IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var message in messages)
            {
                Console.WriteLine(System.Text.UTF8Encoding.UTF8.GetString(message.Body.ToArray()));
            }
            return Task.CompletedTask;
        }
    }
}
