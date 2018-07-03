using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace CaptureProcessor
{
    public class MyProcessor : IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            throw new NotImplementedException();
        }

        public Task OpenAsync(PartitionContext context)
        {
            throw new NotImplementedException();
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            throw new NotImplementedException();
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
