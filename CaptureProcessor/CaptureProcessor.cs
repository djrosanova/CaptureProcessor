using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CaptureProcessor
{
    using Avro;
    using Avro.Generic;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;

    public class CaptureProcessorHost
    {
        public CaptureProcessorHost(string capturePath, string pattern)
        {
            filePath = capturePath;
            searchPattern = pattern;
        }

        string filePath;
        string searchPattern;

        IEventProcessor processor;

        public Task RegisterCaptureProcessorAsync<T>() where T : IEventProcessor
        {
            //add this some day
            //EventProcessorOptions opts = new EventProcessorOptions();

            processor = Activator.CreateInstance(typeof(T)) as IEventProcessor;
            //you don't need real credentials, but the format must be real. 
            EventProcessorHost host = new EventProcessorHost("path", "group", "Endpoint=sb://servicebus.windows.net/;SharedAccessKeyName=NoKey;SharedAccessKey=nokey=", "DefaultEndpointsProtocol=https;AccountName=fakeaccount;AccountKey=NotARealKey==;EndpointSuffix=core.windows.net", "lease");
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            var ctor = typeof(PartitionContext).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];
            var context = (PartitionContext)ctor.Invoke(new object[] { host, "0", "path", "group", token });

            //enumerate files with prefix
            foreach (var file in Directory.EnumerateFiles(filePath, searchPattern))
            {
                //load each file
                processor.ProcessEventsAsync(context, ReadFile(file));
            }

            return Task.CompletedTask;
        }

        IEnumerable<EventData> ReadFile(string path)
        {
            using (var stream = File.OpenRead(path))
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
                    IEnumerator<Field> enu = data.Schema.GetEnumerator();
                    while (enu.MoveNext())
                    {
                        //every EventData will have a "Body" there are also properties
                        if (enu.Current.Name == "Body")
                            continue;
                        Object prop;
                        if (data.TryGetValue(enu.Current.Name, out prop))
                        {
                            //There are only three system properties in a Capture file
                            if (enu.Current.Name == "SequenceNumber" || enu.Current.Name == "Offset" || enu.Current.Name == "EnqueuedTimeUtc")
                                result.SystemProperties[enu.Current.Name] = prop;
                            else
                                result.Properties[enu.Current.Name] = prop;
                        }
                    }
                    yield return result;
                }
            }

        }
    }

}
