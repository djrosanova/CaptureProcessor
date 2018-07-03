using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
//using Newtonsoft.Json;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.WindowsAzure.Storage;

namespace CaptureProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            var processor = new CaptureProcessorHost("..\\..\\", "*.avro");
            processor.RegisterCaptureProcessorAsync<MyProcessor>();
            Console.ReadLine();
        }

    }


}
