using System;

namespace ConsoleAppTest
{
    using Microsoft.Azure.EventHubs.CaptureProcessor;
    class Program
    {
        static void Main(string[] args)
        {

            //{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}
            //"danskafkahub/mytopic/0/2018/06/03/11/45/48.avro"
            /*
            var processor = new CaptureProcessorHost("danskafkahub", "mytopic", 2,
              """,
              "tickercapture", "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}");
            processor.RegisterCaptureProcessorAsync<MyProcessor>();
            */


            //Start here
            //"danskafkahub/mytopic/0/2018/06/10/11/45/48.avro"
            
            DateTime startDate = new DateTime(2018, 06, 10, 11, 45, 48);
            var processor = new CaptureProcessorHost("danskafkahub", "mytopic", 2, 
                "",
                "tickercapture", "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}", startDate);

                //"..\\..\\..\\", "*.avro");
            processor.RegisterCaptureProcessorAsync<MyProcessor>();
            
            Console.ReadLine();
        }
    }
}
