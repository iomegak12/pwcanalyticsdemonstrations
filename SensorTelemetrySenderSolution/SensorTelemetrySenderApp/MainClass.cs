using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SensorTelemetrySenderApp
{
    public static class MainClass
    {
        private const int MAX_LIMIT = 2000;

        public static void Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("EventHubConnectionString");
            var eventHubPath = Environment.GetEnvironmentVariable("EventHubPath");

            Console.WriteLine("Press [ENTER] to start sending sensor readings to EH ...");
            Console.ReadLine();

            try
            {
                var eventHubConnectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                {
                    EntityPath = eventHubPath
                };
                var eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionStringBuilder.ToString());
                var registeredLocations = new string[] { "Bangalore", "Hyderabad", "Chennai", "Madurai", "Trivandrum" };
                var random = new Random();
                var counter = 1;
                var lastTemperature = random.Next(20, 30);

                Parallel.ForEach<string>(registeredLocations,
                    location =>
                    {
                        while (true)
                        {
                            if (counter >= MAX_LIMIT)
                                break;

                            if (counter % 25 == 0)
                            {
                                Thread.Sleep(random.Next(200, 500));

                                Console.WriteLine();
                            }

                            var plusOrMinus = random.Next(1, 100000000) % 2 == 0;
                            var currentTemperature = lastTemperature + (plusOrMinus ? random.Next(1, 5) : (-random.Next(1, 5)));

                            var telemetryData = new SensorReading
                            {
                                SensorId = random.Next(1, 100),
                                Location = location,
                                SensorHeight = random.Next(150, 190),
                                Plant = string.Format(@"PLANT-{0}", random.Next(1, 10)),
                                RecordedTime = DateTime.Now,
                                Temperature = currentTemperature,
                                Humidity = random.Next(70, 90)
                            };

                            var content = JsonConvert.SerializeObject(telemetryData);
                            var eventData = new EventData(Encoding.ASCII.GetBytes(content));

                            eventHubClient.SendAsync(eventData);

                            counter++;

                            Console.Write("*");
                        }
                    });
            }
            catch (Exception exceptionObject)
            {
                Console.WriteLine("Error Occurred, Details : " + exceptionObject.Message);
            }

            Console.WriteLine("End of App!");
            Console.ReadLine();
        }
    }
}