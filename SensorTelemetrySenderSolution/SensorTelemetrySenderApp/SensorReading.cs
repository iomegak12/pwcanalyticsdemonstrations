using System;
using System.Collections.Generic;
using System.Text;

namespace SensorTelemetrySenderApp
{
    public class SensorReading
    {
        public int SensorId { get; set; }
        public int SensorHeight { get; set; }
        public string Location { get; set; }
        public string Plant { get; set; }
        public int Temperature { get; set; }
        public int Humidity { get; set; }
        public DateTime RecordedTime { get; set; }
    }
}
