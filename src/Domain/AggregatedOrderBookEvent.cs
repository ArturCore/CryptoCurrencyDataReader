using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    public class AggregatedOrderBookEvent
    {
        public DateTime Timestamp { get; set; }
        public decimal Price { get; set; }
        public decimal BidVolume { get; set; }
        public decimal AskVolume { get; set; }
    }
}
