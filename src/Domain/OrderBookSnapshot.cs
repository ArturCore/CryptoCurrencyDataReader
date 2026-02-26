using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    public class OrderBookSnapshot
    {
        public string Symbol { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Bids { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Asks { get; set; }
    }
}
