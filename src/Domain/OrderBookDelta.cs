using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    public class OrderBookDelta
    {
        public string Symbol { get; set; }
        public long UpdateId { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Bids { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Asks { get; set; }
    }
}
