using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Inbound.Configurations
{
    internal class OrderBookOptions
    {
        public int Limit { get; set; }
        public List<string> Symbols { get; set; } = new List<string>();
    }
}
