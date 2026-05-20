using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Configurations
{
    public class OrderBookOptions
    {
        public int Limit { get; set; }
        public List<string> Symbols { get; set; } = new List<string>();
    }
}
