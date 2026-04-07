using Core.DTO;
using Core.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Mappers
{
    public class OrderBookMapper : IOrderBookMapper
    {
        public OrderBookSnapshot MapToSnapshot(ExternalOrderBookDto external, string symbol)
        {
            return new OrderBookSnapshot
            {
                Symbol = symbol,
                Bids = external.Bids
                    .Select(b => new OrderBookLevel { Price = b.Price, Volume = b.Volume })
                    .ToList(),
                Asks = external.Asks
                    .Select(a => new OrderBookLevel { Price = a.Price, Volume = a.Volume })
                    .ToList()
            };
        }
    }
}
