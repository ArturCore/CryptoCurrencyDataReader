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
        public OrderBookSnapshot MapToSnapshot(ExternalOrderBookDto external)
        {
            return new OrderBookSnapshot
            {
                Symbol = external.Symbol,
                Bids = external.Bids
                    .Select(b => new OrderBookLevel { Price = b.Price, Volume = b.Volume })
                    .ToList(),
                Asks = external.Asks
                    .Select(a => new OrderBookLevel { Price = a.Price, Volume = a.Volume })
                    .ToList()
            };
        }

        public OrderBookDelta MapToDelta(ExternalOrderBookEventDto external)
        {
            return new OrderBookDelta
            {
                Symbol = external.Symbol,
                FirstUpdateId = external.FirstUpdateId,
                LastUpdateId = external.LastUpdateId,
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
