using Domain;
using Core.DTO;

namespace Core.Interfaces
{
    public interface IOrderBookMapper
    {
        OrderBookSnapshot MapToSnapshot(ExternalOrderBookDto callResult, string symbol);
    }

}