using Domain;
using Core.DTO;

namespace Core.Interfaces
{
    public interface IOrderBookMapper
    {
        OrderBookSnapshot Map(ExternalOrderBookDto callResult);
    }

}