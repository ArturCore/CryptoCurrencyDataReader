namespace Core.DTO
{
    public class ExternalOrderBookDto
    {
        public IReadOnlyCollection<ExternalOrderBookLevelDto> Bids { get; init; } = [];
        public IReadOnlyCollection<ExternalOrderBookLevelDto> Asks { get; init; } = [];
    }
}
