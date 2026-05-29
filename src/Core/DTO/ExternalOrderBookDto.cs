namespace Core.DTO
{
    public class ExternalOrderBookDto
    {
        public required string Symbol { get; set; }
        public IReadOnlyCollection<ExternalOrderBookLevelDto> Bids { get; init; } = [];
        public IReadOnlyCollection<ExternalOrderBookLevelDto> Asks { get; init; } = [];
    }
}
