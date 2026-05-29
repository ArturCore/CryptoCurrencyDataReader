namespace Core.DTO
{
    public class ExternalOrderBookEventDto : ExternalOrderBookDto
    {
        public long? FirstUpdateId { get; set; }
        public long LastUpdateId { get; set; }
    }
}
