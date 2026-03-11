namespace Core.Interfaces
{
    public interface IResponse<T>
    {
        bool IsSuccess { get; }
        T? Response { get; }
        string? ErrorMessage { get; }
    }
}
