namespace Core.Interfaces
{
    public interface IResponse<T>
    {
        bool IsSuccess { get; }
        T? Data { get; }
        string? ErrorMessage { get; }
    }
}
