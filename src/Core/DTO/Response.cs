using Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.DTO
{
    public sealed record Response<T> : IResponse<T>
    {
        public bool IsSuccess { get; init; }
        public T? Data { get; init; }
        public string? ErrorMessage { get; init; }

        public Response(bool isSuccess, T? data, string? errorMessage)
        {
            IsSuccess = isSuccess;
            Data = data;
            ErrorMessage = errorMessage;
        }

        public static Response<T> Success(T data) => new(true, data, null);
        public static Response<T> Failure(string? errorMessage) => new(false, default, errorMessage);
    }
}
