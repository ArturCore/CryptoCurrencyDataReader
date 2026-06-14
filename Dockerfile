FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
WORKDIR /app
COPY . .

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["src/Inbound/Inbound.csproj", "src/Inbound/"]
RUN dotnet restore "src/Inbound/Inbound.csproj"

COPY . .
RUN dotnet publish "src/Inbound/Inbound.csproj" \
    -c Release \
    -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Inbound.dll"]