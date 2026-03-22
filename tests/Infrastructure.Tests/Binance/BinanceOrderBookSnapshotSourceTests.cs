using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.Binance;
using Infrastructure.Binance.Interfaces;
using Infrastructure.Binance.Mappers;
using Moq;
using System.Threading;

namespace Infrastructure.Tests;

public class BinanceOrderBookSnapshotSourceTests
{
    [Fact]
    public async System.Threading.Tasks.Task GetSnapshotAsync_ReturnsMappedSnapshotAndSetsSymbol()
    {
        // Arrange
        var symbol = "BTCUSDT";
        var limit = 100;

        var externalDto = new ExternalOrderBookDto
        {
            Bids = new[] { new ExternalOrderBookLevelDto { Price = 1m, Volume = 2m } },
            Asks = new[] { new ExternalOrderBookLevelDto { Price = 3m, Volume = 4m } }
        };

        var mappedSnapshot = new OrderBookSnapshot
        {
            Symbol = "SHOULD_BE_OVERWRITTEN",
            Bids = new[] { new OrderBookLevel { Price = 1m, Volume = 2m } },
            Asks = new[] { new OrderBookLevel { Price = 3m, Volume = 4m } }
        };

        var binanceClientMock = new Mock<IBinanceClient>();
        binanceClientMock
            .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
            .ReturnsAsync(externalDto);

        var mapperMock = new Mock<IBinanceOrderBookSnapshotMapper>();
        mapperMock
            .Setup(m => m.Map(It.IsAny<ExternalOrderBookDto>()))
            .Returns(mappedSnapshot);

        var source = new BinanceOrderBookSnapshotSource(binanceClientMock.Object, mapperMock.Object);

        // Act
        var response = await source.GetSnapshotAsync(symbol, limit, CancellationToken.None);

        // Assert
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Data);
        Assert.Equal(symbol, response.Data.Symbol);
        Assert.Equal(mappedSnapshot.Bids.Count, response.Data.Bids.Count);
        Assert.Equal(mappedSnapshot.Asks.Count, response.Data.Asks.Count);

        binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Moq.Times.Once);
        mapperMock.Verify(m => m.Map(It.IsAny<ExternalOrderBookDto>()), Moq.Times.Once);
    }
}