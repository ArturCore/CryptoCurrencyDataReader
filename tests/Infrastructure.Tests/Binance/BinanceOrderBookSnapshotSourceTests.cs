using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.SnapshotPublisher.Binance;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;
using Moq;

namespace Infrastructure.Tests
{
    public class BinanceOrderBookSnapshotSourceTests
    {
        [Fact]
        public async Task GetSnapshotAsync_ReturnsMappedSnapshotAndSetsSymbol()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;

            var externalDto = new ExternalOrderBookDto
            {
                Bids = new[] { new ExternalOrderBookLevelDto { Price = 1m, Volume = 2m } },
                Asks = new[] { new ExternalOrderBookLevelDto { Price = 3m, Volume = 4m } }
            };

            var binanceClientMock = new Mock<IBinanceRestClientAdapter>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ReturnsAsync(externalDto);

            var mapperMock = new Mock<IOrderBookMapper>();
            mapperMock
                .Setup(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), It.IsAny<string>()))
                .Returns<ExternalOrderBookDto, string>((ext, s) => new OrderBookSnapshot
                {
                    Symbol = s,
                    Bids = new[] { new OrderBookLevel { Price = 1m, Volume = 2m } },
                    Asks = new[] { new OrderBookLevel { Price = 3m, Volume = 4m } }
                });

            var source = new BinanceOrderBookSnapshot(binanceClientMock.Object, mapperMock.Object);

            // Act
            var response = await source.GetSnapshotAsync(symbol, limit, CancellationToken.None);

            // Assert
            Assert.True(response.IsSuccess);
            Assert.NotNull(response.Data);
            Assert.Equal(symbol, response.Data.Symbol);
            Assert.Equal(1, response.Data.Bids.Count);
            Assert.Equal(1, response.Data.Asks.Count);

            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), symbol), Times.Once);
        }

        [Fact]
        public async Task GetSnapshotAsync_WhenBinanceClientThrows_ExceptionPropagates()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;
            var expectedException = new InvalidOperationException("Network error connecting to Binance API");

            var binanceClientMock = new Mock<IBinanceRestClientAdapter>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ThrowsAsync(expectedException);

            var mapperMock = new Mock<IOrderBookMapper>();

            var source = new BinanceOrderBookSnapshot(binanceClientMock.Object, mapperMock.Object);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => source.GetSnapshotAsync(symbol, limit, CancellationToken.None));

            Assert.Equal(expectedException.Message, exception.Message);
            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), It.IsAny<string>()), Times.Never);
        }

        [Fact]
        public async Task GetSnapshotAsync_WhenMapperThrows_ExceptionPropagates()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;

            var externalDto = new ExternalOrderBookDto
            {
                Bids = new[] { new ExternalOrderBookLevelDto { Price = 1m, Volume = 2m } },
                Asks = new[] { new ExternalOrderBookLevelDto { Price = 3m, Volume = 4m } }
            };

            var mapperException = new InvalidOperationException("Failed to map external DTO to domain model");

            var binanceClientMock = new Mock<IBinanceRestClientAdapter>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ReturnsAsync(externalDto);

            var mapperMock = new Mock<IOrderBookMapper>();
            mapperMock
                .Setup(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), It.IsAny<string>()))
                .Throws(mapperException);

            var source = new BinanceOrderBookSnapshot(binanceClientMock.Object, mapperMock.Object);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => source.GetSnapshotAsync(symbol, limit, CancellationToken.None));

            Assert.Equal(mapperException.Message, exception.Message);
            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), symbol), Times.Once);
        }

        [Fact]
        public async Task GetSnapshotAsync_WhenBinanceClientReturnsNull_ReturnsFailureResponse()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;

            var binanceClientMock = new Mock<IBinanceRestClientAdapter>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ReturnsAsync((ExternalOrderBookDto)null);

            var mapperMock = new Mock<IOrderBookMapper>();

            var source = new BinanceOrderBookSnapshot(binanceClientMock.Object, mapperMock.Object);

            // Act
            var response = await source.GetSnapshotAsync(symbol, limit, CancellationToken.None);

            Assert.False(response.IsSuccess);
            Assert.Contains($"External order book for '{symbol}' is null", response.ErrorMessage ?? string.Empty);

            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>(), It.IsAny<string>()), Times.Never);
        }
    }
}