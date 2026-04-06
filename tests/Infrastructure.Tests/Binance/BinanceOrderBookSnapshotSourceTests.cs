using System;
using System.Threading;
using System.Threading.Tasks;
using Core.DTO;
using Domain;
using Infrastructure.Binance;
using Infrastructure.Binance.Interfaces;
using Moq;
using Xunit;

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
                .Setup(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()))
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
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()), Moq.Times.Once);
        }

        [Fact]
        public async Task GetSnapshotAsync_WhenBinanceClientThrows_ExceptionPropagates()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;
            var expectedException = new HttpRequestException("Network error connecting to Binance API");

            var binanceClientMock = new Mock<IBinanceClient>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ThrowsAsync(expectedException);

            var mapperMock = new Mock<IBinanceOrderBookSnapshotMapper>();

            var source = new BinanceOrderBookSnapshotSource(binanceClientMock.Object, mapperMock.Object);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<HttpRequestException>(
                () => source.GetSnapshotAsync(symbol, limit, CancellationToken.None));

            Assert.Equal(expectedException.Message, exception.Message);
            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Moq.Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()), Moq.Times.Never);
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

            var binanceClientMock = new Mock<IBinanceClient>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ReturnsAsync(externalDto);

            var mapperMock = new Mock<IBinanceOrderBookSnapshotMapper>();
            mapperMock
                .Setup(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()))
                .Throws(mapperException);

            var source = new BinanceOrderBookSnapshotSource(binanceClientMock.Object, mapperMock.Object);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => source.GetSnapshotAsync(symbol, limit, CancellationToken.None));

            Assert.Equal(mapperException.Message, exception.Message);
            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Moq.Times.Once);
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()), Moq.Times.Once);
        }

        [Fact]
        public async Task GetSnapshotAsync_WhenBinanceClientReturnsNull_ExceptionPropagates()
        {
            // Arrange
            var symbol = "BTCUSDT";
            var limit = 100;

            var binanceClientMock = new Mock<IBinanceClient>();
            binanceClientMock
                .Setup(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()))
                .ReturnsAsync((ExternalOrderBookDto)null);

            var mapperMock = new Mock<IBinanceOrderBookSnapshotMapper>();

            var source = new BinanceOrderBookSnapshotSource(binanceClientMock.Object, mapperMock.Object);

            // Act & Assert
            await Assert.ThrowsAsync<NullReferenceException>(
                () => source.GetSnapshotAsync(symbol, limit, CancellationToken.None));

            binanceClientMock.Verify(c => c.GetOrderBookAsync(symbol, limit, It.IsAny<CancellationToken>()), Moq.Times.Once);
            // mapper is invoked with null in current implementation, so expect one call
            mapperMock.Verify(m => m.MapToSnapshot(It.IsAny<ExternalOrderBookDto>()), Moq.Times.Once);
        }
    }
}