using Binance.Net.Interfaces;
using CryptoExchange.Net.Objects.Sockets;
using Core.Interfaces;
using Domain;
using Infrastructure.Common.Binance;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;
using Core.DTO;
using System.Threading.Channels;

namespace Infrastructure.UpdateSnapshot.Binance
{
    public class BinanceOrderBookUpdates : IOrderBookUpdates
    {
        private readonly IBinanceSocketClientAdapter socketClient;

        public BinanceOrderBookUpdates(
            IBinanceSocketClientAdapter socketClient)
        {
            this.socketClient = socketClient ?? throw new ArgumentNullException(nameof(socketClient));
        }

        /// <summary>
        /// Subscribes to Binance order book updates and returns the first mapped delta as an IResponse.
        /// The method subscribes, reads a single update and then unsubscribes. Use a long‑running consumer
        /// if you need continuous processing (subscribe and iterate subscription.Reader.ReadAllAsync()).
        /// </summary>
        public async Task<IResponse<OrderBookDelta>> GetOrderBookUpdatesAsync(IReadOnlyCollection<string> symbols, int updateInterval, CancellationToken cancellationToken)
        {
            if (symbols == null || symbols.Count == 0)
                return Response<OrderBookDelta>.Failure("No symbols supplied.");

            OrderBookSubscription subscription = null!;
            try
            {
                subscription = await socketClient.SubscribeToOrderBookUpdatesRawAsync(
                    symbols,
                    updateInterval,
                    capacity: 1024,
                    cancellationToken).ConfigureAwait(false);

                // Read a single update (caller can call this method repeatedly or use a dedicated consumer)
                var dataEvent = await subscription.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if (dataEvent?.Data is null)
                    return Response<OrderBookDelta>.Failure("Received empty data event from Binance.");

                var delta = MapToOrderBookDelta(dataEvent);
                return Response<OrderBookDelta>.Success(delta);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return Response<OrderBookDelta>.Failure(ex.Message);
            }
            finally
            {
                if (subscription != null)
                {
                    try
                    {
                        await subscription.UnsubscribeAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // best-effort unsubscribe; ignore
                    }
                }
            }
        }

        /// <summary>
        /// Stream order book updates continuously. Yields IResponse<OrderBookDelta> for each incoming event.
        /// The stream completes when the underlying subscription completes or when cancellation is requested.
        /// Implemented with a Channel to avoid yield-return inside try/finally (CS1626).
        /// </summary>
        public IAsyncEnumerable<IResponse<OrderBookDelta>> StreamOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            CancellationToken cancellationToken)
        {
            if (symbols == null || symbols.Count == 0)
            {
                var failureChannel = Channel.CreateUnbounded<IResponse<OrderBookDelta>>(new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });
                failureChannel.Writer.TryWrite(Response<OrderBookDelta>.Failure("No symbols supplied."));
                failureChannel.Writer.TryComplete();
                return failureChannel.Reader.ReadAllAsync(cancellationToken);
            }

            var channel = Channel.CreateUnbounded<IResponse<OrderBookDelta>>(new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });

            // Produce on a background task. The consumer will iterate the returned IAsyncEnumerable.
            _ = Task.Run(async () =>
            {
                OrderBookSubscription? subscription = null;
                try
                {
                    subscription = await socketClient.SubscribeToOrderBookUpdatesRawAsync(
                        symbols,
                        updateInterval,
                        capacity: 1024,
                        cancellationToken).ConfigureAwait(false);

                    await foreach (var dataEvent in subscription.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (dataEvent?.Data is null)
                        {
                            // Best-effort write; if token cancelled writer may be completed.
                            try { await channel.Writer.WriteAsync(Response<OrderBookDelta>.Failure("Received empty data event from Binance."), cancellationToken).ConfigureAwait(false); } catch { }
                            continue;
                        }

                        try
                        {
                            var delta = MapToOrderBookDelta(dataEvent);
                            try { await channel.Writer.WriteAsync(Response<OrderBookDelta>.Success(delta), cancellationToken).ConfigureAwait(false); } catch { }
                        }
                        catch (Exception ex)
                        {
                            try { await channel.Writer.WriteAsync(Response<OrderBookDelta>.Failure($"Failed to map delta: {ex.Message}"), cancellationToken).ConfigureAwait(false); } catch { }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Subscription cancelled - exit producer and complete channel
                }
                catch (Exception ex)
                {
                    // best-effort report error to consumer
                    try { channel.Writer.TryWrite(Response<OrderBookDelta>.Failure(ex.Message)); } catch { }
                }
                finally
                {
                    if (subscription != null)
                    {
                        try
                        {
                            await subscription.UnsubscribeAsync().ConfigureAwait(false);
                        }
                        catch
                        {
                            // best-effort unsubscribe; ignore
                        }
                    }

                    channel.Writer.TryComplete();
                }
            }, CancellationToken.None);

            return channel.Reader.ReadAllAsync(cancellationToken);
        }

        private static OrderBookDelta MapToOrderBookDelta(DataEvent<IBinanceFuturesEventOrderBook> e)
        {
            var payload = e.Data;

            // Defensive mapping: prefer payload.Symbol, fall back to empty string if unavailable.
            var symbol = payload?.Symbol ?? string.Empty;

            // Map bids/asks; interface items expose Price and Quantity
            var bids = payload?.Bids?
                .Select(b => new OrderBookLevel { Price = b.Price, Volume = b.Quantity })
                .ToList() ?? new List<OrderBookLevel>();

            var asks = payload?.Asks?
                .Select(a => new OrderBookLevel { Price = a.Price, Volume = a.Quantity })
                .ToList() ?? new List<OrderBookLevel>();

            var updateId = 0L;
            try
            {
                // common property name
                updateId = payload?.FirstUpdateId ?? payload?.LastUpdateId ?? 0L;
            }
            catch
            {
                updateId = 0L;
            }

            return new OrderBookDelta
            {
                Symbol = symbol,
                UpdateId = updateId,
                Bids = bids,
                Asks = asks
            };
        }
    }
}
