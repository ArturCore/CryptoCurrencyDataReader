using System.Threading.Channels;
using Binance.Net.Clients;
using Binance.Net.Interfaces;
using CryptoExchange.Net.Objects.Sockets;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Common.Binance
{
    public class BinanceSocketClientAdapter : IBinanceSocketClientAdapter
    {
        private readonly BinanceSocketClient _client;
        private readonly ILogger<BinanceSocketClientAdapter> _logger;

        public BinanceSocketClientAdapter(BinanceSocketClient client, ILogger<BinanceSocketClientAdapter> logger)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<OrderBookSubscription> SubscribeToOrderBookUpdatesRawAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            int capacity = 1024,
            CancellationToken cancellationToken = default)
        {
            if (symbols == null) throw new ArgumentNullException(nameof(symbols));
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (updateInterval <= 0) throw new ArgumentOutOfRangeException(nameof(updateInterval));

            _logger.LogInformation("Starting subscription request for {SymbolCount} symbol(s) with updateInterval={UpdateInterval}ms.",
                symbols.Count, updateInterval);

            var channel = Channel.CreateBounded<DataEvent<IBinanceEventOrderBook>>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropOldest
            });

            var internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            internalCts.Token.Register(() =>
            {
                _logger.LogInformation("Cancellation requested for order book subscription for symbols: {Symbols}. Completing channel.", string.Join(",", symbols));
                channel.Writer.TryComplete();
            });

            object subscriptionHandle = null;
            try
            {
                var subscribeResult = await _client.SpotApi.ExchangeData
                    .SubscribeToOrderBookUpdatesAsync(
                        symbols, 
                        updateInterval,
                        (update) =>
                        {
                            try
                            {
                                var written = channel.Writer.TryWrite(update);
                                if (!written)
                                {
                                    _logger.LogDebug("Channel full; dropped an order book update for symbols: {Symbols}.", string.Join(",", symbols));
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Exception in Binance subscription callback for symbols: {Symbols}.", string.Join(",", symbols));
                            }
                        },
                        internalCts.Token)
                    .ConfigureAwait(false);

                // attempt to extract SDK subscription handle if returned inside a wrapper
                try
                {
                    dynamic d = subscribeResult;
                    subscriptionHandle = d?.Data ?? d;
                }
                catch
                {
                    subscriptionHandle = subscribeResult;
                }

                _logger.LogInformation("SubscribeToOrderBookUpdatesAsync established for symbols: {Symbols}.", string.Join(",", symbols));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SubscribeToOrderBookUpdatesAsync failed for symbols: {Symbols}. Completing channel with error.", string.Join(",", symbols));
                channel.Writer.TryComplete(ex);
                throw;
            }

            async Task UnsubscribeImpl()
            {
                try
                {
                    internalCts.Cancel();

                    if (subscriptionHandle is not null)
                    {
                        try
                        {
                            dynamic s = subscriptionHandle;
                            if (HasMember(s, "UnsubscribeAsync"))
                            {
                                await s.UnsubscribeAsync().ConfigureAwait(false);
                            }
                            else if (HasMember(s, "Unsubscribe"))
                            {
                                s.Unsubscribe();
                            }
                            else if (s is IDisposable disposable)
                            {
                                disposable.Dispose();
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error while attempting to unsubscribe SDK subscription for symbols: {Symbols}.", string.Join(",", symbols));
                        }
                    }
                }
                finally
                {
                    channel.Writer.TryComplete();
                }
            }

            return new OrderBookSubscription(channel.Reader, channel.Writer, UnsubscribeImpl);
        }

        private static bool HasMember(dynamic obj, string memberName)
        {
            try
            {
                return obj.GetType().GetMethod(memberName) != null;
            }
            catch
            {
                return false;
            }
        }
    }
}
