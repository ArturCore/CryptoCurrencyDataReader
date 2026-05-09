using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CryptoExchange.Net.Objects.Sockets;
using Binance.Net.Interfaces;

namespace Infrastructure.Common.Binance
{
    public sealed class OrderBookSubscription : IAsyncDisposable
    {
        private readonly Func<Task> _unsubscribe;
        private int _disposed;

        public ChannelReader<DataEvent<IBinanceFuturesEventOrderBook>> Reader { get; }
        internal ChannelWriter<DataEvent<IBinanceFuturesEventOrderBook>> Writer { get; }

        public OrderBookSubscription(
            ChannelReader<DataEvent<IBinanceFuturesEventOrderBook>> reader,
            ChannelWriter<DataEvent<IBinanceFuturesEventOrderBook>> writer,
            Func<Task> unsubscribe)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
            Writer = writer ?? throw new ArgumentNullException(nameof(writer));
            _unsubscribe = unsubscribe ?? throw new ArgumentNullException(nameof(unsubscribe));
        }

        /// <summary>
        /// Idempotent unsubscribe: cancels subscription and completes the channel writer.
        /// </summary>
        public async Task UnsubscribeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

            try
            {
                await _unsubscribe().ConfigureAwait(false);
            }
            catch
            {
                // best-effort unsubscribe; do not throw
            }
            finally
            {
                Writer.TryComplete();
            }
        }

        public ValueTask DisposeAsync() => new ValueTask(UnsubscribeAsync());
    }
}