using System.Collections.Concurrent;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;

namespace Infrastructure
{
    public class OrderBookStore : IOrderBookStore
    {
        private readonly ConcurrentDictionary<string, BookHolder> _books = new(StringComparer.OrdinalIgnoreCase);
        private readonly ILogger<OrderBookStore> _logger;

        public OrderBookStore(ILogger<OrderBookStore> logger)
        {
            _logger = logger;
        }

        public OrderBook? GetRawOrderBook(string symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol)) throw new ArgumentException("symbol", nameof(symbol));
            if(_books.TryGetValue(symbol, out BookHolder? bookHolder))
            {
                return bookHolder.Book;
            }
            return null;
        }

        public async Task<bool> CreateWithSnapshotAsync(OrderBookSnapshot snapshot, CancellationToken cancellationToken = default)
        {
            if (snapshot is null) throw new ArgumentNullException(nameof(snapshot));
            var symbol = snapshot.Symbol ?? throw new ArgumentException("Snapshot must include symbol.", nameof(snapshot));

            var holder = _books.GetOrAdd(symbol, s => new BookHolder(new OrderBook(s)));

            // Ensure only one initializer applies the snapshot
            await holder.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (holder.Initialized)
                {
                    _logger?.LogDebug("Snapshot for {Symbol} was already applied.", symbol);
                    return false;
                }

                holder.Book.ApplySnapshot(snapshot);
                holder.Initialized = true;
                _logger?.LogInformation("Applied initial snapshot for {Symbol}.", symbol);
                return true;
            }
            finally
            {
                holder.Semaphore.Release();
            }
        }

        public async Task<bool> TryApplyDeltaAsync(OrderBookDelta delta, CancellationToken cancellationToken = default)
        {
            if (delta is null) throw new ArgumentNullException(nameof(delta));
            var symbol = delta.Symbol ?? throw new ArgumentException("Delta must include symbol.", nameof(delta));

            if (!_books.TryGetValue(symbol, out var holder))
            {
                _logger?.LogWarning("Received delta for unknown symbol {Symbol}.", symbol);
                return false;
            }

            await holder.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                holder.Book.ApplyDelta(delta);
                return true;
            }
            finally
            {
                holder.Semaphore.Release();
            }
        }

        public OrderBookSnapshot? TryGetSnapshot(string symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol)) return null;
            if (!_books.TryGetValue(symbol, out var holder)) return null;

            // No need to lock for producing a snapshot; ToSnapshot reads internal dictionaries but
            // to be safe take the semaphore briefly to avoid concurrent mutation visibility issues.
            holder.Semaphore.Wait();
            try
            {
                return holder.Book.ToSnapshot();
            }
            finally
            {
                holder.Semaphore.Release();
            }
        }

        private class BookHolder
        {
            public OrderBook Book { get; }
            public SemaphoreSlim Semaphore { get; } = new SemaphoreSlim(1, 1);
            public bool Initialized { get; set; }

            public BookHolder(OrderBook book)
            {
                Book = book ?? throw new ArgumentNullException(nameof(book));
            }
        }
    }
}