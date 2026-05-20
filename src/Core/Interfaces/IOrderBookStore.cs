using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Interfaces
{
    public interface IOrderBookStore
    {
        /// <summary>
        /// Return existing OrderBook for symbol or create a new one (not initialized by snapshot).
        /// </summary>
        OrderBook? GetRawOrderBook(string symbol);

        /// <summary>
        /// Atomically create (if missing) and apply the initial snapshot. Returns true if applied by this call,
        /// false if an existing book already had a snapshot applied.
        /// </summary>
        Task<bool> CreateWithSnapshotAsync(OrderBookSnapshot snapshot, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to apply a delta to an existing book. Returns false if book does not exist.
        /// </summary>
        Task<bool> TryApplyDeltaAsync(OrderBookDelta delta, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to get current snapshot for a symbol. Returns null if book not found.
        /// </summary>
        OrderBookSnapshot? TryGetSnapshot(string symbol);
    }
}
