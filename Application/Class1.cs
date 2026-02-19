namespace Application
{
    /// first feature - connect to exchange & get order book data
    /// so we need connector
    /// and mapper
    /// and aggregator
    /// and db saving


    // source data can be different - different orderbooks, aggregated data from the source etc.
    // adapter layer for diffferent sources? think about it

    // depends on the exchange endpoint (f.e. Binance, Bybit)
    public class IncomingEvent {
        // validation, deduplication & data converting into internal model
    }

    // everything through interfaces
}

// describe data flow
// clarify requirements
// start describe entities (models)
// describe user stories