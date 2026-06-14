Feature => risks => unit tests => integration tests => smoke\contracts tests


Feature 1. Get base snapshot. Save snapshot in local object

Risks:
	Configuration
		No env vars
		Incorrect env vars
	API
		Empty answer
		Timeout exception
		Incorrect answer
		Answer not from library object
	Answer handling
		Incorrect mapping to local DTO
	Saving to local object
		State invariants of OrderBook
		Threads conflict
		Aplly blocking object (no async work)
	Worker
		Resync. If we catch exception - would we repeat request?

Unit tests:
	Env vars existing
	Env vars validation test
	Env vars doesn't exist. Check Log.error. Check project stops

	Mock API:
		empty answer. Create empty order book object
		timeout exception. Log error.
		Incorrect answer. Logs (error) should be created.

	Success maping. Check if all the key-value pairs are mapped successfully.
	Incorrect mapping. Log error. Ready for next request

	OrderBook invariants:
		Bids & Asks ordered the right way
		No duplicates in levels bids & asks
		empty orderbook can exist
		2 threads changes the same object. No errors.
		2 currency pairs changes OrderBook. No errors.

	IClock?
	call function every X time.
	In case of exception \ timeout - repeat request.

Integration tests:
	Call method get snapshot => call API (mock) => map answer => save to locar order book (check chenges in order book)
	Call method get snapshot => API returns exception => Log error => Open for next request (current order book didn't change)
	Call method get snapshot => API timeout => Log error (current order book didn't change)
	Call method get snapshot => API ok => map error => Log error (current order book didn't change)
	Call method get snapshot => API ok => save to local order book error => Log error (current order book didn't change)

No smoke \ contracts tests.


Questions:
Which errors should be fatal? After fatal error - should we stop the program?
Can inconsistent input come to creating OrderBook?
How many times should we repeat requests for external API? Till when?
Recreating order book object according to Binance documentaion. Is it OK? How others handle this? How their order books stable?
