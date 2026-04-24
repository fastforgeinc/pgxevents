package pgxevents

// ListenTyped subscribes to the given table and returns a TypedSubscription
// whose events carry a *T populated from the row snapshot.
//
// T should be a struct whose field names or json tags match the table's
// columns. Unmarshalling uses encoding/json; malformed rows are logged
// via the configured Logger, counted via the configured Metrics, and
// skipped — the subscription stays alive.
//
// ListenTyped is a top-level function rather than a method on Listener
// because Go does not allow type parameters on interface methods.
func ListenTyped[T any](l Listener, table string) (TypedSubscription[T], error) {
	_ = l
	_ = table
	panic("pgxevents: ListenTyped not implemented — v1 scaffold only")
}
