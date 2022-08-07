package pgxevents

import "fmt"

type MaxRetriesError struct {
	maxRetries int
}

func (e MaxRetriesError) Error() string {
	return fmt.Sprintf("reached max retries %d to reconnect: ", e.maxRetries)
}
