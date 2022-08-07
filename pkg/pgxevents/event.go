package pgxevents

import "encoding/json"

type rawEvent struct {
	Table string
	Data  string
}

type Event struct {
	rawEvent
	Action Action
}

func (e *Event) Scan(target interface{}) error {
	if err := json.Unmarshal([]byte(e.rawEvent.Data), target); err != nil {
		return err
	}
	return nil
}
