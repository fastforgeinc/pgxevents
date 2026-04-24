package pgxevents

// Action identifies the kind of mutation that produced an event. Values
// correspond to PostgreSQL's TG_OP trigger variable.
type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

// IsValid reports whether a is one of the defined Action constants.
func (a Action) IsValid() bool {
	switch a {
	case ActionInsert, ActionUpdate, ActionDelete:
		return true
	}
	return false
}
