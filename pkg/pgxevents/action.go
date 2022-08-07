package pgxevents

type Action string

const (
	Insert Action = "INSERT"
	Update Action = "UPDATE"
	Delete Action = "DELETE"
)

func (a Action) IsValid() bool {
	switch a {
	case Insert, Update, Delete:
		return true
	}
	return false
}
