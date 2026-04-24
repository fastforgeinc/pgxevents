package pgxevents

import "testing"

func TestActionIsValid(t *testing.T) {
	cases := []struct {
		a    Action
		want bool
	}{
		{ActionInsert, true},
		{ActionUpdate, true},
		{ActionDelete, true},
		{Action(""), false},
		{Action("INVALID"), false},
		{Action("insert"), false}, // case-sensitive: TG_OP is upper-case
	}
	for _, c := range cases {
		if got := c.a.IsValid(); got != c.want {
			t.Errorf("Action(%q).IsValid() = %v, want %v", c.a, got, c.want)
		}
	}
}
