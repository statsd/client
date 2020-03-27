package statsd

import (
	"testing"
	"time"
)

var millisecondTests = []struct {
	duration time.Duration
	control  int
}{
	{
		duration: 350 * time.Millisecond,
		control:  350,
	},
	{
		duration: 5 * time.Second,
		control:  5000,
	},
	{
		duration: 50 * time.Nanosecond,
		control:  0,
	},
}

func TestMilliseconds(t *testing.T) {
	for i, mt := range millisecondTests {
		value := millisecond(mt.duration)
		if value != mt.control {
			t.Errorf("%d: incorrect value, want %d, got %d", i, mt.control, value)
		}
	}
}
