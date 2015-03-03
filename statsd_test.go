package statsd

import "github.com/statsd/client-interface"

import (
	"bytes"
	"testing"
	"time"
)

var client statsd.Client = &Client{}

func assert(t *testing.T, value, control string) {
	if value != control {
		t.Errorf("incorrect command, want '%s', got '%s'", control, value)
	}
}

func TestPrefix(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	c.Prefix("foo.bar.baz.")
	err := c.Increment("incr", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "foo.bar.baz.incr:1|c")
}

func TestIncr(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	err := c.Incr("incr")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr:1|c")
}

func TestDecr(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	err := c.Decr("decr")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "decr:-1|c")
}

func TestDuration(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	err := c.Duration("timing", time.Duration(123456789))
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "timing:123|ms")
}

func TestGauge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	err := c.Gauge("gauge", 300)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "gauge:300|g")
}

func TestAnnotate(t *testing.T) {
	buf := new(bytes.Buffer)
	c := NewClient(buf)
	err := c.Annotate("deploys", "deploying api 1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "deploys:deploying api 1.2.3|a")
}

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
