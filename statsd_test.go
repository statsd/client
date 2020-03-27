package statsd_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/meterup/statsd-client"
)

// var client statsd.Client = &statsd.Client{}

func assert(t *testing.T, value, control string) {
	if value != control {
		t.Errorf("incorrect command, want '%s', got '%s'", control, value)
	}
}

func TestPrefix(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
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
	c := statsd.NewClient(buf)
	err := c.Incr("incr")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr:1|c")
}

func TestDecr(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	err := c.Decr("decr")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "decr:-1|c")
}

func TestDuration(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	err := c.Duration("timing", time.Duration(123456789))
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "timing:123|ms")
}

func TestGauge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	err := c.Gauge("gauge", 300)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "gauge:300|g")
}

func TestAnnotate(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	err := c.Annotate("deploys", "deploying api 1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "deploys:deploying api 1.2.3|a")
}

func TestMultiPacket(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	err := c.Unique("unique", 765, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Unique("unique", 765, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "unique:765|s\nunique:765|s")
}

func TestMultiPacketOverflow(t *testing.T) {
	buf := new(bytes.Buffer)
	c := statsd.NewClient(buf)
	for i := 0; i < 40; i++ {
		err := c.Unique("unique", 765, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert(t, buf.String(), "unique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s")
	buf.Reset()
	c.Flush()
	assert(t, buf.String(), "unique:765|s")
}
