package statsd

import (
	"bufio"
	"fmt"
	. "github.com/visionmedia/go-debug"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

var debug = Debug("statsd")

const defaultBufSize = 512

// Client is statsd client representing a
// onnection to a statsd server.
type Client struct {
	conn   net.Conn
	buf    *bufio.Writer
	m      sync.Mutex
	prefix string
}

func millisecond(d time.Duration) int {
	return int(d.Seconds() * 1000)
}

// Dial connects to the given address on the
// given network using net.Dial and then returns
// a new Client for the connection.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// NewClient returns a new client with the given writer,
// useful for testing.
func NewClient(w io.Writer) *Client {
	return &Client{
		buf: bufio.NewWriterSize(w, defaultBufSize),
	}
}

// DialTimeout acts like Dial but takes a timeout. The timeout
// includes name resolution, if required.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialSize acts like Dial but takes a packet size.
// By default, the packet size is 512,
// see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialSize(addr string, size int) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, size), nil
}

// new client helper.
func newClient(conn net.Conn, size int) *Client {
	if size <= 0 {
		size = defaultBufSize
	}
	return &Client{
		conn: conn,
		buf:  bufio.NewWriterSize(conn, size),
	}
}

// Prefix adds a prefix to every stat string. The prefix is literal,
// so if you want "foo.bar.baz" from "baz" you should set the prefix
// to "foo.bar." not "foo.bar" as no delimiter is added for you.
func (c *Client) Prefix(s string) {
	c.prefix = s
}

// Increment increments the counter for the given bucket.
func (c *Client) Increment(name string, count int, rate float64) error {
	return c.send(name, rate, "%d|c", count)
}

// Incr increments the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Incr(name string) error {
	return c.Increment(name, 1, 1)
}

// IncrBy increments the counter for the given bucket by N at a rate of 1.
func (c *Client) IncrBy(name string, n int) error {
	return c.Increment(name, n, 1)
}

// Decrement decrements the counter for the given bucket.
func (c *Client) Decrement(name string, count int, rate float64) error {
	return c.Increment(name, -count, rate)
}

// Decr decrements the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Decr(name string) error {
	return c.Increment(name, -1, 1)
}

// DecrBy decrements the counter for the given bucket by N at a rate of 1.
func (c *Client) DecrBy(name string, value int) error {
	return c.Increment(name, -value, 1)
}

// Duration records time spent for the given bucket with time.Duration.
func (c *Client) Duration(name string, duration time.Duration) error {
	return c.send(name, 1, "%d|ms", millisecond(duration))
}

// Histogram is an alias of .Duration() until the statsd protocol figures its shit out.
func (c *Client) Histogram(name string, value int) error {
	return c.send(name, 1, "%d|ms", value)
}

// Gauge records arbitrary values for the given bucket.
func (c *Client) Gauge(name string, value int) error {
	return c.send(name, 1, "%d|g", value)
}

// Annotate sends an annotation.
func (c *Client) Annotate(name string, value string, args ...interface{}) error {
	return c.send(name, 1, "%s|a", fmt.Sprintf(value, args...))
}

// Unique records unique occurences of events.
func (c *Client) Unique(name string, value int, rate float64) error {
	return c.send(name, rate, "%d|s", value)
}

// Flush flushes writes any buffered data to the network.
func (c *Client) Flush() error {
	return c.buf.Flush()
}

// Close closes the connection.
func (c *Client) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	c.buf = nil
	return c.conn.Close()
}

// send stat.
func (c *Client) send(stat string, rate float64, format string, args ...interface{}) error {
	if c.prefix != "" {
		stat = c.prefix + stat
	}

	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%g", format, rate)
		} else {
			return nil
		}
	}

	format = fmt.Sprintf("%s:%s", stat, format)
	debug(format, args...)

	c.m.Lock()
	defer c.m.Unlock()

	// Flush data if we have reach the buffer limit
	if c.buf.Available() < len(format) {
		if err := c.Flush(); err != nil {
			return nil
		}
	}

	// Buffer is not empty, start filling it
	if c.buf.Buffered() > 0 {
		format = fmt.Sprintf("\n%s", format)
	}

	_, err := fmt.Fprintf(c.buf, format, args...)
	return err
}
