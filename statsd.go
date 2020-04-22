package statsd

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

const defaultBufSize = 512

// Client is a statsd client representing a connection to a statsd server.
type Client struct {
	conn   net.Conn
	buf    *bufio.Writer
	m      sync.Mutex
	prefix string
	tags   string
}

func millisecond(d time.Duration) int {
	return int(d.Seconds() * 1000)
}

// Dial connects to the given address on the given network using net.Dial over
// UDP and then returns a new Client for the connection.
func Dial(addr string) (*Client, error) {
	return DialSize(addr, 0)
}

// DialTCP connects to the given address on the given network using net.Dial
// over TCP and then returns a new Client for the connection.
func DialTCP(addr string) (*Client, error) {
	return DialTCPSize(addr, 0)
}

// NewClient returns a new client with the given writer,
// useful for testing.
func NewClient(w io.Writer) *Client {
	return &Client{
		buf: bufio.NewWriterSize(w, defaultBufSize),
	}
}

// DialTimeout acts like Dial but takes a timeout over UDP. The timeout
// includes name resolution, if required.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialTCPTimeout acts like Dial but takes a timeout over TCP. The timeout
// includes name resolution, if required.
func DialTCPTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialSize acts like Dial but takes a packet size over UDP.
// By default, the packet size is 512,
// see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialSize(addr string, size int) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, size), nil
}

// DialTCPSize acts like Dial but takes a packet size over TCP.
// By default, the packet size is 512,
// see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialTCPSize(addr string, size int) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
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

// AddTag adds a "key:value" tag to every stat string. See
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-custom-metrics-statsd.html
// for more information on tags.
func (c *Client) AddTag(key, value string) {
	c.m.Lock()
	defer c.m.Unlock()
	if len(c.tags) > 0 {
		c.tags = c.tags + ","
	}
	c.tags = c.tags + key + ":" + value
}

// Increment increments the counter for the given bucket.
func (c *Client) Increment(name string, count int64, rate float64) error {
	return c.send(name, nil, rate, "%d|c", count)
}

// Incr increments the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Incr(name string) error {
	return c.Increment(name, 1, 1)
}

// IncrBy increments the counter for the given bucket by N at a rate of 1.
func (c *Client) IncrBy(name string, n int64) error {
	return c.Increment(name, n, 1)
}

// Decrement decrements the counter for the given bucket.
func (c *Client) Decrement(name string, count int64, rate float64) error {
	return c.Increment(name, -count, rate)
}

// Decr decrements the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Decr(name string) error {
	return c.Increment(name, -1, 1)
}

// DecrBy decrements the counter for the given bucket by N at a rate of 1.
func (c *Client) DecrBy(name string, value int64) error {
	return c.Increment(name, -value, 1)
}

// Duration records time spent for the given bucket with time.Duration.
func (c *Client) Duration(name string, duration time.Duration) error {
	return c.send(name, nil, 1, "%d|ms", millisecond(duration))
}

// Histogram is an alias of .Duration() until the statsd protocol figures its shit out.
func (c *Client) Histogram(name string, value int64) error {
	return c.send(name, nil, 1, "%d|ms", value)
}

// Gauge records arbitrary values for the given bucket.
func (c *Client) Gauge(name string, value int64, tags ...[2]string) error {
	return c.send(name, tags, 1, "%d|g", value)
}

// Annotate sends an annotation.
func (c *Client) Annotate(name string, value string, args ...interface{}) error {
	return c.send(name, nil, 1, "%s|a", fmt.Sprintf(value, args...))
}

// Unique records unique occurrences of events.
func (c *Client) Unique(name string, value int64, rate float64) error {
	return c.send(name, nil, rate, "%d|s", value)
}

// Flush flushes any buffered data to the network.
func (c *Client) Flush() error {
	c.m.Lock()
	defer c.m.Unlock()
	return c.flush()
}

// flush() flushes data to the network. The caller is expected to hold c.m.
func (c *Client) flush() error {
	return c.buf.Flush()
}

// Close closes the connection.
func (c *Client) Close() error {
	c.m.Lock()
	defer c.m.Unlock()
	if err := c.flush(); err != nil {
		return err
	}
	c.buf = nil
	return c.conn.Close()
}

// send stat.
func (c *Client) send(stat string, statTags [][2]string, rate float64, format string, args ...interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()

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

	format = stat + ":" + format
	tagString := ""
	if c.tags != "" {
		tagString += "|#" + c.tags
	}
	// per-stat tags
	if len(statTags) > 0 {
		if tagString == "" {
			tagString += "|#"
		} else {
			tagString += ","
		}
		for i := range statTags {
			tagString += statTags[i][0] + ":" + statTags[i][1]
			if i < len(statTags)-1 {
				tagString += ","
			}
		}
	}
	format = format + tagString

	// Flush data if we have reach the buffer limit
	if c.buf.Available() < len(format) {
		if err := c.flush(); err != nil {
			return nil
		}
	}

	// Buffer is not empty, start filling it
	if c.buf.Buffered() > 0 {
		format = "\n" + format
	}

	_, err := fmt.Fprintf(c.buf, format, args...)
	return err
}
