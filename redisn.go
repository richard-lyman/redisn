/*
Package redisn provides generic methods to handle redis pubsub calls and wraps redisp with the same.

NDo accepts a net.conn as well as SUBSCRIBE and PSUBSCRIBE commands with an associated list of one or more keys.

NUnDo accepts a net.Conn as well as UNSUBSCRIBE and PUNSUBSCRIBE commands with an associated list of one or more keys.

An example of using this package is as follows:

        package main

        import (
                "fmt"
                "github.com/richard-lyman/redisp"
                "github.com/richard-lyman/redisn"
                "net"
                "time"
        )

        func main() {
                creator := func() net.Conn {
                        c, err := net.Dial("tcp", "localhost:6379")
                        if err != nil {
                                panic(err)
                        }
                        return c
                }
                sizeOfPool := 10
                retryDuration := 300 * time.Millisecond
                n := redisn.New(redisp.New(sizeOfPool, creator, retryDuration))
                h := func(k string, msg string, e error) { fmt.Println("Received:", k,msg, e) }
                err := n.NDo("SUBSCRIBE", h, "something")
                if err != nil {
                        panic("Failed to subscribe")
                }
                time.Sleep(10 * time.Second) // Run some command like: redis-cli publish something else
                err = n.NUnDo("UNSUBSCRIBE", "something")
                if err != nil {
                        fmt.Println("Failed to unsubscribe")
                }
        }

*/
package redisn

import (
	"fmt"
	"github.com/richard-lyman/redisb"
	"github.com/richard-lyman/redisp"
	"net"
	"strings"
)

// Handler defines the function type required for handling notification messages or failures
type Handler func(string, string, error)

// NDo function accepts a net.Conn as well as SUBSCRIBE and PSUBSCRIBE commands along with the associated keys and a handler to be called when there are messages or errors
func NDo(c net.Conn, command string, handler Handler, keys ...string) error {
	if strings.ToUpper(command) == "SUBSCRIBE" || strings.ToUpper(command) == "PSUBSCRIBE" {
		verifySubscription := func(tmp interface{}, err error) error {
			if err != nil {
				return err
			}
			s := tmp.([]interface{})
			if !strings.HasSuffix(strings.ToUpper(s[0].(string)), "SUBSCRIBE") {
				return fmt.Errorf("failed to subscribe to key: %s", s[1].(string))
			}
			return nil
		}
		s, err := redisb.Do(c, append(append([]string{}, command), keys...)...)
		if err := verifySubscription(s, err); err != nil {
			return err
		}
		for _ = range keys[1:] {
			s, err := redisb.Do(c)
			if err := verifySubscription(s, err); err != nil {
				return err
			}
		}
		go handlerWrapper(c, handler)
		return nil
	}
	return fmt.Errorf("the given command '%s' is not supported by NDo. Please use 'SUBSCRIBE' or 'PSUBSCRIBE'", command)
}

// NUnDo function accepts a net.Conn as well as UNSUBSCRIBE and PUNSUBSCRIBE commands along with the associated keys
func NUnDo(c net.Conn, command string, keys ...string) error {
	if strings.ToUpper(command) == "UNSUBSCRIBE" || strings.ToUpper(command) == "PUNSUBSCRIBE" {
		redisb.Out(c, append(append([]string{}, command), keys...)...)
		return nil
	}
	return fmt.Errorf("the given command '%s' is not supported by NUnDo. Please use 'UNSUBSCRIBE' or 'PUNSUBSCRIBE'", command)
}

// New wraps a redisp.Pool with the NDo and NUnDo functionality
func New(pool *redisp.Pool) *NPool {
	return &NPool{pool, nil}
}

// NPool is the redisp.Pool wrapper
type NPool struct {
	*redisp.Pool
	c net.Conn
}

// NDo function will use a net.Conn from the pool and accepts SUBSCRIBE and PSUBSCRIBE commands along with the associated keys and a handler to be called when there are messages or errors
func (n *NPool) NDo(command string, handler Handler, keys ...string) error {
	if n.c == nil {
		n.c = n.Get()
	}
        return NDo(n.c, command, handler, keys...)
}

// NUnDo function will use a net.Conn from the pool and accepts UNSUBSCRIBE and PUNSUBSCRIBE commands along with the associated keys
func (n *NPool) NUnDo(command string, keys ...string) error {
	if n.c == nil {
		n.c = n.Get()
	}
        return NUnDo(n.c, command, keys...)
}

func handlerWrapper(c net.Conn, handler Handler) {
	for {
		tmp, err := redisb.Do(c)
		if err != nil {
			handler("", "", err)
			break
		}
		msga := tmp.([]interface{})
		msgType := strings.ToUpper(msga[0].(string))
		if strings.HasPrefix(msgType, "UN") || strings.HasPrefix(msgType, "PUN") {
			fmt.Println("Unsubscribing")
			remainingSubscriptions := msga[2].(int64)
			if err != nil {
				handler("", "", fmt.Errorf("error converting the remaining subscriptions count from string to int: %s", err))
			} else {
				if remainingSubscriptions == 0 {
					fmt.Println("No subscriptions remaining")
					break
				}
			}
			continue
		}
		if msgType == "MESSAGE" {
			handler(msga[1].(string), msga[2].(string), nil)
		} else if msgType == "PMESSAGE" {
			handler(msga[2].(string), msga[3].(string), nil)
		} else {
			handler("", "", fmt.Errorf("received a non-MESSAGE for key '%s': %s", msga[1].(string), msga[2].(string)))
			break
		}
	}
}

func (n *NPool) handlerWrapper(handler Handler) {
        handlerWrapper(n.c, handler)
	n.Put(n.c)
}
