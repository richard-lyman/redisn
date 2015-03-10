package redisn

import (
	"fmt"
	"github.com/richard-lyman/redisb"
	"github.com/richard-lyman/redisp"
	"net"
	"strings"
)

type Handler func(string, string, error)

func New(pool redisp.Pooler) *NPool {
	return &NPool{pool, nil}
}

type NPooler interface {
        NDo(string, Handler, ...string) error
        NUnDo(string, ...string) error
}

type NPool struct {
	redisp.Pooler
	c net.Conn
}

func (n *NPool) NDo(command string, handler Handler, keys ...string) error {
	if strings.ToUpper(command) == "SUBSCRIBE" || strings.ToUpper(command) == "PSUBSCRIBE" {
		n.c = n.Get()
		verifySubscription := func(tmp interface{}, err error) error {
			if err != nil {
				return err
			}
			s := tmp.([]interface{})
			if !strings.HasSuffix(strings.ToUpper(s[0].(string)), "SUBSCRIBE") {
				return fmt.Errorf("Failed to subscribe to key: %s", s[1].(string))
			}
			return nil
		}
		s, err := redisb.Do(n.c, append(append([]string{}, command), keys...)...)
		if err := verifySubscription(s, err); err != nil {
			return err
		}
		for _ = range keys[1:] {
			s, err := redisb.Do(n.c)
			if err := verifySubscription(s, err); err != nil {
				return err
			}
		}
		go n.handler(handler)
		return nil
	}
	return fmt.Errorf("The given command '%s' is not supported by NDo. Please use 'SUBSCRIBE' or 'PSUBSCRIBE'.", command)
}

func (n *NPool) handler(handler Handler) {
	for {
		tmp, err := redisb.Do(n.c)
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
				handler("", "", fmt.Errorf("Error converting the remaining subscriptions count from string to int: %s", err))
			} else {
				if remainingSubscriptions == 0 {
                                        fmt.Println("No subscriptions remaining")
                                        break
				}
			}
                        continue
		}
		if msgType != "MESSAGE" {
			handler("", "", fmt.Errorf("Received a non-MESSAGE for key '%s': %s", msga[1].(string), msga[2].(string)))
			break
		}
		handler(msga[1].(string), msga[2].(string), nil)
	}
	n.Put(n.c)
}

func (n *NPool) NUnDo(command string, keys ...string) error {
	if strings.ToUpper(command) == "UNSUBSCRIBE" || strings.ToUpper(command) == "PUNSUBSCRIBE" {
		redisb.Out(n.c, append(append([]string{}, command), keys...)...)
                return nil
	}
	return fmt.Errorf("The given command '%s' is not supported by NUnDo. Please use 'UNSUBSCRIBE' or 'PUNSUBSCRIBE'.", command)
}
