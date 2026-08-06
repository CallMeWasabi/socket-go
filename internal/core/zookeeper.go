package core

import (
	"net"
)

// TODO: support safe multithread
type Zookeeper struct {
	Topics map[string][]*net.Conn
}

func (z *Zookeeper) AddTopic(title string) {
	if _, ok := z.Topics[title]; ok {
		return
	}

	z.Topics[title] = make([]*net.Conn, 0, 1)
}

func (z *Zookeeper) RemoveTopic(title string) {
	if _, ok := z.Topics[title]; !ok {
		return
	}

	delete(z.Topics, title)
}
