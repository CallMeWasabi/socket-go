package core

import "github.com/google/uuid"

type Channel struct {
	QID    uuid.UUID // connect to queue id
	Buffer chan MesssageMeta
	Type   uint8 // 0 = publisher, 1 = consumer
}
