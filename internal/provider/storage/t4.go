package storage

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"

	"github.com/emitter-io/emitter/internal/message"
)

// use `tidb ttl table` as storage
type T4 struct {
	db *sql.DB
}

func NewT4() *T4 {
	t4 := &T4{}
	return t4
}

func (t *T4) Name() string {
	return "t4"
}

func (t *T4) Configure(config map[string]interface{}) error {
	db, err := sql.Open("mysql", "local:local@/test")
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *T4) Store(m *message.Message) error {
	_, err := t.db.Exec("INSERT INTO message (mid, mchannel, mpayload, mttl) VALUES (?, ?, ?, ?)", m.ID.Ssid().Encode(), m.Channel, m.Payload, m.TTL)
	return err
}

func (t *T4) Query(ssid message.Ssid, from, until time.Time, limit int) (message.Frame, error) {
	result := make(message.Frame, 0, limit)
	rows, err := t.db.Query("SELECT m1.id, mpayload FROM message m1 JOIN (SELECT id FROM message WHERE mid=? ORDER BY id desc LIMIT ?) m2 ON m1.id=m2.id ORDER BY m1.id", ssid.Encode(), limit)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int
		var payload string
		rows.Scan(&id, &payload)
		result = append(result, message.Message{Payload: []byte(payload)})
	}
	return result, nil
}

func (t *T4) Close() error {
	return t.db.Close()
}
