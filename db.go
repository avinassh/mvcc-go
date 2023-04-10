package mvcc

import (
	"sync"
	"sync/atomic"
)

type row struct {
	beginTs uint64
	endTs   uint64
	tx      bool
	value   int
	next    *row
	*sync.Mutex
}

func newRow() *row {
	return &row{Mutex: &sync.Mutex{}}
}

func (r *row) addNext(update *row) bool {
	r.Lock()
	defer r.Unlock()
	if r.next != nil {
		return false
	}
	r.endTs = update.beginTs
	r.tx = true
	r.next = update
	return true
}

type DB struct {
	rows    map[string]*row
	txMap   map[uint64]*Tx
	counter uint64
	*sync.Mutex
}

func NewDB() *DB {
	return &DB{
		rows:    make(map[string]*row),
		txMap:   make(map[uint64]*Tx),
		counter: 0,
		Mutex:   &sync.Mutex{},
	}
}

func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin()
	if err != nil {
		return err
	}
	if err = fn(t); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

func (db *DB) getRow(rowId string) (*row, error) {
	db.Lock()
	defer db.Unlock()
	r, ok := db.rows[rowId]
	if !ok {
		return nil, ErrNotFound
	}
	return r, nil
}

func (db *DB) insertRow(rowId string) (*row, error) {
	db.Lock()
	defer db.Unlock()
	_, ok := db.rows[rowId]
	if ok {
		return nil, ErrAlreadyExists
	}
	r := newRow()
	db.rows[rowId] = r
	return r, nil
}

func (db *DB) getOrCreateRow(rowId string) (*row, bool) {
	db.Lock()
	defer db.Unlock()
	r, ok := db.rows[rowId]
	if !ok {
		r = newRow()
		db.rows[rowId] = r
	}
	return r, ok
}

func (db *DB) nextTS() uint64 {
	return atomic.AddUint64(&db.counter, 1)
}

func (db *DB) addTx(txId uint64, tx *Tx) {
	db.Lock()
	defer db.Unlock()
	db.txMap[txId] = tx
}
