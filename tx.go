package mvcc

import "github.com/avinassh/mvcc-go/lockless"

type TxState int

const (
	TxStateActive TxState = iota
	TxStatePreparing
	TxStateCommitted
	TxStateAborted
)

type Tx struct {
	db      *DB
	state   TxState
	beginTs uint64
	endTs   uint64

	writeSet  map[string]*lockless.Node[int]
	insertSet map[string]*lockless.Node[int]
}

func (db *DB) Begin() (*Tx, error) {
	txId := db.nextTS()
	tx := &Tx{db: db, beginTs: txId, state: TxStateActive, writeSet: make(map[string]*lockless.Node[int])}
	db.addTx(txId, tx)
	return tx, nil
}

func (tx *Tx) Read(key string) (int, error) {
	if v, ok := tx.readLocally(key); ok {
		return v, nil
	}

	head, err := tx.db.getRow(key)
	if err != nil {
		return 0, err
	}

	// we got the head pointer. Now we will traverse till end
	// and find a visible row
	r := head.Next
	for r != nil {
		if tx.isVisible(r) {
			// TODO: add this row to read set
			return r.Value, nil
		}
		r = r.Next
	}
	// no visible row found
	return 0, ErrNotFound
}

func (tx *Tx) Update(key string, value int) error {
	if tx.updateLocally(key, value) {
		return nil
	}
	head, err := tx.db.getRow(key)
	if err != nil {
		return err
	}

	// we got the head pointer. Now we will traverse till end
	// and find a visible row
	var visibleRow *lockless.Node[int]
	r := head.Next
	for r != nil {
		if tx.isVisible(r) {
			visibleRow = r
			break
		}
		r = r.Next
	}
	if visibleRow == nil {
		return ErrNotFound
	}
	// row is visible, so we will try to add ourselves to it as a next row
	update := lockless.NewNode[int]()
	update.BeginTs = lockless.Timestamp{Tx: true, Ts: tx.beginTs}
	update.Value = value

	// we will try to add ourselves to the visible row, as to claim it
	// if that fails, then some other tx has already claimed it
	if !visibleRow.Append(update) {
		return ErrRowInUse
	}

	// we claimed the row. Now we need to update the visible row
	visibleRow.EndTs = lockless.Timestamp{Tx: true, Ts: tx.beginTs}
	// also add the old row to our write set
	tx.writeSet[key] = visibleRow
	return nil
}

func (tx *Tx) Insert(key string, value int) error {
	oldRow, err := tx.db.getRow(key)
	if err != nil && err != ErrNotFound {
		return err
	}
	// row already exists, so we can't insert
	if oldRow != nil {
		return ErrAlreadyExists
	}
	// we will add it to our insert set
	// note that, this is not added to head, yet!
	node := lockless.NewNode[int]()
	node.BeginTs = lockless.Timestamp{Tx: true, Ts: tx.beginTs}
	node.EndTs = lockless.Timestamp{Inf: true}
	node.Value = value
	tx.insertSet[key] = node
	return nil
}

func (tx *Tx) Commit() error {
	tx.state = TxStatePreparing
	// prepare and validate
	tx.state = TxStateCommitted
	return nil
}

func (tx *Tx) Rollback() error {
	tx.state = TxStateAborted
	return nil
}

// isVisible answers the most important question. Is the row version visible to this tx?
func (tx *Tx) isVisible(node *lockless.Node[int]) bool {
	// we will split this into three cases.
	// The simplest case when this row is not claimed by any tx
	if !node.BeginTs.Tx && !node.EndTs.Tx {
		// this can't happen
		if node.EndTs.Inf && node.EndTs.Tx {
			panic("a node has both inf and endTx set")
		}
		return node.BeginTs.Ts < tx.beginTs
	}

	// the second case is when the row is claimed by some tx, only endTs is set
	if !node.BeginTs.Tx && node.EndTs.Tx {
		oldTxSate, ok := tx.db.getTxState(node.EndTs.Ts)
		if !ok {
			// possible that this transaction is deleted
			// TODO: fix when gc is added
			panic("transaction not found")
		}
		switch oldTxSate {
		case TxStateActive:
			// row is not visible since transaction is still active
			return false
		case TxStatePreparing:
			// row is visible, but we need to
		}
	}

	return false
}

func (tx *Tx) updateLocally(key string, value int) bool {
	// if the row is already in our write set or insert set, then we will update it
	if r, ok := tx.writeSet[key]; ok {
		r.Value = value
		return true
	}
	if r, ok := tx.insertSet[key]; ok {
		r.Value = value
		return true
	}
	return false
}

func (tx *Tx) readLocally(key string) (int, bool) {
	// it is possible that the tx might want to read the row
	// which it just wrote or inserted
	if r, ok := tx.writeSet[key]; ok {
		return r.Value, true
	}
	if r, ok := tx.insertSet[key]; ok {
		return r.Value, true
	}
	return 0, false
}
