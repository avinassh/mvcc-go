package mvcc

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
}

func (db *DB) Begin() (*Tx, error) {
	txId := db.nextTS()
	tx := &Tx{db: db, beginTs: txId, state: TxStateActive}
	db.addTx(txId, tx)
	return tx, nil
}

func (tx *Tx) Read(key string) (int, error) {
	r, err := tx.db.getRow(key)
	if err != nil {
		return 0, err
	}
	for r.next != nil {
		if tx.isVisible(r) {
			// TODO: add this row to read set
			return r.value, nil
		}
		r = r.next
	}
	// no visible row found
	return 0, ErrNotFound
}

func (tx *Tx) Update(key string, value int) error {
	r, err := tx.db.getRow(key)
	if err != nil {
		return err
	}
	var visibleRow *row
	for r.next != nil {
		if tx.isVisible(r) {
			visibleRow = r
			break
		}
		r = r.next
	}
	if visibleRow == nil {
		return ErrNotFound
	}
	// row is visible, so we will try to add ourselves to it as a next row
	update := newRow()
	update.beginTs = tx.beginTs
	update.value = value
	update.tx = true
	// TODO: add to update set
	if !visibleRow.addNext(update) {
		return ErrAlreadyExists
	}
	return nil
}

func (tx *Tx) Insert(key string, value int) error {
	r, err := tx.db.insertRow(key)
	if err != nil {
		return err
	}
	// TODO: add to update set
	_ = r
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

func (tx *Tx) isVisible(row *row) bool {
	return row.beginTs <= tx.beginTs && row.endTs >= tx.beginTs
}
