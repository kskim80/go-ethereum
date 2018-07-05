package ethdb

import (
	"database/sql"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	_ "github.com/lib/pq" // what
)

var schema = `
CREATE TABLE kvstore (
	k bytea,
	v bytea,
	primary key(k)
);
`

const (
	dbuser = "ethereum"
	dbname = "ethereum"
)

// PGDatabase is datastructure for database
type PGDatabase struct {
	db *sql.DB
}

// NewPGDatabase Create new PG database
func NewPGDatabase() (*PGDatabase, error) {
	dbinfo := fmt.Sprintf("user=%s dbname=%s sslmode=disable", dbuser, dbname)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Warn("Connect error")
		return nil, err
	}
	_, err = db.Exec("DROP TABLE kvstore")
	_, err = db.Exec(schema)
	if err != nil {
		log.Warn("Cannot make schema")
		return nil, err
	}
	return &PGDatabase{
		db: db,
	}, nil
}

// Put key/value
func (db *PGDatabase) Put(key []byte, value []byte) error {
	_, err := db.db.Exec("INSERT INTO kvstore VALUES($1, $2) ON CONFLICT (k) DO UPDATE SET v=$2", key, value)
	if err != nil {
		log.Error("Error: INSERT", "key", key, "value", value, "err", err)
		return err
	}
	return nil
}

// PutTransaction puts tx data
func (db *PGDatabase) PutTransaction(blockHash []byte, blockNum uint64, i uint64,
	txNonce uint64, txPrice int64, txGaslimit uint64, txRecipient []byte, txAmount int64, txPayload []byte,
	txV int64, txR int64, txS int64) error {
	_, err := db.db.Exec("INSERT INTO transaction VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ",
		"ON CONFLICT ON CONSTRAINT transaction_pkey DO NOTHING",
		blockHash, blockNum, i, txNonce, txPrice, txGaslimit, txRecipient, txAmount, txPayload, txV, txR, txS)
	if err != nil {
		log.Error("Error: INSERT")
		return err
	}

	return nil
}

//  create table transaction(blockhash bytea, blocknum bigint, idx bigint, nonce bigint, price bigint, gaslimit bigint, recipient bytea, amount bigint, payload bytea, sig_V bigint, sig_R bigint, sig_S bigint, primary key(blockhash, blocknum, idx));

// Get key/value
func (db *PGDatabase) Get(key []byte) ([]byte, error) {
	var value string
	err := db.db.QueryRow("SELECT v FROM kvstore WHERE k = $1", key).Scan(&value)
	switch {
	case err == sql.ErrNoRows:
		return nil, err
	case err != nil:
		log.Warn("Error: Get", "err", err)
		return nil, err
	}
	return []byte(value), nil
}

// Delete deletes
func (db *PGDatabase) Delete(key []byte) error {
	_, err := db.db.Exec("DELETE FROM kvstore WHERE k = $1", key)
	if err != nil {
		log.Warn("Error: DELETE")
		return err
	}
	return nil
}

// Has checks
func (db *PGDatabase) Has(key []byte) (bool, error) {
	var value string
	err := db.db.QueryRow("SELECT v FROM kvstore WHERE k = $1", key).Scan(&value)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		log.Warn("Error: Has")
		return false, err
	}
	return true, nil
}

// Close closes database
func (db *PGDatabase) Close() {
	db.db.Close()
}

// NewBatch return pgBatch
func (db *PGDatabase) NewBatch() Batch {
	return &PGBatch{db: db.db}
}

// PGBatch blah blah...
type PGBatch struct {
	db *sql.DB
}

// Put puts
func (b *PGBatch) Put(key, value []byte) error {
	_, err := b.db.Exec("INSERT INTO kvstore VALUES($1, $2) ON CONFLICT (k) DO UPDATE SET v=$2", key, value)
	if err != nil {
		log.Error("Error: BATCH Put", "key", key, "value", value, "err", err)
		return err
	}
	return nil
}

// Write writes
func (b *PGBatch) Write() error {
	return nil
}

// ValueSize returns
func (b *PGBatch) ValueSize() int {
	return 0
}

// Reset resets
func (b *PGBatch) Reset() {
	return
}

// Delete deletes
func (b *PGBatch) Delete(key []byte) error {
	return nil
}

// PutTransaction puts tx data
func (b *PGBatch) PutTransaction(blockHash []byte, blockNum uint64, i uint64,
	txNonce uint64, txPrice int64, txGaslimit uint64, txRecipient []byte, txAmount int64, txPayload []byte,
	txV int64, txR int64, txS int64) error {
	_, err := b.db.Exec("INSERT INTO transaction VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ",
		"ON CONFLICT ON CONSTRAINT transaction_pkey DO NOTHING",
		blockHash, blockNum, i, txNonce, txPrice, txGaslimit, txRecipient, txAmount, txPayload, txV, txR, txS)
	if err != nil {
		log.Error("Error: INSERT")
		return err
	}
	return nil
}
