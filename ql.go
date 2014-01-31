package ql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/cznic/ql"
	"io"
)

func init() {
	sql.Register("ql", &QlDriver{})
}

type QlDriver struct {
}

type QlConn struct {
	db *ql.DB
}

type QlTx struct {
	c *QlConn
	ctx *ql.TCtx
}

func (tx *QlTx) Commit() error {
	/*
	if err := tx.c.exec("COMMIT"); err != nil {
		return err
	}
	return nil
	*/
	return errors.New("Not implemented yet")
}

func (tx *QlTx) Rollback() error {
	/*
	if err := tx.c.exec("ROLLBACK"); err != nil {
		return err
	}
	return nil
	*/
	return errors.New("Not implemented yet")
}

func (c *QlConn) exec(cmd string) error {
	l, err := ql.Compile("begin transaction;" + cmd + ";commit;")
	if err != nil {
		return err
	}
	_, _, err = c.db.Execute(ql.NewRWCtx(), l)
	if err != nil {
		return err
	}
	return nil
}

func (c *QlConn) Begin() (driver.Tx, error) {
	/*
	if err := c.exec("BEGIN TRANSACTION"); err != nil {
		return nil, err
	}
	return &QlTx{c, ql.NewRWCtx()}, nil
	*/
	return nil, errors.New("Not implemented yet")
}

func (d *QlDriver) Open(dsn string) (driver.Conn, error) {
	db, err := ql.OpenFile(dsn, &ql.Options{CanCreate: true})
	if err != nil {
		return nil, err
	}
	return &QlConn{db}, nil
}

func (c *QlConn) Close() error {
	err := c.db.Close()
	if err != nil {
		return err
	}
	c.db = nil
	return nil
}

type QlStmt struct {
	c *QlConn
	l ql.List
}

func (c *QlConn) Prepare(query string) (driver.Stmt, error) {
	l, err := ql.Compile("begin transaction;" + query + ";commit;")
	if err != nil {
		return nil, err
	}
	return &QlStmt{c, l}, nil
}

func (s *QlStmt) Bind(bind []string) error {
	return nil
}

func (s *QlStmt) Close() error {
	return nil
}

func (s *QlStmt) NumInput() int {
	return -1
}

func (s *QlStmt) Query(args []driver.Value) (driver.Rows, error) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	rs, _, err := s.c.db.Execute(ql.NewRWCtx(), s.l, values...)
	if err != nil {
		return nil, err
	}

	cols := []string{}
	err = rs[0].Do(true, func(data []interface{}) (bool, error) {
		for _, v := range data {
			cols = append(cols, v.(string))
		}
		return false, nil
	})
	dataCh := make(chan []interface{})
	closedCh := make(chan bool)
	go func() {
		rs[0].Do(false, func(data []interface{}) (bool, error) {
			dataCh <- data
			return true, nil
		})
		close(dataCh)
	}()
	return &QlRows{s, cols, dataCh, closedCh}, err
}

type QlResult struct {
	id      int64
	changes int64
}

func (r *QlResult) LastInsertId() (int64, error) {
	return r.id, nil
}

// Return how many rows affected.
func (r *QlResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

func (s *QlStmt) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	_, _, err := s.c.db.Execute(ql.NewRWCtx(), s.l, values...)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

type QlRows struct {
	s        *QlStmt
	cols     []string
	dataCh   chan []interface{}
	closedCh chan bool
}

func (rc *QlRows) Close() error {
	close(rc.closedCh)
	return nil
}

func (rc *QlRows) Columns() []string {
	return rc.cols
}

func (rc *QlRows) Next(dest []driver.Value) error {
	if data, ok := <-rc.dataCh; ok {
		for i, _ := range dest {
			dest[i] = data[i]
		}
		return nil
	}
	return io.EOF
}
