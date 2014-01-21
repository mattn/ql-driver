package ql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/cznic/ql"
	"io"
	"strings"
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
}

func (tx *QlTx) Commit() error {
	if err := tx.c.exec("COMMIT"); err != nil {
		return err
	}
	return nil
}

func (tx *QlTx) Rollback() error {
	if err := tx.c.exec("ROLLBACK"); err != nil {
		return err
	}
	return nil
}

func (c *QlConn) exec(cmd string) error {
	l, err := ql.Compile(cmd)
	if err != nil {
		return err
	}
	_, i, err := c.db.Execute(ql.NewRWCtx(), l)
	if err != nil {
		a := strings.Split(strings.TrimSpace(fmt.Sprint(l)), "\n")
		return fmt.Errorf("%v: %s", err, a[i])
	}
	return nil
}

func (c *QlConn) Begin() (driver.Tx, error) {
	if err := c.exec("BEGIN TRANSACTION"); err != nil {
		return nil, err
	}
	return &QlTx{c}, nil
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
	l, err := ql.Compile(query)
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
	return 0
}

func (s *QlStmt) bind(args []driver.Value) error {
	return nil
}

func (s *QlStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	rs, _, err := s.c.db.Execute(ql.NewRWCtx(), s.l)
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
	return &QlRows{s, rs, cols, 0}, err
}

func (s *QlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	return nil, nil
}

type QlRows struct {
	s    *QlStmt
	r    []ql.Recordset
	cols []string
	i    int
}

func (rc *QlRows) Close() error {
	return nil
}

func (rc *QlRows) Columns() []string {
	return rc.cols
}

func (rc *QlRows) Next(dest []driver.Value) error {
	if rc.i >= len(rc.r) {
		return io.EOF
	}
	err := rc.r[rc.i].Do(false, func(data []interface{}) (bool, error) {
		for i, _ := range dest {
			dest[i] = data[i]
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	rc.i++
	return nil
}
