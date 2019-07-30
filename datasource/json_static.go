package datasource

import (
	"bufio"
	"database/sql/driver"
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	DUMMY_COLS = []string{"user_id", "email", "interests", "reg_date", "item_count"}
)

var (
	_ schema.Source      = (*JSONStaticSource)(nil)
	_ schema.Conn        = (*JSONStaticSource)(nil)
	_ schema.ConnScanner = (*JSONStaticSource)(nil)
)

type JSONStaticSource struct {
	table   string
	rawJSON string
	tbl     *schema.Table
	rowct   uint64
	columns []string

	exit <-chan bool

	r        *bufio.Reader
	complete bool
	err      error
	lh       FileLineHandler
}

func NewJSONStaticSource(tableName string, tableData string, exit <-chan bool) (*JSONStaticSource, error) {
	m := JSONStaticSource{
		table:   tableName,
		rawJSON: tableData,
		exit:    exit, // for iterator
	}

	if m.lh == nil {
		m.lh = m.jsonDefaultLineHandler
	}

	m.r = bufio.NewReader(ioutil.NopCloser(strings.NewReader(tableData)))

	m.loadTable()

	return &m, nil
}

func (m *JSONStaticSource) Init()                      {}
func (m *JSONStaticSource) Setup(*schema.Schema) error { return nil }
func (m *JSONStaticSource) Tables() []string           { return []string{m.table} }
func (m *JSONStaticSource) Columns() []string          { return m.columns }
func (m *JSONStaticSource) Table(table string) (*schema.Table, error) {
	if m.tbl != nil {
		return m.tbl, nil
	}
	return nil, schema.ErrNotFound
}

func (m *JSONStaticSource) loadTable() error {
	tbl := schema.NewTable(strings.ToLower(m.table))

	// TODO temporary constant for getting json table columns
	m.columns = DUMMY_COLS

	for i := range m.columns {
		m.columns[i] = strings.ToLower(m.columns[i])
		tbl.AddField(schema.NewFieldBase(m.columns[i], value.StringType, 64, "string"))
	}
	tbl.SetColumns(m.columns)
	m.tbl = tbl
	return nil
}

func (m *JSONStaticSource) Open(tableName string) (schema.Conn, error) {
	exit := make(<-chan bool, 1)
	return NewJSONStaticSource(tableName, m.rawJSON, exit)
}

func (m *JSONStaticSource) Close() error {
	return nil
}

func (m *JSONStaticSource) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			line, err := m.r.ReadBytes('\n')

			if err != nil {
				if err == io.EOF {
					m.complete = true
				} else {
					m.err = err
					return nil
				}
			}
			if len(line) == 0 {
				return nil
			}
			m.rowct++

			msg, err := m.lh(line)
			if err != nil {
				m.err = err
				return nil
			}
			return msg
		}
	}
}

func (m *JSONStaticSource) jsonDefaultLineHandler(line []byte) (schema.Message, error) {
	jm := make(map[string]interface{})
	err := json.Unmarshal(line, &jm)
	if err != nil {
		return nil, err
	}
	vals := make([]driver.Value, len(jm))
	keys := make(map[string]int, len(jm))
	i := 0
	for k, val := range jm {
		vals[i] = val
		keys[k] = i
		i++
	}

	return NewSqlDriverMessageMap(m.rowct, vals, keys), nil
}
