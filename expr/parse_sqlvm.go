package expr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

// Parses Tokens and returns an request.
func ParseSql(sqlQuery string) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l, pager: NewSqlTokenPager(l), buildVm: false}
	return p.parse()
}
func ParseSqlVm(sqlQuery string) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l, pager: NewSqlTokenPager(l), buildVm: true}
	return p.parse()
}

// generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type Sqlbridge struct {
	buildVm    bool
	l          *lex.Lexer
	pager      *SqlTokenPager
	firstToken lex.Token
	curToken   lex.Token
}

// parse the request
func (m *Sqlbridge) parse() (SqlStatement, error) {
	m.firstToken = m.l.NextToken()
	//u.Info(m.firstToken)
	switch m.firstToken.T {
	case lex.TokenSelect:
		return m.parseSqlSelect()
	case lex.TokenInsert:
		return m.parseSqlInsert()
	case lex.TokenDelete:
		return m.parseSqlDelete()
		// case lex.TokenTypeSqlUpdate:
		// 	return this.parseSqlUpdate()
	case lex.TokenShow:
		return m.parseShow()
	case lex.TokenDescribe:
		return m.parseDescribe()
	}
	return nil, fmt.Errorf("Unrecognized request type")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Sqlbridge) parseSqlSelect() (*SqlSelect, error) {

	req := NewSqlSelect()
	m.curToken = m.l.NextToken()

	// columns
	if m.curToken.T != lex.TokenStar {
		if err := m.parseColumns(req); err != nil {
			u.Error(err)
			return nil, err
		}
	} else if err := m.parseSelectStar(req); err != nil {
		u.Error(err)
		return nil, err
	}

	// select @@myvar limit 1
	if m.curToken.T == lex.TokenLimit {
		if err := m.parseLimit(req); err != nil {
			return req, nil
		}
		if m.isEnd() {
			return req, nil
		}
	}

	// SPECIAL END CASE for simple selects
	// Select last_insert_id();
	if m.curToken.T == lex.TokenEOS || m.curToken.T == lex.TokenEOF {
		// valid end
		return req, nil
	}

	// FROM
	//u.Debugf("token:  %#v", m.curToken)
	if m.curToken.T != lex.TokenFrom {
		return nil, fmt.Errorf("expected From but got: %v", m.curToken)
	} else {
		// table name
		m.curToken = m.l.NextToken()
		//u.Debugf("found from?  %#v  %s", m.curToken, m.curToken.T.String())
		if m.curToken.T != lex.TokenIdentity && m.curToken.T != lex.TokenValue {
			//u.Warnf("No From? %v toktype:%v", m.curToken.V, m.curToken.T.String())
			return nil, errors.New("expected from name")
		} else {
			req.From = m.curToken.V
		}
	}

	// WHERE
	m.curToken = m.l.NextToken()
	//u.Debugf("cur lex.Token: %s", m.curToken.T.String())
	if errreq := m.parseWhere(req); errreq != nil {
		return nil, errreq
	}

	// TODO ORDER BY

	// LIMIT
	if err := m.parseLimit(req); err != nil {
		return req, nil
	}

	// we are good
	return req, nil
}

// First keyword was INSERT
func (m *Sqlbridge) parseSqlInsert() (*SqlInsert, error) {

	// insert into mytable (id, str) values (0, "a")
	req := NewSqlInsert()
	m.curToken = m.l.NextToken()

	// into
	//u.Debugf("token:  %v", m.curToken)
	if m.curToken.T != lex.TokenInto {
		return nil, fmt.Errorf("expected INTO but got: %v", m.curToken)
	} else {
		// table name
		m.curToken = m.l.NextToken()
		//u.Debugf("found into?  %v", m.curToken)
		switch m.curToken.T {
		case lex.TokenTable:
			req.Into = m.curToken.V
		default:
			return nil, fmt.Errorf("expected table name but got : %v", m.curToken.V)
		}
	}

	// list of fields
	m.curToken = m.l.NextToken()
	if err := m.parseFieldList(req); err != nil {
		u.Error(err)
		return nil, err
	}
	m.curToken = m.l.NextToken()
	//u.Debugf("found ?  %v", m.curToken)
	switch m.curToken.T {
	case lex.TokenValues:
		m.curToken = m.l.NextToken()
	default:
		return nil, fmt.Errorf("expected values but got : %v", m.curToken.V)
	}
	//u.Debugf("found ?  %v", m.curToken)
	if err := m.parseValueList(req); err != nil {
		u.Error(err)
		return nil, err
	}
	// we are good
	return req, nil
}

// First keyword was DELETE
func (m *Sqlbridge) parseSqlDelete() (*SqlDelete, error) {

	req := NewSqlDelete()
	m.curToken = m.l.NextToken()

	// from
	u.Debugf("token:  %v", m.curToken)
	if m.curToken.T != lex.TokenFrom {
		return nil, fmt.Errorf("expected FROM but got: %v", m.curToken)
	} else {
		// table name
		m.curToken = m.l.NextToken()
		u.Debugf("found table?  %v", m.curToken)
		switch m.curToken.T {
		case lex.TokenTable:
			req.Table = m.curToken.V
		default:
			return nil, fmt.Errorf("expected table name but got : %v", m.curToken.V)
		}
	}

	m.curToken = m.l.NextToken()
	u.Debugf("cur lex.Token: %s", m.curToken.T.String())
	if errreq := m.parseWhereDelete(req); errreq != nil {
		return nil, errreq
	}
	// we are good
	return req, nil
}

// First keyword was DESCRIBE
func (m *Sqlbridge) parseDescribe() (*SqlDescribe, error) {

	req := &SqlDescribe{}
	m.curToken = m.l.NextToken()

	u.Debugf("token:  %v", m.curToken)
	if m.curToken.T != lex.TokenIdentity {
		return nil, fmt.Errorf("expected idenity but got: %v", m.curToken)
	}
	req.Identity = m.curToken.V
	return req, nil
}

// First keyword was SHOW
func (m *Sqlbridge) parseShow() (*SqlShow, error) {

	req := &SqlShow{}
	m.curToken = m.l.NextToken()

	//u.Debugf("token:  %v", m.curToken)
	if m.curToken.T != lex.TokenIdentity {
		return nil, fmt.Errorf("expected idenity but got: %v", m.curToken)
	}
	req.Identity = m.curToken.V
	return req, nil
}

// Recursively descend down a node looking for first Identity Field
//
//     min(year)                 == min_year
//     eq(min(year), max(month)) == eq_year
func findIdentityField(depth int, node Node, prefix string) string {

	switch n := node.(type) {
	case *IdentityNode:
		if prefix == "" {
			return n.Text
		}
		return fmt.Sprintf("%s_%s", prefix, n.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			return findIdentityField(depth+1, arg, strings.ToLower(arg.String()))
		}
	case *FuncNode:
		if depth > 10 {
			return ""
		}
		for _, arg := range n.Args {
			return findIdentityField(depth+1, arg, strings.ToLower(n.F.Name))
		}
	}
	return ""

}

func (m *Sqlbridge) parseColumns(stmt *SqlSelect) error {

	var col *Column

	for {

		//u.Debug(m.curToken.String())
		switch m.curToken.T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
			m.parseNode(col.Tree)

			if m.curToken.T != lex.TokenAs {
				switch n := col.Tree.Root.(type) {
				case *FuncNode:
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						col.As = n.Name
					}
				case *BinaryNode:
					u.Debugf("udf? %T ", col.Tree.Root)
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						u.Errorf("could not find as name: %#v", col.Tree)
					}
				}
			}
			u.Debugf("next? %v", m.curToken)

		case lex.TokenIdentity:
			//u.Warnf("TODO")
			col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
			m.parseNode(col.Tree)
		case lex.TokenValue:
			// Value Literal
			col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
			m.parseNode(col.Tree)
		}
		//u.Debugf("after colstart?:   %v  ", m.curToken)

		// since we can loop inside switch statement
		switch m.curToken.T {
		case lex.TokenAs:
			m.curToken = m.l.NextToken()
			//u.Debug(m.curToken)
			switch m.curToken.T {
			case lex.TokenIdentity, lex.TokenValue:
				col.As = m.curToken.V
				u.Infof("set AS=%v", col.As)
				m.curToken = m.l.NextToken()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.curToken.String())
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenIf:
			// If guard
			m.curToken = m.l.NextToken()
			//u.Infof("if guard: %v", m.curToken)
			col.Guard = NewTree(m.pager)
			//m.curToken = m.l.NextToken()
			//u.Infof("if guard 2: %v", m.curToken)
			m.parseNode(col.Guard)
			//u.Debugf("after if guard?:   %v  ", m.curToken)
		case lex.TokenCommentSingleLine:
			m.curToken = m.l.NextToken()
			col.Comment = m.curToken.V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.curToken.String())
		}
		m.curToken = m.l.NextToken()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Sqlbridge) parseFieldList(stmt *SqlInsert) error {

	var col *Column
	if m.curToken.T != lex.TokenLeftParenthesis {
		return fmt.Errorf("Expecting opening paren ( but got %v", m.curToken)
	}
	m.curToken = m.l.NextToken()

	for {

		//u.Debug(m.curToken.String())
		switch m.curToken.T {
		// case lex.TokenUdfExpr:
		// 	// we have a udf/functional expression column
		// 	col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
		// 	m.parseNode(col.Tree)
		case lex.TokenIdentity:
			col = &Column{As: m.curToken.V}
			m.curToken = m.l.NextToken()
		}
		//u.Debugf("after colstart?:   %v  ", m.curToken)

		// since we can loop inside switch statement
		switch m.curToken.T {
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF,
			lex.TokenRightParenthesis:
			// This indicates we have come to the End of the columns
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenComma:
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.curToken.String())
		}
		m.curToken = m.l.NextToken()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Sqlbridge) parseValueList(stmt *SqlInsert) error {

	if m.curToken.T != lex.TokenLeftParenthesis {
		return fmt.Errorf("Expecting opening paren ( but got %v", m.curToken)
	}
	//m.curToken = m.l.NextToken()
	stmt.Rows = make([][]value.Value, 0)
	var row []value.Value
	for {

		//u.Debug(m.curToken.String())
		switch m.curToken.T {
		case lex.TokenLeftParenthesis:
			// start of row
			row = make([]value.Value, 0)
		case lex.TokenRightParenthesis:
			stmt.Rows = append(stmt.Rows, row)
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the values
			//u.Debugf("Ending %v ", m.curToken)
			return nil
		case lex.TokenValue:
			row = append(row, value.NewStringValue(m.curToken.V))
		case lex.TokenInteger:
			iv, _ := strconv.ParseInt(m.curToken.V, 10, 64)
			row = append(row, value.NewIntValue(iv))
		case lex.TokenComma:
			//row = append(row, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			u.Warnf("don't know how to handle ?  %v", m.curToken)
			return fmt.Errorf("expected column but got: %v", m.curToken.String())
		}
		m.curToken = m.l.NextToken()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

// Parse an expression tree or root Node
func (m *Sqlbridge) parseNode(tree *Tree) error {
	//u.Debugf("parseNode: %v", m.curToken)
	m.pager.SetCurrent(m.curToken)
	err := tree.BuildTree(m.buildVm)
	m.curToken = tree.Peek()
	//u.Debugf("cur token parse: root?%#v, token=%v", tree.Root, m.curToken)
	return err
}

func (m *Sqlbridge) parseSelectStar(req *SqlSelect) error {

	req.Star = true
	req.Columns = make(Columns, 0)
	col := &Column{Star: true}
	req.Columns = append(req.Columns, col)

	m.curToken = m.l.NextToken()
	return nil
}

func (m *Sqlbridge) parseWhere(req *SqlSelect) error {

	if m.curToken.T != lex.TokenWhere {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("where error? %v", r)
			u.Infof("token: %v", m.curToken)
		}
	}()
	m.curToken = m.l.NextToken()
	tree := NewTree(m.pager)
	m.parseNode(tree)
	req.Where = tree.Root
	//u.Debugf("where: %v", m.curToken)
	return nil
}

func (m *Sqlbridge) parseWhereDelete(req *SqlDelete) error {

	if m.curToken.T != lex.TokenWhere {
		return nil
	}

	m.curToken = m.l.NextToken()
	tree := NewTree(m.pager)
	m.parseNode(tree)
	req.Where = tree.Root
	return nil
}

func (m *Sqlbridge) parseLimit(req *SqlSelect) error {
	m.curToken = m.l.NextToken()
	if m.curToken.T != lex.TokenInteger {
		return fmt.Errorf("Limit must be an integer %v %v", m.curToken.T, m.curToken.V)
	}
	iv, err := strconv.Atoi(m.curToken.V)
	if err != nil {
		return fmt.Errorf("Could not convert limit to integer %v", m.curToken.V)
	}
	req.Limit = int(iv)
	return nil
}

func (m *Sqlbridge) isEnd() bool {
	return m.pager.IsEnd()
}

// TokenPager is responsible for determining end of
// current tree (column, etc)
type SqlTokenPager struct {
	token     [1]lex.Token // one-token lookahead for parser
	peekCount int
	lex       *lex.Lexer
	end       lex.TokenType
}

func NewSqlTokenPager(lex *lex.Lexer) *SqlTokenPager {
	return &SqlTokenPager{
		lex: lex,
	}
}

func (m *SqlTokenPager) SetCurrent(tok lex.Token) {
	m.peekCount = 1
	m.token[0] = tok
}

// next returns the next token.
func (m *SqlTokenPager) Next() lex.Token {
	if m.peekCount > 0 {
		m.peekCount--
	} else {
		m.token[0] = m.lex.NextToken()
	}
	return m.token[m.peekCount]
}
func (m *SqlTokenPager) Last() lex.TokenType {
	return m.end
}
func (m *SqlTokenPager) IsEnd() bool {
	tok := m.Peek()
	//u.Debugf("tok:  %v", tok)
	switch tok.T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenComma, lex.TokenIf,
		lex.TokenAs, lex.TokenLimit, lex.TokenSelect:
		return true
	}
	return false
}

// backup backs the input stream up one token.
func (m *SqlTokenPager) Backup() {
	if m.peekCount > 0 {
		//u.Warnf("PeekCount?  %v: %v", m.peekCount, m.token)
		return
	}
	m.peekCount++
}

// peek returns but does not consume the next token.
func (m *SqlTokenPager) Peek() lex.Token {
	if m.peekCount > 0 {
		//u.Infof("peek:  %v: len=%v", m.peekCount, len(m.token))
		return m.token[m.peekCount-1]
	}
	m.peekCount = 1
	m.token[0] = m.lex.NextToken()
	//u.Infof("peek:  %v: len=%v %v", m.peekCount, len(m.token), m.token[0])
	return m.token[0]
}
