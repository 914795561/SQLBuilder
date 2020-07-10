package builder

import (
	"errors"
	"strings"
)

type SQLBuilder struct {
	_select      string
	_insert      string
	_update      string
	_delete      string
	_limit       string
	_orderBy     string
	_groupBy     string
	_table       string
	_join        string
	_where       string
	_having      string
	_insertParams []interface{}
	_updateParams []interface{}
	_whereParams  []interface{}
	_joinParams   []interface{}
	_havingParams []interface{}
	_limitParams  []interface{}
}

var (
	ErrTableEmpty = errors.New("table empty")
	ErrInsertStatement = errors.New("insert statement empty")
	ErrUpdateStatement = errors.New("update statement empty")
)

func NewSQLBuilder() *SQLBuilder {
	return &SQLBuilder{}
}

//SELECT `t1`.`name`,`t1`.`age`,`t2`.`teacher`,`t3`.`address` FROM `test` as t1 LEFT
//JOIN `test2` as `t2` ON `t1`.`class` = `t2`.`class` INNER JOIN `test3` as t3 ON
//`t1`.`school` = `t3`.`school` WHERE `t1`.`age` >= 20 GROUP BY `t1`.`age`
//HAVING COUNT(`t1`.`age`) > 2 ORDER BY `t1`.`age` DESC LIMIT 10 OFFSET 0

func (sb *SQLBuilder) GetQuerySql() (string, error) {
	if sb._table == "" {
		return "", ErrTableEmpty
	}

	var buf strings.Builder

	buf.WriteString("SELECT ")
	if sb._select != "" {
		buf.WriteString(sb._select)
	} else {
		buf.WriteString("*")
	}

	buf.WriteString(" FROM ")
	buf.WriteString(sb._table)

	if sb._join != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._join)
	}

	if sb._where != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._where)
	}

	if sb._groupBy != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._groupBy)
	}

	if sb._having != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._having)
	}

	if sb._orderBy != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._orderBy)
	}

	if sb._limit != "" {
		buf.WriteString(" ")
		buf.WriteString(sb._limit)
	}

	return buf.String(), nil
}

func (sb *SQLBuilder) GetQueryParams() []interface{} {
	Params := []interface{}{}
	Params = append(Params, sb._joinParams...)
	Params = append(Params, sb._whereParams...)
	Params = append(Params, sb._havingParams...)
	Params = append(Params, sb._limitParams...)

	return Params
}

func (sb *SQLBuilder) Select(cols ...string) *SQLBuilder {
	var buf strings.Builder

	for key, col := range cols {
		buf.WriteString(col)
		if key != len(cols)-1 {
			buf.WriteString(",")
		}
	}

	sb._select = buf.String()

	return sb
}

func (sb *SQLBuilder) Table(table string) *SQLBuilder {
	sb._table = table
	return sb
}

func (sb *SQLBuilder)Insert(fields []string, values ...interface{}) *SQLBuilder{
	var buf strings.Builder

	buf.WriteString("(")

	for key,field := range fields {
		buf.WriteString(field)
		if key != len(fields)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString(") VALUES (")

	for key := range fields {
		buf.WriteString("?")
		if key != len(fields)-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString(")")
	sb._insert = buf.String()

	for _,value := range values {
		sb._insertParams = append(sb._insertParams, value)
	}

	return sb
}

func (sb *SQLBuilder)GetInsertSql() (string, error)  {
	if sb._table == "" {
		return "", ErrTableEmpty
	}

	if sb._insert == "" {
		return "", ErrInsertStatement
	}

	var buf strings.Builder
	buf.WriteString("INSERT INTO ")
	buf.WriteString(sb._table)
	buf.WriteString(" ")
	buf.WriteString(sb._insert)

	return buf.String(),nil
}

func (sb *SQLBuilder)GetInsertParams() []interface{}  {
	return sb._insertParams
}

func (sb *SQLBuilder)Update(fields []string, values ...interface{})*SQLBuilder  {
		var buf strings.Builder

		for key,val := range fields {
			buf.WriteString(val)
			buf.WriteString(" = ")
			buf.WriteString("?")
			if key != len(fields)-1 {
				buf.WriteString(",")
			}
		}

		sb._update = buf.String()

		for _,val := range values {
			sb._updateParams = append(sb._updateParams, val)
		}

		return sb
}

func (sb *SQLBuilder)GetUpdateSql() (string, error)  {
	if sb._table == "" {
		return "",ErrTableEmpty
	}

	if sb._update == "" {
		return "",ErrUpdateStatement
	}

	var buf strings.Builder
	buf.WriteString("UPDATE ")
	buf.WriteString(sb._table)
	buf.WriteString(" SET ")
	buf.WriteString(sb._update)
	buf.WriteString(" ")

	if sb._where != "" {
		buf.WriteString(sb._where)
	}

	return buf.String(),nil
}

func (sb *SQLBuilder)GetUpdateParams() []interface{}  {
	Params := []interface{}{}
	Params = append(Params, sb._updateParams...)
	Params = append(Params, sb._whereParams...)
	return Params
}

func (sb *SQLBuilder)GetDeleteSql() (string, error)  {
	if sb._table == "" {
		return "",ErrTableEmpty
	}

	var buf strings.Builder
	buf.WriteString("DELETE FROM ")
	buf.WriteString(sb._table)
	buf.WriteString(" ")

	if sb._where != "" {
		buf.WriteString(sb._where)
	}

	return buf.String(),nil
}

func (sb *SQLBuilder)GetDeleteParams() []interface{}  {
	return sb._whereParams
}

func (sb *SQLBuilder) Where(field, condition string, value interface{}) *SQLBuilder {
	return sb.where("AND", field, condition, value)
}

func (sb *SQLBuilder) WhereOr(field string, condition string, value interface{}) *SQLBuilder {
	return sb.where("OR", field, condition, value)
}

func (sb *SQLBuilder) WhereRaw(s string, values []interface{}) *SQLBuilder {
	return sb.whereRaw("AND", s, values)
}

func (sb *SQLBuilder) WhereOrRaw(s string, values []interface{}) *SQLBuilder {
	return sb.whereRaw("OR", s, values)
}

func (sb *SQLBuilder) where(operator string, field string, condition string, value interface{}) *SQLBuilder {
	var buf strings.Builder

	buf.WriteString(sb._where)

	if buf.Len() == 0 {
		buf.WriteString("WHERE ")
	} else {
		buf.WriteString(" ")
		buf.WriteString(operator)
		buf.WriteString(" ")
	}

	buf.WriteString(field)
	buf.WriteString(" ")
	buf.WriteString(condition)
	buf.WriteString(" ")
	buf.WriteString("?")

	sb._where = buf.String()
	sb._whereParams = append(sb._whereParams, value)

	return sb
}

func (sb *SQLBuilder) whereRaw(operator string, s string, values []interface{}) *SQLBuilder {
	var buf strings.Builder
	buf.WriteString(sb._where)

	if buf.Len() == 0 {
		buf.WriteString("WHERE ")
	} else {
		buf.WriteString(" ")
		buf.WriteString(operator)
		buf.WriteString(" ")
	}

	buf.WriteString(s)
	sb._where = buf.String()

	for _, value := range values {
		sb._whereParams = append(sb._whereParams, value)
	}

	return sb
}

func (sb *SQLBuilder)WhereIn(field string, value []interface{}) *SQLBuilder  {
	return sb.whereIn("AND", "IN", field, value)
}

func (sb *SQLBuilder)WhereNotIn(field string, value []interface{}) *SQLBuilder  {
	return sb.whereIn("AND", "NOT IN", field, value)
}

func (sb *SQLBuilder)WhereOrIn(field string, value []interface{}) *SQLBuilder  {
	return sb.whereIn("OR", "IN", field, value)
}

func (sb *SQLBuilder)WhereOrNotIn(field string, value []interface{}) *SQLBuilder  {
	return sb.whereIn("OR", "NOT IN", field, value)
}

func (sb *SQLBuilder)whereIn(operator string, condition string, field string, values []interface{}) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString(sb._where)

	if buf.Len() == 0 {
		buf.WriteString("WHERE ")
	}else{
		buf.WriteString(" ")
		buf.WriteString(operator)
		buf.WriteString(" ")
	}

	buf.WriteString(field)
	buf.WriteString(" ")
	buf.WriteString(condition)
	buf.WriteString(" ")
	buf.WriteString("(")
	for i:=0; i<len(values);i++  {
		buf.WriteString("?")
		if i != len(values)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString(")")

	sb._where = buf.String()

	for _,val := range values {
		sb._whereParams = append(sb._whereParams, val)
	}

	return sb
}

func (sb *SQLBuilder)Limit(offset, num interface{}) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString("LIMIT ? OFFSET ?")
	sb._limit = buf.String()
	sb._limitParams = append(sb._limitParams, offset, num)
	return sb
}

func (sb *SQLBuilder)OrderBy(order string, fields ...string) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString("ORDER BY ")
	for key,val := range fields{
		buf.WriteString(val)
		if key != len(fields)-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString(" ")
	buf.WriteString(order)

	sb._orderBy = buf.String()

	return sb
}

func (sb *SQLBuilder)GroupBy(field string) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString("GROUP BY ")
	buf.WriteString(field)

	sb._groupBy = buf.String()
	return sb
}

func (sb *SQLBuilder)JoinRaw(s string, values ...interface{}) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString(sb._join)

	if buf.Len() != 0 {
		buf.WriteString(" ")
	}
	buf.WriteString(s)
	sb._join = buf.String()

	for _, value := range values {
		sb._joinParams = append(sb._joinParams, value)
	}

	return sb
}

func (sb *SQLBuilder)HavingRaw(s string, values ...interface{}) *SQLBuilder  {
	return sb.havingRaw("AND", s, values)
}

func (sb *SQLBuilder)HavingRawOr(s string, values ...interface{}) *SQLBuilder  {
	return sb.havingRaw("OR", s, values)
}

func (sb *SQLBuilder)havingRaw(operator string ,s string, values ...interface{}) *SQLBuilder  {
	var buf strings.Builder
	buf.WriteString(sb._having)

	if buf.Len() == 0{
		buf.WriteString("HAVING ")
	}else {
		buf.WriteString(" ")
		buf.WriteString(operator)
		buf.WriteString(" ")
	}
	buf.WriteString(s)
	sb._having = buf.String()

	for _, value := range values {
		sb._havingParams = append(sb._havingParams, value)
	}

	return sb
}

func (sb *SQLBuilder)Having(field string, condition string, value interface{}) *SQLBuilder  {
	return sb.having("AND", field, condition, value)
}

func (sb *SQLBuilder)HavingOr(field string, condition string, value interface{}) *SQLBuilder  {
	return sb.having("OR", field, condition, value)
}

func (sb *SQLBuilder)having(operator string ,field string, condition string, value interface{}) *SQLBuilder  {
	if sb._groupBy == "" {
		return sb
	}

	var buf strings.Builder
	buf.WriteString(sb._having)

	if buf.Len() == 0 {
		buf.WriteString("HAVING ")
	}else {
		buf.WriteString(" ")
		buf.WriteString(operator)
		buf.WriteString(" ")
	}
	
	buf.WriteString(field)
	buf.WriteString(" ")
	buf.WriteString(condition)
	buf.WriteString(" ")
	buf.WriteString("?")
	
	sb._having = buf.String()

	sb._havingParams = append(sb._havingParams, value)
	return sb
}