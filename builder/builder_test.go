package builder_test

import (
	sql2 "database/sql"
	"fmt"
	"gdb/builder"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"testing"
)

var db *sql2.DB

type Test struct {
	Id   int64
	Name string
	Age  int
}

func init() {
	var err error
	db, err = sql2.Open(
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
			"root",
			"root",
			"localhost",
			"test",
		),
	)

	if err != nil {
		log.Fatal(err)
	}

}

func TestSQLBuilder_GetQuerySql(t *testing.T) {
	sb := builder.NewSQLBuilder()
	sql, err := sb.Table("table1").
		Select("test1", "test2", "test3").
		Where("test1", "<", 13).
		WhereOr("test2", "=", "xxx").
		WhereIn("test6", []interface{}{"asdasda", "dsadada", 1231321}).
		WhereRaw("test3 = ? AND test4= ?", []interface{}{"test123", 789}).
		WhereOrRaw("(test3 = ? AND test4 = ?)", []interface{}{"6566", 777}).
		JoinRaw("LEFT JOIN table2 ON table1.test1 = table2.test1 AND table1.num = ?", 1).
		GroupBy("test3").
		Having("COUNT(table2.test1)", ">", 7).
		HavingOr("COUNT(table2.test1)", ">", 6).
		HavingRaw("COUNT(table2.test1) > ?", 8).
		HavingRawOr("COUNT(table2.test1) > ? AND COUNT(table1.test1) > ?", 8, 9).
		OrderBy("DESC", "test3").
		Limit(0, 10).
		GetQuerySql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetQueryParams()
	fmt.Println(sql)
	fmt.Println(Params)
}

func TestSQLBuilder_GetQuerySql2(t *testing.T) {
	sb := builder.NewSQLBuilder()
	sql, err := sb.Table("test").
		Select("name", "age").
		Limit(0, 5).
		GetQuerySql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetQueryParams()
	fmt.Println(sql, Params)
	rows, err := db.Query(sql, Params...)
	if err != nil {
		log.Fatal(err)
	}

	var name string
	var age int
	for rows.Next() {
		err := rows.Scan(&name, &age)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name, age)
	}
}

func TestSQLBuilder_Insert(t *testing.T) {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		Insert([]string{"name", "age"}, "test-n", 3).
		GetInsertSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetInsertParams()
	result, err := db.Exec(sql, Params...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.LastInsertId())
	fmt.Println(result.RowsAffected())
}

func TestSQLBuilder_GetUpdateSql(t *testing.T) {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		Update([]string{"name", "age"}, "test2-1", 5).
		Where("id", "=", 2).
		GetUpdateSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetUpdateParams()
	result, err := db.Exec(sql, Params...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.LastInsertId())
	fmt.Println(result.RowsAffected())
}

func TestSQLBuilder_GetDeleteSql(t *testing.T) {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		Where("id", "=", 1).
		GetDeleteSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetDeleteParams()
	result, err := db.Exec(sql, Params...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.LastInsertId())
	fmt.Println(result.RowsAffected())
}

func TestSQLBuilder_GetInsertAllSql(t *testing.T) {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		InsertAll(
			[]string{"name", "age"},
			[]interface{}{"test", 18},
			[]interface{}{"test2", 19},
			[]interface{}{"test3", 20},
		).
		GetInsertAllSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetInsertAllParams()

	result, err := db.Exec(sql, Params...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.LastInsertId())
	fmt.Println(result.RowsAffected())
}

func TestSQLBuilder_InsertAllModel(t *testing.T) {
	t1 := Test{
		Name: "t1",
		Age:  19,
	}
	t2 := Test{
		Name: "t2",
		Age:  20,
	}
	t3 := Test{
		Name: "t3",
		Age:  21,
	}
	ts := []*Test{&t1,&t2,&t3}
	sb := builder.NewSQLBuilder()
	sql,err := sb.Table("test").InsertAllModel(ts)
	if err != nil {
		log.Fatal(err)
	}

	params := sb.GetInsertAllParams()
	fmt.Println(sql, params)
}
