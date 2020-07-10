package main

import (
	"fmt"
	"gdb/builder"
	"log"
)

func main() {
	//Query()
	Query()
}

func Query() {
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
		HavingRaw("COUNT(table2.test1) > ?",  8).
		HavingRawOr("COUNT(table2.test1) > ? AND COUNT(table1.test1) > ?",  8, 9).
		OrderBy("DESC", "test3").
		Limit(0,10).
		GetQuerySql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetQueryParams()
	fmt.Println(sql)
	fmt.Println(Params)
}

func Insert() {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").Insert([]string{"a", "b", "c"}, 1, 2, 3).GetInsertSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetInsertParams()
	fmt.Println(sql)
	fmt.Println(Params)
}

func Update() {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		Update([]string{"a", "b", "c"}, 3, 4, 5).
		Where("id", "=", 1).
		GetUpdateSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetUpdateParams()
	fmt.Println(sql)
	fmt.Println(Params)
}

func Delete() {
	sb := builder.NewSQLBuilder()

	sql, err := sb.Table("test").
		Where("id", "=", 1).
		GetDeleteSql()
	if err != nil {
		log.Fatal(err)
	}

	Params := sb.GetDeleteParams()
	fmt.Println(sql)
	fmt.Println(Params)
}
