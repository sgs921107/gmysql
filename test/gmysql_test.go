package test

import (
	"github.com/sgs921107/gmysql"
	"testing"
)

var options = &gmysql.Options{
	Addr:     "172.17.0.1:3306",
	Username: "work",
	Password: "online",
	Charset:  "utf8mb4",
	Database: "sql_test",
}

var mysql = gmysql.NewMysql(options)
var table = "user"

var createTableSQL = "CREATE TABLE IF NOT EXISTS `user` (" +
	"`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT," +
	"`name` VARCHAR(20) NOT NULL DEFAULT ''," +
	"`age` INT(11) NOT NULL DEFAULT '0'," +
	"PRIMARY KEY(`id`)," +
	"UNiQUE INDEX `name`(`name`) USING BTREE" +
	")ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"

func TestExec(t *testing.T) {
	sql := "drop table " + table
	// 尝试删除表
	mysql.Exec(sql)
	_, err := mysql.Exec(createTableSQL)
	if err != nil {
		t.Errorf("create table failed: %s", err.Error())
	}
}

func TestShowTables(t *testing.T) {
	tables := mysql.ShowTables()
	for _, t := range tables {
		if t == table {
			return
		}
	}
	t.Errorf("table user not in %v", tables)
}

func TestInsert(t *testing.T) {
	_, err := mysql.Insert(table, []string{"name", "age"}, []interface{}{"Mike", 15}, []interface{}{"Shine", "28"})
	if err != nil {
		t.Errorf("insert err: %s", err.Error())
	}
}

func TestPrepareInsert(t *testing.T) {
	id := mysql.PrepareInsert(table, []string{"name", "age"}, []interface{}{"Tom", 19}, []interface{}{"Jane", "20"})
	if id != 4 {
		t.Errorf("prepare insert err: want id == 4, have %d", id)
	}
}

func TestUpdate(t *testing.T) {
	data := map[string]interface{}{
		"age": 30,
	}
	ret := mysql.Update(table, data, "where name=?", "Tom")
	if ret != 1 {
		t.Errorf("update failed: ret == %d, want 1", ret)
	}
}

func TestSelectOne(t *testing.T) {
	data := mysql.SelectOne(table, []string{"age"}, "where name=?", "Tom")
	age := data["age"]
	if age != "30" {
		t.Errorf(`data["age"] == "%s", want "30"`, age)
	}
}

func TestDelete(t *testing.T) {
	ret := mysql.Delete(table, "where age<?", 30)
	if ret == 0 {
		t.Errorf("delete data failed: ret == 0, want %d", ret)
	}
}

func TestSelect(t *testing.T) {
	data := mysql.Select(table, []string{"name"}, "")
	length := len(data)
	if length != 1 {
		t.Errorf("len(data) == %d, want 1", length)
	}
}
