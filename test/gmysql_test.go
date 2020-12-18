package test

import (
	"testing"
	"github.com/sgs921107/gmysql"
)

var option = &gmysql.Option{
	Addr: "172.17.0.1:3306",
	Username: "work",
	Password: "online",
	Charset: "utf8mb4",
	Database: "sql_test",
}

var mysql = gmysql.NewMysql(option)
var table = "user"

var createTableSQL = "CREATE TABLE user (" +
    	"`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT," +
    	"`name` VARCHAR(20) NOT NULL DEFAULT ''," +
    	"`age` INT(11) NOT NULL DEFAULT '0'," +
		"PRIMARY KEY(`id`)," +
		"UNiQUE INDEX `name`(`name`) USING BTREE" +
	")ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"

func TestExec(t *testing.T) {
	sql := "drop table " + table
	_, err := mysql.Exec(sql)
	if err != nil {
		t.Errorf("drop table failed: %s", err.Error())
	}
	_, err = mysql.Exec(createTableSQL)
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
	_, err := mysql.Insert(table, []string{"name", "age"}, []interface{}{"Tom", 19}, []interface{}{"Jane", "20"})
	if err != nil {
		t.Errorf("insert err: %s", err.Error())
	}
}

func TestUpdate(t *testing.T) {
	data := map[string]interface{}{
		"age": 30,
	}
	_, err := mysql.Update(table, data, "where name=?", "Tom")
	if err != nil {
		t.Errorf("update failed: %s", err.Error())
	}
}

func TestSelectOne(t *testing.T) {
	data := mysql.SelectOne(table, []string{"age"}, "where name=?", "Tom")
	age := data["age"]
	if age != "30" {
		t.Errorf(`data["age"] == %s, want "30"`, age)
	}
}

func TestDelete(t *testing.T) {
	_, err := mysql.Delete(table, "where age<?", 30)
	if err != nil {
		t.Errorf("delete data failed: %s", err.Error())
	}
}

func TestSelect(t *testing.T) {
	data := mysql.Select(table, []string{"name"}, "")
	length := len(data)
	if length != 1 {
		t.Errorf("len(data) == %d, want 1", length)
	}
}