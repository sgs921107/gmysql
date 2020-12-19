/*************************************************************************
	> File Name: gmysql.go
	> Author: xiangcai
	> Mail: xiangcai@gmail.com
	> Created Time: 2020年12月17日 星期四 17时18分47秒
*************************************************************************/

package gmysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Mysql mysql
type Mysql struct {
	cursor  *sql.DB
	options *Options
}

// ShowOptions show mysql options
func (s *Mysql) ShowOptions() Options {
	return *s.options
}

// GetCursor 获取一个cursor
func (s *Mysql) GetCursor() *sql.DB {
	return s.cursor
}

// ShowTables show database tables
func (s *Mysql) ShowTables() (tables []string) {
	rows, err := s.cursor.Query("show tables;")
	if err != nil {
		return tables
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			break
		}
		tables = append(tables, table)
	}
	return tables
}

// Insert insert data 直接执行的方式
func (s Mysql) Insert(table string, fields []string, values ...[]interface{}) (sql.Result, error) {
	sql := fmt.Sprintf("insert ignore %s(%s) values", table, strings.Join(fields, ","))
	fieldsNum := len(fields)
	var placeholders []string
	for range fields {
		placeholders = append(placeholders, "?")
	}
	var extension = fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
	var extensions []string
	var fill []interface{}
	for _, value := range values {
		if len(value) != fieldsNum {
			log.Printf("ValueError: fields length is not equal to value length, fields %v, value: %v", fields, value)
			continue
		}
		extensions = append(extensions, extension)
		fill = append(fill, value...)
	}
	sql += strings.Join(extensions, ",") + ";"
	ret, err := s.cursor.Exec(sql, fill...)
	if err != nil {
		log.Printf("insert failed: %s, sql: %s, fill: %v", err.Error(), sql, fill)
	}
	return ret, err
}

// PrepareInsert insert data  预处理方式
func (s Mysql) PrepareInsert(table string, fields []string, values ...[]interface{}) int64 {
	sql := fmt.Sprintf("insert ignore %s(%s) values", table, strings.Join(fields, ","))
	var placeholders []string
	for range fields {
		placeholders = append(placeholders, "?")
	}
	var extension = fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
	sql += extension
	stmt, err := s.cursor.Prepare(sql)
	if err != nil {
		log.Printf("Prepare failed: %s, sql: %s", err.Error(), err)
		return 0
	}
	defer stmt.Close()
	var lastID int64
	for _, value := range values {
		ret, err := stmt.Exec(value...)
		if err != nil {
			log.Printf("insert value failed: %s, fields: %v, value: %v", err.Error(), fields, value)
			continue
		}
		lastID, err = ret.LastInsertId()
		if err != nil {
			log.Printf("fetch LastInsertId failed: %s", err.Error())
		}
	}
	return lastID
}

// SelectOne 查询一条数据 不支持select *
func (s *Mysql) SelectOne(table string, fields []string, condition string, fill ...interface{}) map[string]string {
	sql := fmt.Sprintf("select %s from %s %s;", strings.Join(fields, ","), table, condition)
	row := s.cursor.QueryRow(sql, fill...)
	scaner := make([]interface{}, len(fields))
	for index := range fields {
		var val string
		scaner[index] = &val
	}
	err := row.Scan(scaner...)
	if err != nil {
		log.Printf("select failed: %s, sql: %s, fill: %v", err.Error(), sql, fill)
		return nil
	}
	var result = make(map[string]string)
	for index, field := range fields {
		val, _ := scaner[index].(*string)
		result[field] = *val
	}
	return result
}

// Select exec a select statement nonsupport select * statement
func (s *Mysql) Select(table string, fields []string, condition string, fill ...interface{}) (results []map[string]string) {
	sql := fmt.Sprintf("select %s from %s %s;", strings.Join(fields, ","), table, condition)
	rows, err := s.cursor.Query(sql, fill...)
	if err != nil {
		log.Printf("select failed: %s, sql: %s, fill: %v", err.Error(), sql, fill)
		return results
	}
	defer rows.Close()
	for rows.Next() {
		scaner := make([]interface{}, len(fields))
		for index := range fields {
			var val string
			scaner[index] = &val
		}
		err := rows.Scan(scaner...)
		if err != nil {
			log.Printf("Scan Error: %s", err.Error())
			continue
		}
		var item = make(map[string]string, len(fields))
		for index, field := range fields {
			val, _ := scaner[index].(*string)
			item[field] = *val
		}
		results = append(results, item)
	}
	return results
}

// Update update data
func (s *Mysql) Update(
	table string,
	data map[string]interface{},
	condition string,
	fill ...interface{},
) (sql.Result, error) {
	sql := fmt.Sprintf("update %s set ", table)
	var valFill []interface{}
	var extensions []string
	for k, v := range data {
		extensions = append(extensions, k+"=?")
		valFill = append(valFill, v)
	}
	sql += strings.Join(extensions, ",")
	if condition != "" {
		sql += " " + condition + ";"
		valFill = append(valFill, fill...)
	}
	ret, err := s.cursor.Exec(sql, valFill...)
	if err != nil {
		log.Printf("update data failed: %s, sql: %s, fill: %v", err.Error(), sql, valFill)
	}
	return ret, err
}

// Delete delete
func (s *Mysql) Delete(table, condition string, fill ...interface{}) (sql.Result, error) {
	sql := fmt.Sprintf("delete from %s %s;", table, condition)
	ret, err := s.cursor.Exec(sql, fill...)
	if err != nil {
		log.Printf("delete data failed: %s, sql: %s, fill: %v", err.Error(), sql, fill)
	}
	return ret, err
}

// Exec exec a sql statement
func (s *Mysql) Exec(query string, args ...interface{}) (sql.Result, error) {
	return s.cursor.Exec(query, args...)
}

// Query exec a select statement
func (s *Mysql) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.cursor.Query(query, args...)
}

// QueryRow exec a select statement
func (s *Mysql) QueryRow(query string, args ...interface{}) *sql.Row {
	return s.cursor.QueryRow(query, args...)
}

// Begin begin a transction
func (s *Mysql) Begin() (*sql.Tx, error) {
	return s.cursor.Begin()
}

// Prepare 预处理
func (s *Mysql) Prepare(query string) (*sql.Stmt, error) {
	return s.cursor.Prepare(query)
}

// Close 释放资源
func (s *Mysql) Close() {
	s.cursor.Close()
}

// init  init mysql settings
func (s *Mysql) init() {
	s.cursor.SetMaxOpenConns(s.options.MaxOpenConns)
	s.cursor.SetMaxIdleConns(s.options.MaxIdleConns)
	s.cursor.SetConnMaxLifetime(s.options.MaxLifeTime)
	s.cursor.SetConnMaxIdleTime(s.options.MaxIdleTime)
}

// NewMysql 实例化一个mysql
func NewMysql(options *Options) *Mysql {
	options.driver = "mysql"
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		options.Username,
		options.Password,
		options.Addr,
		options.Database,
	)
	if options.Charset != "" {
		dsn += fmt.Sprintf("?charset=%s", options.Charset)
	}
	db, err := sql.Open(options.driver, dsn)
	if err != nil {
		panic(fmt.Sprintf("connect mysql failed: %s", err.Error()))
	}
	mysql := &Mysql{
		cursor:  db,
		options: options,
	}
	mysql.init()
	return mysql
}
