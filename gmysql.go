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
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var log = Logging.GetLogger()

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
			log.WithFields(LogFields{
				"errMsg": err.Error(),
			}).Error("Scan Error")
			return nil
		}
		tables = append(tables, table)
	}
	return tables
}

// Insert insert data 直接执行的方式 注意：一次插入多个值，则结果的最新id为第一个插入的id
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
			log.WithFields(LogFields{
				"fields": fields,
				"fill":   fill,
				"errMsg": "fields length is not equal to value length",
			}).Error("ValueError")
			continue
		}
		extensions = append(extensions, extension)
		fill = append(fill, value...)
	}
	sql += strings.Join(extensions, ",") + ";"
	return s.cursor.Exec(sql, fill...)
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
		log.WithFields(LogFields{
			"sql":    sql,
			"errMsg": err.Error(),
		}).Error("Prepare failed")
		return 0
	}
	defer stmt.Close()
	var lastID int64
	for _, value := range values {
		ret, err := stmt.Exec(value...)
		if err != nil {
			log.WithFields(LogFields{
				"sql":    sql,
				"fields": fields,
				"value":  value,
				"errMsg": err.Error(),
			}).Error("insert value failed")
			continue
		}
		lastID, err = ret.LastInsertId()
		if err != nil {
			log.WithFields(LogFields{
				"errMsg": err.Error(),
			}).Error("Fetch LastInsertId failed")
		}
	}
	return lastID
}

// SelectOne 查询一条数据 不支持select *
// func (s *Mysql) SelectOne(table string, fields []string, condition string, fill ...interface{}) map[string]string {
// 	sql := fmt.Sprintf("select %s from %s %s;", strings.Join(fields, ","), table, condition)
// 	scaner := make([]interface{}, len(fields))
// 	for index := range fields {
// 		var val string
// 		scaner[index] = &val
// 	}
// 	err := s.cursor.QueryRow(sql, fill...).Scan(scaner...)
// 	if err != nil {
// 		// 空数据也会引发错误，使用debug
// 		log.WithFields(LogFields{
// 			"sql": sql,
// 			"fill": fill,
// 			"errMsg": err.Error(),
// 		}).Debug("Select failed")
// 		return nil
// 	}
// 	var result = make(map[string]string)
// 	for index, field := range fields {
// 		val, _ := scaner[index].(*string)
// 		result[field] = *val
// 	}
// 	return result
// }

// baseSelect exec a select statement
func (s *Mysql) baseSelect(
	distinct bool,
	table string,
	fields []string,
	condition string,
	fill ...interface{},
) (results []map[string]string) {
	var sql string
	if distinct {
		sql = fmt.Sprintf("select distinct %s from %s %s;", strings.Join(fields, ","), table, condition)
	} else {
		sql = fmt.Sprintf("select %s from %s %s;", strings.Join(fields, ","), table, condition)
	}
	rows, err := s.cursor.Query(sql, fill...)
	if err != nil {
		// 空数据也会引发错误，使用debug
		log.WithFields(LogFields{
			"sql":    sql,
			"fill":   fill,
			"errMsg": err.Error(),
		}).Debug("Select failed")
		return results
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.WithFields(LogFields{
			"errMsg": err.Error(),
		}).Error("Fetch columns failed")
		return results
	}
	for rows.Next() {
		scaner := make([]interface{}, len(columns))
		for index := range columns {
			var val string
			scaner[index] = &val
		}
		err := rows.Scan(scaner...)
		if err != nil {
			log.WithFields(LogFields{
				"errMsg": err.Error(),
			}).Error("Scan Error")
			continue
		}
		var item = make(map[string]string, len(columns))
		for index, column := range columns {
			val, _ := scaner[index].(*string)
			item[column] = *val
		}
		results = append(results, item)
	}
	return results
}

// Select exec a select statement
func (s *Mysql) Select(
	table string,
	fields []string,
	condition string,
	fill ...interface{},
) (results []map[string]string) {
	return s.baseSelect(false, table, fields, condition, fill...)
}

// SelectDistinct exec a select distinct statement
func (s *Mysql) SelectDistinct(
	table string,
	fields []string,
	condition string,
	fill ...interface{},
) (results []map[string]string) {
	return s.baseSelect(true, table, fields, condition, fill...)
}

// SelectOne 查询一条数据
func (s *Mysql) SelectOne(table string, fields []string, condition string, fill ...interface{}) map[string]string {
	condition += " limit 1"
	results := s.Select(table, fields, condition, fill...)
	if len(results) == 0 {
		return nil
	}
	return results[0]
}

// Update update data
func (s *Mysql) Update(
	table string,
	data map[string]interface{},
	condition string,
	fill ...interface{},
) int64 {
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
		log.WithFields(LogFields{
			"sql":    sql,
			"fill":   valFill,
			"errMsg": err.Error(),
		}).Error("Update Failed")
		return 0
	}
	id, err := ret.RowsAffected()
	if err != nil {
		log.WithFields(LogFields{
			"errMsg": err.Error(),
		}).Error("Update Succeed But Fetch RowsAffected Failed")
		return 0
	}
	return id
}

// Delete delete
func (s *Mysql) Delete(table, condition string, fill ...interface{}) int64 {
	sql := fmt.Sprintf("delete from %s %s;", table, condition)
	ret, err := s.cursor.Exec(sql, fill...)
	if err != nil {
		log.WithFields(LogFields{
			"sql":    sql,
			"fill":   fill,
			"errMsg": err.Error(),
		}).Error("Delete Data Failed")
		return 0
	}
	id, err := ret.RowsAffected()
	if err != nil {
		log.WithFields(LogFields{
			"errMsg": err.Error(),
		}).Error("Delete Data Succeed But Fetch RowsAffected Failed")
		return 0
	}
	return id
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
	db, err := sql.Open(options.GetDriver(), options.GetDSN())
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
