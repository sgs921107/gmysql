/*************************************************************************
	> File Name: option.go
	> Author: xiangcai
	> Mail: xiangcai@gmail.com
	> Created Time: 2020年12月17日 星期四 17时18分47秒
*************************************************************************/

package gmysql

import (
	"fmt"
	"reflect"
	"time"
)

// Options 实例化mysql的参数
type Options struct {
	Addr         string `default:"127.0.0.1:3306"`
	Username     string
	Password     string
	Database     string
	Charset      string
	MaxOpenConns int
	MaxIdleConns int
	MaxLifeTime  time.Duration
	MaxIdleTime  time.Duration
	ParseTime    bool
	Driver       string `default:"mysql"`
}

func (o Options) GetDriver() string {
	if o.Driver == "" {
		field, _ := reflect.TypeOf(o).FieldByName("Driver")
		return field.Tag.Get("default")
	}
	return o.Driver
}

// DSN 获取mysql的dsn
func (o *Options) GetDSN() string {
	if o.Addr == "" || o.Username == "" || o.Password == "" || o.Database == "" {
		panic("Addr/Username/Password/Database must not be empty str")
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?parseTime=%t",
		o.Username,
		o.Password,
		o.Addr,
		o.Database,
		o.ParseTime,
	)
	if o.Charset != "" {
		dsn += fmt.Sprintf("&charset=%s", o.Charset)
	}
	return dsn
}
