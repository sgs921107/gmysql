/*************************************************************************
	> File Name: option.go
	> Author: xiangcai
	> Mail: xiangcai@gmail.com
	> Created Time: 2020年12月17日 星期四 17时18分47秒
*************************************************************************/

package gmysql

import (
	"fmt"
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
	driver       string `default:"mysql"`
}

// DSN 获取mysql的dsn
func (o *Options) DSN() string {
	if o.Addr == "" || o.Username == "" || o.Password == "" || o.Database == "" {
		panic("Addr/Username/Password/Database must not be empty str")
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		o.Username,
		o.Password,
		o.Addr,
		o.Database,
	)
	if o.Charset != "" {
		dsn += fmt.Sprintf("?charset=%s", o.Charset)
	}
	return dsn
}
