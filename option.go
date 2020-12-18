/*************************************************************************
	> File Name: option.go
	> Author: xiangcai
	> Mail: xiangcai@gmail.com
	> Created Time: 2020年12月17日 星期四 17时18分47秒
*************************************************************************/

package gmysql

import (
	"time"
)

// Option 实例化mysql的参数
type Option struct {
	Addr         string
	Username     string
	Password     string
	Database     string
	Charset      string
	MaxOpenConns int
	MaxIdleConns int
	MaxLifeTime  time.Duration
	MaxIdleTime  time.Duration
	driver       string
}
