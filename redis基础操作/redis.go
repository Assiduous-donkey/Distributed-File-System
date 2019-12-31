package main

import (
	"fmt"
	"time"
	"log"
	"os"
	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
	redisServer="127.0.0.1:6379"
	masterLog *log.Logger
)

func main() {
	redisPool=initPool(redisServer)
	// test()
	// 创建日志文件
	logfile,err:= os.OpenFile("master.log", os.O_APPEND|os.O_CREATE,666)
	if err!=nil {
		log.Fatalln("无法创建日志文件")
	}
	defer logfile.Close()
	masterLog=log.New(logfile, "", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
	test()
	test()
}

func test() {

	// 连接Redis服务器
	connect:=redisPool.Get()	//Get获取连接池里的连接
	defer connect.Close()
	// redis的set操作
	fmt.Println(typeof(connect))
	_,err:=connect.Do("SET","go","123")
	if err!=nil {
		fmt.Println("set操作失败: ",err)
	}

	value,err:=redis.String(connect.Do("GET","go"))
	if err!=nil {
		fmt.Println("get操作失败: ",err)
	} else {
		fmt.Println("key: go  ;  value: ",value)
	}
}

func initPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 		3,			// 最大空闲连接
		MaxActive: 		3,			// 最大激活连接数
		IdleTimeout:	6*time.Hour,	// 最大的空闲连接等待时间
		Dial: func() (redis.Conn,error) {
			conn,err:=redis.Dial("tcp",server)
			fmt.Println("建立连接")
			if err!=nil{
				return nil,err
			}
			return conn,err
		},
	}
}

// 返回参数类型的函数  是我测试用的 跟RPC无关
func typeof(v interface{}) string {
	return fmt.Sprintf("%T",v)
}