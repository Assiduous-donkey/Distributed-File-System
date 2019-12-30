package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func main() {
	// 连接Redis服务器
	conn,err:=redis.Dial("tcp","127.0.0.1:6379")
	if err!=nil {
		fmt.Println("连接失败： ",err)
		return
	}
	defer conn.Close()
	// redis的set操作
	_,err=conn.Do("SET","go","dfs")
	if err!=nil {
		fmt.Println("set操作失败: ",err)
	}

	value,err:=redis.String(conn.Do("GET","go"))
	if err!=nil {
		fmt.Println("get操作失败: ",err)
	} else {
		fmt.Println("key: go  ;  value: ",value)
	}
}