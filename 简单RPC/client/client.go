package main

import (
	"log"
	"net/rpc"
	"time"
	"fmt"
)

type Args struct {
	A,B int
}

func main() {
	//先与RPC服务器建立连接
	client,err:=rpc.DialHTTP("tcp","127.0.0.1:8080")
	if err!=nil{
		log.Fatal("dailHttp error",err)
		return
	}
	args:=Args{7,8}
	var reply int
	// Call 同步调用
	err=client.Call("Arith.Multiply",&args,&reply)
	// fmt.Println(typeof(client))
	if err!=nil{
		log.Fatal("call arith.Multiply error",err)
	}
	fmt.Printf("Arith:%d*%d=%d\n",args.A,args.B,reply)

	args.A,args.B=2,4
	// Go 异步调用
	mulCall:=client.Go("Arith.Multiply",&args,&reply,nil)
	for {
		select {
		case <-mulCall.Done:	//调用得到响应
			fmt.Printf("Arith:%d*%d=%d\n",args.A,args.B,reply)
		default:
			fmt.Println("继续向下执行...")
			time.Sleep(time.Second*2)
		}
	}
}

// 返回参数类型的函数  是我测试用的 跟RPC无关
func typeof(v interface{}) string {
	return fmt.Sprintf("%T",v)
}