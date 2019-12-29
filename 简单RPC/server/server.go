package main

import (
	"net/rpc"
	"time"
	"net"
	"log"
	"net/http"
	"os"
)

type Args struct {
	A,B int
}

type Arith struct {
	
}

// 下面定义的是golang中的方法
// 定义了 Arith 的 Multiply 方法，要求Arith必须由type声明
// Multiply后的()内是参数 若向传递多个参数一般需要结构体 如下面使用了结构体Args
// 目前学到的 似乎最多只能有两个参数 且都是接口类型
// 参数列表之后是该方法的返回值
func (t *Arith) Multiply(args *Args,reply *int) error{
	time.Sleep(time.Second*2)	// 延时 可有可无
	*reply=args.A * args.B
	return nil
}

func main(){
	arith:=new(Arith)		// 创建对象
	rpc.Register(arith)		// 注册对象的RPC服务，公开对象的方法供客户端调用
	rpc.HandleHTTP()		// 指定RPC的传输协议为HTTP
	l,err:=net.Listen("tcp",":8080")	//监听8080端口 TCP协议
	if err!=nil {
		log.Fatal("listen error",err)
	}
	go http.Serve(l,nil)			//创建协程 开启服务
	os.Stdin.Read(make([]byte, 1))		//这个是读取控制台输入 只是为了让server一直运行直到控制台有输入再关闭
}