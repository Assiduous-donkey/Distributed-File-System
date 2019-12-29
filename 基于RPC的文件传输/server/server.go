package main

import (
	"net/rpc"
	"fmt"
	"net"
	"log"
	"net/http"
	"os"
)

type FileInfo struct {
	Filename string
	Content []byte
}

type FileReceive struct {
	// Filename string
}

// 可以将文件分块传输
func (fr *FileReceive) Upload(fileinfo *FileInfo,reply *string) error{
	// O_CREATE：如果文件不存在则先创建文件
	// O_APPEND：往文件中添加
	// OpenFile最后一个参数 mode：设置文件权限 一般是0666 表示可读可写 这个函数是unix风格的函数
	file,err:=os.OpenFile(fileinfo.Filename,os.O_CREATE|os.O_APPEND,0666)
	if err!=nil{
		return err 
	} else {
		fmt.Println(fileinfo.Filename)
	}
	file.Write(fileinfo.Content[:])	// 写文件
	*reply="ok"
	file.Close()					// 记得关闭文件 好习惯
	return nil
}

func main(){
	fileReceive:=new(FileReceive)		// 创建对象
	rpc.Register(fileReceive)			// 注册RPC服务
	rpc.HandleHTTP()					// 采用HTTP协议传输
	listen,err:=net.Listen("tcp",":8080")
	if err!=nil {
		log.Fatal("listen error",err)
	}
	go http.Serve(listen,nil)			
	os.Stdin.Read(make([]byte, 1))		//这个是读取控制台输入 只是为了让server一直运行直到控制台有输入再关闭
}