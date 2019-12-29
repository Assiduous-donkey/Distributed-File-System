package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	listen,err:=net.Listen("tcp","127.0.0.1:8080")
	if err!=nil {
		fmt.Println("net.Listen error: ",err)
		return
	}
	// 实际上就是套接字socket
	// 监听
	conn,err:=listen.Accept()
	if err!=nil {
		fmt.Println("listen.Accept error ",err)
		return
	}

	// 读取文件名
	buf:=make([]byte,4096)
	n,err:=conn.Read(buf)
	if err!=nil {
		fmt.Println("conn.Read error:",err)
		return
	}
	filename:=string(buf[:n])
	fmt.Println("filename: ",filename)
	if filename != "" {
		_,err=conn.Write([]byte("ok"))
		if err!=nil{
			fmt.Println("conn.Write error:",err)
			return
		}
	} else {
		return
	}

	// 创建文件并写入内容
	file,err:=os.Create(filename)
	if err!=nil {
		fmt.Println("os.Create error: ",err)
		return
	}

	for {
		n,err:=conn.Read(buf)
		if n==0 {
			fmt.Println("文件读取完毕")
			break
		}
		if err!=nil {
			fmt.Println("conn.Read error:",err)
			return
		}
		file.Write(buf[:n])
	}
}