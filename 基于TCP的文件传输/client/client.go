package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	list:=os.Args		// 获取命令行参数
	filepath:=list[1]	// 文件路径
	fileinfo,err:=os.Stat(filepath)	//os.Stat获取文件属性
	if err!=nil {
		fmt.Println("os.Stat error ",err)
		return
	}
	filename:=fileinfo.Name()

	// 建立连接
	conn,err:=net.Dial("tcp","127.0.0.1:8080")
	if err!=nil{
		fmt.Println("net.Dial error ",err)
		return
	}

	// 向服务器发送信息
	_,err=conn.Write([]byte(filename))
	if err!=nil {
		fmt.Println("conn.Write error ",err)
		return
	}

	// 接收服务器传来的信息
	buf:=make([]byte,4096)
	n,err:=conn.Read(buf)
	if err!=nil {
		fmt.Println("conn.Read error ",err)
		return
	}
	if string(buf[:n])=="k" {
		sendFile(conn,filepath)
	}
}

func sendFile(conn net.Conn,filepath string) {
	// 打开要传输的文件
	file,err:=os.Open(filepath)
	if err!=nil {
		fmt.Println("os.Open error",err)
		return
	}
	buf:=make([]byte,4096)
	// 读取文件内容写入远程链接
	for {
		n,err:=file.Read(buf)
		if err==io.EOF {
			fmt.Println("文件读取完毕")
			return
		}
		if err!=nil {
			fmt.Println("file.Read err:",err)
			return
		}
		_,err=conn.Write(buf[:n])
		if err!=nil {
			fmt.Println("conn.Write error ",err)
			return
		}
	}
}