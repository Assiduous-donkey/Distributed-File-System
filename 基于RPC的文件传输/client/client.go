package main

import (
	"log"
	"net/rpc"
	"os"
	"io"
	"fmt"
)

type FileInfo struct {
	Filename string
	Content []byte
}

func main() {
	list:=os.Args		// 获取命令行参数
	filepath:=list[1]	// 文件路径
	fileinfo,err:=os.Stat(filepath)	//os.Stat获取文件属性
	if err!=nil {
		log.Fatal(err)
		return
	}
	filename:=fileinfo.Name()
	//先与RPC服务器建立连接
	client,err:=rpc.DialHTTP("tcp","127.0.0.1:8080")
	if err!=nil{
		log.Fatal("dailHttp error",err)
		return
	}
	err=GetFile(filename,client)
	if err!=nil{
		log.Fatal(err)
	}
}

func GetFile(filename string,client *rpc.Client) error {
	file,err:=os.Open(filename)
	if err!=nil {
		log.Fatal(err)
		return err
	}
	content:=make([]byte,4096)
	for {
		n,err:=file.Read(content)
		writer:=FileInfo{filename,content[:n]}
		var reply string
		if err==io.EOF {	//文件读完了
			err=client.Call("FileReceive.Upload",&writer,&reply)		// Call 同步调用
			return err
		}
		if err!=nil {
			return err
		}
		err=client.Call("FileReceive.Upload",&writer,&reply)
		if err!=nil {
			return err
		}
		fmt.Println(reply)
	}
	
}