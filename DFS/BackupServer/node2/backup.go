package main

import (
	"fmt"
	"log"
	"os"
	"net/rpc"
	"net/http"
	"net"
)

var (
	serverLog *log.Logger
	fileServer="127.0.0.1:8083"
	logFile="backup2.log"
	serverPort=":8084"
)

func main() {
	logfile,err:=os.OpenFile(logFile,os.O_APPEND|os.O_CREATE,666)
	if err!=nil {
		log.Fatalln("无法创建日志文件")
	}
	defer logfile.Close()
	serverLog=log.New(logfile, "", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
	//注册RPC服务
	RegisterRpcServer()
	//监听端口
	listen,err:=net.Listen("tcp",serverPort)
	if err!=nil {
		serverLog.Fatal("listen error",err)
	}
	defer listen.Close()
	//不需要自定义监听函数 因为我用的是RPC
	fmt.Println("开启文件服务器")
	go http.Serve(listen,nil)
	os.Stdin.Read(make([]byte, 1))	
}
func RegisterRpcServer() {
	rpc.Register(new(BackupServer))
	rpc.HandleHTTP()
}

// 提供RPC服务
type BackupServer struct {

}

type FileInfo struct {
	Path string
}
type FileReply struct {
	LastTime string
}
// 创建文件的RPC
func (this *BackupServer) CreateFile(fileinfo *FileInfo,reply *FileReply) error {
	serverLog.Println("调用CreateFile")
	file,err:=os.Create(fileinfo.Path)
	if err!=nil {
		return err
	}
	defer file.Close()
	return nil
}
// 删除文件的RPC
func (this *BackupServer) DeleteFile(delinfo *FileInfo,reply *FileReply) error {
	serverLog.Println("调用DeleteFile")
	err:=os.Remove(delinfo.Path)
	if err!=nil {
		return err
	}
	return nil
}

// 读文件的RPC
type ReadFileInfo struct {
	Path string
	Offset int64
}
type ReadFileReply struct {
	ServerIP string
	LastTime string
	Content []byte
	Count 	int
}
func (this *BackupServer) ReadFile(fileinfo *ReadFileInfo,reply *ReadFileReply) error {
	serverLog.Println("调用ReadFile")
	filemsg,err:=os.Stat(fileinfo.Path)
	if os.IsNotExist(err) {
		serverLog.Println(err)
		return err
	}
	filesize:=filemsg.Size()
	content:=make([]byte,4096)
	count:=0
	// 正式读文件
	file,_:=os.Open(fileinfo.Path)
	defer file.Close()
	if fileinfo.Offset+4096>=filesize{
		_,err=file.Read(content)
		if err!=nil{
			return err
		}
		count=int(filesize-fileinfo.Offset)
	} else {
		count,err=file.ReadAt(content,fileinfo.Offset)
		if err!=nil {
			serverLog.Println(err)
			return err
		}
	}
	reply.Content=content
	reply.Count=count
	return nil
}

// 写文件的RPC
type WriteFileInfo struct {
	Path string
	Mode int
	Content []byte
}
type WriteFileReply struct {
	ServerIP string
	Count int
}
func (this *BackupServer) WriteFile(fileinfo *WriteFileInfo,reply *WriteFileReply) error{
	serverLog.Println("调用WriteFile")
	file,err:=os.OpenFile(fileinfo.Path,fileinfo.Mode,0777)
	if err!=nil{
		serverLog.Println(err)
		return err
	}
	defer file.Close()
	reply.Count,err=file.Write(fileinfo.Content)
	if err!=nil {
		serverLog.Println(err)
		return err
	}	
	return nil
}