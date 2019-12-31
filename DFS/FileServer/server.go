package main

import (
	"log"
	"os"
	"time"
	"errors"
	"net/rpc"
	"net/http"
	"net"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
	redisServer="127.0.0.1:6379"
	serverLog *log.Logger
	masterPort="127.0.0.1:8090"
	logFile="8081.log"
	serverPort=":8081"
)

func initPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 		3,			// 最大空闲连接
		MaxActive: 		3,			// 最大激活连接数
		IdleTimeout:	6*time.Hour,	// 最大的空闲连接等待时间
		Dial: func() (redis.Conn,error) {
			conn,err:=redis.Dial("tcp",server)
			if err!=nil{
				return nil,err
			}
			return conn,err
		},
	}
}

func main() {
	// 创建日志文件
	logfile,err:= os.OpenFile(logFile, os.O_APPEND|os.O_CREATE,666)
	if err!=nil {
		log.Fatalln("无法创建日志文件")
	}
	defer logfile.Close()
	serverLog=log.New(logfile, "", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
	// 初始化连接池
	redisPool=initPool(redisServer)
	//注册RPC服务
	RegisterRpcServer()
	//监听8000端口
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

// 注册RPC服务器
func RegisterRpcServer() {
	rpc.Register(new(FileServer))
	rpc.HandleHTTP()
}

// 所有文件服务器操作的实体
type FileServer struct {

}

// 创建目录的RPC
type DirInfo struct {
	Path string
}
type DirReply struct {
	Status bool
}
func (this *FileServer) MakeDirectory(dirinfo *DirInfo,reply *DirReply) error {
	serverLog.Println("调用MakeDirectory")
	err:=os.Mkdir(dirinfo.Path,os.ModePerm)
	if err!=nil {
		serverLog.Println(err)
		reply.Status=false
		return errors.New("创建目录失败")
	}
	reply.Status=true
	serverLog.Println("创建目录："+dirinfo.Path)
	return nil
}

// 创建文件的RPC
type FileInfo struct {
	Path string
}
type FileReply struct {
	LastTime string
}
func (this *FileServer) CreateFile(fileinfo *FileInfo,reply *FileReply) error {
	serverLog.Println("调用CreateFile")
	file,err:=os.Create(fileinfo.Path)
	if err!=nil {
		return err
	}
	defer file.Close()
	reply.LastTime=time.Now().Format("1999-01-24 00:00:00")
	return nil
}

// 删除目录的RPC 和 删除文件的RPC
type DelInfo struct {
	Path string
}
type DelReply struct {
	Status bool
}
func (this *FileServer) DeleteDir(delinfo *DelInfo,reply *DelReply) error {
	serverLog.Println("调用DeleteDir")
	err:=os.RemoveAll(delinfo.Path)
	if err!=nil {
		return err
	}
	return nil
}
func (this *FileServer) DeleteFile(delinfo *DelInfo,reply *DelReply) error {
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
func (this *FileServer) ReadFile(fileinfo *ReadFileInfo,reply *ReadFileReply) error {
	serverLog.Println("调用ReadFile")
	// 先不实现锁机制
	filemsg,err:=os.Stat(fileinfo.Path)
	if os.IsNotExist(err) {
		serverLog.Println(err)
		return err
	}
	// 获取文件大小 因为ReadAt函数在buffer容量大于剩余的要读取的字节数时会出错
	filesize:=filemsg.Size()
	content:=make([]byte,4096)
	count:=0
	file,_:=os.Open(fileinfo.Path)
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
	// serverLog.Println(reply.Content)
	reply.Count=count
	return nil
}