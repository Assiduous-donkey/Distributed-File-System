package main

import (
	"log"
	"os"
	"time"
	"net/rpc"
	"net/http"
	"net"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
	redisServer="127.0.0.1:6379"
	masterLog *log.Logger
	logFile="master.log"
	masterPort="127.0.0.1:8090"
	node1="127.0.0.1:8081"
	node2="127.0.0.1:8083"
	nodes=[2]string{node1,node2}
	curnode=0
	// node1Client *rpc.Client
)

func initPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 		3,			// 最大空闲连接
		MaxActive: 		3,			// 最大激活连接数
		IdleTimeout:	6*time.Hour,	// 最大的空闲连接等待时间
		Dial: func() (redis.Conn,error) {
			conn,err:=redis.Dial("tcp",server)
			if err!=nil{
				masterLog.Println(err)
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
	masterLog=log.New(logfile, "", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
	// 初始化连接池
	redisPool=initPool(redisServer)
	// 注册RPC服务
	RegisterRpcServer()
	//监听8000端口
	listen,err:=net.Listen("tcp",masterPort)
	if err!=nil {
		masterLog.Fatal("listen error",err)
	}
	defer listen.Close()
	//不需要自定义监听函数 因为我用的是RPC
	fmt.Println("开启目录服务器")
	go http.Serve(listen,nil)	
	os.Stdin.Read(make([]byte, 1))
}

func RegisterRpcServer() {
	rpc.Register(new(MasterOptions))
	rpc.HandleHTTP()
}

//有关目录服务器所有操作的实体
type MasterOptions struct {

}

// 创建文件的RPC
type FileInfo struct {
	Path string
}
type FileReply struct {
	LastTime string
}
func (this *MasterOptions) CreateFile(fileinfo *FileInfo,reply *FileReply) error {
	masterLog.Println("调用CreateFile")
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	// 首先检查有没有该文件
	_,err:=redis.String(redisconn.Do("GET",fileinfo.Path))
	if err==nil {
		return errors.New("文件已存在")
	}
	//没有该文件 则与文件服务器建立连接 调用文件服务器提供的RPC方法
	node:=depatch()
	client,err:=rpc.DialHTTP("tcp",node)
	if err!=nil {
		masterLog.Println(err)
		return err
	}
	sendMessage:=FileInfo{Path:fileinfo.Path}
	err=client.Call("FileServer.CreateFile",&sendMessage,&reply)
	if err!=nil {
		return err
	}
	redisconn.Do("SET",fileinfo.Path,node)
	redisconn.Do("SET","master_"+fileinfo.Path,reply.LastTime)
	return nil
}
// 删除文件的RPC
func (this *MasterOptions) DeleteFile(delinfo *FileInfo,reply *FileReply) error {
	masterLog.Println("调用DeleteFile")
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	node,err:=redis.String(redisconn.Do("GET",delinfo.Path))
	if err!=nil {
		return errors.New("文件不存在")
	}
	// 目录存在 访问目录所在的服务器执行指定的删除操作
	// 先建立连接
	client,err:=rpc.DialHTTP("tcp",node)
	if err!=nil {
		masterLog.Println(err)
		return err
	}
	sendMessage:=FileInfo{Path:delinfo.Path}
	err=client.Call("FileServer.DeleteFile",&sendMessage,&reply)
	if err!=nil {
		return err
	}
	_,err=redisconn.Do("DEL",delinfo.Path)
	if err!=nil {
		masterLog.Println(err)
	}
	return err
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
func (this *MasterOptions) ReadFile(fileinfo *ReadFileInfo,reply *ReadFileReply) error {
	masterLog.Println("调用ReadFile")
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	serverip,err:=redis.String(redisconn.Do("GET",fileinfo.Path))
	if err!=nil{ 	// 文件不存在
		return err
	}
	lasttime,err:=redis.String(redisconn.Do("GET","master_"+fileinfo.Path))
	if err!=nil{	
		return err
	}
	reply.ServerIP=serverip
	reply.LastTime=lasttime
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
func (this *MasterOptions) WriteFile(fileinfo *WriteFileInfo,reply *WriteFileReply) error {
	masterLog.Println("调用WriteFile")
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	serverip,err:=redis.String(redisconn.Do("GET",fileinfo.Path))
	if err!=nil{ 	// 文件不存在
		return err
	}
	reply.ServerIP=serverip
	return nil
}

// 调度文件服务器
func depatch() string {
	node:=nodes[curnode]
	curnode=(curnode+1)%len(nodes)
	return node
}