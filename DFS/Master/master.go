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
	rpc.Register(new(MasterMakeDir))
	rpc.Register(new(MasterCreateFile))
	rpc.Register(new(MasterCd))
	rpc.HandleHTTP()
}

// 创建目录的RPC
type DirInfo struct {
	Path string
}
type DirReply struct {
	Status bool
}
type MasterMakeDir struct {

}
func (this *MasterMakeDir) MakeDirectory(dirinfo *DirInfo,reply *DirReply) error {
	masterLog.Println("调用MakeDirectory")
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	// 首先检查有没有该目录
	_,err:=redis.String(redisconn.Do("GET",dirinfo.Path))
	if err==nil {
		reply.Status=false
		return errors.New("目录已存在")
	}
	//没有该目录 则与文件服务器建立连接 调用文件服务器提供的RPC方法
	client,err:=rpc.DialHTTP("tcp",node1)
	if err!=nil {
		masterLog.Println(err)
		reply.Status=false
		return err
	}
	sendMessage:=DirInfo{Path:dirinfo.Path}
	err=client.Call("FileMakeDir.MakeDirectory",&sendMessage,&reply)
	if err!=nil {
		return err
	}
	masterLog.Println("成功创建目录: "+dirinfo.Path)
	_,err=redisconn.Do("SET",dirinfo.Path,node1)
	if err!=nil {
		masterLog.Println(err)
	}
	return err
}

// 创建文件的RPC
type FileInfo struct {
	Path string
}
type FileReply struct {
	Status bool
}
type MasterCreateFile struct {

}
func (this *MasterCreateFile) CreateFile(fileinfo *FileInfo,reply *FileReply) error {
	masterLog.Println("调用CreateFile")
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	// 首先检查有没有该文件
	_,err:=redis.String(redisconn.Do("GET",fileinfo.Path))
	if err==nil {
		reply.Status=false
		return errors.New("文件已存在")
	}
	//没有该文件 则与文件服务器建立连接 调用文件服务器提供的RPC方法
	client,err:=rpc.DialHTTP("tcp",node1)
	if err!=nil {
		masterLog.Println(err)
		reply.Status=false
		return err
	}
	sendMessage:=FileInfo{Path:fileinfo.Path}
	err=client.Call("FileCreateFile.CreateFile",&sendMessage,&reply)
	if err!=nil {
		return err
	}
	redisconn.Do("SET",fileinfo.Path,node1)
	return nil
}

// 切换目录的RPC
type CdInfo struct {
	Path string
}
type CdReply struct {
	Status bool
}
type MasterCd struct {

}
func (this *MasterCd) ChangeDirectory(cdinfo *CdInfo,reply *CdReply) error {
	masterLog.Println("调用ChangeDirectory")
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	// 检查有没有该目录
	_,err:=redis.String(redisconn.Do("GET",cdinfo.Path))
	if err!=nil {
		reply.Status=false
		return errors.New("目录不存在")
	}
	reply.Status=true
	return nil
}