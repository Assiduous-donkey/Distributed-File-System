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
	reply.LastTime=time.Now().Format("2006/1/2 15:04:05")
	return nil
}
// 删除文件的RPC
func (this *FileServer) DeleteFile(delinfo *FileInfo,reply *FileReply) error {
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
	filemsg,err:=os.Stat(fileinfo.Path)
	if os.IsNotExist(err) {
		serverLog.Println(err)
		return err
	}
	// 获取文件大小 因为ReadAt函数在buffer容量大于剩余的要读取的字节数时会出错
	filesize:=filemsg.Size()
	content:=make([]byte,4096)
	count:=0
	// 检查是否有写锁
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	_,err=redis.String(redisconn.Do("GET","lock_"+fileinfo.Path))
	if err==nil {	//有写锁
		err=errors.New("文件正在被写")
		serverLog.Println(err)
		return err
	}
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
func (this *FileServer) WriteFile(fileinfo *WriteFileInfo,reply *WriteFileReply) error {
	serverLog.Println("调用WriteFile")
	// 检查是否有写锁
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	_,err:=redis.String(redisconn.Do("GET","lock_"+fileinfo.Path))
	if err==nil {	//有写锁
		err=errors.New("文件正在被写")
		serverLog.Println(err)
		return err
	}
	// 无写锁 则新建写锁
	redisconn.Do("SET","lock_"+fileinfo.Path,"yes")
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
	// 成功写文件后更新时间戳
	// 与目录服务器共享同一个redis服务器 所以直接更新
	newtime:=time.Now().Format("2006/1/2 15:04:05")
	_,err=redisconn.Do("SET","master_"+fileinfo.Path,newtime)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	redisconn.Do("DEL","lock_"+fileinfo.Path)
	return nil
}
