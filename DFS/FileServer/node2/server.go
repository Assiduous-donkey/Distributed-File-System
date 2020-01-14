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
	"strconv"
	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
	redisServer="127.0.0.1:6379"
	serverLog *log.Logger
	masterPort="127.0.0.1:8090"
	logFile="node2.log"
	serverPort=":8083"
	backupPort="127.0.0.1:8084"
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
	backupServer,err:=rpc.DialHTTP("tcp",backupPort)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	err=backupServer.Call("BackupServer.CreateFile",fileinfo,reply)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
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
	backupServer,err:=rpc.DialHTTP("tcp",backupPort)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	backupServer.Call("BackupServer.DeleteFile",delinfo,reply)
	err=os.Remove(delinfo.Path)
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
	needBackup:=false
	if os.IsNotExist(err) {	// 主文件文件不存在 读取备份服务器的
		serverLog.Println(err)
		needBackup=true
	}
	// 检查是否有写锁
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	is,err:=redis.String(redisconn.Do("GET","lock_"+fileinfo.Path))
	if is!="" {	//有写锁
		err=errors.New("文件正在被写")
		serverLog.Println(err)
		return err
	}
	// 检查是否有用户准备写
	is,err=redis.String(redisconn.Do("GET","noread_"+fileinfo.Path))
	if is!="" {
		err=errors.New("有写者在等待")
		serverLog.Println(err)
		return err	
	}
	// 没事了 可以读文件了 先加个读锁 要互斥加锁 采用事务
	redisconn.Send("MULTI")
	value,err:=redis.String(redisconn.Do("GET","read_"+fileinfo.Path))
	if value=="" {	//无锁
		redisconn.Do("SET","read_"+fileinfo.Path,"1")
	} else {
		intvalue,_:=strconv.Atoi(value)
		intvalue+=1
		value=strconv.Itoa(intvalue)
		redisconn.Do("SET","read_"+fileinfo.Path,value)
	}
	redisconn.Do("EXEC")
	// 正式读文件
	// 获取文件大小 因为ReadAt函数在buffer容量大于剩余的要读取的字节数时会出错
	if needBackup==true {	// 文件服务器无文件 需要先从备份服务器下载
		err=DownloadFile(fileinfo.Path)
		if err!=nil {
			serverLog.Println(err)
			return err
		}
	}
	filesize:=filemsg.Size()
	content:=make([]byte,4096)
	count:=0
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
	// 读好文件 要解锁 仍然需要事务操作
	redisconn.Send("MULTI")
	value,err=redis.String(redisconn.Do("GET","read_"+fileinfo.Path))
	if value=="" {	//无锁
		return err
	} else {
		intvalue,_:=strconv.Atoi(value)
		if intvalue<=0 {
			redisconn.Do("DEL","read_"+fileinfo.Path)
		} else {
			value=strconv.Itoa(intvalue-1)
			redisconn.Do("SET","read_"+fileinfo.Path,value)
		}
	}
	redisconn.Do("EXEC")
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
	redisconn:=redisPool.Get()	//redis连接
	defer redisconn.Close()
	// 采用SETNX方式 互斥写入键值对
	lock,err:=redisconn.Do("SETNX","write_"+fileinfo.Path,"yes")
	if lock==0{	// 有锁
		err=errors.New("文件正在被写入")
		serverLog.Println(err)
		return err
	}
	// 检查是否有用户正在读 即要写的文件正在被读的过程
	redisconn.Do("SET","noread_"+fileinfo.Path,"yes")	// 不让在写请求之后收到的读请求再读取这个文件
	for {	// 待当前在读的用户读完
		remainder,_:=redis.String(redisconn.Do("GET","read_"+fileinfo.Path))
		if remainder==""{
			break
		}
	}
	// 正式写文件
	// 先写备份服务器
	backupServer,err:=rpc.DialHTTP("tcp",backupPort)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	err=backupServer.Call("BackupServer.WriteFile",fileinfo,reply)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	// 再写本地文件服务器
	file,err:=os.OpenFile(fileinfo.Path,fileinfo.Mode,0777)
	if err!=nil{	// 本地服务器无该文件 则先从备份服务器下载文件
		err=DownloadFile(fileinfo.Path)
		if err!=nil {
			serverLog.Println(err)
			return err
		}
	} else {	// 直接写本地文件
		defer file.Close()
		reply.Count,err=file.Write(fileinfo.Content)
		if err!=nil {
			serverLog.Println(err)
			return err
		}	
	}
	// 成功写文件后更新时间戳
	// 与目录服务器共享同一个redis服务器 所以直接更新
	newtime:=time.Now().Format("2006/1/2 15:04:05")
	_,err=redisconn.Do("SET","master_"+fileinfo.Path,newtime)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	redisconn.Do("DEL","write_"+fileinfo.Path)
	redisconn.Do("DEL","noread_"+fileinfo.Path)
	return nil
}

// 从备份服务器下载文件
func DownloadFile(filename string) error {
	// 本地缓存的文件名以文件在文件系统中的路径命名
	// filename=strings.Replace(filename,"/","_",-1)
	file,err:=os.Create(filename)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	fileclient,err:=rpc.DialHTTP("tcp",backupPort)
	if err!=nil {
		serverLog.Println(err)
		return err
	}
	
	// 下载文件
	fileinfo:=ReadFileInfo{Path:filename}
	fileinfo.Offset=0	//从偏移0开始读写
	var reply *ReadFileReply
	for {
		err=fileclient.Call("BackupServer.ReadFile",&fileinfo,&reply)
		fmt.Println(reply.Count)
		if err!=nil {
			serverLog.Println(err)
			return err
		}
		_,err=file.WriteAt(reply.Content[:reply.Count],fileinfo.Offset)
		if err!=nil{
			serverLog.Println(err)
			return err
		}
		fileinfo.Offset+=int64(reply.Count)
		if reply.Count<4096{	//文件已经全部传输完成了
			break
		}
	}
	serverLog.Println("从备份服务器下载文件："+filename)
	return nil
}