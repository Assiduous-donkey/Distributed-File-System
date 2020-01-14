package main

import (
	"log"
	"os"
	"time"
	"net/rpc"
	"bufio"
	"strings"
	// "net/http"
	// "net"
	// "errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
	redisServer="127.0.0.1:6379"
	clientLog *log.Logger
	logFile="client.log"
	masterPort="127.0.0.1:8090"
	node1="127.0.0.1:8081"
	masterClient *rpc.Client
	curpath=""
)

func initPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 		5,			// 最大空闲连接
		MaxActive: 		5,			// 最大激活连接数
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

func init() {
	// 创建日志文件
	logfile,err:= os.OpenFile(logFile, os.O_APPEND|os.O_CREATE,666)
	if err!=nil {
		log.Fatalln("无法创建日志文件")
	}
	clientLog=log.New(logfile,"", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
	// 初始化连接池
	redisPool=initPool(redisServer)
	fmt.Println("正在初始化客户端")
	client,err:=rpc.DialHTTP("tcp","127.0.0.1:8090")
	if err!=nil {
		fmt.Println(err)
	} else {
		masterClient=client
	}
}

func main() {
	// 从控制台读取命令
	reader := bufio.NewReader(os.Stdin)
    for {
        fmt.Print(curpath+"-> ")
		input, _ := reader.ReadString('\n')
		// strings.Replace(s,old,new string, n int) string
		// 返回 将s的前n个字符(n<0时为全部字符)中的old替换为new 得到的新字符串
		input = strings.Replace(input,"\n", "", -1)
		command:=strings.Fields(input)
        if strings.Compare("create", command[0]) == 0{
			err:=CreateServerFile(command[1])
			fmt.Println(err)
		} else if (strings.Compare("q", command[0]) == 0){
			break
		} else if strings.Compare("rm",command[0])==0 {
			err:=DeleteServerFile(command[1])
			fmt.Println(err)
		} else if strings.Compare("read",command[0])==0 {
			err:=ReadServerFile(command[1])
			fmt.Println(err)
		} else if strings.Compare("write",command[0])==0 {
			str:="append"
			content:=[]byte(str)
			err:=WriteServerFile(command[1],os.O_APPEND,content)
			fmt.Println(err)
		} else if strings.Compare("test",command[0])==0 {
			test()
		}
    }
}

// 测试程序
func test() {
	file:="f1.txt"
	content:=[]byte("test")
	go WriteServerFile(file,os.O_APPEND,content)
	go WriteServerFile(file,os.O_APPEND,content)
	go WriteServerFile(file,os.O_APPEND,content)
}

// 创建文件的RPC
type FileInfo struct {
	Path string
}
type FileReply struct {
	LastTime string
}
func CreateServerFile(filepath string) error{
	fmt.Println("create")
	fileinfo:=FileInfo{Path:filepath}
	var reply FileReply
	err:=masterClient.Call("MasterOptions.CreateFile",&fileinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	return nil
}
// 删除文件的RPC
func DeleteServerFile(filepath string) error {
	clientLog.Println("调用DeleteFile")
	delinfo:=FileInfo{Path:filepath}
	var reply FileReply
	err:=masterClient.Call("MasterOptions.DeleteFile",&delinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	os.Remove(filepath)			// 删除本地缓存
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	redisconn.Do("DEL","client_"+filepath)
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
func ReadServerFile(filepath string) error {
	clientLog.Println("调用ReadFile")
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	fileinfo:=ReadFileInfo{Path:filepath}
	lasttime,err:=redis.String(redisconn.Do("GET","client_"+filepath))
	have:=false		//本地有无缓存
	if lasttime!="" {
		have=true
	}
	var reply ReadFileReply
	// 调用master的RPC查询该文件的
	err=masterClient.Call("MasterOptions.ReadFile",&fileinfo,&reply)
	if err!=nil {
		clientLog.Println(err)
		return err
	}

	if have==false || reply.LastTime>lasttime { //服务器上的文件更新
		// 调用文件服务器的RPC下载文件
		err=ReadFileFromServer(filepath,&fileinfo,&reply)
		if err!=nil {
			return err
		}
		//更新本地缓存
		redisconn.Do("SET","client_"+filepath,reply.LastTime)
	} else {	//直接读本地的就好
		fmt.Println("本地文件已是最新文件")
		clientLog.Println("本地文件已是最新文件")
	}
	return nil
}
func ReadFileFromServer(filename string,fileinfo *ReadFileInfo,reply *ReadFileReply) error {
	file,err:=os.Create(filename)
	if err!=nil {
		clientLog.Println(err)
		return err
	}
	fileclient,err:=rpc.DialHTTP("tcp",reply.ServerIP)
	if err!=nil {
		clientLog.Println(err)
		return err
	}
	fileinfo.Offset=0	//从偏移0开始读写
	for {
		err=fileclient.Call("FileServer.ReadFile",&fileinfo,&reply)	//从文件服务器读取文件内容
		fmt.Println(reply.Count)
		if err!=nil {
			clientLog.Println(err)
			return err
		}
		_,err=file.WriteAt(reply.Content[:reply.Count],fileinfo.Offset)	// 按块写入本地缓存文件
		if err!=nil{
			clientLog.Println(err)
			return err
		}
		fileinfo.Offset+=int64(reply.Count)	// 计算下次要写入的位置
		if reply.Count<4096{	//文件已经全部传输完成了
			break
		}
	}
	clientLog.Println("文件："+filename+"下载成功")
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
func WriteServerFile(filepath string,mode int,content []byte) error {
	clientLog.Println("调用WriteFile")
	redisconn:=redisPool.Get()
	defer redisconn.Close()
	fileinfo:=WriteFileInfo{Path:filepath,Mode:mode}
	var reply WriteFileReply
	err:=masterClient.Call("MasterOptions.WriteFile",&fileinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	fileclient,err:=rpc.DialHTTP("tcp",reply.ServerIP)
	if err!=nil {
		clientLog.Println(err)
		return err
	}
	fileinfo.Content=content
	err=fileclient.Call("FileServer.WriteFile",&fileinfo,&reply)
	if err!=nil {
		clientLog.Println(err)
		return err
	}
	return nil
}

// 自定义文件操作的接口
// 包括创建文件、删除文件、读文件和写文件
func CreateFile(filename string) error {	// 创建
	return CreateServerFile(filename)
}
func DeleteFile(filename string) error {	// 删除
	return DeleteServerFile(filename)
}
func ReadFile(filename string,content []byte) (int,error){	// 读文件
	err:=ReadServerFile(filename)	// 从文件服务器下载文件或者采用本地缓存
	if err!=nil {
		clientLog.Println(err)
		return 0,err
	}
	file,_:=os.Open(filename)		// 再在本地读文件
	count,err:=file.Read(content)
	if err!=nil{
		clientLog.Println(err)
		return 0,err
	}
	return count,nil
}
func WriteFile(filename string,mode int,content []byte) error {	//写文件
	return WriteServerFile(filename,mode,content)
}