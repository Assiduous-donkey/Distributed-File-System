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

func init() {
	// 创建日志文件
	logfile,err:= os.OpenFile(logFile, os.O_APPEND|os.O_CREATE,666)
	if err!=nil {
		log.Fatalln("无法创建日志文件")
	}
	defer logfile.Close()
	clientLog=log.New(logfile, "", log.LstdFlags|log.Lshortfile) // 日志文件格式:log包含时间及文件行数
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
        if strings.Compare("mkdir", command[0]) == 0 {
			err:=MakeDirectory(command[1])
			fmt.Println(err)
		} else if strings.Compare("create", command[0]) == 0{
			err:=CreateFile(command[1])
			fmt.Println(err)
		} else if (strings.Compare("q", command[0]) == 0){
			break
		} else if strings.Compare("cd",command[0])==0 {
			err:=ChangeDirectory(command[1])
			fmt.Println(err)
		} else if strings.Compare("rmall",command[0])==0 {
			err:=DeleteDir(command[1])
			fmt.Println(err)
		} else if strings.Compare("rm",command[0])==0 {
			err:=DeleteFile(command[1])
			fmt.Println(err)
		}
    }
}

// 创建目录的RPC
type DirInfo struct {
	Path string
}
type DirReply struct {
	Status bool
	Msg error
}
func MakeDirectory(dirpath string) error{
	fmt.Println("mkdir")
	if curpath!="" {
		dirpath=curpath+"/"+dirpath
	}
	dirinfo:=DirInfo{Path:dirpath}
	var reply DirReply
	err:=masterClient.Call("MasterOptions.MakeDirectory",&dirinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	return nil
}

// 创建文件的RPC
type FileInfo struct {
	Path string
}
type FileReply struct {
	Status bool
}
func CreateFile(filepath string) error{
	fmt.Println("create")
	if curpath!="" {
		filepath=curpath+"/"+filepath
	}
	fileinfo:=FileInfo{Path:filepath}
	var reply FileReply
	err:=masterClient.Call("MasterOptions.CreateFile",&fileinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	return nil
}

// 切换目录的RPC
type CdInfo struct {
	Path string
}
type CdReply struct {
	Status bool
}
func ChangeDirectory(dir string) error{
	fmt.Println("cd")
	if strings.Compare("..",dir)==0 {
		if curpath=="" {
			return nil
		}
		pos:=-1
		for i:=len(curpath)-1;i>=0;i-=1{
			if(curpath[i]=='/') {
				pos=i
				break
			}
		}
		newpath:=""
		for i:=0;i<=pos;i+=1{
			newpath+=string(curpath[i])
		}
		curpath=newpath
		return nil
	} else {
		cdinfo:=CdInfo{Path:curpath+dir}
		var reply CdReply
		err:=masterClient.Call("MasterOptions.ChangeDirectory",&cdinfo,&reply)
		if err!=nil {
			clientLog.Println(err)
			return err
		}
		curpath+=dir
		return nil
	}
}

// 删除目录和文件的RPC
type DelInfo struct {
	Path string
}
type DelReply struct {
	Status bool
}
func DeleteDir(dirpath string) error {
	clientLog.Println("调用DeleteDir")
	if curpath!="" {
		dirpath=curpath+"/"+dirpath
	}
	delinfo:=DelInfo{Path:dirpath}
	var reply DelReply
	err:=masterClient.Call("MasterOptions.DeleteDir",&delinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	return nil
}
func DeleteFile(filepath string) error {
	clientLog.Println("调用DeleteFile")
	if curpath!="" {
		filepath=curpath+"/"+filepath
	}
	delinfo:=DelInfo{Path:filepath}
	var reply DelReply
	err:=masterClient.Call("MasterOptions.DeleteFile",&delinfo,&reply)
	if err!=nil{
		clientLog.Println(err)
		return err
	}
	return nil	
}