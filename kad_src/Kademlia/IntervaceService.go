package Kademlia

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"time"
)

func (ser *KademliaServer) Run()  {
	logrus.Infof("<Run> start in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	ser.RealServer = rpc.NewServer()
	err := ser.RealServer.Register(&ser.NodeNetWrapper)
	if err != nil{
		fmt.Printf("<Run> Register fail error: %s\n",err.Error())
		logrus.Errorf("<Run> Register fail error: %s",err.Error())
		return
	}
	ser.RealListener , err = net.Listen("tcp",ser.NodeNetWrapper.RealNode.Addr)
	if err != nil{
		fmt.Printf("<Run> Listen Fail error: %s\n",err.Error())
		logrus.Errorf("<Run> Listen Fail error: %s",err.Error())
		return
	}
	ser.NodeNetWrapper.RealNode.IsListening = true ;
	go ser.RealServer.Accept(ser.RealListener)
	logrus.Infof("<Run> Successfully run\n")
}

func (ser *KademliaServer) Create()  {
	logrus.Infof("<Create> service in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	ser.NodeNetWrapper.RealNode.AllData = make(map[string]string)
	ser.NodeNetWrapper.RealNode.ExpireTime = make(map[string]time.Time)
	ser.NodeNetWrapper.RealNode.FlushTime = make(map[string]time.Time)
	go ser.NodeNetWrapper.RealNode.Maintain()
	logrus.Infof("<Create> success in addr %s",ser.NodeNetWrapper.RealNode.Addr)
}

func (ser *KademliaServer) Join( addr string ) bool {
	logrus.Infof("<Join> service in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	ser.NodeNetWrapper.RealNode.AllData = make(map[string]string)
	ser.NodeNetWrapper.RealNode.ExpireTime = make(map[string]time.Time)
	ser.NodeNetWrapper.RealNode.FlushTime = make(map[string]time.Time)
	BucketNum := GetBucketNum(ser.NodeNetWrapper.RealNode.HashCode,Get_hash_code(addr))
	ser.NodeNetWrapper.RealNode.tableLock.Lock()
	ser.NodeNetWrapper.RealNode.TableBucket[BucketNum].Push_back(addr)
	ser.NodeNetWrapper.RealNode.tableLock.Unlock()
	ser.NodeNetWrapper.RealNode.LookUpCloseNode(ser.NodeNetWrapper.RealNode.Addr) // create nearby network
	go ser.NodeNetWrapper.RealNode.Maintain()
	logrus.Infof("<Join> success in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	return true
}

func (ser *KademliaServer) Quit()  { // todo normal quit -> check if the flush list has self / change k size / look up change /
	ser.ForceQuit()
//	ser.NodeNetWrapper.RealNode.LookUpCloseNode(ser.NodeNetWrapper.RealNode.Addr)
//	ser.NodeNetWrapper.RealNode.SelfFlush()
}

func (ser *KademliaServer) ForceQuit()  {
	logrus.Infof("<Quit> service in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	ser.NodeNetWrapper.RealNode.IsListening = false
	err := ser.RealListener.Close()
	if err != nil{
//		logrus.Error("<Quit> Close error: %s",err.Error())
//		fmt.Printf("<Quit> Close error: %s\n",err.Error())
		return
	}
	logrus.Infof("<Quit> success in addr %s",ser.NodeNetWrapper.RealNode.Addr)
}

func (ser *KademliaServer) Ping( addr string ) bool {
	return CheckClientRunning(addr)
}

func (ser *KademliaServer) Put( key string , value string ) bool {
	ser.NodeNetWrapper.RealNode.Store(key,value)
	searchBucket := ser.NodeNetWrapper.RealNode.LookUpCloseNode(key)
	for i := 0 ; i < searchBucket.Size ; i++{
		client , _ := GetClient(searchBucket.RealAddr[i])
		if client == nil{
			fmt.Printf("<Put> Fail to get client addr %s\n",searchBucket.RealAddr[i])
			logrus.Errorf("<Put> Fail to get client addr %s",searchBucket.RealAddr[i])
			continue
		}
		output := OutputPKG{}
		err := client.Call("Wrapper.Store",InputPKG{Addr: ser.NodeNetWrapper.RealNode.Addr,Key: key,Val: value,HashCode: ser.NodeNetWrapper.RealNode.HashCode},&output)
		if err != nil{
			fmt.Printf("<Put> Fail to call client, error:%s\n",err.Error())
			logrus.Errorf("<Put> Fail to call client, error: %s",err.Error())
		}
		client.Close()
	}
	return true
}

func (ser *KademliaServer) Get(key string) (bool,string) {
	logrus.Infof("<Get> service start in addr %s",ser.NodeNetWrapper.RealNode.Addr)
	ok , val := ser.NodeNetWrapper.RealNode.Find_Value(key)
	if ok{
		logrus.Infof("<Get> success in addr %s key %s",ser.NodeNetWrapper.RealNode.Addr,key)
		return ok , val
	}
	searchBucket := ser.NodeNetWrapper.RealNode.LookUpCloseNode(key)
	for i := 0 ; i < searchBucket.Size ; i++{
		client , _ := GetClient(searchBucket.RealAddr[i])
		if client == nil{
			fmt.Printf("<Get> Fail to get client %s\n",searchBucket.RealAddr[i])
			logrus.Errorf("<Get> Fail to get client %s",searchBucket.RealAddr[i])
			continue
		}
		input := InputPKG{Addr: ser.NodeNetWrapper.RealNode.Addr,Key: key}
		output := OutputPKG{}
		err := client.Call("Wrapper.Find_Value",input,&output)
		if err != nil{
			fmt.Printf("<Get> Fail to call func in addr %s\n",searchBucket.RealAddr[i])
			logrus.Errorf("<Get> Fail to call func in addr %s\n",searchBucket.RealAddr[i])
			continue
		}
		if output.Val != ""{
			logrus.Infof("<Get> success in addr %s key %s",searchBucket.RealAddr[i],key)
			return true , output.Val
		}
	}
	logrus.Errorf("<Get> Fail to get key %s in addr %s",key,ser.NodeNetWrapper.RealNode.Addr)
	fmt.Printf("<Get> Fail to get key %s in addr %s\n",key,ser.NodeNetWrapper.RealNode.Addr)
	return false , ""
}

func (ser *KademliaServer) Delete( key string ) bool {
	return true
}