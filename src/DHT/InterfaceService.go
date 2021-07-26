package DHT

import (
	"github.com/sirupsen/logrus"
	"log"
	"net"
	"net/rpc"
)

func (ser *ChordServer) Run() {
	logrus.Infof("<Run> Service start in addr %s", ser.nodeNetWrapper.RealNode.Addr)
	ser.realServer = rpc.NewServer()
	err := ser.realServer.Register(&ser.nodeNetWrapper)
	if err != nil {
		panic("Run fail: register fail")
	}
	ser.realListener, err = net.Listen("tcp", ser.nodeNetWrapper.RealNode.Addr)
	if err != nil {
		log.Panicf("<Run> register fail in addr %s", ser.nodeNetWrapper.RealNode.Addr)
		return
		//		panic("Run fail: register fail")
	}
	ser.nodeNetWrapper.RealNode.IsListening = true
	go ser.realServer.Accept(ser.realListener) // 单开进程监听端口
	logrus.Infof("<Run> Service finished successfully")
}

func (ser *ChordServer) Create() { // a non-lazy set_up
	logrus.Infof("<Create> Service start in addr %s", ser.nodeNetWrapper.RealNode.Addr)
	ser.nodeNetWrapper.RealNode.AllData = make(map[string]string) // 初始化 allData
	ser.nodeNetWrapper.RealNode.Backup = make(map[string]string) // initialize Backup

	for i := 0; i < SuccessorLen; i++ { // 初始化有效后继
		ser.nodeNetWrapper.RealNode.SuccessorList[i] = ser.nodeNetWrapper.RealNode.Addr
	}
	ser.nodeNetWrapper.RealNode.Predecessor = ser.nodeNetWrapper.RealNode.Addr // 初始化前继
	ser.nodeNetWrapper.RealNode.Next = 0
	ser.nodeNetWrapper.RealNode.Maintain()
}

func (ser *ChordServer) Join(addr string) bool {
	logrus.Infof("<Join> Service start in addr %s -> %s", ser.nodeNetWrapper.RealNode.Addr, addr)
	ser.nodeNetWrapper.RealNode.AllData = make(map[string]string) // 初始化 allData
	var successorID *string
	successorID = new(string)
	//	ser.nodeNetWrapper.RealNode.FindSuccessor(Get_hash_code(addr),successorID)
	client, _ := GetClient(addr)
	var otherData *ChordNode = &ChordNode{}
	err := client.Call("Wrapper.AllDataDeliver", 0, otherData)
	client.Close()
	if err != nil || otherData.Addr == "" { // 添加失败 不允许出现的情况
		logrus.Infof("<Join> Service fail in addr %s -> %s", ser.nodeNetWrapper.RealNode.Addr, addr)
		return false
	}
	otherData.FindSuccessor(ser.nodeNetWrapper.RealNode.HashCode, successorID) // 此处的 otherData 并不是真的后继
	real_client, _ := GetClient(*successorID)
	err = real_client.Call("Wrapper.AllDataDeliver", 0, otherData) // todo map here cannot lock up
	real_client.Close()
	if err != nil {
		logrus.Errorf("<Join> Service fail in addr %s -> %s successor: %s", ser.nodeNetWrapper.RealNode.Addr, addr, *successorID)
		return false
	}
	//	ser.nodeNetWrapper.RealNode.FingerTable = otherData.FingerTable // todo 不允许初始化 finger table
	ser.nodeNetWrapper.RealNode.FingerTable[0] = otherData.Addr
	ser.nodeNetWrapper.RealNode.SuccessorList[0] = otherData.Addr
	ser.nodeNetWrapper.RealNode.Backup = otherData.Backup // initialize the backup
	otherData.Backup = make(map[string]string)
	for i := 1; i < SuccessorLen; i++ { // 初始化后继表
		ser.nodeNetWrapper.RealNode.SuccessorList[i] = otherData.SuccessorList[i-1] // 错位一个
	} // todo forget to receive data from the successor
	for key , val := range otherData.AllData{
		if IsBetween(ser.nodeNetWrapper.RealNode.HashCode,otherData.HashCode,Get_hash_code(key),true){
			continue
		}else{ // data transfer
			ser.nodeNetWrapper.RealNode.AllData[key] = val
			delete(otherData.AllData,key)
			otherData.Backup[key] = val
		}
	}
	ser.nodeNetWrapper.RealNode.Predecessor = otherData.Predecessor // 初始化前继
	ser.nodeNetWrapper.RealNode.Next = 0
	ser.nodeNetWrapper.RealNode.Maintain() // maintain 开始
	logrus.Infof("<Join> Service finished successfully in addr %s -> %s  successor: %s", ser.nodeNetWrapper.RealNode.Addr, addr, *successorID)
	return true
}

func (ser *ChordServer) Quit() {
	//deliverData := &SmallData{}
	//deliverData.Copy(ser.nodeNetWrapper.RealNode)
	//ser.ShutDown()
	//pre_client, _ := GetClient(ser.nodeNetWrapper.RealNode.Predecessor)
	//if pre_client == nil {
	//	logrus.Errorf("<Quit> fail to update predecessor information in %s", ser.nodeNetWrapper.RealNode.Addr)
	//	return // todo pre_client -> invalid
	//}
	//pre_client.Call("Wrapper.ChangeSuccList", *deliverData, nil)
	//post_client, _ := GetClient(ser.nodeNetWrapper.RealNode.SuccessorList[0])
	//if post_client == nil {
	//	logrus.Errorf("<Quit> fail to update predecessor information in %s", ser.nodeNetWrapper.RealNode.Addr)
	//	return
	//}
	//post_client.Call("Wrapper.ReceivePreData", *deliverData, nil)
	//ser.nodeNetWrapper.RealNode.AllData = make(map[string]string)
	//logrus.Infof("<Quit> finished successfully in addr %s", ser.nodeNetWrapper.RealNode.Addr)
	ser.ShutDown()
	post_client , _ := GetClient(ser.nodeNetWrapper.RealNode.SuccessorList[0])
	if post_client == nil{
		return
	}
	var uselessInt int
	post_client.Call("Wrapper.CheckPredecessorOnline",0,&uselessInt)
	pre_client , _ := GetClient(ser.nodeNetWrapper.RealNode.Predecessor)
	if pre_client == nil{
		return
	}
	pre_client.Call("Wrapper.Stabilize",0,&uselessInt)
	logrus.Infof("<Quit> force quit in addr %s" , ser.nodeNetWrapper.RealNode.Addr)
//	time.Sleep(MaintainPeriod*3)
}

func (ser *ChordServer) ForceQuit() { // todo 所有函数需要 listening 特判 listening 结束后调用 address 禁止任何操作
	ser.ShutDown()
	logrus.Infof("<Quit> force quit in addr %s",ser.nodeNetWrapper.RealNode.Addr)
}

func (ser *ChordServer) Ping(addr string) bool {
	return CheckClientRunning(addr)
}

func (ser *ChordServer) Put(key string, value string) bool {
	var succ *string = new(string)
	ser.nodeNetWrapper.RealNode.FindSuccessor(Get_hash_code(key), succ)
	client, _ := GetClient(*succ)
	if client == nil { // todo put find nil client ???
		logrus.Errorf("<Put> fail to find the valid successor")
		return false
//		panic("Put fail: no such client")
	}
	var bool_succ *bool = new(bool)
	client.Call("Wrapper.PutVal", KVpair{key, value}, bool_succ)
	return *bool_succ
}

func (ser *ChordServer) Get(key string) (bool, string) {
	logrus.Infof("<Get> Service start find key %s in addr %s", key, ser.nodeNetWrapper.RealNode.Addr)
	var succ *string = new(string)
	ser.nodeNetWrapper.RealNode.FindSuccessor(Get_hash_code(key), succ)
	logrus.Infof("<Get> locate key %s in addr %s", key, *succ)
	client, _ := GetClient(*succ)
	if client == nil {
		logrus.Errorf("<Get> fail to connect client in addr %s to %s",ser.nodeNetWrapper.RealNode.Addr,*succ)
		return false, ""
//		panic("Get fail: no such client")
	}
	var ans *string = new(string)
	client.Call("Wrapper.GetVal", KVpair{key, ""}, ans)
	if *ans == "" {
		logrus.Warningf("<Get> find key %s fail in addr %s", key, ser.nodeNetWrapper.RealNode.Addr)
		return false, ""
	}
	logrus.Infof("<Get> find key %s in addr %s", key, ser.nodeNetWrapper.RealNode.Addr)
	return true, *ans
}

func (ser *ChordServer) Delete(key string) bool {
	logrus.Infof("<Delete> Service start delete key %s in addr %s",key,ser.nodeNetWrapper.RealNode.Addr)
	var succ *string = new(string)
	ser.nodeNetWrapper.RealNode.FindSuccessor(Get_hash_code(key), succ)
	logrus.Infof("<Delete> locate key %s in addr %s",key,*succ)
	client, _ := GetClient(*succ)
	if client == nil {
		logrus.Errorf("<Delete> delete key %s fail to connect client in addr %s to %s",key,ser.nodeNetWrapper.RealNode.Addr,*succ)
		return false
//		panic("Delete fail: no such client")
	}
	var bool_succ *bool = new(bool)
	client.Call("Wrapper.DeleteVal", KVpair{key, ""}, bool_succ)
	logrus.Infof("<Delete> delete key -> %t in addr %s",*bool_succ,*succ)
	return *bool_succ
}
