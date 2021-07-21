package DHT

import (
	"github.com/sirupsen/logrus"
	"log"
	"net"
	"net/rpc"
)

func (ser *ChordServer) Run() {
	logrus.Infof("<Run> Service start in addr %s", ser.nodeNetWrapper.realNode.Addr)
	ser.realServer = rpc.NewServer()
	err := ser.realServer.Register(&ser.nodeNetWrapper)
	if err != nil {
		panic("Run fail: register fail")
	}
	ser.realListener, err = net.Listen("tcp", ser.nodeNetWrapper.realNode.Addr)
	if err != nil {
		log.Panicf("<Run> register fail in addr %s", ser.nodeNetWrapper.realNode.Addr)
		return
		//		panic("Run fail: register fail")
	}
	ser.nodeNetWrapper.realNode.IsListening = true
	go ser.realServer.Accept(ser.realListener) // 单开进程监听端口
	logrus.Infof("<Run> Service finished successfully")
}

func (ser *ChordServer) Create() { // a non-lazy set_up
	logrus.Infof("<Create> Service start in addr %s", ser.nodeNetWrapper.realNode.Addr)
	ser.nodeNetWrapper.realNode.AllData = make(map[string]string) // 初始化 allData
	//for i := 0 ; i < RingSize ; i++ { // 初始化 finger table
	//	ser.nodeNetWrapper.realNode.FingerTable[i] = ser.nodeNetWrapper.realNode.Addr
	//} 禁止 FingerTable 有自己
	for i := 0; i < SuccessorLen; i++ { // 初始化有效后继
		ser.nodeNetWrapper.realNode.SuccessorList[i] = ser.nodeNetWrapper.realNode.Addr
	}
	ser.nodeNetWrapper.realNode.Predecessor = ser.nodeNetWrapper.realNode.Addr // 初始化前继
	ser.nodeNetWrapper.realNode.Next = 0
	ser.nodeNetWrapper.realNode.Maintain()
}

func (ser *ChordServer) Join(addr string) bool {
	logrus.Infof("<Join> Service start in addr %s -> %s", ser.nodeNetWrapper.realNode.Addr, addr)
	ser.nodeNetWrapper.realNode.AllData = make(map[string]string) // 初始化 allData
	var successorID *string
	successorID = new(string)
	//	ser.nodeNetWrapper.realNode.FindSuccessor(Get_hash_code(addr),successorID)
	client, _ := GetClient(addr)
	var otherData *ChordNode = &ChordNode{}
	err := client.Call("Wrapper.AllDataDeliver", 0, otherData)
	if err != nil || otherData.Addr == "" { // 添加失败 不允许出现的情况
		logrus.Infof("<Join> Service fail in addr %s -> %s", ser.nodeNetWrapper.realNode.Addr, addr)
		return false
	}
	otherData.FindSuccessor(ser.nodeNetWrapper.realNode.HashCode, successorID) // 此处的 otherData 并不是真的后继
	real_client, _ := GetClient(*successorID)
	err = real_client.Call("Wrapper.AllDataDeliver", 0, otherData) // todo map here cannot lock up
	if err != nil {
		logrus.Errorf("<Join> Service fail in addr %s -> %s successor: %s", ser.nodeNetWrapper.realNode.Addr, addr, *successorID)
		return false
	}
	//	ser.nodeNetWrapper.realNode.FingerTable = otherData.FingerTable // todo 不允许初始化 finger table
	ser.nodeNetWrapper.realNode.FingerTable[0] = otherData.Addr
	ser.nodeNetWrapper.realNode.SuccessorList[0] = otherData.Addr
	for i := 1; i < SuccessorLen; i++ { // 初始化后继表
		ser.nodeNetWrapper.realNode.SuccessorList[i] = otherData.SuccessorList[i-1] // 错位一个
	} // todo forget to receive data from the successor
	for key , val := range otherData.AllData{
		if IsBetween(ser.nodeNetWrapper.realNode.HashCode,otherData.HashCode,Get_hash_code(key),true){
			continue
		}else{ // data transfer
			ser.nodeNetWrapper.realNode.AllData[key] = val
			delete(otherData.AllData,key)
		}
	}
	ser.nodeNetWrapper.realNode.Predecessor = otherData.Predecessor // 初始化前继
	ser.nodeNetWrapper.realNode.Next = 0
	ser.nodeNetWrapper.realNode.Maintain() // maintain 开始
	logrus.Infof("<Join> Service finished successfully in addr %s -> %s  successor: %s", ser.nodeNetWrapper.realNode.Addr, addr, *successorID)
	return true
}

func (ser *ChordServer) Quit() {
	deliverData := &SmallData{}
	deliverData.Copy(ser.nodeNetWrapper.realNode)
	ser.ShutDown()
	pre_client, _ := GetClient(ser.nodeNetWrapper.realNode.Predecessor)
	if pre_client == nil {
		logrus.Errorf("<Quit> fail to update predecessor information in %s", ser.nodeNetWrapper.realNode.Addr)
		return // todo pre_client -> invalid
	}
	pre_client.Call("Wrapper.ChangeSuccList", *deliverData, nil)
	post_client, _ := GetClient(ser.nodeNetWrapper.realNode.SuccessorList[0])
	if post_client == nil {
		logrus.Errorf("<Quit> fail to update predecessor information in %s", ser.nodeNetWrapper.realNode.Addr)
		return
	}
	post_client.Call("Wrapper.ReceivePreData", *deliverData, nil)
	ser.nodeNetWrapper.realNode.AllData = make(map[string]string)
	logrus.Infof("<Quit> finished successfully in addr %s", ser.nodeNetWrapper.realNode.Addr)
}

func (ser *ChordServer) ForceQuit() { // todo 所有函数需要 listening 特判 listening 结束后调用 address 禁止任何操作

}

func (ser *ChordServer) Ping(addr string) bool {
	return CheckClientRunning(addr)
}

func (ser *ChordServer) Put(key string, value string) bool {
	var succ *string = new(string)
	ser.nodeNetWrapper.realNode.FindSuccessor(Get_hash_code(key), succ)
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
	logrus.Infof("<Get> Service start find key %s in addr %s", key, ser.nodeNetWrapper.realNode.Addr)
	var succ *string = new(string)
	ser.nodeNetWrapper.realNode.FindSuccessor(Get_hash_code(key), succ)
	logrus.Infof("<Get> locate key %s in addr %s", key, *succ)
	client, _ := GetClient(*succ)
	if client == nil {
		logrus.Errorf("<Get> fail to connect client in addr %s to %s",ser.nodeNetWrapper.realNode.Addr,*succ)
		return false, ""
//		panic("Get fail: no such client")
	}
	var ans *string = new(string)
	client.Call("Wrapper.GetVal", KVpair{key, ""}, ans)
	if *ans == "" {
		logrus.Warningf("<Get> find key %s fail in addr %s", key, ser.nodeNetWrapper.realNode.Addr)
		return false, ""
	}
	logrus.Infof("<Get> find key %s in addr %s", key, ser.nodeNetWrapper.realNode.Addr)
	return true, *ans
}

func (ser *ChordServer) Delete(key string) bool {
	logrus.Infof("<Delete> Service start delete key %s in addr %s",key,ser.nodeNetWrapper.realNode.Addr)
	var succ *string = new(string)
	ser.nodeNetWrapper.realNode.FindSuccessor(Get_hash_code(key), succ)
	logrus.Infof("<Delete> locate key %s in addr %s",key,*succ)
	client, _ := GetClient(*succ)
	if client == nil {
		logrus.Errorf("<Delete> delete key %s fail to connect client in addr %s to %s",key,ser.nodeNetWrapper.realNode.Addr,*succ)
		return false
//		panic("Delete fail: no such client")
	}
	var bool_succ *bool = new(bool)
	client.Call("Wrapper.DeleteVal", KVpair{key, ""}, bool_succ)
	logrus.Infof("<Delete> delete key -> %t in addr %s",*bool_succ,*succ)
	return *bool_succ
}
