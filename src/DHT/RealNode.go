package DHT

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type ChordNode struct {
	Addr        string
	HashCode    *big.Int
	IsListening bool

	AllData  map[string]string
	dataLock sync.Mutex

	Backup map[string]string
	backupLock sync.Mutex

	SuccessorList [SuccessorLen]string
	successorLock sync.Mutex

	Predecessor string

	FingerTable [RingSize]string
	fingerLock  sync.Mutex
	Next        int
}

type SmallData struct { // todo all force quit -> no need for SmallData
	Addr          string
	HashCode      *big.Int
	AllData       map[string]string
	SuccessorList [SuccessorLen]string
	Predecessor   string
	FingerTable   [RingSize]string
}

func (sd *SmallData) Copy( t_n *ChordNode )  { // copy the basic data of a certain ChordNode
	sd.Addr = t_n.Addr
	sd.HashCode = new(big.Int).Add(t_n.HashCode,big.NewInt(0))
	sd.AllData = make(map[string]string)
	for key , val := range t_n.AllData{
		sd.AllData[key] = val // copy the realData
	}
	sd.FingerTable = t_n.FingerTable
	sd.Predecessor = t_n.Predecessor
	sd.SuccessorList = t_n.SuccessorList
}

type KVpair struct {
	Key string
	Val string
}

type Wrapper struct {
	RealNode *ChordNode
}

type ChordServer struct { // 与 NewNode 对接的类
	realServer     *rpc.Server
	realListener   net.Listener
	nodeNetWrapper Wrapper
}

type myError struct {
	err_msg string
}

func (err myError) Error() string {
	return err.err_msg
}

func (ser *ChordServer) SetPort(port int) { // todo 空指针一定要 new
	ser.nodeNetWrapper.RealNode = new(ChordNode)
	ser.nodeNetWrapper.RealNode.Addr = Get_server_address(port)
	ser.nodeNetWrapper.RealNode.HashCode = Get_hash_code(ser.nodeNetWrapper.RealNode.Addr)
}

func GetClient(temp_addr string) (*rpc.Client, error) { // 拨打失败需要继续拨打
	if temp_addr == "" {
		logrus.Warningf("<GetClient> empty address")
		return nil, myError{"GetClient void address"}
	}

	var client *rpc.Client
	var err error
	ch := make(chan error)

	for i := 0; i < 5; i++ {
		go func() {
			client, err = rpc.Dial("tcp", temp_addr)
			ch <- err
		}()
		select {
		case <-ch:
			if err == nil {
				return client, nil
			} else {
				time.Sleep(WaitTime)
				continue
			}
		case <-time.After(WaitTime):
			continue
		}
	}
	logrus.Errorf("<GetCient> cannot access %s", temp_addr)
	return nil, myError{"access fail"}
	
}

func CheckClientRunning(temp_addr string) bool {
	for i := 0; i < 3; i++ {
		client, err := rpc.Dial("tcp", temp_addr)
		if err == nil {
			client.Close()
			return true
		}
		time.Sleep(WaitTime)
	}
	return false
}

func (t_n *ChordNode) First_online_successor() string {
	for i := 0 ; i < SuccessorLen ; i++{
		if CheckClientRunning(t_n.SuccessorList[i]){
			return t_n.SuccessorList[i]
		}
	}
	logrus.Errorf("<First_online_successor> broken core in addr %s",t_n.Addr)
	return ""
}

//func GetService( client *rpc.Client , func_name string , allInput interface{} , allOutput interface{} ){ // GetService 的核心目的是为了让远程服务端调用函数 wrapper 的目的是封装好来被调用
//	client.Call(func_name,allInput,allOutput)
//} // todo GetService 貌似没有必要


func (t_n *ChordNode) FindSuccessor(temp_int *big.Int, ans *string) { // ID >= temp_int 或者 环
	logrus.Infof("<FindSuccessor> find %s in %s", temp_int.String(), t_n.HashCode.String())
	pre_int := Get_hash_code(t_n.Predecessor)
	next_int := Get_hash_code(t_n.SuccessorList[0])
	//	temp_int := Get_hash_code(temp_addr)
	if IsBetween(pre_int, t_n.HashCode, temp_int, true) { // 目标在自己和前继之间
		logrus.Infof("<FindSuccessor> success: target %s between pre %s (code: %s) and self %s (code: %s)", temp_int.String(), t_n.Predecessor, pre_int.String(), t_n.Addr, t_n.HashCode.String())
		*ans = t_n.Addr
		return
	}
	if IsBetween(t_n.HashCode, next_int, temp_int, true) { // 目标在自己和后继之间
		logrus.Infof("<FindSuccessor> success: target %s between self %s (code: %s) and successor %s (code: %s)", temp_int.String(), t_n.Addr, t_n.HashCode.String(), t_n.SuccessorList[0], next_int.String())
		*ans = t_n.SuccessorList[0]
		return
	}
	//if t_n.HashCode.Cmp(temp_int) > 0 { // 左右顺序不同
	//	temp_int.Add(temp_int, all_modder)
	//}

	for i := RingSize - 1; i >= 0; i-- {
		if t_n.FingerTable[i] == "" { // 未设置完成
			continue
		}
		jump := IntJump(t_n.HashCode, i)
		if jump.Cmp(temp_int) > 0 { // 跳过头
			continue
		}
		trans_int := Get_hash_code(t_n.FingerTable[i])
		if !IsBetween(t_n.HashCode,temp_int,trans_int,true) { // 跳过头
			continue
		}
		ok := CheckClientRunning(t_n.FingerTable[i])
		if !ok { // 节点失效
			continue
		}
		client, _ := GetClient(t_n.FingerTable[i])
		if client != nil {
			logrus.Infof("<FindSuccessor> find %s , now at %s , jump to %s , which is %s", temp_int.String(), t_n.HashCode.String(), trans_int.String(), t_n.FingerTable[i])
			client.Call("Wrapper.FindSuccessor", temp_int, ans)
			client.Close() // 用完即关
			return
		}
	}
	// here -> not only rely on the finger table: be careful, the successor list is the only reliable list
	logrus.Warningf("<FindSuccessor> fail at find %s in %s by using finger table", temp_int.String(), t_n.HashCode.String())
	client , _ := GetClient(t_n.SuccessorList[0])
	if client == nil {
		logrus.Errorf("<FindSuccessor> fail to find any client at %s -> broken core",t_n.Addr)
		return
	}
	client.Call("Wrapper.FindSuccessor",temp_int,ans)
}

func (t_n *ChordNode) AllDataDeliver( ans *ChordNode )  {
	t_n.dataLock.Lock()
	t_n.backupLock.Lock()
	ans.Addr = t_n.Addr
	ans.SuccessorList = t_n.SuccessorList
	ans.AllData = t_n.AllData
	ans.Backup = t_n.Backup
	ans.FingerTable = t_n.FingerTable
	ans.Predecessor = t_n.Predecessor
	ans.HashCode = t_n.HashCode
	t_n.dataLock.Unlock()
	t_n.backupLock.Unlock()
}

func (t_n *ChordNode) SmallDataDeliver(ans *ChordNode) {
	ans.Addr = t_n.Addr
	ans.SuccessorList = t_n.SuccessorList
	ans.HashCode = t_n.HashCode // 拙劣拷贝
	//t_n.dataLock.Lock()
	//ans.AllData = t_n.AllData
	//t_n.dataLock.Unlock()
	ans.FingerTable = t_n.FingerTable
	ans.Predecessor = t_n.Predecessor
}

func (t_n *ChordNode) Maintain() { // todo stabilize notify fix_finger
	// todo 打破维护后要重新继续维护
	go func() {
		for t_n.IsListening {
			t_n.successorLock.Lock()
			t_n.Stabilize()
			t_n.successorLock.Unlock()
			time.Sleep(MaintainPeriod)
		}
	}()
	go func() {
		for t_n.IsListening { // 通知后继维护前继
			t_n.CheckPredecessorOnline()
			time.Sleep(MaintainPeriod)
		}
	}()
	go func() {
		for t_n.IsListening {
			t_n.fingerLock.Lock()
			t_n.FixFinger()
			t_n.fingerLock.Unlock()
			time.Sleep(MaintainPeriod)
		}
	}()
}

func (t_n *ChordNode) Stabilize() { // todo 未上锁
	logrus.Infof("<Stabilize> Service start in addr %s", t_n.Addr)
	var tempSucc *ChordNode = new(ChordNode) // todo 貌似没有处理失效情况
	var uselessInt int
	firstSuccessor := t_n.First_online_successor()
	client, _ := GetClient(firstSuccessor) // todo 现在只处理了节点添加的情况，没有处理节点失效的情况
	if client == nil { // todo 未查找前继失效情况
		logrus.Errorf("<Stabilize> client connection fail in addr %s -> %s", t_n.Addr, t_n.SuccessorList[0])
		return
	}
	client.Call("Wrapper.SmallDataDeliver", 0, tempSucc)
	if tempSucc.Addr == "" { // 如果是自己的话 干脆 return
		logrus.Errorf("<Stabilize> client connection success but can not find successor in addr %s -> %s", t_n.Addr, t_n.SuccessorList[0])
		return
	}
	if tempSucc.Predecessor != t_n.Addr && tempSucc.Predecessor != "" && IsBetween(t_n.HashCode, tempSucc.HashCode, Get_hash_code(tempSucc.Predecessor), false) { // 后继的前继合法
		t_n.SuccessorList[0] = tempSucc.Predecessor
		client.Close() // 更新最新后继
		client, _ = GetClient(t_n.SuccessorList[0])
		if client == nil {
			logrus.Errorf("<Stabilize> client connection fail in addr %s -> %s", t_n.Addr, t_n.SuccessorList[0])
			return
		}
		client.Call("Wrapper.SmallDataDeliver", 0, tempSucc) // 真正的 tempSucc // todo change it to small data deliver
	} // todo linkDataDeliver to prohibit frequent map transfer
	if tempSucc.Predecessor == ""{ // tempSucc's predecessor is lost
//		fmt.Printf("<Stabilize> check %s offline\n",t_n.SuccessorList[0])
		t_n.SuccessorList[0] = tempSucc.Addr
		client.Call("Wrapper.TransferBackup",t_n.AllData,&uselessInt)
	}
	for i := 1; i < SuccessorLen; i++ { // 更新后继表
		t_n.SuccessorList[i] = tempSucc.SuccessorList[i-1] // 错位
	}
	client.Call("Wrapper.Notify",t_n.Addr,&uselessInt)
	logrus.Infof("<Stabilize> Service finished in addr %s , old successor %s , new successor %s .", t_n.Addr, tempSucc.Addr, t_n.SuccessorList[0])
	client.Close()
}

// todo apply_backup

func (t_n *ChordNode) CheckPredecessorOnline()  {
	var uselessInt int
	if t_n.Predecessor != "" && !CheckClientRunning(t_n.Predecessor){
//		fmt.Printf("<CheckPredecessorOnline> check %s offline\n",t_n.Predecessor)
		t_n.Predecessor = ""
		// todo -> backup transfer
		t_n.backupLock.Lock()
		t_n.dataLock.Lock()
		for key , val := range t_n.Backup{
			t_n.AllData[key] = val
		}
		t_n.Backup = make(map[string]string)
		t_n.dataLock.Unlock()
		t_n.backupLock.Unlock()
		client , _ := GetClient(t_n.SuccessorList[0]) // inform the successor to update the backup
		if client == nil{
			logrus.Errorf("<CheckPredecessorOnline> fail to transfer backup to the successor in addr %s -> %s",t_n.Addr,t_n.SuccessorList[0])
			return
		}
		t_n.dataLock.Lock()
		client.Call("Wrapper.TransferBackup",t_n.AllData,&uselessInt)
		t_n.dataLock.Unlock()
		client.Close()
	}
}

func (t_n *ChordNode) Notify(temp_addr string) { // todo notify 修改非自身,而是他人的节点 call 调用
	logrus.Infof("<Notify> Service start in addr %s -> %s", t_n.Addr, t_n.Predecessor)
	if t_n.Predecessor == temp_addr{
		logrus.Infof("<Notify> no need to notify in addr %s",t_n.Addr)
		return
	}
	if t_n.Predecessor == "" || IsBetween(Get_hash_code(t_n.Predecessor), t_n.HashCode, Get_hash_code(temp_addr), false) {
		t_n.Predecessor = temp_addr
		// todo 信息传递尚未完成
		client , _ := GetClient(t_n.Predecessor)
		if client == nil{
			logrus.Errorf("<Notify> fail to update successor in addr %s -> %s" ,t_n.Addr , t_n.Predecessor )
			return
		}
		t_n.backupLock.Lock()
		client.Call("Wrapper.ReceiveBackup",0,&t_n.Backup)
		t_n.backupLock.Unlock()
		client.Close()
	}

	logrus.Infof("<Notify> Service finished successfully in addr %s -> %s", t_n.Addr, t_n.SuccessorList[0])
}

func (t_n *ChordNode) FixFinger() { // 禁止 FingerTable 有自己
	logrus.Infof("<FixFinger> Service start in addr %s , fix %d", t_n.Addr, t_n.Next)
	temp_int := big.NewInt(2)
	temp_int.Exp(temp_int, big.NewInt(int64(t_n.Next)), nil)
	temp_int.Add(temp_int, t_n.HashCode)
	var ans *string
	ans = new(string)
	t_n.FindSuccessor(temp_int, ans)
	logrus.Infof("<FixFinger> Service finish in addr %s , fix %d, old finger %s , new finger %s", t_n.Addr, t_n.Next, t_n.FingerTable[t_n.Next], *ans)
	if *ans == t_n.Addr {
		t_n.FingerTable[t_n.Next] = "" // ban self to avoid client overflow
		t_n.Next = (t_n.Next + 1) % RingSize
		return
	}
	t_n.FingerTable[t_n.Next] = *ans
	t_n.Next = (t_n.Next + 1) % RingSize
}

func (t_n *ChordNode) PutVal(key string, val string) bool { // todo 未完善备用列表
	var uselessInt int
	t_n.dataLock.Lock()
	t_n.AllData[key] = val
	t_n.dataLock.Unlock()
	client , _ := GetClient(t_n.SuccessorList[0])
	if client == nil {
		return true
	}
	client.Call("Wrapper.AddBackup",KVpair{key,val},&uselessInt)
	client.Close()
//	fmt.Printf("<Put> put key %s in addr %s , backup in %s\n",key,t_n.Addr,t_n.SuccessorList[0])
	return true
}

func (t_n *ChordNode) GetVal(key string) string {
	t_n.dataLock.Lock()
	ans, exi := t_n.AllData[key]
	t_n.dataLock.Unlock()
	if exi == false {
		fmt.Printf("<Get> fail to get %s in addr %s\n",key,t_n.Addr)
		return ""
	}
	return ans
}

func (t_n *ChordNode) DeleteVal(key string) bool { // todo 未完善备用列表
	var uselessInt int
	t_n.dataLock.Lock()
	_, exi := t_n.AllData[key]
	if exi == false {
		t_n.dataLock.Unlock()
		fmt.Printf("<Delete> fail to delete key %s in addr %s\n",key,t_n.Addr)
		return false // 不存在此键
	}
	delete(t_n.AllData, key)
	t_n.dataLock.Unlock()
	client , _ := GetClient(t_n.SuccessorList[0])
	if client == nil{
		return true
	}
	client.Call("Wrapper.DelBackup",KVpair{key,""},&uselessInt)
	client.Close()
//	fmt.Printf("<Delete> delete backup key %s in addr %s\n",key,t_n.SuccessorList[0])
	return true
}

func (t_n *ChordNode) ChangeSuccList( newList [SuccessorLen]string )  {
	t_n.successorLock.Lock()
	t_n.SuccessorList = newList // only need to correct the successorList
	t_n.successorLock.Unlock()
}

func (t_n *ChordNode) ReceivePreData( receivedData SmallData )  {
	t_n.Predecessor = receivedData.Predecessor // change the predecessor
	t_n.dataLock.Lock()
	for key , val := range receivedData.AllData{
		t_n.AllData[key] = val
	}
	t_n.dataLock.Unlock()
}

func (ser *ChordServer) ShutDown()  {
	err := ser.realListener.Close()
	if err != nil {
		logrus.Errorf("<ShutDown> fail in addr %s , err: %s",ser.nodeNetWrapper.RealNode.Addr,err)
//		fmt.Errorf("<ShutDown> fail in addr %s , err: %s",ser.nodeNetWrapper.RealNode.Addr,err)
		return
	}
	ser.nodeNetWrapper.RealNode.IsListening = false
	logrus.Infof("<ShutDown> finished successfully in addr %s",ser.nodeNetWrapper.RealNode.Addr)
//	fmt.Printf("<ShutDown> happen in addr %s \n",ser.nodeNetWrapper.RealNode.Addr)
}

func (t_n *ChordNode) TransferBackup( temp_data map[string]string )  {
	t_n.backupLock.Lock()
	for key , val := range temp_data{
		t_n.Backup[key] = val
	}
	t_n.backupLock.Unlock()
}

func (t_n *ChordNode) ReceiveBackup( neededData *map[string]string )  {
	*neededData = make(map[string]string)
	t_n.dataLock.Lock()
	for key ,val := range t_n.AllData{
		(*neededData)[key] = val
	}
	t_n.dataLock.Unlock()
}

