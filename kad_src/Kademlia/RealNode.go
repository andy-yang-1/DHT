package Kademlia

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Bucket struct {
	RealAddr [Bucket_k*Bucket_k + Bucket_k]string
	Size     int
}

type KademliaNode struct {
	Addr        string
	HashCode    *big.Int
	IsListening bool

	AllData  map[string]string
	dataLock sync.Mutex

	ExpireTime map[string]time.Time
	expireLock sync.Mutex

	FlushTime map[string]time.Time
	flushLock sync.Mutex

	TableBucket [Bucket_size]Bucket
	tableLock   sync.Mutex

	tableStatus [Bucket_size]int
	tableSum    int
}

type Wrapper struct {
	RealNode *KademliaNode
}

type KademliaServer struct {
	RealServer     *rpc.Server
	RealListener   net.Listener
	NodeNetWrapper Wrapper
}

type myError struct {
	errMsg string
}

func (err myError) Error() string {
	return err.errMsg
}

func (ser *KademliaServer) SetPort(port int) {
	ser.NodeNetWrapper.RealNode = new(KademliaNode)
	ser.NodeNetWrapper.RealNode.Addr = Get_server_address(port)
	ser.NodeNetWrapper.RealNode.HashCode = Get_hash_code(ser.NodeNetWrapper.RealNode.Addr)
}

// the following is about the maintenance of realBucket inside the node

func (t_b *Bucket) Push_back(addr string) { // newest -> back
	if t_b.Size < Bucket_k {
		t_b.RealAddr[t_b.Size] = addr
		t_b.Size++
		return
	}
	for i := 0; i < t_b.Size; i++ {
		t_b.RealAddr[i] = t_b.RealAddr[i+1]
	}
	t_b.RealAddr[Bucket_k-1] = addr
}

func (t_b *Bucket) Exist(addr string) (bool, int) { // exi , num
	for i := 0; i < t_b.Size; i++ {
		if t_b.RealAddr[i] == addr {
			return true, i
		}
	}
	return false, -1
}

func (t_b *Bucket) GetPrior(target int) { // let the latter part become newer
	temp := t_b.RealAddr[target]
	for i := target; i < t_b.Size-1; i++ {
		t_b.RealAddr[i] = t_b.RealAddr[i+1]
	}
	t_b.RealAddr[t_b.Size-1] = temp
}

func (t_b *Bucket) DeleteEle(target int) {
	for i := target; i < t_b.Size-1; i++ {
		t_b.RealAddr[i] = t_b.RealAddr[i+1]
	}
	t_b.RealAddr[t_b.Size-1] = ""
	t_b.Size--
}

func (t_b *Bucket) Clear() {
	temp := Bucket{}
	*t_b = temp
}

func (t_b *Bucket) GetSize() int {
	return t_b.Size
}

// the following is about the outside bucket

func (t_b *Bucket) Equal(otherDat *Bucket) bool {
	if t_b.Size != otherDat.Size {
		return false
	}
	for i := 0; i < t_b.Size; i++ {
		ok, _ := t_b.Exist(otherDat.RealAddr[i])
		if !ok {
			return false
		}
	}
	return true
}

func (t_b *Bucket) Receive(otherDat *Bucket) {
	for i := 0; i < otherDat.Size; i++ {
		if ok, _ := t_b.Exist(otherDat.RealAddr[i]); !ok {
			t_b.RealAddr[t_b.Size] = otherDat.RealAddr[i]
			t_b.Size++
		}
	}
}

func (t_b *Bucket) GetInsideBucket(pivot *big.Int) Bucket { // get the closest k addr
	if t_b.Size < Bucket_k {
		return *t_b
	}
	var allHash [Bucket_k * Bucket_k]*big.Int
	for i := 0; i < t_b.Size; i++ {
		allHash[i] = Get_hash_code(t_b.RealAddr[i])
		allHash[i].Xor(allHash[i], pivot)
	}
	for round := 0; round < Bucket_k; round++ {
		greatInt := big.NewInt(2)
		greatInt.Exp(greatInt, big.NewInt(162), nil)
		target := round
		var tempStr string
		for i := round; i < t_b.Size; i++ {
			if greatInt.Cmp(allHash[i]) > 0 {
				target = i
				greatInt = allHash[i]
				tempStr = t_b.RealAddr[i]
			}
		}
		t_b.RealAddr[target] = t_b.RealAddr[round]
		allHash[target] = allHash[round]
		t_b.RealAddr[round] = tempStr
		allHash[round] = greatInt
	}
	ret_bucket := Bucket{}
	for i := 0; i < Bucket_k; i++ {
		ret_bucket.RealAddr[i] = t_b.RealAddr[i]
	}
	ret_bucket.Size = Bucket_k
	return ret_bucket
}

// the following is about the inside function of Kademlia node

func (t_n *KademliaNode) Find_Node(pivot *big.Int) Bucket {
	bucketNum := GetBucketNum(t_n.HashCode, pivot)
	searchBucket := Bucket{}
	t_n.tableLock.Lock()
	for i := 0; i < t_n.TableBucket[bucketNum].Size; i++ {
		if CheckClientRunning(t_n.TableBucket[bucketNum].RealAddr[i]) {
			searchBucket.Push_back(t_n.TableBucket[bucketNum].RealAddr[i])
		} else {
//			fmt.Printf("<Find_Node> Detect addr %s offline\n", t_n.TableBucket[bucketNum].RealAddr[i])
			logrus.Warningf("<Find_Node> Detect addr %s offline",t_n.TableBucket[bucketNum].RealAddr)
			t_n.TableBucket[bucketNum].DeleteEle(i)
			i-- // to avoid the invalid read
		}
	}

	for i := 0; i < Bucket_size && searchBucket.Size < Bucket_k; i++ {
		if i == bucketNum {
			continue
		}
		for j := 0; j < t_n.TableBucket[i].Size && searchBucket.Size < Bucket_k; j++ {
			if CheckClientRunning(t_n.TableBucket[i].RealAddr[j]) {
				searchBucket.Push_back(t_n.TableBucket[i].RealAddr[j])
			} else {
//				fmt.Printf("<Find_Node> Detect addr %s offline\n", t_n.TableBucket[i].RealAddr[j])
				logrus.Warningf("<Find_Node> Detect addr %s offline",t_n.TableBucket[i].RealAddr[j])
				t_n.TableBucket[i].DeleteEle(j)
				j--
			}
		}
	}
	t_n.tableLock.Unlock()
	return searchBucket
}

func (t_n *KademliaNode) Find_Value(key string) (bool, string) {
	t_n.dataLock.Lock()
	ret, ok := t_n.AllData[key]
	t_n.dataLock.Unlock()
	return ok, ret
}

func (t_n *KademliaNode) Ping(addr string) bool {
	return CheckClientRunning(addr)
}

func (t_n *KademliaNode) Store(key, val string) {
	t_n.dataLock.Lock()
	t_n.flushLock.Lock()
	t_n.AllData[key] = val
	t_n.FlushTime[key] = time.Now().Add(FlushInterval)
	t_n.flushLock.Unlock()
	t_n.dataLock.Unlock()
	logrus.Infof("<Store> Put key %s in addr %s", key, t_n.Addr)
}

func (t_n *KademliaNode) Notice(addr string) {
	if addr == "" {
		fmt.Printf("<Notice> empty address\n")
		return
	}
	bucketNum := GetBucketNum(t_n.HashCode, Get_hash_code(addr))
	t_n.tableLock.Lock()
	if exi, pos := t_n.TableBucket[bucketNum].Exist(addr); exi {
		t_n.TableBucket[bucketNum].GetPrior(pos)
	} else {
		t_n.TableBucket[bucketNum].Push_back(addr)
	}
	t_n.tableLock.Unlock()
}

func (t_n *KademliaNode) SelfFlush() {
	t_n.dataLock.Lock()
	t_n.flushLock.Lock()
	realNow := time.Now()
	tempMap := make(map[string]string)
	for key, temp_time := range t_n.FlushTime {
		if !realNow.After(temp_time) {
			tempMap[key] = t_n.AllData[key]
		}
	}
	t_n.dataLock.Unlock()
	t_n.flushLock.Unlock()
	for key, val := range tempMap {
		searchBucket := t_n.LookUpCloseNode(key)
		for i := 0; i < searchBucket.Size; i++ {
			input := InputPKG{Addr: t_n.Addr, HashCode: t_n.HashCode, Key: key, Val: val}
			output := OutputPKG{}
			client, _ := GetClient(searchBucket.RealAddr[i])
			if client == nil {
				logrus.Errorf("<SelfFlush> Fail to update key %s in addr %s", key, searchBucket.RealAddr[i])
				fmt.Printf("<SelfFlush> Fail to update key %s in addr %s\n", key, searchBucket.RealAddr[i])
				continue
			}
			err := client.Call("Wrapper.Store", input, &output)
			client.Close()
			if err != nil {
				fmt.Printf("<SelfFlush> Fail to Call Client error: %s\n", err.Error())
				logrus.Errorf("<SelfFlush> Fail to Call Client error: %s", err.Error())
			}
		}
	}
}

// todo change the frequency of flush

func (t_n *KademliaNode) Split_Answer_Bucket(tempBucket Bucket) {
	for i := 0; i < tempBucket.Size; i++ {
		if tempBucket.RealAddr[i] == t_n.Addr { // prohibit self addr to become an element
			continue
		}
		BucketNum := GetBucketNum(t_n.HashCode, Get_hash_code(tempBucket.RealAddr[i]))
		t_n.tableLock.Lock()
		exi, pos := t_n.TableBucket[BucketNum].Exist(tempBucket.RealAddr[i])
		if exi {
			t_n.TableBucket[BucketNum].GetPrior(pos)
		} else {
			t_n.TableBucket[BucketNum].Push_back(tempBucket.RealAddr[i])
		}
		t_n.tableLock.Unlock()
	}
}

func (t_n *KademliaNode) LookUpCloseNode(tempAddr string) Bucket { // todo forget to add self?
	ansBucket := Bucket{}
	pivot := Get_hash_code(tempAddr)
	searchBucket := t_n.Find_Node(pivot)  // make a difference
	for !searchBucket.Equal(&ansBucket) { // ans has only Bucket_k elements search_bucket can have k ^ 2
		ansBucket = searchBucket
		for i := 0; i < ansBucket.Size; i++ { // todo add go func
			targetAddr := ansBucket.RealAddr[i]
			output := OutputPKG{}
			input := InputPKG{
				Addr:     t_n.Addr,
				HashCode: t_n.HashCode,
				Pivot:    pivot,
			}
			client, _ := GetClient(targetAddr)
			if client == nil {
				logrus.Errorf("<LookUpCloseNode> Fail to get client addr %s", targetAddr)
//				fmt.Printf("<LookUpCloseNode> Fail to get client addr %s\n", targetAddr)
				return ansBucket
			}
			err := client.Call("Wrapper.Find_Node", input, &output)
			client.Close()
			if err != nil {
				logrus.Errorf("<LookUpCloseNode> Fail to call func to addr %s , error : %s", targetAddr, err.Error())
//				fmt.Printf("<LookUpCloseNode> Fail to Call func to addr %s , error : %s \n", targetAddr, err.Error())
				return ansBucket
			}
			searchBucket.Receive(&(output.SearchBucket))
		}
		t_n.Split_Answer_Bucket(searchBucket)
		searchBucket = searchBucket.GetInsideBucket(pivot)
	}
	return ansBucket
}

func (t_n *KademliaNode) Maintain() {
	//go func() {
	//	for t_n.IsListening {
	//		t_n.DebugFlush()
	//		time.Sleep(1 * time.Second)
	//	}
	//}()
	go func() {
		for t_n.IsListening {
			t_n.SelfFlush()
			time.Sleep(10*time.Second) // todo set a reasonable value
		}
	}()
}

func (t_n *KademliaNode) DebugFlush() {
	t_n.tableSum = 0
	for i := 0; i < Bucket_size; i++ {
		t_n.tableSum = t_n.tableSum + t_n.tableStatus[i]
		t_n.tableLock.Lock()
		t_n.tableStatus[i] = t_n.TableBucket[i].GetSize()
		t_n.tableLock.Unlock()
	}
}
