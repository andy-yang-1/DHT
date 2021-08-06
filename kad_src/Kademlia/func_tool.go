package Kademlia

import (
	"crypto/sha1"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

const Bucket_k = 20
const Bucket_size = 160

var BigOne = big.NewInt(1)
var BigTwo = big.NewInt(2)

var FlushInterval = 1 * time.Second

func Get_hash_code(ID string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(ID))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func Get_server_address(port int) string {
	IP := GetLocalAddress()
	IP = IP + ":" + fmt.Sprintf("%d", port)
	return IP
}

// function to get local address(ip address)
func GetLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

func CheckClientRunning( addr string )bool{
	for i := 0 ; i < 3 ; i++{
		client , err := rpc.Dial("tcp",addr)
		if err == nil{
			client.Close()
			return true
		}
		time.Sleep(30*time.Millisecond)
	}
	return false
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
				time.Sleep(30*time.Millisecond)
				continue
			}
		case <-time.After(30*time.Millisecond):
			continue
		}
	}
	logrus.Errorf("<GetCient> cannot access %s", temp_addr)
	return nil, myError{"access fail"}
}

func GetBucketNum( hashCode , target *big.Int ) int {
	dist := new(big.Int)
	dist.Xor(hashCode,target) // get the distance
	counter := 0
	for dist.Cmp(BigOne) > 0{
		dist.Div(dist,BigTwo)
		counter++
	}
	return counter
}