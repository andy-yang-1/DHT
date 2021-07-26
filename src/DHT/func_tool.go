package DHT

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"time"
)

var all_modder = big.NewInt(2).Exp(big.NewInt(2), big.NewInt(160), nil)

const SuccessorLen int = 5
const RingSize int = 160
const MaintainPeriod time.Duration = 100 * time.Millisecond
const WaitTime time.Duration = 30 * time.Millisecond

func Get_hash_code(ID string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(ID))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func IsBetween(l *big.Int, r *big.Int, mid *big.Int, belong bool) bool { // 只有一个点时也需要存储数据，belong 存在应该允许左端点 belong -> 正确情况
	// belong 指是否包含右端点
	// 注意这是一个环
	if l.Cmp(r) < 0 { // 分布在环上
		return mid.Cmp(l) > 0 && mid.Cmp(r) <= 0 && (mid.Cmp(r) < 0 || belong)
	} else { // 分布在首尾
		return (mid.Cmp(l) > 0) || (mid.Cmp(r) <= 0 && (mid.Cmp(r) < 0 || belong))
	}
}

func IntJump(num *big.Int, step int) *big.Int {
	new_val := new(big.Int)
	new_val.Exp(big.NewInt(2), big.NewInt(int64(step)), nil)
	new_val.Add(num, new_val)
	new_val.Mod(new_val, all_modder)
	return new_val
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
