package Torrent

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/jackpal/bencode-go"
	"io"
)

type TorrentFile struct {
	Announce    string
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int
	Length      int
	Name        string
}

func Read(r io.Reader) TorrentFile {
	t_file := TorrentFile{}
	err := bencode.Unmarshal(r, &t_file)
	if err != nil {
		return TorrentFile{}
	}
	return t_file
}

func MyHash(tempData []byte) [20]byte {
	var buf bytes.Buffer
	tempStr := string(tempData)
	bencode.Marshal(&buf, tempStr)
	ans := sha1.Sum(buf.Bytes())
	return ans
}

func VerifyHash(temp_key, strData string) bool {
	tempHash := MyHash([]byte(strData))
	fakeKey := fmt.Sprintf("%x", tempHash)
	if temp_key == fakeKey {
		return true
	}
	return false
}
