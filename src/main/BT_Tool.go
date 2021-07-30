package main

import (
	"Torrent"
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/jackpal/bencode-go"
	"os"
)

func Upload(filename string, realPath string, node *dhtNode) string { // notice: unfinished pattern
	t_file := UploadAllData(filename, realPath, node)
	if t_file.Name == "" {
		fmt.Println("Fail to upload any data")
		return ""
	}
	temp_key := fmt.Sprintf("%x", t_file.InfoHash)
	return "magnet:?xt=urn:sha1:" + temp_key
}

func Download(TorrentFilename string, realPath string, node *dhtNode) {
	torrentReader, err := os.Open(TorrentFilename)
	if err != nil {
		fmt.Printf("Fail to open file %s error: %s\n", TorrentFilename, err.Error())
		return
	}
	//	benecodeTorrent , _ := Torrent.Open(torrentReader)
	torrentFile := Torrent.Read(torrentReader)
	allDat := make([]byte, torrentFile.Length+262144)
	blockNum := torrentFile.Length / torrentFile.PieceLength
	if torrentFile.Length%torrentFile.PieceLength != 0 {
		blockNum++
	}
	for i := 0; i < blockNum; i++ {
		temp_key := fmt.Sprintf("%x", torrentFile.PieceHashes[i])
		ok, strData := (*node).Get(temp_key)
		if !ok {
			fmt.Println("Fail to download: missing data")
			return
		}
		if !Torrent.VerifyHash(temp_key, strData) {
			fmt.Println("Fail to download: verify hash failed")
			return
		}
		tempData := []byte(strData)
		copy(allDat[i*torrentFile.PieceLength:(i+1)*torrentFile.PieceLength], tempData)
	}
	os.WriteFile(torrentFile.Name, allDat, 0644)
	fmt.Println("Download success")
}

// test 2

func UploadAllData(filename string, realPath string, node *dhtNode) Torrent.TorrentFile {
	all_data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Fail to read file %s", filename)
		return Torrent.TorrentFile{}
	}
	fileStat, _ := os.Stat(filename)
	t_file := Torrent.TorrentFile{
		Announce:    "take pride and let the chase begin",
		PieceLength: 262144,
		Length:      int(fileStat.Size()),
		Name:        fileStat.Name(),
	}
	blocksize := t_file.Length / t_file.PieceLength
	if t_file.Length%t_file.PieceLength != 0 {
		blocksize++
	}
	t_file.PieceHashes = make([][20]byte, blocksize)
	for i := 0; i < blocksize-1; i++ {
		specData := all_data[i*t_file.PieceLength : (i+1)*t_file.PieceLength]
		tempHash := Torrent.MyHash(specData)
		t_file.PieceHashes[i] = tempHash
		temp_key := fmt.Sprintf("%x", tempHash)
		(*node).Put(temp_key, string(specData))
	}
	specData := all_data[(blocksize-1)*t_file.PieceLength : t_file.Length]
	tempHash := Torrent.MyHash(specData)
	t_file.PieceHashes[blocksize-1] = tempHash
	temp_key := fmt.Sprintf("%x", tempHash)
	(*node).Put(temp_key, string(specData))
	var torrentCode bytes.Buffer
	bencode.Marshal(&torrentCode, t_file)
	tempHash = sha1.Sum(torrentCode.Bytes())
	t_file.InfoHash = tempHash
	temp_key = fmt.Sprintf("%x", tempHash)
	(*node).Put(temp_key, string(torrentCode.Bytes()))
	os.WriteFile(fileStat.Name()+".torrent", torrentCode.Bytes(), 0644)
	return t_file
}

// merge branch test
