package Kademlia

import "math/big"

type InputPKG struct {
	Key      string
	Val      string
	Addr     string
	HashCode *big.Int
	Pivot    *big.Int
}

type OutputPKG struct {
	Key          string
	Val          string
	Addr         string
	SearchBucket Bucket
}

func (ser *Wrapper) Find_Node( input InputPKG , output *OutputPKG ) error {
	output.SearchBucket = ser.RealNode.Find_Node(input.Pivot)
	ser.RealNode.Notice(input.Addr)
	return nil
}

func (ser *Wrapper) Find_Value( input InputPKG , output *OutputPKG ) error {
	_ , output.Val = ser.RealNode.Find_Value(input.Key)
	ser.RealNode.Notice(input.Addr)
	return nil
}

func (ser *Wrapper) Store( input InputPKG , output *OutputPKG ) error {
	ser.RealNode.Store(input.Key,input.Val)
	ser.RealNode.Notice(input.Addr)
	return nil
}
