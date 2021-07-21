package DHT

import "math/big"

func (t_w *Wrapper) FindSuccessor(temp_int *big.Int , ans *string) error  {
	t_w.realNode.FindSuccessor(temp_int,ans)
	return nil
}

func (t_w *Wrapper) AllDataDeliver( user_less int , ans *ChordNode ) error  {
//	*ans = *t_w.realNode // copy
	ans.Addr = t_w.realNode.Addr
	ans.SuccessorList = t_w.realNode.SuccessorList
	ans.AllData = t_w.realNode.AllData
	ans.FingerTable = t_w.realNode.FingerTable
	ans.Predecessor = t_w.realNode.Predecessor
	ans.HashCode = t_w.realNode.HashCode
	return nil
}

func (t_w *Wrapper) SmallDataDeliver( use_less int , ans *SmallData ) error {
	t_w.realNode.SmallDataDeliver(ans)
	return nil
}

func (t_w *Wrapper) ChangeSuccList( sd SmallData , use_less *int ) error {
	t_w.realNode.ChangeSuccList(sd.SuccessorList)
	return nil
}

func (t_w *Wrapper) ReceivePreData( sd SmallData , use_less *int ) error {
	t_w.realNode.ReceivePreData(sd)
	return nil
}

func (t_w *Wrapper) Notify( temp_addr string , user_less *int ) error  {
	t_w.realNode.Notify(temp_addr)
	return nil
}

func (t_w *Wrapper) PutVal( temp_pair KVpair , put_succ *bool ) error {
	*put_succ = t_w.realNode.PutVal(temp_pair.Key,temp_pair.Val)
	return nil
}

func (t_w *Wrapper) GetVal( temp_pair KVpair , ans *string ) error {
	*ans = t_w.realNode.GetVal(temp_pair.Key)
	return nil
}

func (t_w *Wrapper) DeleteVal( temp_pair KVpair , dele_succ *bool ) error {
	*dele_succ = t_w.realNode.DeleteVal(temp_pair.Key)
	return nil
}