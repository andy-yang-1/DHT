package DHT

import "math/big"

func (t_w *Wrapper) FindSuccessor(temp_int *big.Int , ans *string) error  {
	t_w.RealNode.FindSuccessor(temp_int,ans)
	return nil
}

func (t_w *Wrapper) AllDataDeliver( user_less int , ans *ChordNode ) error  {
//	ans = t_w.RealNode // copy
	t_w.RealNode.AllDataDeliver(ans)
	return nil
}

func (t_w *Wrapper) SmallDataDeliver( use_less int , ans *ChordNode ) error {
	t_w.RealNode.SmallDataDeliver(ans)
	return nil
}

func (t_w *Wrapper) ChangeSuccList( sd SmallData , use_less *int ) error {
	t_w.RealNode.ChangeSuccList(sd.SuccessorList)
	return nil
}

func (t_w *Wrapper) ReceivePreData( sd SmallData , use_less *int ) error {
	t_w.RealNode.ReceivePreData(sd)
	return nil
}

func (t_w *Wrapper) Notify( temp_addr string , user_less *int ) error  {
	t_w.RealNode.Notify(temp_addr)
	return nil
}

func (t_w *Wrapper) PutVal( temp_pair KVpair , put_succ *bool ) error {
	*put_succ = t_w.RealNode.PutVal(temp_pair.Key,temp_pair.Val)
	return nil
}

func (t_w *Wrapper) GetVal( temp_pair KVpair , ans *string ) error {
	*ans = t_w.RealNode.GetVal(temp_pair.Key)
	return nil
}

func (t_w *Wrapper) DeleteVal( temp_pair KVpair , dele_succ *bool ) error {
	*dele_succ = t_w.RealNode.DeleteVal(temp_pair.Key)
	return nil
}

func (t_w *Wrapper) AddBackup( temp_pair KVpair , use_less *int ) error {
	t_w.RealNode.backupLock.Lock()
	t_w.RealNode.Backup[temp_pair.Key] = temp_pair.Val
	t_w.RealNode.backupLock.Unlock()
	return nil
}

func (t_w *Wrapper) DelBackup( temp_pair KVpair , use_less *int ) error {
	t_w.RealNode.backupLock.Lock()
	delete(t_w.RealNode.Backup,temp_pair.Key)
	t_w.RealNode.backupLock.Unlock()
	return nil
}

func (t_w *Wrapper) TransferBackup( temp_data map[string]string , use_less *int ) error {
	t_w.RealNode.TransferBackup(temp_data)
	return nil
}

func (t_w *Wrapper) ReceiveBackup( use_less int , neededData *map[string]string ) error {
	t_w.RealNode.ReceiveBackup(neededData)
	return nil
}

func (t_w *Wrapper) Stabilize( uselessInt int , uselessPtr *int ) error  {
	t_w.RealNode.Stabilize()
	return nil
}

func (t_w *Wrapper) CheckPredecessorOnline( uselessInt int , uselessPtr *int ) error {
	t_w.RealNode.CheckPredecessorOnline()
	return nil
}