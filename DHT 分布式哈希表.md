## DHT 分布式哈希表

_by andy_yang_



**Chord protocal**

- 封装

  - ChordServer   -- > listener / server

    ​						|-- > wrapper -- > ChordNode -> addr  / hashcode / all data / backup ...

  - Wrapper 作用：提供 rpc 接口，使方法可以被 register

- 节点查找

  - 从 fingerTable 倒序查找，找到第一个满足 NID < target ID 时停止查找
  - 判断 target ID 是否在 local addr <--> successor / predecessor 是则前往 successor / local

- 节点添加

  - 通知真实后继，将后继的 backup 转移到自身，将 all data 按照 ID 划分
  - 继承后继的前继，修改后继的前继

- 元素增删查

  - 增：找到真实后继，添加并将后继 backup++
  - 删：找到真实后继，删除并将后继 backup--
  - 查：去真实后继查找判存

- 节点退出

  - 强退：listener shutdown
  - 退：shutdown 后 调用后继的 checkPredecessor 并调用前继的 stabilize 来传递数据

- 节点维护

  - checkPredecessor -> 判断前继存在，若不存在则前继置空
  - notify -> 通知前继修改 -> 
  - stabilize -> 找到第一个有效后继，若非第一个后继



**Application**

- Upload：创建 .torrent 文件并生成磁力链接
- Download：根据 .torrent 文件内的 pieces 找到文件各部分字节流
- DownloadByMagnet：根据磁力链接创建 temp.torrent 并 Download 

