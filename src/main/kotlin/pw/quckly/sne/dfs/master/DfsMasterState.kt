package pw.quckly.sne.dfs.master

class DfsMasterState(val lastServerId: Int,
                     val fsRoot: MemoryDirectory,
                     val servers: List<SlaveServer>) {

}