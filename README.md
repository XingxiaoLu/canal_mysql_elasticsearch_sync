# canal_mysql_elasticsearch_sync 支持请star✨

在王超同学canal_mysql_elasticsearch_sync基础上进行了改进，对原有全量同步接口进行了部分改进，采用多线程的方式进行同步，避免同步进度赶不上数据写入速度的问题。

接口调整如下：

1. 全量同步所有表

   适用于生产环境已经有大量数据表的情况，不需要按照byTable接口一个一个同步，一个接口开启所有的表的全量同步 。系统会自动为每个表格分配一定数量的线程进行处理 。线程分配逻辑目前是按照数据表内的数据量多少决定的，目前是每500万条数据一个处理线程。

   请求方式：GET

   Url: http://localhost:8828/sync/all

2. 开始binlog同步

   原有的逻辑中，无法控制canel同步的开始时间，加入该接口手动控制同步canel的开始时间。

   请求方式： GET

   Url: http://localhost:8828/sync/binlog