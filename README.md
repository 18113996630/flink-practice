# flink-practice
flink中DataSet、DataStream、Window、缓存、Source、Sink相关说明及示例代码
```
├─src
   ├─main
      ├─java
      │  └─com
      │      └─hrong
      │          └─flink
      │              ├─model
      │              ├─source 自定义mysql source
      │              ├─utils 
      │              ├─sink   以输出方式sink数据
      │              ├─table  
      │              └─query
      ├─scala
      │  └─com
      │      └─hrong
      │          └─flink
      │              ├─cache             广播变量及将文件加入分布式缓存
      │              ├─session    
      │              ├─sink              自定义mysql sink及测试
      │              ├─wordcount         基于流式数据和batch数据的wordcount
      │              ├─window            滑动和翻滚窗口的countWindow和timeWindow
      │              ├─dataset_function  dataset相关方法          
      │              ├─tolerance         flink的容错相关
      │              ├─time              event-time和processing-time
      │              ├─model
      │              ├─outformat         多文件输出
      │              ├─parameter         传参
      │              ├─source            自定义mysql source
      │              ├─utils             
      │              ├─state             基于状态的计算
      │              ├─table_sql         table和sql
      │              ├─watermark         水印和延迟数据的处理
      │              └─ttl               生存时间
      └─resources
   
