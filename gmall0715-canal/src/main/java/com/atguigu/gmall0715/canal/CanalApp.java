package com.atguigu.gmall0715.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        //1.客户端连接服务器端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example", "", "");
        //2.利用连接器抓取数据
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall0715.*");
            //一个message=一次抓取  一次抓取可以抓多个sql的执行结果集
            //每条sql执行的结果放在一个entry 一个message包含多个entry
            Message message = canalConnector.get(100);

            if(message.getEntries().size()==0){
                System.out.println("没有数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                // 3 抓取数据后，提取数据
                //一个entry 代表一个sql执行的结果集
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //业务数据 StoreValue
                    if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                        continue;
                    }
                        //取出序列化后的值集合
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化工具
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        if (rowChange!=null) {
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集
                            String tableName = entry.getHeader().getTableName();//表名
                            CanalEntry.EventType eventType = rowChange.getEventType();//事件类型 insert?update?delete

                            // 4 处理业务数据  发送kafka 到对应的topic
                            CanalHandler canalHandler = new CanalHandler(rowChange.getEventType(), tableName, rowDatasList);
                            canalHandler.handle();
                        }


                }

            }

        }

    }
}
