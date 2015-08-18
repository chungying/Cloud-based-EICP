package hbase;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
public class test {
	public static void main(String... args){
		System.out.println("start!");
		System.out.println(String.format("%d", 0));
		HTable table = null;
		try {
			table = new HTable(HBaseConfiguration.create(), ICPEnhanced.R1tablename);
			Get get = new Get(Bytes.toBytes(ICPEnhanced.rowkey06));
//			get.addColumn(ICPEnhanced.family00.getBytes(), "0".getBytes());
			Result r = table.get(get);
			System.out.println(
					Bytes.toString(ICPEnhanced.family00.getBytes())+":"+
							Bytes.toString("0".getBytes())
					);
//			ICPEnhanced.ByteToDouble(
					if(r.getValue(
							ICPEnhanced.family00.getBytes(),
							"0".getBytes()) == null)
							System.out.println("NULLLLLLLL");
//							)
//					);
					int index;
					int count1 = 0, count2 = 0, count3 = 0;
					for(Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> e: r.getMap().entrySet()){
						for(Entry<byte[], NavigableMap<Long, byte[]>> e2: e.getValue().entrySet()){
							for(Entry<Long, byte[]> e3: e2.getValue().entrySet()){
								//family
//								System.out.println(Bytes.toString(e.getKey()));
								//qualifier
//								System.out.println(Bytes.toString(e2.getKey()));
								//value
//								System.out.println(Bytes.toString(e3.getValue()));
			
								if(Bytes.compareTo( e.getKey(), ICPEnhanced.family00.getBytes())==0 ){
									try{
										index = Integer.parseInt(Bytes.toString(e2.getKey()));
										if( index < ICPEnhanced.SIZE2 &&  index >= 0){
//											refPset[index].x = ICPEnhanced.ByteToDouble(e3.getValue());
											
										}
										count1++;
									}catch(Exception excep){
										//do nothing, just pass the exception to avoid unparseble integer.
									}
									
								}else if(Bytes.compareTo( e.getKey(), ICPEnhanced.family01.getBytes())==0 ){
									try{
										index = Integer.parseInt(Bytes.toString(e2.getKey()));
										if( index < ICPEnhanced.SIZE2 &&  index >= 0){
//											refPset[index].y = ICPEnhanced.ByteToDouble(e3.getValue());
											
										}
										count2++;
									}catch(Exception excep){
										//do nothing, just pass the exception to avoid unparseble integer.
									}
								}
								count3++;
							}
						}
					}
					System.out.println("count1:" + count1);
					System.out.println("count2:" + count2);
					System.out.println("count1:" + count3);
//			for (int i = 0; i < ICPEnhanced.SIZE2; i++) {
//				ICPEnhanced.ByteToDouble(r.getValue(
//						Bytes.toBytes(ICPEnhanced.family00), ICPEnhanced.IntToByte(i)));
//				ICPEnhanced.ByteToDouble(r.getValue(
//						Bytes.toBytes(ICPEnhanced.family01), ICPEnhanced.IntToByte(i)));
//				
//			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}
}
