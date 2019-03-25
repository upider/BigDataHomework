import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import javax.xml.stream.events.EndDocument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

/**
 * @author yuzezhong
 * @version v1.0
 */

public class Hw1Grp0 {
	public static void main(String[] args)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, URISyntaxException {
		// if (args.length < 6) {
		// System.out.println("Usage: Hw1Grp0 R=<file 1> S=<file 2> join:R2=S3
		// res:R4,S5");
		// System.exit(1);
		// }
		String R = args[0].substring(2);
		String S = args[1].substring(2);
		int joinR = Integer.parseInt(args[2].substring(6, 7));
		int joinS = Integer.parseInt(args[2].substring(9, 10));
		int resR = Integer.parseInt(args[3].substring(5, 6));
		int resS = Integer.parseInt(args[3].substring(8, 9));

		// open R files
		Configuration conf = new Configuration();
		FileSystem fsR = FileSystem.get(URI.create(R), conf);
		Path pathR = new Path(R);
		FSDataInputStream inR_stream = fsR.open(pathR);
		BufferedReader inR = new BufferedReader(new InputStreamReader(inR_stream));
		// open S files
		FileSystem fsS = FileSystem.get(URI.create(S), conf);
		Path pathS = new Path(S);
		FSDataInputStream inS_stream = fsS.open(pathS);
		BufferedReader inS = new BufferedReader(new InputStreamReader(inS_stream));

		// create tow hashtable
		Hashtable<Integer, String> hTableR = new Hashtable<Integer, String>();
		String s = "";
		String ss[];
		while ((s = inR.readLine()) != null) {
			ss = s.split("\\|");
			hTableR.put(ss[joinR].hashCode(), ss[resR]);
		}

		// close R & S files
		// inR.close();
		// fsR.close();
		// inS.close();
		// fsS.close();

		// compare two hasetable and return result
		// create table descriptor
		String tableName = "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
		// create column descriptor
		HColumnDescriptor cf = new HColumnDescriptor("res");
		htd.addFamily(cf);
		// configure HBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(configuration);

		// if tableName exists, delete it
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		admin.createTable(htd);
		admin.close();
		HTable table = new HTable(configuration, tableName.getBytes());

		String rowKey = "RandS";
		Put put = new Put(rowKey.getBytes());

		// compute S.hashkey; if S.hashkey = R.hashkey then input their value into Result
		for (int i = 0; i < hTableR.size(); i++) {
			while ((s = inS.readLine()) != null) {
				ss = s.split("\\|");
				if (hTableR.containsKey(ss[joinS].hashCode())) {
					//System.out.println(hTableR.get(ss[joinS].hashCode()));
					//System.out.println("returnS[resR]:" + ss[resS]);
					put.add("res".getBytes(),hTableR.get(ss[joinS].hashCode()).getBytes(),ss[resS].getBytes());
					table.put(put);
					System.out.println("put successfully");
				}
			}
		}
		table.close();
	}
}
