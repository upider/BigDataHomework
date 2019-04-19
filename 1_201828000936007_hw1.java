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

class Hw1Grp1 {
	public static void main(String[] args)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, URISyntaxException {
		if (args.length < 4) {
			System.out.println("Usage: Hw1Grp0 R=<file 1> S=<file 2> join:R2=S3 res:R4,S5");
			System.exit(1);
		}
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

		// create two map to storage two tables,// sort tables
		LinkedList<String[]> lstR = new LinkedList<String[]>();
		LinkedList<String[]> lstS = new LinkedList<String[]>();
		Map<String, String> mapR = new TreeMap<String, String>();
		Map<String, String> mapS = new TreeMap<String, String>();

		String s = "";
		String ss[] = new String[2];
		while ((s = inR.readLine()) != null) {
			ss = s.split("\\|");
			mapR.put(ss[joinR], ss[resR]);
		}

		while ((s = inS.readLine()) != null) {
			ss = s.split("\\|");
			mapS.put(ss[joinS], ss[resS]);
		}

		Iterator<Map.Entry<String, String>> iteratorR = mapR.entrySet().iterator();
		while (iteratorR.hasNext()) {
			String[] sss = { "", "" };
			Map.Entry<String, String> entry = iteratorR.next();
			sss[0] = entry.getKey();
			sss[1] = entry.getValue();
			lstR.add(sss);
		}
		Iterator<Map.Entry<String, String>> iteratorS = mapS.entrySet().iterator();
		while (iteratorS.hasNext()) {
			String[] sss = { "", "" };
			Map.Entry<String, String> entry = iteratorS.next();
			sss[0] = entry.getKey();
			sss[1] = entry.getValue();
			lstS.add(sss);
		}
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

		// merge join
		ss[0] = "endR";
		ss[1] = "endR";
		lstR.add(ss);
		ss[0] = "endS";
		ss[1] = "endS";
		lstS.add(ss);
		ListIterator<String[]> iterR = lstR.listIterator();
		ListIterator<String[]> iterS = lstS.listIterator();
		ss = iterR.next();
		String keyr = ss[0];
		String valuer = ss[1];
		ss = iterS.next();
		String keys = ss[0];
		String values = ss[1];
		while (iterR.hasNext() && iterS.hasNext()) {
			if (keyr.compareTo(keys) < 0) {
				ss = iterR.next();
				keyr = ss[0];
				valuer = ss[1];
			}
			if (keyr.compareTo(keys) > 0) {
				ss = iterS.next();
				keys = ss[0];
				values = ss[1];

			}
			if (keyr.compareTo(keys) == 0) {
				String rowKey = keyr;
				Put put = new Put(rowKey.getBytes());
				put.add("res".getBytes(), ("R" + resR).getBytes(), valuer.getBytes());
				put.add("res".getBytes(), ("S" + resS).getBytes(), values.getBytes());
				table.put(put);

				ss = iterR.next();
				keyr = ss[0];
				valuer = ss[1];

				int count = 1;
				while (keyr.compareTo(keys) == 0 && iterR.hasNext()) {
					rowKey = keyr;
					Put putRR = new Put(rowKey.getBytes());
					putRR.add("res".getBytes(), ("R" + resR + "." + count).getBytes(), valuer.getBytes());
					table.put(putRR);
					rowKey = keys;
					Put putSS = new Put(rowKey.getBytes());
					putSS.add("res".getBytes(), ("S" + resS + "." + count).getBytes(), values.getBytes());
					table.put(putSS);
					ss = iterR.next();
					keyr = ss[0];
					valuer = ss[1];
					count++;
				}
				ss = iterR.previous();
				ss = iterR.previous();
				keyr = ss[0];
				valuer = ss[1];

				System.out.println("before keyr:" + keyr);
				System.out.println("before keys:" + keys);
				ss = iterS.next();
				keys = ss[0];
				values = ss[1];
				count = 1;
				System.out.println("after keyr:" + keyr);
				System.out.println("after keys:" + keys);
				while (keys.compareTo(keyr) == 0 && iterS.hasNext()) {
					rowKey = keys;
					Put putSS = new Put(rowKey.getBytes());
					putSS.add("res".getBytes(), ("S" + resS + "." + count).getBytes(), values.getBytes());
					table.put(putSS);
					rowKey = keyr;
					Put putRR = new Put(rowKey.getBytes());
					putRR.add("res".getBytes(), ("R" + resR + "." + count).getBytes(), valuer.getBytes());
					table.put(putRR);
					ss = iterS.next();
					keys = ss[0];
					values = ss[1];
					count++;
				}
				ss = iterR.next();
				keyr = ss[0];
				valuer = ss[1];
			}
		}

		table.close();
		inR.close();
		inS.close();
	}
}
