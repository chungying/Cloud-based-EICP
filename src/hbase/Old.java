package hbase;

import java.io.*;
import java.net.URI;
import java.sql.Ref;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.awt.*;
import java.awt.event.*;
import java.awt.Graphics.*;

import javax.swing.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.awt.image.BufferedImage;

class Point2D2 {
	public double x;
	public double y;

	public Point2D2(double p1, double p2) {
		x = p1;
		y = p2;
	}
}

public class Old extends BaseRegionObserver {

	public static int SIZE = 361;
	public static int SIZE2 = 722;
	public static int SIZE3 = 1444;
	// public static int LASER_NUM = 25;
	public static int LASER_NUM = 50;
	public static int LASER_NUM2 = 6;
	public static int LASER_NUM3 = 2;
	public static double pair_dis_threshold = 230;
	public static int laserdata[][] = new int[LASER_NUM][SIZE];

	public static String tablename = "icptable";
	public static String rowkey00 = "00";
	public static String rowkey01 = "01";
	public static String rowkey02 = "02";
	public static String rowkey03 = "03";
	public static String rowkey04 = "04";
	public static String rowkey05 = "05";
	public static String rowkey06 = "06";

	public static String rowkey07 = "07";
	public static String rowkey08 = "08";
	public static String rowkey09 = "09";
	public static String rowkey10 = "10";
	public static String rowkey11 = "11";
	public static String rowkey12 = "12";
	public static String rowkey13 = "13";
	public static String rowkey14 = "14";

	public static String family = "fam";
	public static String family00 = "fam00";
	public static String family01 = "fam01";
	public static String family10 = "fam10";
	public static String family11 = "fam11";
	public static String family20 = "fam20";
	public static String family21 = "fam21";
	public static String family30 = "fam30";
	public static String family31 = "fam31";
	public static String family40 = "fam40";
	public static String family41 = "fam41";
	public static String family50 = "fam50";
	public static String family51 = "fam51";

	// ICP algorithm
	public static void ReadData() throws IOException {

		Configuration conf = new Configuration();
		String uriFile = "hdfs:///user/wuser/wen/icp_data_R1_50";
		// String uriFile = "hdfs:///user/eeuser/icp_data_L1";

		FileSystem fs = FileSystem.get(URI.create(uriFile), conf);
		Path file = new Path(uriFile);

		FSDataInputStream fsStream = fs.open(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsStream,
				"UTF-8"));

		String line, tempstring;
		String[] tempArray = null;
		ArrayList mylist = new ArrayList();

		while ((line = br.readLine()) != null) {
			tempstring = line;
			tempArray = tempstring.split("\\s");

			for (int i = 0; i < tempArray.length; i++) {
				mylist.add(tempArray[i]);
			}
		}

		int count = 0;
		// for (int j = 0; j < mylist.size() / SIZE; j++) {
		for (int j = 0; j < LASER_NUM; j++) {
			for (int k = 0; k < SIZE; k++) {
				laserdata[j][k] = (int) Double.parseDouble((String) mylist
						.get(count));
				count++;
				// System.out.printf("laserdata[%d][%d]=%d\n",j,k,laserdata[j][k]);
			}
		}

		br.close();
		fsStream.close();
	}

	public static void WriteData(Point2D2 Pset[], boolean n, boolean close)
			throws IOException {

		File file = new File("hdfs:///user/wuser/wen/out_data2.csv");
		file.createNewFile();
		FileOutputStream fout = new FileOutputStream(file, true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout));
		bw.flush();

		for (int i = 0; i < SIZE; i++) {
			bw.write(String.valueOf(Pset[i].x));
			bw.write(",");
			bw.write(String.valueOf(Pset[i].y));
			bw.write("\n");
		}
		if (close) {
			bw.flush();
			bw.close();
			fout.close();
		}
	}

	public static void LaserSetting(Point2D2 refPset[], Point2D2 tgtPset[],
			Point2D2 step_tgtPset[], int num, boolean first) throws IOException {

		double testrange = 180.5;
		double degree = 0.5;
		double temp_x, temp_y = 0;

		if (first) {
			for (int i = 0; i < SIZE; i++) {
				temp_x = ((laserdata[num][i] * Math
						.cos((testrange - ((i + 1) * degree)) / 180 * 3.14)));
				temp_y = ((laserdata[num][i] * Math
						.sin((testrange - ((i + 1) * degree)) / 180 * 3.14)));

				refPset[i].x = temp_x;
				refPset[i].y = temp_y;
				tgtPset[i] = new Point2D2(refPset[i].x, refPset[i].y);
				step_tgtPset[i] = new Point2D2(refPset[i].x, refPset[i].y);
				// System.out.printf("refPset[%d] = (%f,%f)\n", i,
				// refPset.get(i).x, refPset.get(i).y);
			}
			first = false;
		} else {
			for (int i = 0; i < SIZE; i++) {
				refPset[i] = new Point2D2(step_tgtPset[i].x, step_tgtPset[i].y);
			}
			for (int i = 0; i < SIZE; i++) {

				temp_x = ((laserdata[num][i] * Math
						.cos((testrange - ((i + 1) * degree)) / 180 * 3.14)));
				temp_y = ((laserdata[num][i] * Math
						.sin((testrange - ((i + 1) * degree)) / 180 * 3.14)));

				tgtPset[i].x = temp_x;
				tgtPset[i].y = temp_y;
				step_tgtPset[i] = new Point2D2(tgtPset[i].x, tgtPset[i].y);
			}
		}

	}

	public static void TR(Point2D2 step_tgtPset[], double T_x, double T_y,
			double R_th, double sum_R_th, int num) {

		double tmp_x, tmp_y = 0;

		for (int i = 0; i < num; i++) {
			tmp_x = (step_tgtPset[i].x * Math.cos(R_th))
					- (step_tgtPset[i].y * Math.sin(R_th)) + T_x;
			tmp_y = (step_tgtPset[i].x * Math.sin(R_th))
					+ (step_tgtPset[i].y * Math.cos(R_th)) + T_y;

			step_tgtPset[i].x = tmp_x;
			step_tgtPset[i].y = tmp_y;

		}
		R_th += R_th;
	}

	public static double p2p_length(Point2D2 p, Point2D2 q) {
		return Math.sqrt(Math.pow(p.x - q.x, 2) + Math.pow(p.y - q.y, 2));
	}

	public static Point2D2 find_closest_point(Point2D2 step_tgtPset,
			Point2D2 refPset[], int size) {

		Point2D2 ret = new Point2D2(0, 0);
		double min_distance = 0;
		ret = refPset[0];

		min_distance = p2p_length(step_tgtPset, refPset[0]);

		for (int i = 1; i < size; i++) {
			if (p2p_length(step_tgtPset, refPset[i]) < min_distance) {
				ret = refPset[i];
				min_distance = p2p_length(step_tgtPset, refPset[i]);
			}
		}

		return ret;
	}

	public static double[] icp_step(Point2D2 step_tgtPset[], Point2D2 refPset[],
			int t_size, int r_size) {

		double sum_bx_times_ay = 0;
		double sum_by_times_ax = 0;
		double sum_bx_times_ax = 0;
		double sum_by_times_ay = 0;
		double mean_bx = 0;
		double mean_by = 0;
		double mean_ax = 0;
		double mean_ay = 0;
		int ignore = 0;
		double d_data[] = new double[3];

		final Point2D2 ClosestPoint[] = new Point2D2[t_size];

		for (int i = 0; i < t_size; i++) {

			ClosestPoint[i] = find_closest_point(step_tgtPset[i], refPset,
					r_size);

			if (p2p_length(step_tgtPset[i], ClosestPoint[i]) > pair_dis_threshold) {
				ignore++;
			} else {
				mean_bx += step_tgtPset[i].x;
				mean_by += step_tgtPset[i].y;
				mean_ax += ClosestPoint[i].x;
				mean_ay += ClosestPoint[i].y;
			}
		}

		mean_bx = mean_bx / (t_size - ignore);
		mean_by = mean_by / (t_size - ignore);
		mean_ax = mean_ax / (t_size - ignore);
		mean_ay = mean_ay / (t_size - ignore);

		sum_bx_times_ay = 0; // bx*ays
		sum_by_times_ax = 0;
		sum_bx_times_ax = 0;
		sum_by_times_ay = 0;

		for (int i = 0; i < t_size; i++) {
			if (p2p_length(step_tgtPset[i], ClosestPoint[i]) < pair_dis_threshold) {

				sum_bx_times_ay += (step_tgtPset[i].x - mean_bx)
						* (ClosestPoint[i].y - mean_ay);
				sum_by_times_ax += (step_tgtPset[i].y - mean_by)
						* (ClosestPoint[i].x - mean_ax);
				sum_bx_times_ax += (step_tgtPset[i].x - mean_bx)
						* (ClosestPoint[i].x - mean_ax);
				sum_by_times_ay += (step_tgtPset[i].y - mean_by)
						* (ClosestPoint[i].y - mean_ay);
			}
		}

		d_data[2] = Math.atan2(sum_bx_times_ay - sum_by_times_ax,
				sum_bx_times_ax + sum_by_times_ay);
		d_data[0] = mean_ax
				- ((mean_bx * Math.cos(d_data[2])) - (mean_by * Math
						.sin(d_data[2])));
		d_data[1] = mean_ay
				- ((mean_bx * Math.sin(d_data[2])) + (mean_by * Math
						.cos(d_data[2])));

		return d_data;

	}

	public static double[] icp(Point2D2 step_tgtPset[], Point2D2 refPset[],
			int t_size, int r_size, boolean fs, Point2D2 position) {

		double d_data[] = new double[4];
		double step_d_data[] = new double[3];
		double error = 0;
		double pre_err = 0;
		int iterative = 0;
		double r_tmp_x, r_tmp_y = 0;
		double tmp_x, tmp_y = 0;
		double dth = 0;

		final Point2D2 closest[] = new Point2D2[t_size];

		do {
			step_d_data = icp_step(step_tgtPset, refPset, t_size, r_size);

			if (fs) {
				r_tmp_x = position.x * Math.cos(step_d_data[2])
						- (position.y * Math.sin(step_d_data[2]))
						+ step_d_data[0];
				r_tmp_y = position.y * Math.sin(step_d_data[2])
						+ (position.y * Math.cos(step_d_data[2]))
						+ step_d_data[1];
				position.x = r_tmp_x;
				position.y = r_tmp_y;
				dth = step_d_data[2];
			}
			pre_err = error;
			error = 0;

			for (int i = 0; i < t_size; i++) {
				tmp_x = (step_tgtPset[i].x * Math.cos(step_d_data[2]))
						- (step_tgtPset[i].y * Math.sin(step_d_data[2]))
						+ step_d_data[0];
				tmp_y = (step_tgtPset[i].x * Math.sin(step_d_data[2]))
						+ (step_tgtPset[i].y * Math.cos(step_d_data[2]))
						+ step_d_data[1];

				step_tgtPset[i].x = tmp_x;
				step_tgtPset[i].y = tmp_y;
				// error += (Math.Pow(step_tgtPset[i].x - refPset[i].x, 2) +
				// Math.Pow(step_tgtPset[i].y - refPset[i].y, 2));
			}
			int point_num = 0;

			for (int i = 0; i < t_size; i++) {
				closest[i] = find_closest_point(step_tgtPset[i], refPset,
						r_size);
				// if (p2p_length(step_tgtPset[i], closest[i]) <
				// pair_dis_threshold)
				// {
				// error += (Math.Pow(step_tgtPset[i].x - closest[i].x, 2) +
				// Math.Pow(step_tgtPset[i].y - closest[i].y, 2));
				error += Math.sqrt((Math.pow(step_tgtPset[i].x - closest[i].x,
						2) + Math.pow(step_tgtPset[i].y - closest[i].y, 2)));
				point_num++;
				// }

			}

			d_data[0] += step_d_data[0];
			d_data[1] += step_d_data[1];
			d_data[2] += step_d_data[2];

			error /= point_num;
			iterative++;
			d_data[3] += dth;

		} while (Math.abs(error - pre_err) > 0.00001);

		// System.out.printf("dx=%f, dy=%f, dth=%f, error=%f\n", d_data[0],
		// d_data[1], d_data[2] / (Math.PI / 180), error);

		return d_data;

	}

	// HBase
	public static byte[] IntToByte(int i) {
		return Bytes.toBytes(String.format("%d", i));
	}

	public static byte[] DoubleToByte(double d) {
		return Bytes.toBytes(String.format("%.4f", d));
	}

	public static double ByteToDouble(byte[] b) {
		return Double.parseDouble(Bytes.toString(b));
	}

	public static void TableSetting(Point2D2 pSet[], Put name, String rowkey,
			String family_x, String family_y, int num, int x)
			throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name
		name = new Put(Bytes.toBytes(rowkey)); // row key

		for (int i = x; i < num; i++) {
			name.add(Bytes.toBytes(family_x), ICPEnhanced.IntToByte(i),
					ICPEnhanced.DoubleToByte(pSet[i - x].x));
			name.add(Bytes.toBytes(family_y), ICPEnhanced.IntToByte(i),
					ICPEnhanced.DoubleToByte(pSet[i - x].y));
		}
		table.put(name);
		table.close();
	}

	public static void DataSetting(double dx, double dy, double dth, Put name,
			String rowkey, String family) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name
		name = new Put(Bytes.toBytes(rowkey)); // row key

		name.add(Bytes.toBytes(family), Bytes.toBytes("dx"),
				ICPEnhanced.DoubleToByte(dx));
		name.add(Bytes.toBytes(family), Bytes.toBytes("dy"),
				ICPEnhanced.DoubleToByte(dy));
		name.add(Bytes.toBytes(family), Bytes.toBytes("dth"),
				ICPEnhanced.DoubleToByte(dth));
		table.put(name);
		table.close();
	}

	public static void SplitRow(String rowkey, HRegion region, Get get,
			int r_num, int t_num, String rowkey2, String f1, String f2)
			throws IOException {

		final Point2D2 refPset[] = new Point2D2[SIZE];
		final Point2D2 tgtPset[] = new Point2D2[SIZE];
		final Point2D2 step_tgtPset[] = new Point2D2[SIZE];
		final Point2D2 all_step[][] = new Point2D2[(LASER_NUM / 5) - 1][SIZE];
		final Point2D2 position = new Point2D2(0, 0);
		final Point2D2 r_position[] = new Point2D2[(LASER_NUM / 5)];
		final Point2D2 like_step[] = new Point2D2[SIZE * ((LASER_NUM / 5) - 1)];

		double r_angle = 0;
		double sum_r_angle = 0;
		double d_data[] = new double[3];
		int space_num = 0;

		boolean fs = false;
		boolean first_match = true;

		for (int i = 0; i < (LASER_NUM / 5) - 1; i++) {
			for (int j = 0; j < SIZE; j++) {
				all_step[i][j] = new Point2D2(0, 0);
			}
		}
		for (int i = 0; i < SIZE; i++) {
			refPset[i] = new Point2D2(0, 0);
			tgtPset[i] = new Point2D2(0, 0);
			step_tgtPset[i] = new Point2D2(0, 0);
		}
		for (int i = 0; i < (LASER_NUM / 5); i++) {
			r_position[i] = new Point2D2(0, 0);
		}

		for (int i = 0; i < SIZE * ((LASER_NUM / 5) - 1); i++) {
			like_step[i] = new Point2D2(0, 0);
		}

		position.x = 0;
		position.y = 0;
		r_position[0].x = position.x;
		r_position[0].y = position.y;

		// ====================================================

		Put put = null;

		LaserSetting(refPset, tgtPset, step_tgtPset, r_num, true);

		TableSetting(refPset, put, rowkey2, f1, f2, SIZE, 0); // for combine

		for (int i = 0; i < SIZE; i++) {
			all_step[0][i] = new Point2D2(refPset[i].x, refPset[i].y);
		}

		for (int n = 1; n < 5; n++) {

			LaserSetting(refPset, tgtPset, step_tgtPset, t_num, false);

			TR(step_tgtPset, r_position[n - 1].x, r_position[n - 1].y, r_angle,
					sum_r_angle, SIZE);

			if (first_match) {
				fs = true;
				d_data = icp(step_tgtPset, refPset, SIZE, SIZE, fs, position);
				if (fs) {
					r_position[n] = position;
					r_angle = d_data[3];
				}
				// System.out.printf("dx=%f, dy=%f, dth=%f\n", d_data[0],
				// d_data[1], d_data[2] / (Math.PI / 180));

				for (int i = 0; i < SIZE; i++) {
					all_step[n][i] = new Point2D2(step_tgtPset[i].x,
							step_tgtPset[i].y);
				}
				first_match = false;
			} else {
				final Point2D2 temp_step[] = new Point2D2[SIZE];

				for (int i = 0; i < SIZE; i++) {
					temp_step[i] = new Point2D2(0, 0);
				}

				for (int i = 0; i < SIZE; i++) {
					temp_step[i] = new Point2D2(step_tgtPset[i].x,
							step_tgtPset[i].y);
				}

				// all_data_to_align
				for (int i = 0; i < n; i++) {
					for (int j = 0; j < SIZE; j++) {
						like_step[j + (i * SIZE)] = new Point2D2(
								all_step[i][j].x, all_step[i][j].y);
					}
				}

				fs = false;
				d_data = icp(temp_step, like_step, SIZE, SIZE * n, fs, position);
				// ===============================================================
				double x1, x2, y1, y2;
				boolean side = false;
				x1 = temp_step[0].x;
				x2 = temp_step[SIZE - 1].x;
				y1 = temp_step[0].y;
				y2 = temp_step[SIZE - 1].y;

				if ((temp_step[SIZE / 2].x * (y2 - y1) - temp_step[SIZE / 2].y
						* (x2 - x1)) < (x1 * y2 - y1 * x2)) {
					side = true;
				} else if ((temp_step[SIZE / 2].x * (y2 - y1) - temp_step[SIZE / 2].y
						* (x2 - x1)) > (x1 * y2 - y1 * x2)) {
					side = false;
				}
				// int c = 0;
				for (int i = 0; i < n; i++) {
					for (int j = 0; j < SIZE; j++) {
						if (side) {
							if (((all_step[i][j].x * (y2 - y1) - all_step[i][j].y
									* (x2 - x1)) <= (x1 * y2 - y1 * x2))) {
								like_step[j + (i * SIZE)] = new Point2D2(
										all_step[i][j].x, all_step[i][j].y);
							} else {
								like_step[j + (i * SIZE)].x = 0;
								like_step[j + (i * SIZE)].y = 0;
								// c++;
							}
						} else {
							if (((all_step[i][j].x * (y2 - y1) - all_step[i][j].y
									* (x2 - x1)) >= (x1 * y2 - y1 * x2))) {
								like_step[j + (i * SIZE)] = all_step[i][j];
							} else {
								like_step[j + (i * SIZE)].x = 0;
								like_step[j + (i * SIZE)].y = 0;
								// c++;
							}
						}
					}
				}

				fs = true;
				d_data = icp(step_tgtPset, like_step, SIZE, SIZE * n, fs,
						position);
				if (fs) {
					r_position[n] = position;
					r_angle += d_data[3];
				}
				// System.out.printf("dx=%f, dy=%f, dth=%f\n", d_data[0],
				// d_data[1], d_data[2] / (Math.PI / 180));

				if (LASER_NUM == 50) {
					if (space_num % 2 == 0) {
						for (int i = 0; i < SIZE; i++) {
							all_step[0][i].x = 0;
							all_step[0][i].y = 0;
							all_step[0][i] = new Point2D2(step_tgtPset[i].x,
									step_tgtPset[i].y);
						}
					}
					space_num++;
				} else {
					if (space_num % 2 == 0) {
						for (int i = 0; i < SIZE; i++) {
							all_step[n - (space_num / 2)][i] = new Point2D2(
									step_tgtPset[i].x, step_tgtPset[i].y);
						}
					}
					space_num++;
				}
			}
			t_num++;
			// if (write) {
			// for (int i = 0; i < SIZE; i++) {
			// out.writeBytes(String.valueOf(step_tgtPset[i].x));
			// out.writeBytes("	");
			// out.writeBytes(String.valueOf(step_tgtPset[i].y));
			// out.writeBytes("\n");
			// }
			// }

			// for combine
			// TableSetting(step_tgtPset, put, rowkey2, f1, f2, SIZE * (n + 1),
			// SIZE * n);
		}
		// for combine
		TableSetting(step_tgtPset, put, rowkey2, f1, f2, SIZE2, SIZE);

		DataSetting(r_position[4].x, r_position[4].y, r_angle, put, rowkey2, f1);

		get = new Get(Bytes.toBytes(rowkey));
		// Result r = region.get(get);

		Cell dx = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("dx"), ICPEnhanced.DoubleToByte(r_position[4].x));
		Cell dy = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("dy"), ICPEnhanced.DoubleToByte(r_position[4].y));
		Cell dth = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("dth"), ICPEnhanced.DoubleToByte(r_angle
						/ (Math.PI / 180)));

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name

		Put re = new Put(Bytes.toBytes(rowkey));

		re.add(dx);
		re.add(dy);
		re.add(dth);
		table.put(re);
		table.close();

	}

	public static void CombineRow(String rowkey, HRegion region,
			String rowkey2, String f1, String f2) throws IOException {

		final Point2D2 refPset[] = new Point2D2[SIZE2];
		final Point2D2 tgtPset[] = new Point2D2[SIZE2];
		final Point2D2 step_tgtPset[] = new Point2D2[SIZE2];
		final Point2D2 all_step[][] = new Point2D2[LASER_NUM2 - 1][SIZE2];
		final Point2D2 position = new Point2D2(0, 0);
		final Point2D2 r_position[] = new Point2D2[LASER_NUM2];
		final Point2D2 like_step[] = new Point2D2[SIZE2 * LASER_NUM2 - 1];

		double r_angle = 0;
		double sum_r_angle = 0;
		double d_data[] = new double[3];
		int space_num = 0;

		boolean fs = false;
		boolean first_match = true;

		for (int i = 0; i < 3; i++) {
			d_data[i] = 0;
		}

		for (int i = 0; i < LASER_NUM2 - 1; i++) {
			for (int j = 0; j < SIZE2; j++) {
				all_step[i][j] = new Point2D2(0, 0);
			}
		}
		for (int i = 0; i < SIZE2; i++) {
			refPset[i] = new Point2D2(0, 0);
			tgtPset[i] = new Point2D2(0, 0);
			step_tgtPset[i] = new Point2D2(0, 0);
		}
		for (int i = 0; i < LASER_NUM2; i++) {
			r_position[i] = new Point2D2(0, 0);
		}

		for (int i = 0; i < SIZE2 * (LASER_NUM2 - 1); i++) {
			like_step[i] = new Point2D2(0, 0);
		}

		position.x = 0;
		position.y = 0;
		r_position[0].x = position.x;
		r_position[0].y = position.y;

		// ====================================================

		Put put = null;

		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = region.get(get);

		for (int i = 0; i < SIZE2; i++) {
			refPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family00), ICPEnhanced.IntToByte(i)));
			refPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family01), ICPEnhanced.IntToByte(i)));
			tgtPset[i] = new Point2D2(refPset[i].x, refPset[i].y);
			step_tgtPset[i] = new Point2D2(refPset[i].x, refPset[i].y);
			all_step[0][i] = new Point2D2(refPset[i].x, refPset[i].y);
		}

		// for final combine
		TableSetting(refPset, put, rowkey2, f1, f2, SIZE2, 0);

		for (int n = 1; n < LASER_NUM2; n++) {

			String f00 = null;
			String f01 = null;

			if (n == 1) {
				f00 = family10;
				f01 = family11;
			}
			if (n == 2) {
				f00 = family20;
				f01 = family21;
			}
			if (n == 3) {
				f00 = family30;
				f01 = family31;
			}
			if (n == 4) {
				f00 = family40;
				f01 = family41;
			}
			if (n == 5) {
				f00 = family50;
				f01 = family51;
			}

			for (int i = 0; i < SIZE2; i++) {
				refPset[i] = new Point2D2(step_tgtPset[i].x, step_tgtPset[i].y);
			}

			for (int i = 0; i < SIZE2; i++) {
				tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
						Bytes.toBytes(f00), ICPEnhanced.IntToByte(i)));
				tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
						Bytes.toBytes(f01), ICPEnhanced.IntToByte(i)));
				step_tgtPset[i] = new Point2D2(tgtPset[i].x, tgtPset[i].y);
			}

			TR(step_tgtPset, r_position[n - 1].x, r_position[n - 1].y, r_angle,
					sum_r_angle, SIZE2);

			if (first_match) {
				fs = true;
				d_data = icp(step_tgtPset, refPset, SIZE2, SIZE2, fs, position);
				if (fs) {
					r_position[n] = position;
					r_angle = d_data[3];
				}
				// System.out.printf("dx=%f, dy=%f, dth=%f\n", d_data[0],
				// d_data[1], d_data[2] / (Math.PI / 180));

				for (int i = 0; i < SIZE2; i++) {
					all_step[n][i] = new Point2D2(step_tgtPset[i].x,
							step_tgtPset[i].y);
				}
				first_match = false;
			} else {
				final Point2D2 temp_step[] = new Point2D2[SIZE2];

				for (int i = 0; i < SIZE2; i++) {
					temp_step[i] = new Point2D2(0, 0);
				}

				for (int i = 0; i < SIZE2; i++) {
					temp_step[i] = new Point2D2(step_tgtPset[i].x,
							step_tgtPset[i].y);
				}

				// all_data_to_align
				for (int i = 0; i < n; i++) {
					for (int j = 0; j < SIZE2; j++) {
						like_step[j + (i * SIZE2)] = new Point2D2(
								all_step[i][j].x, all_step[i][j].y);
					}
				}
				fs = false;
				d_data = icp(temp_step, like_step, SIZE2, SIZE2 * n, fs,
						position);
				// ===============================================================
				double x1, x2, y1, y2;
				boolean side = false;
				// x1 = temp_step[0].x;
				// x2 = temp_step[SIZE2 - 1].x;
				// y1 = temp_step[0].y;
				// y2 = temp_step[SIZE2 - 1].y;

				x1 = temp_step[SIZE2 - 361].x;
				x2 = temp_step[SIZE2 - 1].x;
				y1 = temp_step[SIZE2 - 361].y;
				y2 = temp_step[SIZE2 - 1].y;

				double a, b, c, v, m = 0;
				a = y2 - y1;
				b = x1 - x2;
				c = (y1 * x2) - (x1 * y2);
				v = (temp_step[(SIZE2 - 361) - ((SIZE2 / 5) / 2)].x * a)
						+ (temp_step[(SIZE2 - 361) - ((SIZE2 / 5) / 2)].y * b)
						+ c;

				for (int i = 0; i < n; i++) {
					for (int j = 0; j < SIZE2; j++) {

						m = v
								* ((like_step[j + (i * SIZE2)].x * a)
										+ (like_step[j + (i * SIZE2)].y * b) + c);

						if (m >= 0) {
							// like_step[j + (i * SIZE)] = new Point2D2(
							// all_step[i][j].x, all_step[i][j].y);
						} else {
							like_step[j + (i * SIZE2)].x = 0;
							like_step[j + (i * SIZE2)].y = 0;
						}

					}
				}

				// if ((temp_step[SIZE2 / 2].x * (y2 - y1) - temp_step[SIZE2 /
				// 2].y
				// * (x2 - x1)) < (x1 * y2 - y1 * x2)) {
				// side = true;
				// } else if ((temp_step[SIZE2 / 2].x * (y2 - y1) -
				// temp_step[SIZE2 / 2].y
				// * (x2 - x1)) > (x1 * y2 - y1 * x2)) {
				// side = false;
				// }
				// // int c = 0;
				// for (int i = 0; i < n; i++) {
				// for (int j = 0; j < SIZE2; j++) {
				// if (side) {
				// if (((all_step[i][j].x * (y2 - y1) - all_step[i][j].y
				// * (x2 - x1)) <= (x1 * y2 - y1 * x2))) {
				// like_step[j + (i * SIZE2)] = new Point2D(
				// all_step[i][j].x, all_step[i][j].y);
				// } else {
				// like_step[j + (i * SIZE2)].x = 0;
				// like_step[j + (i * SIZE2)].y = 0;
				// // c++;
				// }
				// } else {
				// if (((all_step[i][j].x * (y2 - y1) - all_step[i][j].y
				// * (x2 - x1)) >= (x1 * y2 - y1 * x2))) {
				// like_step[j + (i * SIZE2)] = all_step[i][j];
				// } else {
				// like_step[j + (i * SIZE2)].x = 0;
				// like_step[j + (i * SIZE2)].y = 0;
				// // c++;
				// }
				// }
				// }
				// }
				fs = true;
				d_data = icp(step_tgtPset, like_step, SIZE2, SIZE2 * n, fs,
						position);
				if (fs) {
					r_position[n] = position;
					r_angle += d_data[3];
				}
				// System.out.printf("dx=%f, dy=%f, dth=%f\n", d_data[0],
				// d_data[1], d_data[2] / (Math.PI / 180));

				// if (n >= 17) {
				// if (space_num % 2 == 0) {
				// for (int i = 0; i < SIZE2; i++) {
				// all_step[0][i].x = 0;
				// all_step[0][i].y = 0;
				// all_step[0][i] = new Point2D(step_tgtPset[i].x,
				// step_tgtPset[i].y);
				// }
				// }
				// space_num++;
				// } else {
				// if (space_num % 2 == 0) {
				// for (int i = 0; i < SIZE2; i++) {
				// all_step[n - (space_num / 2)][i] = new Point2D(
				// step_tgtPset[i].x, step_tgtPset[i].y);
				// }
				// }
				// space_num++;
				// }
			}

		}
		// for final combine
		TableSetting(step_tgtPset, put, rowkey2, f1, f2, SIZE3, SIZE2);
		// DataSetting(r_position[LASER_NUM2 - 1].x, r_position[LASER_NUM2 -
		// 1].y,
		// r_angle, put, rowkey2, f1);

		// Cell tr_dx = new KeyValue(get.getRow(), Bytes.toBytes(family),
		// Bytes.toBytes("tr_dx"),
		// ICPEnhanced.DoubleToByte(r_position[LASER_NUM2 - 1].x));
		// Cell tr_dy = new KeyValue(get.getRow(), Bytes.toBytes(family),
		// Bytes.toBytes("tr_dy"),
		// ICPEnhanced.DoubleToByte(r_position[LASER_NUM2 - 1].y));
		// Cell tr_dth = new KeyValue(get.getRow(), Bytes.toBytes(family),
		// Bytes.toBytes("tr_dth"), ICPEnhanced.DoubleToByte(r_angle
		// / (Math.PI / 180)));

		double sum_dx, sum_dy, sum_dth = 0;
		sum_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
						Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
						Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
						Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
						Bytes.toBytes("dx"))) + r_position[LASER_NUM2 - 1].x;

		sum_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
						Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
						Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
						Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
						Bytes.toBytes("dy"))) + r_position[LASER_NUM2 - 1].y;

		sum_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
						Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
						Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
						Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
						Bytes.toBytes("dth"))) + r_angle;

		// for final combine
		DataSetting(sum_dx, sum_dy, sum_dth, put, rowkey2, f1);

		Cell r_dx = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dx"), ICPEnhanced.DoubleToByte(sum_dx));
		Cell r_dy = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dy"), ICPEnhanced.DoubleToByte(sum_dy));
		Cell r_dth = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dth"), ICPEnhanced.DoubleToByte(sum_dth
						/ (Math.PI / 180)));

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name

		Put re = new Put(Bytes.toBytes(rowkey));

		// re.add(tr_dx);
		// re.add(tr_dy);
		// re.add(tr_dth);
		re.add(r_dx);
		re.add(r_dy);
		re.add(r_dth);
		table.put(re);
		table.close();

	}

	public static void FinalCombine(String rowkey, HRegion region)
			throws IOException {

		final Point2D2 refPset[] = new Point2D2[SIZE3];
		final Point2D2 tgtPset[] = new Point2D2[SIZE3];
		final Point2D2 step_tgtPset[] = new Point2D2[SIZE3];
		final Point2D2 position = new Point2D2(0, 0);
		final Point2D2 r_position[] = new Point2D2[LASER_NUM3];

		double r_angle = 0;
		double sum_r_angle = 0;
		double d_data[] = new double[3];

		boolean fs = false;

		for (int i = 0; i < 3; i++) {
			d_data[i] = 0;
		}

		for (int i = 0; i < SIZE3; i++) {
			refPset[i] = new Point2D2(0, 0);
			tgtPset[i] = new Point2D2(0, 0);
			step_tgtPset[i] = new Point2D2(0, 0);
		}
		for (int i = 0; i < LASER_NUM3; i++) {
			r_position[i] = new Point2D2(0, 0);
		}

		position.x = 0;
		position.y = 0;
		r_position[0].x = position.x;
		r_position[0].y = position.y;

		// ====================================================

		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = region.get(get);

		for (int i = 0; i < SIZE3; i++) {
			refPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family00), ICPEnhanced.IntToByte(i)));
			refPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family01), ICPEnhanced.IntToByte(i)));
		}

		for (int i = 0; i < SIZE3; i++) {
			tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family10), ICPEnhanced.IntToByte(i)));
			tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family11), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i] = new Point2D2(tgtPset[i].x, tgtPset[i].y);
		}

		TR(step_tgtPset, r_position[0].x, r_position[0].y, r_angle,
				sum_r_angle, SIZE3);

		fs = true;
		d_data = icp(step_tgtPset, refPset, SIZE3, SIZE3, fs, position);
		if (fs) {
			r_position[1] = position;
			r_angle = d_data[3];
		}
		// System.out.printf("dx=%f, dy=%f, dth=%f\n", d_data[0],
		// d_data[1], d_data[2] / (Math.PI / 180));

		double sum_dx, sum_dy, sum_dth = 0;
		sum_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dx")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dx"))) + r_position[LASER_NUM3 - 1].x;

		sum_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dy")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dy"))) + r_position[LASER_NUM3 - 1].y;

		sum_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dth")))
				+ ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
						Bytes.toBytes("dth"))) + r_angle;

		Cell r_dx = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dx"), ICPEnhanced.DoubleToByte(sum_dx));
		Cell r_dy = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dy"), ICPEnhanced.DoubleToByte(sum_dy));
		Cell r_dth = new KeyValue(get.getRow(), Bytes.toBytes(family),
				Bytes.toBytes("r_dth"), ICPEnhanced.DoubleToByte(sum_dth
						/ (Math.PI / 180)));

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name

		Put re = new Put(Bytes.toBytes(rowkey));

		re.add(r_dx);
		re.add(r_dy);
		re.add(r_dth);
		table.put(re);
		table.close();

	}

	public static void TRdata(String rowkey, HRegion region) throws IOException {

		final Point2D2 step_tgtPset[] = new Point2D2[SIZE2];
		// double temp_x, temp_y = 0;
		double sum_r_angle = 0;
		double f0_dx, f0_dy, f0_dth = 0;
		double f1_dx, f1_dy, f1_dth = 0;
		double f2_dx, f2_dy, f2_dth = 0;
		double f3_dx, f3_dy, f3_dth = 0;
		double f4_dx, f4_dy, f4_dth = 0;
		double f5_dx, f5_dy, f5_dth = 0;
		Put put = null;

		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i] = new Point2D2(0, 0);
		}

		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = region.get(get);

		f0_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dx")));
		f0_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dy")));
		f0_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dth")));

		f1_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dx")));
		f1_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dy")));
		f1_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dth")));

		f2_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
				Bytes.toBytes("dx")));
		f2_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
				Bytes.toBytes("dy")));
		f2_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family20),
				Bytes.toBytes("dth")));

		f3_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
				Bytes.toBytes("dx")));
		f3_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
				Bytes.toBytes("dy")));
		f3_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family30),
				Bytes.toBytes("dth")));

		f4_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
				Bytes.toBytes("dx")));
		f4_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
				Bytes.toBytes("dy")));
		f4_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family40),
				Bytes.toBytes("dth")));

		f5_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
				Bytes.toBytes("dx")));
		f5_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
				Bytes.toBytes("dy")));
		f5_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family50),
				Bytes.toBytes("dth")));

		// 1
		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family10), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family11), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx, f0_dy, f0_dth, sum_r_angle, SIZE2);
		TableSetting(step_tgtPset, put, rowkey, family10, family11, SIZE2, 0);

		// 2
		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family20), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family21), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx + f1_dx, f0_dy + f1_dy, f0_dth + f1_dth,
				sum_r_angle, SIZE2);
		TableSetting(step_tgtPset, put, rowkey, family20, family21, SIZE2, 0);

		// 3
		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family30), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family31), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx + f1_dx + f2_dx, f0_dy + f1_dy + f2_dy, f0_dth
				+ f1_dth + f2_dth, sum_r_angle, SIZE2);
		TableSetting(step_tgtPset, put, rowkey, family30, family31, SIZE2, 0);

		// 4
		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family40), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family41), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx + f1_dx + f2_dx + f3_dx, f0_dy + f1_dy + f2_dy
				+ f3_dy, f0_dth + f1_dth + f2_dth + f3_dth, sum_r_angle, SIZE2);

		TableSetting(step_tgtPset, put, rowkey, family40, family41, SIZE2, 0);

		// 5
		for (int i = 0; i < SIZE2; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family50), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family51), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx + f1_dx + f2_dx + f3_dx + f4_dx, f0_dy + f1_dy
				+ f2_dy + f3_dy + f4_dy, f0_dth + f1_dth + f2_dth + f3_dth
				+ f4_dth, sum_r_angle, SIZE2);

		TableSetting(step_tgtPset, put, rowkey, family50, family51, SIZE2, 0);

	}

	public static void FinalTRdata(String rowkey, HRegion region)
			throws IOException {

		final Point2D2 step_tgtPset[] = new Point2D2[SIZE3];
		// double temp_x, temp_y = 0;
		double sum_r_angle = 0;
		double f0_dx, f0_dy, f0_dth = 0;
		double f1_dx, f1_dy, f1_dth = 0;
		Put put = null;

		for (int i = 0; i < SIZE3; i++) {
			step_tgtPset[i] = new Point2D2(0, 0);
		}

		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = region.get(get);

		f0_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dx")));
		f0_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dy")));
		f0_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family00),
				Bytes.toBytes("dth")));

		f1_dx = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dx")));
		f1_dy = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dy")));
		f1_dth = ICPEnhanced.ByteToDouble(r.getValue(Bytes.toBytes(family10),
				Bytes.toBytes("dth")));

		// 1
		for (int i = 0; i < SIZE3; i++) {
			step_tgtPset[i].x = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family10), ICPEnhanced.IntToByte(i)));
			step_tgtPset[i].y = ICPEnhanced.ByteToDouble(r.getValue(
					Bytes.toBytes(family11), ICPEnhanced.IntToByte(i)));
		}
		TR(step_tgtPset, f0_dx, f0_dy, f0_dth, sum_r_angle, SIZE3);
		TableSetting(step_tgtPset, put, rowkey, family10, family11, SIZE3, 0);
	}

	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> c,
			Get get, List<KeyValue> result) throws IOException {

		ReadData();

		HRegion region = c.getEnvironment().getRegion();

		if (Bytes.toString(get.getRow()).contains("a")) {
			SplitRow(rowkey00, region, get, 0, 1, rowkey06, family00, family01);
		}
		if (Bytes.toString(get.getRow()).contains("b")) {
			SplitRow(rowkey01, region, get, 4, 5, rowkey06, family10, family11);
		}
		if (Bytes.toString(get.getRow()).contains("c")) {
			SplitRow(rowkey02, region, get, 8, 9, rowkey06, family20, family21);
		}
		if (Bytes.toString(get.getRow()).contains("d")) {
			SplitRow(rowkey03, region, get, 12, 13, rowkey06, family30,
					family31);
		}
		if (Bytes.toString(get.getRow()).contains("e")) {
			SplitRow(rowkey04, region, get, 16, 17, rowkey06, family40,
					family41);
		}
		if (Bytes.toString(get.getRow()).contains("f")) {
			SplitRow(rowkey05, region, get, 20, 21, rowkey06, family50,
					family51);
		}
		if (Bytes.toString(get.getRow()).contains("g")) {
			SplitRow(rowkey07, region, get, 25, 26, rowkey13, family00,
					family01);
		}
		if (Bytes.toString(get.getRow()).contains("h")) {
			SplitRow(rowkey08, region, get, 29, 30, rowkey13, family10,
					family11);
		}
		if (Bytes.toString(get.getRow()).contains("i")) {
			SplitRow(rowkey09, region, get, 33, 34, rowkey13, family20,
					family21);
		}
		if (Bytes.toString(get.getRow()).contains("j")) {
			SplitRow(rowkey10, region, get, 37, 38, rowkey13, family30,
					family31);
		}
		if (Bytes.toString(get.getRow()).contains("k")) {
			SplitRow(rowkey11, region, get, 41, 42, rowkey13, family40,
					family41);
		}
		if (Bytes.toString(get.getRow()).contains("l")) {
			SplitRow(rowkey12, region, get, 45, 46, rowkey13, family50,
					family51);
		}
		if (Bytes.toString(get.getRow()).contains("tx")) {
			TRdata(rowkey06, region);
		}
		if (Bytes.toString(get.getRow()).contains("ty")) {
			TRdata(rowkey13, region);
		}
		if (Bytes.toString(get.getRow()).contains("rx")) {
			CombineRow(rowkey06, region, rowkey14, family00, family01);
		}
		if (Bytes.toString(get.getRow()).contains("ry")) {
			CombineRow(rowkey13, region, rowkey14, family10, family11);
		}
		if (Bytes.toString(get.getRow()).contains("tz")) {
			FinalTRdata(rowkey14, region);
		}
		if (Bytes.toString(get.getRow()).contains("rz")) {
			FinalCombine(rowkey14, region);
		}

	}

	public static void main(String[] args) throws IOException {

		double start_time = System.currentTimeMillis();

		System.out.printf("start time: %.0f\n", start_time);

		Configuration conf = HBaseConfiguration.create();
		final HTablePool pool = new HTablePool(conf, 20);
		final HTableInterface[] tables = new HTableInterface[20];
		for (int n = 0; n < 20; n++) {
			tables[n] = pool.getTable(tablename);
		}

		Thread get00 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("00a"));
				try {
					tables[0].get(get);
					pool.putTable(tables[0]);
					System.out.println("get00");
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		});

		Thread get01 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("01b"));
				try {
					tables[1].get(get);
					pool.putTable(tables[1]);
					System.out.println("get01");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get02 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("02c"));
				try {
					tables[2].get(get);
					pool.putTable(tables[2]);
					System.out.println("get02");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get03 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("03d"));
				try {
					tables[3].get(get);
					pool.putTable(tables[3]);
					System.out.println("get03");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get04 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("04e"));
				try {
					tables[4].get(get);
					pool.putTable(tables[4]);
					System.out.println("get04");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get05 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("05f"));
				try {
					tables[5].get(get);
					pool.putTable(tables[5]);
					System.out.println("get05");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get07 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("07g"));
				try {
					tables[6].get(get);
					pool.putTable(tables[6]);
					System.out.println("get07");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get08 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("08h"));
				try {
					tables[7].get(get);
					pool.putTable(tables[7]);
					System.out.println("get08");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get09 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("09i"));
				try {
					tables[8].get(get);
					pool.putTable(tables[8]);
					System.out.println("get09");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get10 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("10j"));
				try {
					tables[9].get(get);
					pool.putTable(tables[9]);
					System.out.println("get10");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get11 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("11k"));
				try {
					tables[10].get(get);
					pool.putTable(tables[10]);
					System.out.println("get11");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get12 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("12l"));
				try {
					tables[11].get(get);
					pool.putTable(tables[11]);
					System.out.println("get12");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		get00.start();
		get01.start();
		get02.start();
		get03.start();
		get04.start();
		get05.start();
		get07.start();
		get08.start();
		get09.start();
		get10.start();
		get11.start();
		get12.start();

		Thread getTR_0 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("06tx"));
				try {
					tables[12].get(get);
					pool.putTable(tables[12]);
					System.out.println("getTR_0");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread getTR_1 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("13ty"));
				try {
					tables[13].get(get);
					pool.putTable(tables[13]);
					System.out.println("getTR_1");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			getTR_0.sleep(5000);
			getTR_1.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		getTR_0.start();
		getTR_1.start();

		Thread get06 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("06rx"));
				try {
					tables[14].get(get);
					pool.putTable(tables[14]);
					System.out.println("get06");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get13 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("13ry"));
				try {
					tables[15].get(get);
					pool.putTable(tables[15]);
					System.out.println("get13");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			get06.sleep(1000);
			get13.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		get06.start();
		get13.start();

		Thread getTR_F = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("14tz"));
				try {
					tables[16].get(get);
					pool.putTable(tables[16]);
					System.out.println("getTR_F");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread get14 = new Thread(new Runnable() {
			public void run() {
				Get get = new Get(Bytes.toBytes("14rz"));
				try {
					tables[17].get(get);
					pool.putTable(tables[17]);
					System.out.println("get14");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			getTR_F.sleep(8000);
			get14.sleep(8000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		getTR_F.run();
		get14.run();

		pool.closeTablePool(tablename);

		double end_time = System.currentTimeMillis();
		System.out.println("Using time:" + (end_time - start_time) / 1000);
		// System.exit(0);
	}
}
