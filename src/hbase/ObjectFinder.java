package hbase;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;

import com.googlecode.javacpp.Loader;
import com.googlecode.javacv.BaseChildSettings;
import com.googlecode.javacv.CanvasFrame;
import com.googlecode.javacv.cpp.opencv_nonfree;

import static com.googlecode.javacv.cpp.opencv_calib3d.*;
import static com.googlecode.javacv.cpp.opencv_core.*;
import static com.googlecode.javacv.cpp.opencv_flann.*;
import static com.googlecode.javacv.cpp.opencv_highgui.*;
import static com.googlecode.javacv.cpp.opencv_imgproc.*;
import static com.googlecode.javacv.cpp.opencv_legacy.*;

import com.googlecode.javacv.cpp.opencv_nonfree;
import com.googlecode.javacv.cpp.opencv_nonfree.*;

import static com.googlecode.javacv.cpp.opencv_nonfree.SURF;

import com.googlecode.javacv.cpp.opencv_core.IplImage;
import com.googlecode.javacv.cpp.opencv_features2d.*;
import com.googlecode.javacv.cpp.opencv_objdetect.*;
import com.googlecode.javacpp.Loader;

public class ObjectFinder {

	static {
		Loader.load(opencv_nonfree.class);
	}

	public static String SURFtablename = "surftable";

	public ObjectFinder(IplImage objectImage) {
		settings = new Settings();
		settings.objectImage = objectImage;
		setSettings(settings);
	}

	public ObjectFinder(Settings settings) {
		setSettings(settings);
	}

	public static class Settings extends BaseChildSettings {
		IplImage objectImage = null;
		CvSURFParams parameters = cvSURFParams(500, 1);
		double distanceThreshold = 0.6;
		int matchesMin = 4;
		double ransacReprojThreshold = 1.0;
		boolean useFLANN = false;

		public IplImage getObjectImage() {
			return objectImage;
		}

		public void setObjectImage(IplImage objectImage) {
			this.objectImage = objectImage;
		}

		public boolean isExtended() {
			return parameters.extended() != 0;
		}

		public void setExtended(boolean extended) {
			parameters.extended(extended ? 1 : 0);
		}

		public boolean isUpright() {
			return parameters.upright() != 0;
		}

		public void setUpright(boolean upright) {
			parameters.upright(upright ? 1 : 0);
		}

		public double getHessianThreshold() {
			return parameters.hessianThreshold();
		}

		public void setHessianThreshold(double hessianThreshold) {
			parameters.hessianThreshold(hessianThreshold);
		}

		public int getnOctaves() {
			return parameters.nOctaves();
		}

		public void setnOctaves(int nOctaves) {
			parameters.nOctaves(nOctaves);
		}

		public int getnOctaveLayers() {
			return parameters.nOctaveLayers();
		}

		public void setnOctaveLayers(int nOctaveLayers) {
			parameters.nOctaveLayers(nOctaveLayers);
		}

		public double getDistanceThreshold() {
			return distanceThreshold;
		}

		public void setDistanceThreshold(double distanceThreshold) {
			this.distanceThreshold = distanceThreshold;
		}

		public int getMatchesMin() {
			return matchesMin;
		}

		public void setMatchesMin(int matchesMin) {
			this.matchesMin = matchesMin;
		}

		public double getRansacReprojThreshold() {
			return ransacReprojThreshold;
		}

		public void setRansacReprojThreshold(double ransacReprojThreshold) {
			this.ransacReprojThreshold = ransacReprojThreshold;
		}

		public boolean isUseFLANN() {
			return useFLANN;
		}

		public void setUseFLANN(boolean useFLANN) {
			this.useFLANN = useFLANN;
		}
	}

	private Settings settings;

	public Settings getSettings() {
		return settings;
	}

	public void setSettings(Settings settings) {
		this.settings = settings;

		CvSeq keypoints = new CvSeq(null), descriptors = new CvSeq(null);
		cvClearMemStorage(storage);

		cvExtractSURF(settings.objectImage, null, keypoints, descriptors,
				storage, settings.parameters, 0);

		int total = descriptors.total();
		int size = descriptors.elem_size();
		objectKeypoints = new CvSURFPoint[total];
		objectDescriptors = new FloatBuffer[total];
		for (int i = 0; i < total; i++) {
			objectKeypoints[i] = new CvSURFPoint(cvGetSeqElem(keypoints, i));
			objectDescriptors[i] = cvGetSeqElem(descriptors, i).capacity(size)
					.asByteBuffer().asFloatBuffer();
		}
		if (settings.useFLANN) {
			int length = objectDescriptors[0].capacity();
			objectMat = CvMat.create(total, length, CV_32F, 1);
			imageMat = CvMat.create(total, length, CV_32F, 1);
			indicesMat = CvMat.create(total, 2, CV_32S, 1);
			distsMat = CvMat.create(total, 2, CV_32F, 1);

			flannIndex = new Index();
			indexParams = new KDTreeIndexParams(4); // using 4 randomized
													// kdtrees
			searchParams = new SearchParams(64, 0, true); // maximum number of
															// leafs checked
		}
		pt1 = CvMat.create(1, total, CV_32F, 2);
		pt2 = CvMat.create(1, total, CV_32F, 2);
		mask = CvMat.create(1, total, CV_8U, 1);
		H = CvMat.create(3, 3);
		ptpairs = new ArrayList<Integer>(2 * objectDescriptors.length);
		// System.out.println(total + " object descriptors");
		// logger.info(total + " object descriptors");

	}

	private static final Logger logger = Logger.getLogger(ObjectFinder.class
			.getName());

	private CvMemStorage storage = CvMemStorage.create();
	private CvMemStorage tempStorage = CvMemStorage.create();
	private CvSURFPoint[] objectKeypoints = null, imageKeypoints = null;
	private FloatBuffer[] objectDescriptors = null, imageDescriptors = null;
	private CvMat objectMat, imageMat, indicesMat, distsMat;
	private Index flannIndex = null;
	private IndexParams indexParams = null;
	private SearchParams searchParams = null;
	private CvMat pt1 = null, pt2 = null, mask = null, H = null;
	private ArrayList<Integer> ptpairs = null;
	private static int matching_num = 0;

	public double[] find(IplImage image) {
		CvSeq keypoints = new CvSeq(null), descriptors = new CvSeq(null);
		cvClearMemStorage(tempStorage);
		cvExtractSURF(image, null, keypoints, descriptors, tempStorage,
				settings.parameters, 0);

		int total = descriptors.total();
		int size = descriptors.elem_size();
		imageKeypoints = new CvSURFPoint[total];
		imageDescriptors = new FloatBuffer[total];

		for (int i = 0; i < total; i++) {
			imageKeypoints[i] = new CvSURFPoint(cvGetSeqElem(keypoints, i));
			imageDescriptors[i] = cvGetSeqElem(descriptors, i).capacity(size)
					.asByteBuffer().asFloatBuffer();
		}
		// System.out.println(total + " image descriptors");
		// logger.info(total + " image descriptors");

		int w = settings.objectImage.width();
		int h = settings.objectImage.height();
		double[] srcCorners = { 0, 0, w, 0, w, h, 0, h };
		double[] dstCorners = locatePlanarObject(objectKeypoints,
				objectDescriptors, imageKeypoints, imageDescriptors, srcCorners);
		cvReleaseImage(settings.objectImage);
		return dstCorners;
	}

	private double compareSURFDescriptors(FloatBuffer d1, FloatBuffer d2,
			double best) {
		double totalCost = 0;
		assert (d1.capacity() == d2.capacity() && d1.capacity() % 4 == 0);
		for (int i = 0; i < d1.capacity(); i += 4) {
			double t0 = d1.get(i) - d2.get(i);
			double t1 = d1.get(i + 1) - d2.get(i + 1);
			double t2 = d1.get(i + 2) - d2.get(i + 2);
			double t3 = d1.get(i + 3) - d2.get(i + 3);
			totalCost += t0 * t0 + t1 * t1 + t2 * t2 + t3 * t3;
			if (totalCost > best)
				break;
		}
		return totalCost;
	}

	private int naiveNearestNeighbor(FloatBuffer vec, int laplacian,
			CvSURFPoint[] modelKeypoints, FloatBuffer[] modelDescriptors) {
		int neighbor = -1;
		double d, dist1 = 1e6, dist2 = 1e6;

		for (int i = 0; i < modelDescriptors.length; i++) {
			CvSURFPoint kp = modelKeypoints[i];
			FloatBuffer mvec = modelDescriptors[i];
			if (laplacian != kp.laplacian())
				continue;
			d = compareSURFDescriptors(vec, mvec, dist2);
			if (d < dist1) {
				dist2 = dist1;
				dist1 = d;
				neighbor = i;
			} else if (d < dist2) {
				dist2 = d;
			}
		}
		if (dist1 < settings.distanceThreshold * dist2)
			return neighbor;
		return -1;
	}

	private void findPairs(CvSURFPoint[] objectKeypoints,
			FloatBuffer[] objectDescriptors, CvSURFPoint[] imageKeypoints,
			FloatBuffer[] imageDescriptors) {
		for (int i = 0; i < objectDescriptors.length; i++) {
			CvSURFPoint kp = objectKeypoints[i];
			FloatBuffer descriptor = objectDescriptors[i];
			int nearestNeighbor = naiveNearestNeighbor(descriptor,
					kp.laplacian(), imageKeypoints, imageDescriptors);
			if (nearestNeighbor >= 0) {
				ptpairs.add(i);
				ptpairs.add(nearestNeighbor);
			}
		}
	}

	private void flannFindPairs(FloatBuffer[] objectDescriptors,
			FloatBuffer[] imageDescriptors) {
		int length = objectDescriptors[0].capacity();

		if (imageMat.rows() < imageDescriptors.length) {
			imageMat = CvMat.create(imageDescriptors.length, length, CV_32F, 1);
		}
		int imageRows = imageMat.rows();
		imageMat.rows(imageDescriptors.length);

		// copy descriptors
		FloatBuffer objectBuf = objectMat.getFloatBuffer();
		for (int i = 0; i < objectDescriptors.length; i++) {
			objectBuf.put(objectDescriptors[i]);
		}

		FloatBuffer imageBuf = imageMat.getFloatBuffer();
		for (int i = 0; i < imageDescriptors.length; i++) {
			imageBuf.put(imageDescriptors[i]);
		}

		// find nearest neighbors using FLANN
		flannIndex.build(imageMat, indexParams, FLANN_DIST_L2);
		flannIndex.knnSearch(objectMat, indicesMat, distsMat, 2, searchParams);

		IntBuffer indicesBuf = indicesMat.getIntBuffer();
		FloatBuffer distsBuf = distsMat.getFloatBuffer();
		for (int i = 0; i < objectDescriptors.length; i++) {
			if (distsBuf.get(2 * i) < settings.distanceThreshold
					* distsBuf.get(2 * i + 1)) {
				ptpairs.add(i);
				ptpairs.add(indicesBuf.get(2 * i));
			}
		}
		imageMat.rows(imageRows);
	}

	/* a rough implementation for object location */
	private double[] locatePlanarObject(CvSURFPoint[] objectKeypoints,
			FloatBuffer[] objectDescriptors, CvSURFPoint[] imageKeypoints,
			FloatBuffer[] imageDescriptors, double[] srcCorners) {
		ptpairs.clear();
		if (settings.useFLANN) {
			flannFindPairs(objectDescriptors, imageDescriptors);
		} else {
			findPairs(objectKeypoints, objectDescriptors, imageKeypoints,
					imageDescriptors);
		}

		// int n = ptpairs.size()/2;
		matching_num = ptpairs.size() / 2;
		// System.out.println("*" + matching_num + "*" +
		// " matching pairs found");
		// logger.info(n + " matching pairs found");
		if (matching_num < settings.matchesMin) {
			return null;
		}

		pt1.cols(matching_num);
		pt2.cols(matching_num);
		mask.cols(matching_num);
		for (int i = 0; i < matching_num; i++) {
			CvPoint2D32f p1 = objectKeypoints[ptpairs.get(2 * i)].pt();
			pt1.put(2 * i, p1.x());
			pt1.put(2 * i + 1, p1.y());
			CvPoint2D32f p2 = imageKeypoints[ptpairs.get(2 * i + 1)].pt();
			pt2.put(2 * i, p2.x());
			pt2.put(2 * i + 1, p2.y());
		}

		if (cvFindHomography(pt1, pt2, H, CV_RANSAC,
				settings.ransacReprojThreshold, mask) == 0) {
			return null;
		}
		if (cvCountNonZero(mask) < settings.matchesMin) {
			return null;
		}

		double[] h = H.get();
		double[] dstCorners = new double[srcCorners.length];
		for (int i = 0; i < srcCorners.length / 2; i++) {
			double x = srcCorners[2 * i], y = srcCorners[2 * i + 1];
			double Z = 1 / (h[6] * x + h[7] * y + h[8]);
			double X = (h[0] * x + h[1] * y + h[2]) * Z;
			double Y = (h[3] * x + h[4] * y + h[5]) * Z;
			dstCorners[2 * i] = X;
			dstCorners[2 * i + 1] = Y;
		}

		pt1.cols(objectDescriptors.length);
		pt2.cols(objectDescriptors.length);
		mask.cols(objectDescriptors.length);

		return dstCorners;
	}

	public static void TableSetting(String family_surf, int num, int a, int b,
			int matching, Put name, String rowkey, String tablename)
			throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tablename); // table name
		name = new Put(Bytes.toBytes(rowkey)); // row key

		name.add(Bytes.toBytes(family_surf), ICPEnhanced.IntToByte(a + b),
				ICPEnhanced.DoubleToByte(matching));

		table.put(name);
		table.close();
	}

	public static IplImage ReadImage(String imagename) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(URI.create(imagename), conf);
		BufferedImage bImage = ImageIO.read(fs.open(new Path(imagename)));
		IplImage image = IplImage.createFrom(bImage);

		return image;
	}

	public static void SURF(int num, String family_surf, String rowkey, int a,
			int b) throws InterruptedException, IOException {

		// String objectFilename = null;
		// String sceneFilename = null;

		// for (int a = num; a < num + 10; a += 2) {
		// objectFilename = String.format("hdfs:///user/wuser/wen/R1_%d.jpg",
		// a);
		// System.out.sprintf(objectFilename, "R1_%d.jpg", i);
		// for (int b = 0; b < 10; b += 2) {
		// System.out.sprintf(sceneFilename, "R2_%d.jpg", j);
		// sceneFilename = String.format("hdfs:///user/wuser/wen/R2_%d.jpg", b);

		// String objectFilename = args.length == 2 ? args[0] : "R1_0.jpg";
		// String sceneFilename = args.length == 2 ? args[1] : "R2_0.jpg";

		//String objectFilename = "hdfs:///user/wuser/wen/R1_0.jpg";
		//String sceneFilename = "hdfs:///user/wuser/wen/R2_0.jpg";

		String objectFilename = "hdfs:///user/eeuser/lib/R1_0.jpg";
		String sceneFilename = "hdfs:///user/eeuser/lib/R2_0.jpg";
		
		// Logger.getRootLogger().setLevel(Level.WARN);

		IplImage object = ReadImage(objectFilename);
		IplImage image = ReadImage(sceneFilename);

		// cvNamedWindow("surf", 0);
		// cvShowImage("surf", bIpl);
		// cvWaitKey(0);

		// IplImage object = cvLoadImage(objectFilename,
		// CV_LOAD_IMAGE_GRAYSCALE);
		// IplImage image = cvLoadImage(sceneFilename, CV_LOAD_IMAGE_GRAYSCALE);
		if (object == null || image == null) {
			System.err.println("Can not load " + objectFilename + " and/or "
					+ sceneFilename);
			System.exit(-1);
		}

//		IplImage objectColor = IplImage.create(object.width(), object.height(),
//				8, 3);
//		cvCvtColor(object, objectColor, CV_GRAY2BGR);
//
//		IplImage correspond = IplImage.create(image.width(), object.height()
//				+ image.height(), 8, 1);
//		cvSetImageROI(correspond, cvRect(0, 0, object.width(), object.height()));
//		cvCopy(object, correspond);
//		cvSetImageROI(
//				correspond,
//				cvRect(0, object.height(), correspond.width(),
//						correspond.height()));
//		cvCopy(image, correspond);
//		cvResetImageROI(correspond);
		//
		// ObjectFinder.Settings settings = new ObjectFinder.Settings();
		// settings.objectImage = object;
		// settings.useFLANN = true;
		// settings.ransacReprojThreshold = 5;
		// ObjectFinder finder = new ObjectFinder(settings);

		// double[] dst_corners = finder.find(image);

		Put put = null;

		// if (matching_num > 70) {
		TableSetting(family_surf, num, a, b, matching_num, put, rowkey,
				SURFtablename);
		// System.out.println(num + "->" + "[" + a + "]" + "[" + b
		// + "]" + matching_num + " matching pairs found");

		// if (dst_corners != null) {
		// for (int i = 0; i < 4; i++) {
		// int j = (i + 1) % 4;
		// int x1 = (int) Math.round(dst_corners[2 * i]);
		// int y1 = (int) Math.round(dst_corners[2 * i + 1]);
		// int x2 = (int) Math.round(dst_corners[2 * j]);
		// int y2 = (int) Math.round(dst_corners[2 * j + 1]);
		// cvLine(correspond,
		// cvPoint(x1, y1 + object.height()),
		// cvPoint(x2, y2 + object.height()),
		// CvScalar.WHITE, 1, 8, 0);
		// }
		// }
		//
		// for (int i = 0; i < finder.ptpairs.size(); i += 2) {
		// CvPoint2D32f pt1 = finder.objectKeypoints[finder.ptpairs
		// .get(i)].pt();
		// CvPoint2D32f pt2 = finder.imageKeypoints[finder.ptpairs
		// .get(i + 1)].pt();
		// cvLine(correspond,
		// cvPointFrom32f(pt1),
		// cvPoint(Math.round(pt2.x()),
		// Math.round(pt2.y() + object.height())),
		// CvScalar.WHITE, 1, 8, 0);
		// }
		//
		// // CanvasFrame objectFrame = new CanvasFrame("Object");
		// CanvasFrame correspondFrame = new CanvasFrame(
		// "Object Correspond");
		//
		// correspondFrame.showImage(correspond);
		//
		// for (int i = 0; i < finder.objectKeypoints.length; i++) {
		// CvSURFPoint r = finder.objectKeypoints[i];
		// CvPoint center = cvPointFrom32f(r.pt());
		// int radius = Math.round(r.size() * 1.2f / 9 * 2);
		// cvCircle(objectColor, center, radius, CvScalar.RED, 1,
		// 8, 0);
		// }
		// // objectFrame.showImage(objectColor);
		// // objectFrame.waitKey();
		// // objectFrame.dispose();
		//
		// correspondFrame.waitKey(1000);
		// correspondFrame.dispose();
		// }

		// }
		// }
	}

	// public static void main(String[] args) throws Exception {
	//
	// long start = System.currentTimeMillis();
	//
	// SURF(0);
	// SURF(20);
	// SURF(40);
	// SURF(60);
	// SURF(80);
	// SURF(100);

	// Thread surf0 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(0);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// Thread surf20 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(20);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// Thread surf40 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(40);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// Thread surf60 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(60);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// Thread surf80 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(80);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// Thread surf100 = new Thread(new Runnable() {
	// public void run() {
	// try {
	// SURF(100);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });

	// surf0.start();
	// surf20.start();
	// surf40.start();
	// surf60.start();
	// surf80.start();
	// surf100.start();

	// System.out.println("Finding time = "
	// + (System.currentTimeMillis() - start) / 1000 + " s");
	// }
}
