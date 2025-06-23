package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_11
{

	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d29", cols0);
		String [][] records_d29 = new String[135][cols0.length];
		for(int i=0;i<135;i++)
		{
			records_d29[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d29[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d29", records_d29[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8]
		String[] ConditionColumns0 = {"i","g","h","f"};
		String[] ConditionColumnsValues0 = {"i8","g2","h4","f2"};
		DBApp.createBitMapIndex("d29","i");
		DBApp.createBitMapIndex("d29","g");
		DBApp.createBitMapIndex("d29","h");
		DBApp.createBitMapIndex("d29","f");
		DBApp.selectIndex("d29", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a44, b0, c2, d0, e4, f2, g2, h4, i8]
		String tableTrace0 = DBApp.getLastTrace("d29");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n8", cols1);
		String [][] records_n8 = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_n8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n8", records_n8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2]
		String[] ConditionColumns1 = {"j","n","m","k","h","o"};
		String[] ConditionColumnsValues1 = {"j3","n1","m4","k10","h3","o13"};
		DBApp.createBitMapIndex("n8","j");
		DBApp.createBitMapIndex("n8","n");
		DBApp.createBitMapIndex("n8","m");
		DBApp.createBitMapIndex("n8","k");
		DBApp.createBitMapIndex("n8","h");
		DBApp.createBitMapIndex("n8","o");
		DBApp.selectIndex("n8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13]
		String tableTrace1 = DBApp.getLastTrace("n8");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s51j", cols2);
		String [][] records_s51j = new String[252][cols2.length];
		for(int i=0;i<252;i++)
		{
			records_s51j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s51j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s51j", records_s51j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11]
		String[] ConditionColumns2 = {"g","i","e","j","h"};
		String[] ConditionColumnsValues2 = {"g4","i8","e3","j3","h5"};
		DBApp.createBitMapIndex("s51j","g");
		DBApp.createBitMapIndex("s51j","i");
		DBApp.createBitMapIndex("s51j","e");
		DBApp.createBitMapIndex("s51j","j");
		DBApp.createBitMapIndex("s51j","h");
		DBApp.selectIndex("s51j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5]
		String tableTrace2 = DBApp.getLastTrace("s51j");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hgxs6", cols3);
		String [][] records_hgxs6 = new String[119][cols3.length];
		for(int i=0;i<119;i++)
		{
			records_hgxs6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hgxs6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hgxs6", records_hgxs6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8]
		String[] ConditionColumns3 = {"g","b","f"};
		String[] ConditionColumnsValues3 = {"g4","b0","f0"};
		DBApp.createBitMapIndex("hgxs6","g");
		DBApp.createBitMapIndex("hgxs6","b");
		DBApp.createBitMapIndex("hgxs6","f");
		DBApp.selectIndex("hgxs6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 3, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record1: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5]
		//record2: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3]
		String tableTrace3 = DBApp.getLastTrace("hgxs6");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c3h", cols4);
		String [][] records_c3h = new String[340][cols4.length];
		for(int i=0;i<340;i++)
		{
			records_c3h[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_c3h[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("c3h", records_c3h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record335: [a335, b1, c2, d3, e0, f5, g6]
		//record336: [a336, b0, c0, d0, e1, f0, g0]
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		String[] ConditionColumns4 = {"e","f"};
		String[] ConditionColumnsValues4 = {"e1","f5"};
		DBApp.createBitMapIndex("c3h","e");
		DBApp.createBitMapIndex("c3h","f");
		DBApp.selectIndex("c3h", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 11, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a41, b1, c2, d1, e1, f5, g6]
		//record2: [a71, b1, c2, d3, e1, f5, g1]
		//record3: [a101, b1, c2, d1, e1, f5, g3]
		//record4: [a131, b1, c2, d3, e1, f5, g5]
		String tableTrace4 = DBApp.getLastTrace("c3h");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+11));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s71v", cols0);
		String [][] records_s71v = new String[457][cols0.length];
		for(int i=0;i<457;i++)
		{
			records_s71v[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s71v[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s71v", records_s71v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		//record453: [a453, b1, c0, d1, e3, f3, g5]
		//record454: [a454, b0, c1, d2, e4, f4, g6]
		//record455: [a455, b1, c2, d3, e0, f5, g0]
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		String[] ConditionColumns0 = {"d","f","g"};
		String[] ConditionColumnsValues0 = {"d0","f0","g3"};
		DBApp.createBitMapIndex("s71v","d");
		DBApp.createBitMapIndex("s71v","f");
		DBApp.createBitMapIndex("s71v","g");
		DBApp.selectIndex("s71v", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3]
		//record1: [a108, b0, c0, d0, e3, f0, g3]
		//record2: [a192, b0, c0, d0, e2, f0, g3]
		//record3: [a276, b0, c0, d0, e1, f0, g3]
		//record4: [a360, b0, c0, d0, e0, f0, g3]
		String tableTrace0 = DBApp.getLastTrace("s71v");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+6));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s65", cols1);
		String [][] records_s65 = new String[348][cols1.length];
		for(int i=0;i<348;i++)
		{
			records_s65[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s65[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s65", records_s65[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3]
		String[] ConditionColumns1 = {"c","a","e"};
		String[] ConditionColumnsValues1 = {"c0","a147","e2"};
		DBApp.createBitMapIndex("s65","c");
		DBApp.createBitMapIndex("s65","a");
		DBApp.createBitMapIndex("s65","e");
		DBApp.selectIndex("s65", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a147, b1, c0, d3, e2, f3, g0, h3]
		String tableTrace1 = DBApp.getLastTrace("s65");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("re5", cols0);
		String [][] records_re5 = new String[414][cols0.length];
		for(int i=0;i<414;i++)
		{
			records_re5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_re5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("re5", records_re5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2, l1, m6]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2, m7]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3, m8]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10]
		String[] ConditionColumns0 = {"g","m","b","c","l"};
		String[] ConditionColumnsValues0 = {"g6","m9","b1","c1","l7"};
		DBApp.createBitMapIndex("re5","g");
		DBApp.createBitMapIndex("re5","m");
		DBApp.createBitMapIndex("re5","b");
		DBApp.createBitMapIndex("re5","c");
		DBApp.createBitMapIndex("re5","l");
		DBApp.selectIndex("re5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9]
		String tableTrace0 = DBApp.getLastTrace("re5");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("tq", cols1);
		String [][] records_tq = new String[224][cols1.length];
		for(int i=0;i<224;i++)
		{
			records_tq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tq", records_tq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9, o9, p11, q15, r3]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10, o10, p12, q16, r4]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0, n11, o11, p13, q0, r5]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12, p14, q1, r6]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2, n13, o13, p15, q2, r7]
		String[] ConditionColumns1 = {"i","b","m","p","c","h","o","q"};
		String[] ConditionColumnsValues1 = {"i3","b1","m6","p9","c0","h1","o6","q14"};
		DBApp.createBitMapIndex("tq","i");
		DBApp.createBitMapIndex("tq","b");
		DBApp.createBitMapIndex("tq","m");
		DBApp.createBitMapIndex("tq","p");
		DBApp.createBitMapIndex("tq","c");
		DBApp.createBitMapIndex("tq","h");
		DBApp.createBitMapIndex("tq","o");
		DBApp.createBitMapIndex("tq","q");
		DBApp.selectIndex("tq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3, l9, m6, n5, o6, p9, q14, r3]
		String tableTrace1 = DBApp.getLastTrace("tq");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("nv1", cols0);
		String [][] records_nv1 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_nv1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nv1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nv1", records_nv1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		String[] ConditionColumns0 = {"f","e"};
		String[] ConditionColumnsValues0 = {"f1","e1"};
		DBApp.createBitMapIndex("nv1","f");
		DBApp.createBitMapIndex("nv1","e");
		DBApp.selectIndex("nv1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		String tableTrace0 = DBApp.getLastTrace("nv1");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x96", cols1);
		String [][] records_x96 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			records_x96[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x96[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x96", records_x96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2]
		String[] ConditionColumns1 = {"e","g","c","d"};
		String[] ConditionColumnsValues1 = {"e1","g1","c1","d1"};
		DBApp.createBitMapIndex("x96","e");
		DBApp.createBitMapIndex("x96","g");
		DBApp.createBitMapIndex("x96","c");
		DBApp.createBitMapIndex("x96","d");
		DBApp.selectIndex("x96", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		String tableTrace1 = DBApp.getLastTrace("x96");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hb", cols2);
		String [][] records_hb = new String[452][cols2.length];
		for(int i=0;i<452;i++)
		{
			records_hb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hb", records_hb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record447: [a447, b1, c0, d3, e2, f3, g6]
		//record448: [a448, b0, c1, d0, e3, f4, g0]
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		//record450: [a450, b0, c0, d2, e0, f0, g2]
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		String[] ConditionColumns2 = {"e","c"};
		String[] ConditionColumnsValues2 = {"e0","c2"};
		DBApp.createBitMapIndex("hb","e");
		DBApp.createBitMapIndex("hb","c");
		DBApp.selectIndex("hb", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 30, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a20, b0, c2, d0, e0, f2, g6]
		//record2: [a35, b1, c2, d3, e0, f5, g0]
		//record3: [a50, b0, c2, d2, e0, f2, g1]
		//record4: [a65, b1, c2, d1, e0, f5, g2]
		String tableTrace2 = DBApp.getLastTrace("hb");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+30));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m4i", cols3);
		String [][] records_m4i = new String[267][cols3.length];
		for(int i=0;i<267;i++)
		{
			records_m4i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m4i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m4i", records_m4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		//record263: [a263, b1, c2, d3, e3, f5, g4]
		//record264: [a264, b0, c0, d0, e4, f0, g5]
		//record265: [a265, b1, c1, d1, e0, f1, g6]
		//record266: [a266, b0, c2, d2, e1, f2, g0]
		String[] ConditionColumns3 = {"d","a"};
		String[] ConditionColumnsValues3 = {"d0","a36"};
		DBApp.createBitMapIndex("m4i","d");
		DBApp.createBitMapIndex("m4i","a");
		DBApp.selectIndex("m4i", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a36, b0, c0, d0, e1, f0, g1]
		String tableTrace3 = DBApp.getLastTrace("m4i");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k70d6", cols4);
		String [][] records_k70d6 = new String[274][cols4.length];
		for(int i=0;i<274;i++)
		{
			records_k70d6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_k70d6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("k70d6", records_k70d6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7]
		String[] ConditionColumns4 = {"j","m","h","i","b","n"};
		String[] ConditionColumnsValues4 = {"j2","m9","h0","i8","b0","n12"};
		DBApp.createBitMapIndex("k70d6","j");
		DBApp.createBitMapIndex("k70d6","m");
		DBApp.createBitMapIndex("k70d6","h");
		DBApp.createBitMapIndex("k70d6","i");
		DBApp.createBitMapIndex("k70d6","b");
		DBApp.createBitMapIndex("k70d6","n");
		DBApp.selectIndex("k70d6", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9, n12]
		String tableTrace4 = DBApp.getLastTrace("k70d6");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qb0v4", cols0);
		String [][] records_qb0v4 = new String[115][cols0.length];
		for(int i=0;i<115;i++)
		{
			records_qb0v4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qb0v4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qb0v4", records_qb0v4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record110: [a110, b0, c2, d2, e0, f2, g5]
		//record111: [a111, b1, c0, d3, e1, f3, g6]
		//record112: [a112, b0, c1, d0, e2, f4, g0]
		//record113: [a113, b1, c2, d1, e3, f5, g1]
		//record114: [a114, b0, c0, d2, e4, f0, g2]
		String[] ConditionColumns0 = {"a","f"};
		String[] ConditionColumnsValues0 = {"a68","f2"};
		DBApp.createBitMapIndex("qb0v4","a");
		DBApp.createBitMapIndex("qb0v4","f");
		DBApp.selectIndex("qb0v4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a68, b0, c2, d0, e3, f2, g5]
		String tableTrace0 = DBApp.getLastTrace("qb0v4");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y46", cols1);
		String [][] records_y46 = new String[333][cols1.length];
		for(int i=0;i<333;i++)
		{
			records_y46[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y46[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y46", records_y46[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3]
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7]
		String[] ConditionColumns1 = {"i","d","j","a","l","e"};
		String[] ConditionColumnsValues1 = {"i8","d3","j5","a35","l11","e0"};
		DBApp.createBitMapIndex("y46","i");
		DBApp.createBitMapIndex("y46","d");
		DBApp.createBitMapIndex("y46","j");
		DBApp.createBitMapIndex("y46","a");
		DBApp.createBitMapIndex("y46","l");
		DBApp.createBitMapIndex("y46","e");
		DBApp.selectIndex("y46", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		String tableTrace1 = DBApp.getLastTrace("y46");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o0w", cols2);
		String [][] records_o0w = new String[161][cols2.length];
		for(int i=0;i<161;i++)
		{
			records_o0w[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o0w[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o0w", records_o0w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2]
		//record157: [a157, b1, c1, d1, e2, f1, g3]
		//record158: [a158, b0, c2, d2, e3, f2, g4]
		//record159: [a159, b1, c0, d3, e4, f3, g5]
		//record160: [a160, b0, c1, d0, e0, f4, g6]
		String[] ConditionColumns2 = {"b","c"};
		String[] ConditionColumnsValues2 = {"b1","c2"};
		DBApp.createBitMapIndex("o0w","b");
		DBApp.createBitMapIndex("o0w","c");
		DBApp.selectIndex("o0w", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 26, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		String tableTrace2 = DBApp.getLastTrace("o0w");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+26));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("d8040", cols0);
		String [][] records_d8040 = new String[127][cols0.length];
		for(int i=0;i<127;i++)
		{
			records_d8040[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d8040[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d8040", records_d8040[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6]
		String[] ConditionColumns0 = {"h","d","g"};
		String[] ConditionColumnsValues0 = {"h1","d1","g6"};
		DBApp.createBitMapIndex("d8040","h");
		DBApp.createBitMapIndex("d8040","d");
		DBApp.createBitMapIndex("d8040","g");
		DBApp.selectIndex("d8040", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1]
		//record1: [a97, b1, c1, d1, e2, f1, g6, h1]
		String tableTrace0 = DBApp.getLastTrace("d8040");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o99yc", cols1);
		String [][] records_o99yc = new String[293][cols1.length];
		for(int i=0;i<293;i++)
		{
			records_o99yc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o99yc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o99yc", records_o99yc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record288: [a288, b0, c0, d0, e3, f0, g1]
		//record289: [a289, b1, c1, d1, e4, f1, g2]
		//record290: [a290, b0, c2, d2, e0, f2, g3]
		//record291: [a291, b1, c0, d3, e1, f3, g4]
		//record292: [a292, b0, c1, d0, e2, f4, g5]
		String[] ConditionColumns1 = {"e","d"};
		String[] ConditionColumnsValues1 = {"e3","d2"};
		DBApp.createBitMapIndex("o99yc","e");
		DBApp.createBitMapIndex("o99yc","d");
		DBApp.selectIndex("o99yc", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a18, b0, c0, d2, e3, f0, g4]
		//record1: [a38, b0, c2, d2, e3, f2, g3]
		//record2: [a58, b0, c1, d2, e3, f4, g2]
		//record3: [a78, b0, c0, d2, e3, f0, g1]
		//record4: [a98, b0, c2, d2, e3, f2, g0]
		String tableTrace1 = DBApp.getLastTrace("o99yc");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+14));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eav", cols2);
		String [][] records_eav = new String[204][cols2.length];
		for(int i=0;i<204;i++)
		{
			records_eav[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eav[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eav", records_eav[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record199: [a199, b1, c1, d3, e4, f1, g3]
		//record200: [a200, b0, c2, d0, e0, f2, g4]
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		//record203: [a203, b1, c2, d3, e3, f5, g0]
		String[] ConditionColumns2 = {"f","d","c"};
		String[] ConditionColumnsValues2 = {"f1","d1","c1"};
		DBApp.createBitMapIndex("eav","f");
		DBApp.createBitMapIndex("eav","d");
		DBApp.createBitMapIndex("eav","c");
		DBApp.selectIndex("eav", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 17, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a25, b1, c1, d1, e0, f1, g4]
		//record3: [a37, b1, c1, d1, e2, f1, g2]
		//record4: [a49, b1, c1, d1, e4, f1, g0]
		String tableTrace2 = DBApp.getLastTrace("eav");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+17));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kya", cols0);
		String [][] records_kya = new String[130][cols0.length];
		for(int i=0;i<130;i++)
		{
			records_kya[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kya[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kya", records_kya[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9]
		String[] ConditionColumns0 = {"d","h","g"};
		String[] ConditionColumnsValues0 = {"d2","h2","g6"};
		DBApp.createBitMapIndex("kya","d");
		DBApp.createBitMapIndex("kya","h");
		DBApp.createBitMapIndex("kya","g");
		DBApp.selectIndex("kya", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		//record1: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0]
		String tableTrace0 = DBApp.getLastTrace("kya");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ja", cols0);
		String [][] records_ja = new String[175][cols0.length];
		for(int i=0;i<175;i++)
		{
			records_ja[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ja[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ja", records_ja[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3]
		String[] ConditionColumns0 = {"c","b"};
		String[] ConditionColumnsValues0 = {"c1","b1"};
		DBApp.createBitMapIndex("ja","c");
		DBApp.createBitMapIndex("ja","b");
		DBApp.selectIndex("ja", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		String tableTrace0 = DBApp.getLastTrace("ja");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+29));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yrz93", cols1);
		String [][] records_yrz93 = new String[119][cols1.length];
		for(int i=0;i<119;i++)
		{
			records_yrz93[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yrz93[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yrz93", records_yrz93[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record114: [a114, b0, c0, d2, e4, f0, g2]
		//record115: [a115, b1, c1, d3, e0, f1, g3]
		//record116: [a116, b0, c2, d0, e1, f2, g4]
		//record117: [a117, b1, c0, d1, e2, f3, g5]
		//record118: [a118, b0, c1, d2, e3, f4, g6]
		String[] ConditionColumns1 = {"f","b","e"};
		String[] ConditionColumnsValues1 = {"f1","b1","e1"};
		DBApp.createBitMapIndex("yrz93","f");
		DBApp.createBitMapIndex("yrz93","b");
		DBApp.createBitMapIndex("yrz93","e");
		DBApp.selectIndex("yrz93", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a61, b1, c1, d1, e1, f1, g5]
		//record3: [a91, b1, c1, d3, e1, f1, g0]
		String tableTrace1 = DBApp.getLastTrace("yrz93");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("g5njc", cols2);
		String [][] records_g5njc = new String[65][cols2.length];
		for(int i=0;i<65;i++)
		{
			records_g5njc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g5njc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g5njc", records_g5njc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0, m8, n4, o0, p12, q9]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1, p13, q10]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6, o2, p14, q11]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3, p15, q12]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0, q13]
		String[] ConditionColumns2 = {"h","k"};
		String[] ConditionColumnsValues2 = {"h0","k2"};
		DBApp.createBitMapIndex("g5njc","h");
		DBApp.createBitMapIndex("g5njc","k");
		DBApp.selectIndex("g5njc", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		String tableTrace2 = DBApp.getLastTrace("g5njc");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k0nj", cols3);
		String [][] records_k0nj = new String[357][cols3.length];
		for(int i=0;i<357;i++)
		{
			records_k0nj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k0nj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k0nj", records_k0nj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5]
		String[] ConditionColumns3 = {"f","c","m","g","d","h"};
		String[] ConditionColumnsValues3 = {"f0","c0","m3","g0","d2","h2"};
		DBApp.createBitMapIndex("k0nj","f");
		DBApp.createBitMapIndex("k0nj","c");
		DBApp.createBitMapIndex("k0nj","m");
		DBApp.createBitMapIndex("k0nj","g");
		DBApp.createBitMapIndex("k0nj","d");
		DBApp.createBitMapIndex("k0nj","h");
		DBApp.selectIndex("k0nj", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		String tableTrace3 = DBApp.getLastTrace("k0nj");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x9", cols4);
		String [][] records_x9 = new String[244][cols4.length];
		for(int i=0;i<244;i++)
		{
			records_x9[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x9[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x9", records_x9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4, f5, g1]
		//record240: [a240, b0, c0, d0, e0, f0, g2]
		//record241: [a241, b1, c1, d1, e1, f1, g3]
		//record242: [a242, b0, c2, d2, e2, f2, g4]
		//record243: [a243, b1, c0, d3, e3, f3, g5]
		String[] ConditionColumns4 = {"e","c"};
		String[] ConditionColumnsValues4 = {"e2","c0"};
		DBApp.createBitMapIndex("x9","e");
		DBApp.createBitMapIndex("x9","c");
		DBApp.selectIndex("x9", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 16, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a27, b1, c0, d3, e2, f3, g6]
		//record2: [a42, b0, c0, d2, e2, f0, g0]
		//record3: [a57, b1, c0, d1, e2, f3, g1]
		//record4: [a72, b0, c0, d0, e2, f0, g2]
		String tableTrace4 = DBApp.getLastTrace("x9");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+16));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ng27", cols0);
		String [][] records_ng27 = new String[157][cols0.length];
		for(int i=0;i<157;i++)
		{
			records_ng27[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ng27[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ng27", records_ng27[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0]
		String[] ConditionColumns0 = {"d","i","k","c"};
		String[] ConditionColumnsValues0 = {"d2","i6","k1","c0"};
		DBApp.createBitMapIndex("ng27","d");
		DBApp.createBitMapIndex("ng27","i");
		DBApp.createBitMapIndex("ng27","k");
		DBApp.createBitMapIndex("ng27","c");
		DBApp.selectIndex("ng27", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6]
		String tableTrace0 = DBApp.getLastTrace("ng27");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vl1", cols1);
		String [][] records_vl1 = new String[398][cols1.length];
		for(int i=0;i<398;i++)
		{
			records_vl1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vl1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vl1", records_vl1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record393: [a393, b1, c0, d1, e3, f3, g1]
		//record394: [a394, b0, c1, d2, e4, f4, g2]
		//record395: [a395, b1, c2, d3, e0, f5, g3]
		//record396: [a396, b0, c0, d0, e1, f0, g4]
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		String[] ConditionColumns1 = {"g","d"};
		String[] ConditionColumnsValues1 = {"g4","d0"};
		DBApp.createBitMapIndex("vl1","g");
		DBApp.createBitMapIndex("vl1","d");
		DBApp.selectIndex("vl1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 15, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a32, b0, c2, d0, e2, f2, g4]
		//record2: [a60, b0, c0, d0, e0, f0, g4]
		//record3: [a88, b0, c1, d0, e3, f4, g4]
		//record4: [a116, b0, c2, d0, e1, f2, g4]
		String tableTrace1 = DBApp.getLastTrace("vl1");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+15));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r3d", cols2);
		String [][] records_r3d = new String[29][cols2.length];
		for(int i=0;i<29;i++)
		{
			records_r3d[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r3d[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r3d", records_r3d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		String[] ConditionColumns2 = {"a","e"};
		String[] ConditionColumnsValues2 = {"a27","e2"};
		DBApp.createBitMapIndex("r3d","a");
		DBApp.createBitMapIndex("r3d","e");
		DBApp.selectIndex("r3d", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6]
		String tableTrace2 = DBApp.getLastTrace("r3d");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u7n1", cols3);
		String [][] records_u7n1 = new String[153][cols3.length];
		for(int i=0;i<153;i++)
		{
			records_u7n1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u7n1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u7n1", records_u7n1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record148: [a148, b0, c1, d0, e3, f4, g1]
		//record149: [a149, b1, c2, d1, e4, f5, g2]
		//record150: [a150, b0, c0, d2, e0, f0, g3]
		//record151: [a151, b1, c1, d3, e1, f1, g4]
		//record152: [a152, b0, c2, d0, e2, f2, g5]
		String[] ConditionColumns3 = {"c","b"};
		String[] ConditionColumnsValues3 = {"c2","b1"};
		DBApp.createBitMapIndex("u7n1","c");
		DBApp.createBitMapIndex("u7n1","b");
		DBApp.selectIndex("u7n1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 25, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		String tableTrace3 = DBApp.getLastTrace("u7n1");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+25));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v096j", cols0);
		String [][] records_v096j = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			records_v096j[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v096j[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v096j", records_v096j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record77: [a77, b1, c2, d1, e2, f5, g0]
		//record78: [a78, b0, c0, d2, e3, f0, g1]
		//record79: [a79, b1, c1, d3, e4, f1, g2]
		//record80: [a80, b0, c2, d0, e0, f2, g3]
		//record81: [a81, b1, c0, d1, e1, f3, g4]
		String[] ConditionColumns0 = {"a","d"};
		String[] ConditionColumnsValues0 = {"a75","d3"};
		DBApp.createBitMapIndex("v096j","a");
		DBApp.createBitMapIndex("v096j","d");
		DBApp.selectIndex("v096j", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a75, b1, c0, d3, e0, f3, g5]
		String tableTrace0 = DBApp.getLastTrace("v096j");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jl", cols0);
		String [][] records_jl = new String[173][cols0.length];
		for(int i=0;i<173;i++)
		{
			records_jl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jl", records_jl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3]
		String[] ConditionColumns0 = {"i","f","h","k","e"};
		String[] ConditionColumnsValues0 = {"i7","f1","h1","k3","e0"};
		DBApp.createBitMapIndex("jl","i");
		DBApp.createBitMapIndex("jl","f");
		DBApp.createBitMapIndex("jl","h");
		DBApp.createBitMapIndex("jl","k");
		DBApp.createBitMapIndex("jl","e");
		DBApp.selectIndex("jl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		String tableTrace0 = DBApp.getLastTrace("jl");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rjc", cols1);
		String [][] records_rjc = new String[194][cols1.length];
		for(int i=0;i<194;i++)
		{
			records_rjc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rjc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rjc", records_rjc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record189: [a189, b1, c0, d1, e4, f3, g0]
		//record190: [a190, b0, c1, d2, e0, f4, g1]
		//record191: [a191, b1, c2, d3, e1, f5, g2]
		//record192: [a192, b0, c0, d0, e2, f0, g3]
		//record193: [a193, b1, c1, d1, e3, f1, g4]
		String[] ConditionColumns1 = {"e","f"};
		String[] ConditionColumnsValues1 = {"e4","f1"};
		DBApp.createBitMapIndex("rjc","e");
		DBApp.createBitMapIndex("rjc","f");
		DBApp.selectIndex("rjc", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5]
		//record1: [a49, b1, c1, d1, e4, f1, g0]
		//record2: [a79, b1, c1, d3, e4, f1, g2]
		//record3: [a109, b1, c1, d1, e4, f1, g4]
		//record4: [a139, b1, c1, d3, e4, f1, g6]
		String tableTrace1 = DBApp.getLastTrace("rjc");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+6));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p25", cols2);
		String [][] records_p25 = new String[108][cols2.length];
		for(int i=0;i<108;i++)
		{
			records_p25[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p25[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p25", records_p25[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3]
		String[] ConditionColumns2 = {"d","b","e"};
		String[] ConditionColumnsValues2 = {"d0","b0","e1"};
		DBApp.createBitMapIndex("p25","d");
		DBApp.createBitMapIndex("p25","b");
		DBApp.createBitMapIndex("p25","e");
		DBApp.selectIndex("p25", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record1: [a36, b0, c0, d0, e1, f0, g1, h4]
		//record2: [a56, b0, c2, d0, e1, f2, g0, h0]
		//record3: [a76, b0, c1, d0, e1, f4, g6, h4]
		//record4: [a96, b0, c0, d0, e1, f0, g5, h0]
		String tableTrace2 = DBApp.getLastTrace("p25");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wg", cols3);
		String [][] records_wg = new String[132][cols3.length];
		for(int i=0;i<132;i++)
		{
			records_wg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wg", records_wg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		//record129: [a129, b1, c0, d1, e4, f3, g3]
		//record130: [a130, b0, c1, d2, e0, f4, g4]
		//record131: [a131, b1, c2, d3, e1, f5, g5]
		String[] ConditionColumns3 = {"f","g"};
		String[] ConditionColumnsValues3 = {"f1","g1"};
		DBApp.createBitMapIndex("wg","f");
		DBApp.createBitMapIndex("wg","g");
		DBApp.selectIndex("wg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a43, b1, c1, d3, e3, f1, g1]
		//record2: [a85, b1, c1, d1, e0, f1, g1]
		//record3: [a127, b1, c1, d3, e2, f1, g1]
		String tableTrace3 = DBApp.getLastTrace("wg");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dc5", cols0);
		String [][] records_dc5 = new String[163][cols0.length];
		for(int i=0;i<163;i++)
		{
			records_dc5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dc5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dc5", records_dc5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record158: [a158, b0, c2, d2, e3, f2, g4]
		//record159: [a159, b1, c0, d3, e4, f3, g5]
		//record160: [a160, b0, c1, d0, e0, f4, g6]
		//record161: [a161, b1, c2, d1, e1, f5, g0]
		//record162: [a162, b0, c0, d2, e2, f0, g1]
		String[] ConditionColumns0 = {"d","g"};
		String[] ConditionColumnsValues0 = {"d1","g4"};
		DBApp.createBitMapIndex("dc5","d");
		DBApp.createBitMapIndex("dc5","g");
		DBApp.selectIndex("dc5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4]
		//record1: [a53, b1, c2, d1, e3, f5, g4]
		//record2: [a81, b1, c0, d1, e1, f3, g4]
		//record3: [a109, b1, c1, d1, e4, f1, g4]
		//record4: [a137, b1, c2, d1, e2, f5, g4]
		String tableTrace0 = DBApp.getLastTrace("dc5");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("m38", cols1);
		String [][] records_m38 = new String[113][cols1.length];
		for(int i=0;i<113;i++)
		{
			records_m38[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m38[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m38", records_m38[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12, q6, r0]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13, q7, r1]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14, q8, r2]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15, q9, r3]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7, p0, q10, r4]
		String[] ConditionColumns1 = {"i","d","j","r","n","h","f","a"};
		String[] ConditionColumnsValues1 = {"i1","d2","j0","r10","n10","h2","f4","a10"};
		DBApp.createBitMapIndex("m38","i");
		DBApp.createBitMapIndex("m38","d");
		DBApp.createBitMapIndex("m38","j");
		DBApp.createBitMapIndex("m38","r");
		DBApp.createBitMapIndex("m38","n");
		DBApp.createBitMapIndex("m38","h");
		DBApp.createBitMapIndex("m38","f");
		DBApp.createBitMapIndex("m38","a");
		DBApp.selectIndex("m38", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		String tableTrace1 = DBApp.getLastTrace("m38");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("t1", cols2);
		String [][] records_t1 = new String[431][cols2.length];
		for(int i=0;i<431;i++)
		{
			records_t1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t1", records_t1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7]
		String[] ConditionColumns2 = {"h","b","g"};
		String[] ConditionColumnsValues2 = {"h7","b1","g1"};
		DBApp.createBitMapIndex("t1","h");
		DBApp.createBitMapIndex("t1","b");
		DBApp.createBitMapIndex("t1","g");
		DBApp.selectIndex("t1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 8, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record1: [a71, b1, c2, d3, e1, f5, g1, h7, i8]
		//record2: [a127, b1, c1, d3, e2, f1, g1, h7, i1]
		//record3: [a183, b1, c0, d3, e3, f3, g1, h7, i3]
		//record4: [a239, b1, c2, d3, e4, f5, g1, h7, i5]
		String tableTrace2 = DBApp.getLastTrace("t1");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+8));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("szmo", cols3);
		String [][] records_szmo = new String[360][cols3.length];
		for(int i=0;i<360;i++)
		{
			records_szmo[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_szmo[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("szmo", records_szmo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9, o14]
		String[] ConditionColumns3 = {"d","b","l","f","n","a","i"};
		String[] ConditionColumnsValues3 = {"d2","b0","l6","f0","n12","a222","i6"};
		DBApp.createBitMapIndex("szmo","d");
		DBApp.createBitMapIndex("szmo","b");
		DBApp.createBitMapIndex("szmo","l");
		DBApp.createBitMapIndex("szmo","f");
		DBApp.createBitMapIndex("szmo","n");
		DBApp.createBitMapIndex("szmo","a");
		DBApp.createBitMapIndex("szmo","i");
		DBApp.selectIndex("szmo", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12]
		String tableTrace3 = DBApp.getLastTrace("szmo");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n2y", cols0);
		String [][] records_n2y = new String[211][cols0.length];
		for(int i=0;i<211;i++)
		{
			records_n2y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n2y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n2y", records_n2y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record206: [a206, b0, c2, d2, e1, f2, g3]
		//record207: [a207, b1, c0, d3, e2, f3, g4]
		//record208: [a208, b0, c1, d0, e3, f4, g5]
		//record209: [a209, b1, c2, d1, e4, f5, g6]
		//record210: [a210, b0, c0, d2, e0, f0, g0]
		String[] ConditionColumns0 = {"e","c","g"};
		String[] ConditionColumnsValues0 = {"e4","c2","g1"};
		DBApp.createBitMapIndex("n2y","e");
		DBApp.createBitMapIndex("n2y","c");
		DBApp.createBitMapIndex("n2y","g");
		DBApp.selectIndex("n2y", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1]
		//record1: [a134, b0, c2, d2, e4, f2, g1]
		String tableTrace0 = DBApp.getLastTrace("n2y");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("axakr", cols1);
		String [][] records_axakr = new String[301][cols1.length];
		for(int i=0;i<301;i++)
		{
			records_axakr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_axakr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("axakr", records_axakr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9]
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1, l10]
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2, l11]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0]
		String[] ConditionColumns1 = {"g","l"};
		String[] ConditionColumnsValues1 = {"g6","l7"};
		DBApp.createBitMapIndex("axakr","g");
		DBApp.createBitMapIndex("axakr","l");
		DBApp.selectIndex("axakr", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7]
		//record1: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7]
		//record2: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7]
		String tableTrace1 = DBApp.getLastTrace("axakr");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("r7", cols2);
		String [][] records_r7 = new String[145][cols2.length];
		for(int i=0;i<145;i++)
		{
			records_r7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r7", records_r7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4]
		String[] ConditionColumns2 = {"j","l"};
		String[] ConditionColumnsValues2 = {"j4","l2"};
		DBApp.createBitMapIndex("r7","j");
		DBApp.createBitMapIndex("r7","l");
		DBApp.selectIndex("r7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record1: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4]
		//record2: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8]
		String tableTrace2 = DBApp.getLastTrace("r7");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y1", cols3);
		String [][] records_y1 = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			records_y1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y1", records_y1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0, m8, n4, o0]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6, o2]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4]
		String[] ConditionColumns3 = {"l","o"};
		String[] ConditionColumnsValues3 = {"l7","o10"};
		DBApp.createBitMapIndex("y1","l");
		DBApp.createBitMapIndex("y1","o");
		DBApp.selectIndex("y1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10]
		String tableTrace3 = DBApp.getLastTrace("y1");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y1z", cols4);
		String [][] records_y1z = new String[331][cols4.length];
		for(int i=0;i<331;i++)
		{
			records_y1z[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_y1z[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("y1z", records_y1z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		//record330: [a330, b0, c0, d2, e0, f0, g1]
		String[] ConditionColumns4 = {"d","g","e"};
		String[] ConditionColumnsValues4 = {"d3","g3","e2"};
		DBApp.createBitMapIndex("y1z","d");
		DBApp.createBitMapIndex("y1z","g");
		DBApp.createBitMapIndex("y1z","e");
		DBApp.selectIndex("y1z", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a87, b1, c0, d3, e2, f3, g3]
		//record1: [a227, b1, c2, d3, e2, f5, g3]
		String tableTrace4 = DBApp.getLastTrace("y1z");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1479);
		return (int) (random.nextDouble()*max);
	}
	static String genRandString() 
	{
		char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		char[] digits = "0123456789".toCharArray();
		int nameSize = genRandNum(5);
		if (nameSize < 2)
			nameSize += 2;
		String res = "";
		res += alphabet[(genRandNum(alphabet.length)) - 1];
		for (int i = 1; i < nameSize; i++)
			if (genRandNum(4)>2)
				res += alphabet[(genRandNum(alphabet.length)) - 1];
			else
				res += digits[(genRandNum(digits.length)) - 1];
		return res;
	}
}