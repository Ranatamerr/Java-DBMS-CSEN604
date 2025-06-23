package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_10
{

	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n23", cols0);
		String [][] records_n23 = new String[128][cols0.length];
		for(int i=0;i<128;i++)
		{
			records_n23[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n23[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n23", records_n23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record123: [a123, b1, c0, d3, e3, f3, g4]
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		String[] ConditionColumns0 = {"a","c","g"};
		String[] ConditionColumnsValues0 = {"a115","c1","g3"};
		DBApp.createBitMapIndex("n23","a");
		DBApp.selectIndex("n23", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a115, b1, c1, d3, e0, f1, g3]
		String tableTrace0 = DBApp.getLastTrace("n23");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i7a", cols1);
		String [][] records_i7a = new String[230][cols1.length];
		for(int i=0;i<230;i++)
		{
			records_i7a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i7a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i7a", records_i7a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1]
		String[] ConditionColumns1 = {"b","h","g","d","c","l","j","i","e"};
		String[] ConditionColumnsValues1 = {"b1","h7","g4","d3","c2","l11","j5","i5","e0"};
		DBApp.createBitMapIndex("i7a","g");
		DBApp.createBitMapIndex("i7a","l");
		DBApp.selectIndex("i7a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11]
		String tableTrace1 = DBApp.getLastTrace("i7a");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, d, e, h, i, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jic", cols2);
		String [][] records_jic = new String[321][cols2.length];
		for(int i=0;i<321;i++)
		{
			records_jic[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jic[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jic", records_jic[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5]
		String[] ConditionColumns2 = {"b","f","j"};
		String[] ConditionColumnsValues2 = {"b0","f2","j6"};
		DBApp.createBitMapIndex("jic","f");
		DBApp.selectIndex("jic", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 10, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record1: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0, o11]
		//record2: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11]
		//record3: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12, n4, o11]
		//record4: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11]
		String tableTrace2 = DBApp.getLastTrace("jic");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+10));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("hgf", cols3);
		String [][] records_hgf = new String[393][cols3.length];
		for(int i=0;i<393;i++)
		{
			records_hgf[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hgf[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hgf", records_hgf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2]
		String[] ConditionColumns3 = {"f","j","h","d","k","l","g","e","i","m"};
		String[] ConditionColumnsValues3 = {"f5","j5","h3","d3","k2","l11","g0","e0","i8","m9"};
		DBApp.createBitMapIndex("hgf","h");
		DBApp.createBitMapIndex("hgf","d");
		DBApp.createBitMapIndex("hgf","k");
		DBApp.createBitMapIndex("hgf","l");
		DBApp.createBitMapIndex("hgf","i");
		DBApp.selectIndex("hgf", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		String tableTrace3 = DBApp.getLastTrace("hgf");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[d, h, i, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[e, f, g, j, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("g0qy", cols0);
		String [][] records_g0qy = new String[260][cols0.length];
		for(int i=0;i<260;i++)
		{
			records_g0qy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g0qy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g0qy", records_g0qy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0, p15]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1, p0]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7, o4, p3]
		String[] ConditionColumns0 = {"m","p","k","g","f","n","l","b","h","e","c","i","o"};
		String[] ConditionColumnsValues0 = {"m2","p9","k7","g4","f3","n11","l9","b1","h1","e4","c0","i6","o9"};
		DBApp.createBitMapIndex("g0qy","m");
		DBApp.createBitMapIndex("g0qy","p");
		DBApp.createBitMapIndex("g0qy","g");
		DBApp.createBitMapIndex("g0qy","f");
		DBApp.createBitMapIndex("g0qy","n");
		DBApp.createBitMapIndex("g0qy","e");
		DBApp.createBitMapIndex("g0qy","c");
		DBApp.createBitMapIndex("g0qy","o");
		DBApp.selectIndex("g0qy", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11, o9, p9]
		String tableTrace0 = DBApp.getLastTrace("g0qy");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[c, e, f, g, m, n, o, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, h, i, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ne", cols1);
		String [][] records_ne = new String[318][cols1.length];
		for(int i=0;i<318;i++)
		{
			records_ne[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ne[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ne", records_ne[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5]
		String[] ConditionColumns1 = {"a","h","g","b","e","c","f"};
		String[] ConditionColumnsValues1 = {"a315","h3","g0","b1","e0","c0","f3"};
		DBApp.createBitMapIndex("ne","h");
		DBApp.createBitMapIndex("ne","b");
		DBApp.createBitMapIndex("ne","f");
		DBApp.selectIndex("ne", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a315, b1, c0, d3, e0, f3, g0, h3]
		String tableTrace1 = DBApp.getLastTrace("ne");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[b, f, h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, c, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("eg2", cols2);
		String [][] records_eg2 = new String[296][cols2.length];
		for(int i=0;i<296;i++)
		{
			records_eg2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eg2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eg2", records_eg2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5, n11, o6, p3, q2, r3]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12, o7, p4, q3, r4]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7, n13, o8, p5, q4, r5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8, n0, o9, p6, q5, r6]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7, m9, n1, o10, p7, q6, r7]
		String[] ConditionColumns2 = {"p","n","i","b","e","h","l","o","j"};
		String[] ConditionColumnsValues2 = {"p4","n10","i2","b0","e4","h4","l8","o14","j4"};
		DBApp.createBitMapIndex("eg2","p");
		DBApp.createBitMapIndex("eg2","n");
		DBApp.createBitMapIndex("eg2","i");
		DBApp.createBitMapIndex("eg2","b");
		DBApp.createBitMapIndex("eg2","e");
		DBApp.createBitMapIndex("eg2","l");
		DBApp.selectIndex("eg2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14, p4, q11, r2]
		String tableTrace2 = DBApp.getLastTrace("eg2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, e, i, l, n, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[h, j, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wai", cols3);
		String [][] records_wai = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			records_wai[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wai[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wai", records_wai[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		String[] ConditionColumns3 = {"g","h","f"};
		String[] ConditionColumnsValues3 = {"g3","h1","f5"};
		DBApp.createBitMapIndex("wai","g");
		DBApp.selectIndex("wai", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		String tableTrace3 = DBApp.getLastTrace("wai");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[f, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q59", cols4);
		String [][] records_q59 = new String[391][cols4.length];
		for(int i=0;i<391;i++)
		{
			records_q59[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q59[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q59", records_q59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record386: [a386, b0, c2, d2, e1, f2, g1]
		//record387: [a387, b1, c0, d3, e2, f3, g2]
		//record388: [a388, b0, c1, d0, e3, f4, g3]
		//record389: [a389, b1, c2, d1, e4, f5, g4]
		//record390: [a390, b0, c0, d2, e0, f0, g5]
		String[] ConditionColumns4 = {"b","e","f","a","g"};
		String[] ConditionColumnsValues4 = {"b0","e4","f2","a74","g4"};
		DBApp.createBitMapIndex("q59","b");
		DBApp.createBitMapIndex("q59","e");
		DBApp.createBitMapIndex("q59","a");
		DBApp.selectIndex("q59", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a74, b0, c2, d2, e4, f2, g4]
		String tableTrace4 = DBApp.getLastTrace("q59");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[a, b, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b4ry3", cols0);
		String [][] records_b4ry3 = new String[290][cols0.length];
		for(int i=0;i<290;i++)
		{
			records_b4ry3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b4ry3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b4ry3", records_b4ry3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0, n6]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1, n7]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0, m2, n8]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3, n9]
		String[] ConditionColumns0 = {"f","i","g","n","l","c","m"};
		String[] ConditionColumnsValues0 = {"f0","i6","g0","n0","l6","c0","m3"};
		DBApp.createBitMapIndex("b4ry3","i");
		DBApp.createBitMapIndex("b4ry3","g");
		DBApp.selectIndex("b4ry3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		String tableTrace0 = DBApp.getLastTrace("b4ry3");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f, l, m, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("sf", cols1);
		String [][] records_sf = new String[499][cols1.length];
		for(int i=0;i<499;i++)
		{
			records_sf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sf", records_sf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7]
		//record498: [a498, b0, c0, d2, e3, f0, g1, h2, i3, j8]
		String[] ConditionColumns1 = {"g","a","b","c","h"};
		String[] ConditionColumnsValues1 = {"g5","a180","b0","c0","h4"};
		DBApp.createBitMapIndex("sf","g");
		DBApp.createBitMapIndex("sf","a");
		DBApp.createBitMapIndex("sf","b");
		DBApp.selectIndex("sf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0]
		String tableTrace1 = DBApp.getLastTrace("sf");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[a, b, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ifa", cols2);
		String [][] records_ifa = new String[370][cols2.length];
		for(int i=0;i<370;i++)
		{
			records_ifa[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ifa[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ifa", records_ifa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6]
		String[] ConditionColumns2 = {"a","d","b","c","f","i","e","k","j"};
		String[] ConditionColumnsValues2 = {"a45","d1","b1","c0","f3","i0","e0","k1","j5"};
		DBApp.createBitMapIndex("ifa","d");
		DBApp.createBitMapIndex("ifa","b");
		DBApp.createBitMapIndex("ifa","i");
		DBApp.selectIndex("ifa", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		String tableTrace2 = DBApp.getLastTrace("ifa");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, d, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[a, c, e, f, j, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q83", cols0);
		String [][] records_q83 = new String[138][cols0.length];
		for(int i=0;i<138;i++)
		{
			records_q83[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q83[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q83", records_q83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7]
		String[] ConditionColumns0 = {"e","i","b","h"};
		String[] ConditionColumnsValues0 = {"e4","i2","b1","h7"};
		DBApp.createBitMapIndex("q83","e");
		DBApp.createBitMapIndex("q83","i");
		DBApp.selectIndex("q83", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9]
		String tableTrace0 = DBApp.getLastTrace("q83");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("q00", cols1);
		String [][] records_q00 = new String[436][cols1.length];
		for(int i=0;i<436;i++)
		{
			records_q00[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q00[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q00", records_q00[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15, q6, r17, s13]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7, r0, s14]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8, r1, s15]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2, q9, r2, s16]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0, p3, q10, r3, s17]
		String[] ConditionColumns1 = {"j","n","l","m","a","f","k","b","i","g","d"};
		String[] ConditionColumnsValues1 = {"j4","n4","l8","m11","a284","f2","k9","b0","i5","g4","d0"};
		DBApp.createBitMapIndex("q00","k");
		DBApp.selectIndex("q00", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4, o14, p12, q12, r14, s18]
		String tableTrace1 = DBApp.getLastTrace("q00");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[k]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, d, f, g, i, j, l, m, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("im7", cols2);
		String [][] records_im7 = new String[200][cols2.length];
		for(int i=0;i<200;i++)
		{
			records_im7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_im7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("im7", records_im7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13, o0, p3]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0, o1, p4]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2, n1, o2, p5]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3, n2, o3, p6]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1, l7, m4, n3, o4, p7]
		String[] ConditionColumns2 = {"o","b","k","j","c","d","h","m","e","l","n","i","f","a","g"};
		String[] ConditionColumnsValues2 = {"o3","b0","k0","j8","c0","d2","h6","m3","e3","l6","n2","i0","f0","a198","g2"};
		DBApp.createBitMapIndex("im7","j");
		DBApp.createBitMapIndex("im7","d");
		DBApp.createBitMapIndex("im7","h");
		DBApp.createBitMapIndex("im7","e");
		DBApp.createBitMapIndex("im7","l");
		DBApp.createBitMapIndex("im7","i");
		DBApp.createBitMapIndex("im7","f");
		DBApp.createBitMapIndex("im7","a");
		DBApp.createBitMapIndex("im7","g");
		DBApp.selectIndex("im7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3, n2, o3, p6]
		String tableTrace2 = DBApp.getLastTrace("im7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a, d, e, f, g, h, i, j, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c, k, m, n, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ut8", cols0);
		String [][] records_ut8 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			records_ut8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ut8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ut8", records_ut8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5]
		String[] ConditionColumns0 = {"h","g","f","e","b","d","c"};
		String[] ConditionColumnsValues0 = {"h5","g1","f5","e4","b1","d1","c2"};
		DBApp.createBitMapIndex("ut8","g");
		DBApp.createBitMapIndex("ut8","e");
		DBApp.selectIndex("ut8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1, h5]
		String tableTrace0 = DBApp.getLastTrace("ut8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c, d, f, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z2c8m", cols1);
		String [][] records_z2c8m = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			records_z2c8m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z2c8m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z2c8m", records_z2c8m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		String[] ConditionColumns1 = {"g","c","f","b","e","d"};
		String[] ConditionColumnsValues1 = {"g1","c1","f1","b1","e1","d1"};
		DBApp.createBitMapIndex("z2c8m","g");
		DBApp.createBitMapIndex("z2c8m","f");
		DBApp.createBitMapIndex("z2c8m","e");
		DBApp.selectIndex("z2c8m", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		String tableTrace1 = DBApp.getLastTrace("z2c8m");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[e, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wkww", cols2);
		String [][] records_wkww = new String[318][cols2.length];
		for(int i=0;i<318;i++)
		{
			records_wkww[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wkww[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wkww", records_wkww[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1, g5]
		//record314: [a314, b0, c2, d2, e4, f2, g6]
		//record315: [a315, b1, c0, d3, e0, f3, g0]
		//record316: [a316, b0, c1, d0, e1, f4, g1]
		//record317: [a317, b1, c2, d1, e2, f5, g2]
		String[] ConditionColumns2 = {"b","c","g"};
		String[] ConditionColumnsValues2 = {"b0","c2","g5"};
		DBApp.createBitMapIndex("wkww","b");
		DBApp.selectIndex("wkww", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5]
		//record1: [a68, b0, c2, d0, e3, f2, g5]
		//record2: [a110, b0, c2, d2, e0, f2, g5]
		//record3: [a152, b0, c2, d0, e2, f2, g5]
		//record4: [a194, b0, c2, d2, e4, f2, g5]
		String tableTrace2 = DBApp.getLastTrace("wkww");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+7));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k76", cols3);
		String [][] records_k76 = new String[191][cols3.length];
		for(int i=0;i<191;i++)
		{
			records_k76[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k76[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k76", records_k76[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record186: [a186, b0, c0, d2, e1, f0, g4]
		//record187: [a187, b1, c1, d3, e2, f1, g5]
		//record188: [a188, b0, c2, d0, e3, f2, g6]
		//record189: [a189, b1, c0, d1, e4, f3, g0]
		//record190: [a190, b0, c1, d2, e0, f4, g1]
		String[] ConditionColumns3 = {"d","e","c","b","f","g"};
		String[] ConditionColumnsValues3 = {"d2","e3","c1","b0","f4","g3"};
		DBApp.createBitMapIndex("k76","d");
		DBApp.createBitMapIndex("k76","c");
		DBApp.createBitMapIndex("k76","b");
		DBApp.createBitMapIndex("k76","f");
		DBApp.selectIndex("k76", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a178, b0, c1, d2, e3, f4, g3]
		String tableTrace3 = DBApp.getLastTrace("k76");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b, c, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mkq", cols0);
		String [][] records_mkq = new String[219][cols0.length];
		for(int i=0;i<219;i++)
		{
			records_mkq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mkq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mkq", records_mkq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		//record216: [a216, b0, c0, d0, e1, f0, g6]
		//record217: [a217, b1, c1, d1, e2, f1, g0]
		//record218: [a218, b0, c2, d2, e3, f2, g1]
		String[] ConditionColumns0 = {"a","e","g","f","d","c"};
		String[] ConditionColumnsValues0 = {"a65","e0","g2","f5","d1","c2"};
		DBApp.createBitMapIndex("mkq","a");
		DBApp.createBitMapIndex("mkq","e");
		DBApp.createBitMapIndex("mkq","g");
		DBApp.createBitMapIndex("mkq","f");
		DBApp.selectIndex("mkq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2]
		String tableTrace0 = DBApp.getLastTrace("mkq");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, e, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z8tt1", cols1);
		String [][] records_z8tt1 = new String[433][cols1.length];
		for(int i=0;i<433;i++)
		{
			records_z8tt1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z8tt1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z8tt1", records_z8tt1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8, o8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12]
		String[] ConditionColumns1 = {"n","l","j","c","f","d","a","k","g","e","o","i"};
		String[] ConditionColumnsValues1 = {"n13","l11","j1","c2","f5","d3","a251","k9","g6","e1","o11","i8"};
		DBApp.createBitMapIndex("z8tt1","n");
		DBApp.createBitMapIndex("z8tt1","l");
		DBApp.createBitMapIndex("z8tt1","j");
		DBApp.createBitMapIndex("z8tt1","a");
		DBApp.createBitMapIndex("z8tt1","k");
		DBApp.createBitMapIndex("z8tt1","g");
		DBApp.createBitMapIndex("z8tt1","e");
		DBApp.selectIndex("z8tt1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11]
		String tableTrace1 = DBApp.getLastTrace("z8tt1");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[a, e, g, j, k, l, n]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, d, f, i, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c1fl8", cols2);
		String [][] records_c1fl8 = new String[232][cols2.length];
		for(int i=0;i<232;i++)
		{
			records_c1fl8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c1fl8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c1fl8", records_c1fl8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record227: [a227, b1, c2, d3, e2, f5, g3]
		//record228: [a228, b0, c0, d0, e3, f0, g4]
		//record229: [a229, b1, c1, d1, e4, f1, g5]
		//record230: [a230, b0, c2, d2, e0, f2, g6]
		//record231: [a231, b1, c0, d3, e1, f3, g0]
		String[] ConditionColumns2 = {"b","e","f","c","g"};
		String[] ConditionColumnsValues2 = {"b0","e2","f4","c1","g6"};
		DBApp.createBitMapIndex("c1fl8","f");
		DBApp.createBitMapIndex("c1fl8","g");
		DBApp.selectIndex("c1fl8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a202, b0, c1, d2, e2, f4, g6]
		String tableTrace2 = DBApp.getLastTrace("c1fl8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("psvy2", cols3);
		String [][] records_psvy2 = new String[260][cols3.length];
		for(int i=0;i<260;i++)
		{
			records_psvy2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_psvy2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("psvy2", records_psvy2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3]
		//record256: [a256, b0, c1, d0, e1, f4, g4]
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		String[] ConditionColumns3 = {"g","b","c"};
		String[] ConditionColumnsValues3 = {"g1","b1","c1"};
		DBApp.createBitMapIndex("psvy2","g");
		DBApp.selectIndex("psvy2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a43, b1, c1, d3, e3, f1, g1]
		//record2: [a85, b1, c1, d1, e0, f1, g1]
		//record3: [a127, b1, c1, d3, e2, f1, g1]
		//record4: [a169, b1, c1, d1, e4, f1, g1]
		String tableTrace3 = DBApp.getLastTrace("psvy2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("exu", cols0);
		String [][] records_exu = new String[474][cols0.length];
		for(int i=0;i<474;i++)
		{
			records_exu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_exu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("exu", records_exu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4, p5, q10]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5, p6, q11]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6, p7, q12]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7, p8, q13]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8, p9, q14]
		String[] ConditionColumns0 = {"j","f","g","h","m","c","i"};
		String[] ConditionColumnsValues0 = {"j4","f4","g0","h4","m0","c1","i4"};
		DBApp.createBitMapIndex("exu","f");
		DBApp.createBitMapIndex("exu","g");
		DBApp.createBitMapIndex("exu","h");
		DBApp.createBitMapIndex("exu","m");
		DBApp.createBitMapIndex("exu","c");
		DBApp.selectIndex("exu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4, p12, q7]
		String tableTrace0 = DBApp.getLastTrace("exu");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[c, f, g, h, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[i, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("s5h", cols1);
		String [][] records_s5h = new String[370][cols1.length];
		for(int i=0;i<370;i++)
		{
			records_s5h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s5h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s5h", records_s5h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5, m1, n1, o5, p13, q8, r5, s4]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6, m2, n2, o6, p14, q9, r6, s5]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7, m3, n3, o7, p15, q10, r7, s6]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8, m4, n4, o8, p0, q11, r8, s7]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5, n5, o9, p1, q12, r9, s8]
		String[] ConditionColumns1 = {"b","k","g"};
		String[] ConditionColumnsValues1 = {"b1","k1","g3"};
		DBApp.createBitMapIndex("s5h","b");
		DBApp.selectIndex("s5h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9, s7]
		//record1: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1, l7, m4, n3, o4, p7, q12, r1, s9]
		//record2: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2, n3, o8, p1, q13, r11, s11]
		String tableTrace1 = DBApp.getLastTrace("s5h");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[g, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nf6ck", cols2);
		String [][] records_nf6ck = new String[102][cols2.length];
		for(int i=0;i<102;i++)
		{
			records_nf6ck[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nf6ck[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nf6ck", records_nf6ck[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record97: [a97, b1, c1, d1, e2, f1, g6]
		//record98: [a98, b0, c2, d2, e3, f2, g0]
		//record99: [a99, b1, c0, d3, e4, f3, g1]
		//record100: [a100, b0, c1, d0, e0, f4, g2]
		//record101: [a101, b1, c2, d1, e1, f5, g3]
		String[] ConditionColumns2 = {"a","g","c","f","b","d","e"};
		String[] ConditionColumnsValues2 = {"a58","g2","c1","f4","b0","d2","e3"};
		DBApp.createBitMapIndex("nf6ck","a");
		DBApp.createBitMapIndex("nf6ck","g");
		DBApp.createBitMapIndex("nf6ck","f");
		DBApp.createBitMapIndex("nf6ck","e");
		DBApp.selectIndex("nf6ck", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a58, b0, c1, d2, e3, f4, g2]
		String tableTrace2 = DBApp.getLastTrace("nf6ck");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a, e, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("evd4s", cols3);
		String [][] records_evd4s = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			records_evd4s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_evd4s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("evd4s", records_evd4s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record60: [a60, b0, c0, d0, e0, f0, g4]
		//record61: [a61, b1, c1, d1, e1, f1, g5]
		//record62: [a62, b0, c2, d2, e2, f2, g6]
		//record63: [a63, b1, c0, d3, e3, f3, g0]
		//record64: [a64, b0, c1, d0, e4, f4, g1]
		String[] ConditionColumns3 = {"d","e","c","f"};
		String[] ConditionColumnsValues3 = {"d3","e2","c2","f5"};
		DBApp.createBitMapIndex("evd4s","d");
		DBApp.selectIndex("evd4s", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5]
		String tableTrace3 = DBApp.getLastTrace("evd4s");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[c, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qlp", cols0);
		String [][] records_qlp = new String[451][cols0.length];
		for(int i=0;i<451;i++)
		{
			records_qlp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qlp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qlp", records_qlp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0]
		String[] ConditionColumns0 = {"b","i","f"};
		String[] ConditionColumnsValues0 = {"b0","i4","f4"};
		DBApp.createBitMapIndex("qlp","i");
		DBApp.selectIndex("qlp", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record1: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record2: [a40, b0, c1, d0, e0, f4, g5, h0, i4]
		//record3: [a58, b0, c1, d2, e3, f4, g2, h2, i4]
		//record4: [a76, b0, c1, d0, e1, f4, g6, h4, i4]
		String tableTrace0 = DBApp.getLastTrace("qlp");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+25));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("bx5", cols1);
		String [][] records_bx5 = new String[341][cols1.length];
		for(int i=0;i<341;i++)
		{
			records_bx5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bx5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bx5", records_bx5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6, p0, q13, r12, s13]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7, p1, q14, r13, s14]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2, q15, r14, s15]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3, q16, r15, s16]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4, q0, r16, s17]
		String[] ConditionColumns1 = {"d","a","h","p","f","c","q","i","m","r","e","j","s","o","k","n","g","b","l"};
		String[] ConditionColumnsValues1 = {"d0","a160","h0","p0","f4","c1","q7","i7","m4","r16","e0","j0","s8","o10","k6","n6","g6","b0","l4"};
		DBApp.createBitMapIndex("bx5","d");
		DBApp.createBitMapIndex("bx5","h");
		DBApp.createBitMapIndex("bx5","f");
		DBApp.createBitMapIndex("bx5","i");
		DBApp.createBitMapIndex("bx5","r");
		DBApp.createBitMapIndex("bx5","e");
		DBApp.createBitMapIndex("bx5","l");
		DBApp.selectIndex("bx5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10, p0, q7, r16, s8]
		String tableTrace1 = DBApp.getLastTrace("bx5");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, e, f, h, i, l, r]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, c, g, j, k, m, n, o, p, q, s]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q1dfy", cols2);
		String [][] records_q1dfy = new String[247][cols2.length];
		for(int i=0;i<247;i++)
		{
			records_q1dfy[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q1dfy[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q1dfy", records_q1dfy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record242: [a242, b0, c2, d2, e2, f2, g4]
		//record243: [a243, b1, c0, d3, e3, f3, g5]
		//record244: [a244, b0, c1, d0, e4, f4, g6]
		//record245: [a245, b1, c2, d1, e0, f5, g0]
		//record246: [a246, b0, c0, d2, e1, f0, g1]
		String[] ConditionColumns2 = {"f","b","g","c","d"};
		String[] ConditionColumnsValues2 = {"f1","b1","g3","c1","d3"};
		DBApp.createBitMapIndex("q1dfy","b");
		DBApp.createBitMapIndex("q1dfy","g");
		DBApp.selectIndex("q1dfy", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3]
		//record1: [a115, b1, c1, d3, e0, f1, g3]
		//record2: [a199, b1, c1, d3, e4, f1, g3]
		String tableTrace2 = DBApp.getLastTrace("q1dfy");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qky", cols0);
		String [][] records_qky = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			records_qky[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qky[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qky", records_qky[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		String[] ConditionColumns0 = {"c","d","f"};
		String[] ConditionColumnsValues0 = {"c0","d2","f0"};
		DBApp.createBitMapIndex("qky","f");
		DBApp.selectIndex("qky", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a18, b0, c0, d2, e3, f0, g4]
		//record2: [a30, b0, c0, d2, e0, f0, g2]
		//record3: [a42, b0, c0, d2, e2, f0, g0]
		String tableTrace0 = DBApp.getLastTrace("qky");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n0k8", cols1);
		String [][] records_n0k8 = new String[107][cols1.length];
		for(int i=0;i<107;i++)
		{
			records_n0k8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n0k8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n0k8", records_n0k8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2, f0, g4]
		//record103: [a103, b1, c1, d3, e3, f1, g5]
		//record104: [a104, b0, c2, d0, e4, f2, g6]
		//record105: [a105, b1, c0, d1, e0, f3, g0]
		//record106: [a106, b0, c1, d2, e1, f4, g1]
		String[] ConditionColumns1 = {"e","f","d"};
		String[] ConditionColumnsValues1 = {"e2","f5","d1"};
		DBApp.createBitMapIndex("n0k8","e");
		DBApp.selectIndex("n0k8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3]
		//record1: [a77, b1, c2, d1, e2, f5, g0]
		String tableTrace1 = DBApp.getLastTrace("n0k8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d99", cols0);
		String [][] records_d99 = new String[210][cols0.length];
		for(int i=0;i<210;i++)
		{
			records_d99[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d99[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d99", records_d99[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1]
		String[] ConditionColumns0 = {"g","d","c","a","f","l","k","i","m","e","b","j","h"};
		String[] ConditionColumnsValues0 = {"g4","d1","c1","a193","f1","l1","k6","i4","m11","e3","b1","j3","h1"};
		DBApp.createBitMapIndex("d99","g");
		DBApp.createBitMapIndex("d99","d");
		DBApp.createBitMapIndex("d99","a");
		DBApp.createBitMapIndex("d99","l");
		DBApp.createBitMapIndex("d99","i");
		DBApp.createBitMapIndex("d99","m");
		DBApp.createBitMapIndex("d99","e");
		DBApp.createBitMapIndex("d99","b");
		DBApp.createBitMapIndex("d99","j");
		DBApp.selectIndex("d99", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11]
		String tableTrace0 = DBApp.getLastTrace("d99");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, b, d, e, g, i, j, l, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f, h, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("obz", cols1);
		String [][] records_obz = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			records_obz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_obz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("obz", records_obz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		String[] ConditionColumns1 = {"d","c","b","f","a","g","e"};
		String[] ConditionColumnsValues1 = {"d3","c0","b1","f3","a3","g3","e3"};
		DBApp.createBitMapIndex("obz","c");
		DBApp.createBitMapIndex("obz","g");
		DBApp.createBitMapIndex("obz","e");
		DBApp.selectIndex("obz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		String tableTrace1 = DBApp.getLastTrace("obz");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a549", cols2);
		String [][] records_a549 = new String[360][cols2.length];
		for(int i=0;i<360;i++)
		{
			records_a549[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a549[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a549", records_a549[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9]
		String[] ConditionColumns2 = {"b","i","h","c","g","d"};
		String[] ConditionColumnsValues2 = {"b1","i7","h7","c1","g4","d3"};
		DBApp.createBitMapIndex("a549","i");
		DBApp.createBitMapIndex("a549","h");
		DBApp.selectIndex("a549", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1]
		String tableTrace2 = DBApp.getLastTrace("a549");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[h, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c, d, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u780k", cols3);
		String [][] records_u780k = new String[88][cols3.length];
		for(int i=0;i<88;i++)
		{
			records_u780k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u780k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u780k", records_u780k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7]
		String[] ConditionColumns3 = {"d","g","f"};
		String[] ConditionColumnsValues3 = {"d3","g3","f5"};
		DBApp.createBitMapIndex("u780k","d");
		DBApp.selectIndex("u780k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a59, b1, c2, d3, e4, f5, g3, h3]
		String tableTrace3 = DBApp.getLastTrace("u780k");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j4", cols4);
		String [][] records_j4 = new String[113][cols4.length];
		for(int i=0;i<113;i++)
		{
			records_j4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j4", records_j4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3]
		//record109: [a109, b1, c1, d1, e4, f1, g4]
		//record110: [a110, b0, c2, d2, e0, f2, g5]
		//record111: [a111, b1, c0, d3, e1, f3, g6]
		//record112: [a112, b0, c1, d0, e2, f4, g0]
		String[] ConditionColumns4 = {"f","d","g","c"};
		String[] ConditionColumnsValues4 = {"f1","d3","g0","c1"};
		DBApp.createBitMapIndex("j4","f");
		DBApp.createBitMapIndex("j4","g");
		DBApp.selectIndex("j4", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0]
		//record1: [a91, b1, c1, d3, e1, f1, g0]
		String tableTrace4 = DBApp.getLastTrace("j4");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mgz", cols0);
		String [][] records_mgz = new String[369][cols0.length];
		for(int i=0;i<369;i++)
		{
			records_mgz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mgz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mgz", records_mgz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4, f4, g0]
		//record365: [a365, b1, c2, d1, e0, f5, g1]
		//record366: [a366, b0, c0, d2, e1, f0, g2]
		//record367: [a367, b1, c1, d3, e2, f1, g3]
		//record368: [a368, b0, c2, d0, e3, f2, g4]
		String[] ConditionColumns0 = {"c","d","f"};
		String[] ConditionColumnsValues0 = {"c1","d0","f4"};
		DBApp.createBitMapIndex("mgz","c");
		DBApp.selectIndex("mgz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 31, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a28, b0, c1, d0, e3, f4, g0]
		//record3: [a40, b0, c1, d0, e0, f4, g5]
		//record4: [a52, b0, c1, d0, e2, f4, g3]
		String tableTrace0 = DBApp.getLastTrace("mgz");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[c]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+31));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tca", cols1);
		String [][] records_tca = new String[63][cols1.length];
		for(int i=0;i<63;i++)
		{
			records_tca[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tca[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tca", records_tca[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record58: [a58, b0, c1, d2, e3, f4, g2]
		//record59: [a59, b1, c2, d3, e4, f5, g3]
		//record60: [a60, b0, c0, d0, e0, f0, g4]
		//record61: [a61, b1, c1, d1, e1, f1, g5]
		//record62: [a62, b0, c2, d2, e2, f2, g6]
		String[] ConditionColumns1 = {"e","f","a","g","b"};
		String[] ConditionColumnsValues1 = {"e0","f1","a25","g4","b1"};
		DBApp.createBitMapIndex("tca","g");
		DBApp.selectIndex("tca", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4]
		String tableTrace1 = DBApp.getLastTrace("tca");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qp3", cols2);
		String [][] records_qp3 = new String[451][cols2.length];
		for(int i=0;i<451;i++)
		{
			records_qp3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qp3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qp3", records_qp3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0]
		String[] ConditionColumns2 = {"h","d","e"};
		String[] ConditionColumnsValues2 = {"h6","d2","e1"};
		DBApp.createBitMapIndex("qp3","d");
		DBApp.selectIndex("qp3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 12, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record1: [a46, b0, c1, d2, e1, f4, g4, h6, i1]
		//record2: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		//record3: [a126, b0, c0, d2, e1, f0, g0, h6, i0]
		//record4: [a166, b0, c1, d2, e1, f4, g5, h6, i4]
		String tableTrace2 = DBApp.getLastTrace("qp3");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[e, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+12));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o87a", cols3);
		String [][] records_o87a = new String[191][cols3.length];
		for(int i=0;i<191;i++)
		{
			records_o87a[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o87a[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o87a", records_o87a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record186: [a186, b0, c0, d2, e1, f0, g4]
		//record187: [a187, b1, c1, d3, e2, f1, g5]
		//record188: [a188, b0, c2, d0, e3, f2, g6]
		//record189: [a189, b1, c0, d1, e4, f3, g0]
		//record190: [a190, b0, c1, d2, e0, f4, g1]
		String[] ConditionColumns3 = {"g","d","f","c","b"};
		String[] ConditionColumnsValues3 = {"g3","d0","f0","c0","b0"};
		DBApp.createBitMapIndex("o87a","g");
		DBApp.createBitMapIndex("o87a","d");
		DBApp.createBitMapIndex("o87a","f");
		DBApp.selectIndex("o87a", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3]
		//record1: [a108, b0, c0, d0, e3, f0, g3]
		String tableTrace3 = DBApp.getLastTrace("o87a");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[d, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l8", cols4);
		String [][] records_l8 = new String[198][cols4.length];
		for(int i=0;i<198;i++)
		{
			records_l8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_l8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("l8", records_l8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3, f1, g4]
		//record194: [a194, b0, c2, d2, e4, f2, g5]
		//record195: [a195, b1, c0, d3, e0, f3, g6]
		//record196: [a196, b0, c1, d0, e1, f4, g0]
		//record197: [a197, b1, c2, d1, e2, f5, g1]
		String[] ConditionColumns4 = {"b","f","d","e"};
		String[] ConditionColumnsValues4 = {"b0","f4","d2","e1"};
		DBApp.createBitMapIndex("l8","f");
		DBApp.createBitMapIndex("l8","d");
		DBApp.selectIndex("l8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 3, records:
		//record0: [a46, b0, c1, d2, e1, f4, g4]
		//record1: [a106, b0, c1, d2, e1, f4, g1]
		//record2: [a166, b0, c1, d2, e1, f4, g5]
		String tableTrace4 = DBApp.getLastTrace("l8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[d, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[b, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nx6kp", cols0);
		String [][] records_nx6kp = new String[270][cols0.length];
		for(int i=0;i<270;i++)
		{
			records_nx6kp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nx6kp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nx6kp", records_nx6kp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9]
		String[] ConditionColumns0 = {"g","h","b"};
		String[] ConditionColumnsValues0 = {"g0","h3","b1"};
		DBApp.createBitMapIndex("nx6kp","g");
		DBApp.selectIndex("nx6kp", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		//record1: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1]
		//record2: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7]
		//record3: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3]
		//record4: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9]
		String tableTrace0 = DBApp.getLastTrace("nx6kp");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j7o", cols1);
		String [][] records_j7o = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			records_j7o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j7o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j7o", records_j7o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		//record49: [a49, b1, c1, d1, e4, f1, g0]
		String[] ConditionColumns1 = {"d","c","g"};
		String[] ConditionColumnsValues1 = {"d2","c0","g6"};
		DBApp.createBitMapIndex("j7o","g");
		DBApp.selectIndex("j7o", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		String tableTrace1 = DBApp.getLastTrace("j7o");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d79o", cols0);
		String [][] records_d79o = new String[410][cols0.length];
		for(int i=0;i<410;i++)
		{
			records_d79o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d79o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d79o", records_d79o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6]
		//record406: [a406, b0, c1, d2, e1, f4, g0]
		//record407: [a407, b1, c2, d3, e2, f5, g1]
		//record408: [a408, b0, c0, d0, e3, f0, g2]
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		String[] ConditionColumns0 = {"e","g","b","a","d","c"};
		String[] ConditionColumnsValues0 = {"e3","g4","b1","a333","d1","c0"};
		DBApp.createBitMapIndex("d79o","e");
		DBApp.createBitMapIndex("d79o","g");
		DBApp.createBitMapIndex("d79o","b");
		DBApp.createBitMapIndex("d79o","a");
		DBApp.selectIndex("d79o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a333, b1, c0, d1, e3, f3, g4]
		String tableTrace0 = DBApp.getLastTrace("d79o");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, b, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("usi92", cols0);
		String [][] records_usi92 = new String[137][cols0.length];
		for(int i=0;i<137;i++)
		{
			records_usi92[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_usi92[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("usi92", records_usi92[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4]
		String[] ConditionColumns0 = {"e","g","i"};
		String[] ConditionColumnsValues0 = {"e0","g0","i6"};
		DBApp.createBitMapIndex("usi92","g");
		DBApp.selectIndex("usi92", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9]
		String tableTrace0 = DBApp.getLastTrace("usi92");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[e, i]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("bvkm", cols0);
		String [][] records_bvkm = new String[363][cols0.length];
		for(int i=0;i<363;i++)
		{
			records_bvkm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bvkm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bvkm", records_bvkm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13, p6, q1, r16, s16]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9, o14, p7, q2, r17, s17]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9, n10, o0, p8, q3, r0, s18]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11, o1, p9, q4, r1, s0]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11, n12, o2, p10, q5, r2, s1]
		String[] ConditionColumns0 = {"j","k","g","s"};
		String[] ConditionColumnsValues0 = {"j5","k3","g4","s6"};
		DBApp.createBitMapIndex("bvkm","j");
		DBApp.createBitMapIndex("bvkm","k");
		DBApp.selectIndex("bvkm", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		String tableTrace0 = DBApp.getLastTrace("bvkm");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[j, k]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[g, s]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("lhbg", cols1);
		String [][] records_lhbg = new String[239][cols1.length];
		for(int i=0;i<239;i++)
		{
			records_lhbg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lhbg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lhbg", records_lhbg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10, o9, p10, q13, r0, s6, t14]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10, p11, q14, r1, s7, t15]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11, p12, q15, r2, s8, t16]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13, o12, p13, q16, r3, s9, t17]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13, p14, q0, r4, s10, t18]
		String[] ConditionColumns1 = {"t","d","h","e","g","r","q","p","f","o","i","b","l","k","m","s","n"};
		String[] ConditionColumnsValues1 = {"t7","d3","h7","e2","g3","r15","q2","p7","f3","o12","i6","b1","l3","k10","m9","s11","n3"};
		DBApp.createBitMapIndex("lhbg","g");
		DBApp.createBitMapIndex("lhbg","r");
		DBApp.createBitMapIndex("lhbg","q");
		DBApp.createBitMapIndex("lhbg","f");
		DBApp.createBitMapIndex("lhbg","o");
		DBApp.createBitMapIndex("lhbg","b");
		DBApp.createBitMapIndex("lhbg","l");
		DBApp.createBitMapIndex("lhbg","s");
		DBApp.selectIndex("lhbg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11, t7]
		String tableTrace1 = DBApp.getLastTrace("lhbg");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[b, f, g, l, o, q, r, s]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[d, e, h, i, k, m, n, p, t]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hb62n", cols2);
		String [][] records_hb62n = new String[145][cols2.length];
		for(int i=0;i<145;i++)
		{
			records_hb62n[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hb62n[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hb62n", records_hb62n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0]
		String[] ConditionColumns2 = {"l","c","a","i","h","k","d","f","g","b","e","j"};
		String[] ConditionColumnsValues2 = {"l11","c2","a59","i5","h3","k4","d3","f5","g3","b1","e4","j9"};
		DBApp.createBitMapIndex("hb62n","c");
		DBApp.createBitMapIndex("hb62n","a");
		DBApp.createBitMapIndex("hb62n","d");
		DBApp.createBitMapIndex("hb62n","f");
		DBApp.createBitMapIndex("hb62n","g");
		DBApp.createBitMapIndex("hb62n","j");
		DBApp.selectIndex("hb62n", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11]
		String tableTrace2 = DBApp.getLastTrace("hb62n");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a, c, d, f, g, j]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, e, h, i, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("epq63", cols0);
		String [][] records_epq63 = new String[208][cols0.length];
		for(int i=0;i<208;i++)
		{
			records_epq63[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_epq63[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("epq63", records_epq63[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5, l11, m8, n7, o8, p11, q16]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4, k6, l0, m9, n8, o9, p12, q0]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10, n9, o10, p13, q1]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10, o11, p14, q2]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11, o12, p15, q3]
		String[] ConditionColumns0 = {"k","q","e","l","c"};
		String[] ConditionColumnsValues0 = {"k8","q10","e4","l9","c0"};
		DBApp.createBitMapIndex("epq63","l");
		DBApp.selectIndex("epq63", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1, q10]
		String tableTrace0 = DBApp.getLastTrace("epq63");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, e, k, q]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i46", cols1);
		String [][] records_i46 = new String[313][cols1.length];
		for(int i=0;i<313;i++)
		{
			records_i46[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i46[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i46", records_i46[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4]
		String[] ConditionColumns1 = {"d","g","k"};
		String[] ConditionColumnsValues1 = {"d2","g4","k2"};
		DBApp.createBitMapIndex("i46","d");
		DBApp.selectIndex("i46", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		String tableTrace1 = DBApp.getLastTrace("i46");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[g, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e658", cols2);
		String [][] records_e658 = new String[454][cols2.length];
		for(int i=0;i<454;i++)
		{
			records_e658[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e658[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e658", records_e658[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		//record450: [a450, b0, c0, d2, e0, f0, g2]
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		//record453: [a453, b1, c0, d1, e3, f3, g5]
		String[] ConditionColumns2 = {"c","e","f","d"};
		String[] ConditionColumnsValues2 = {"c2","e0","f5","d1"};
		DBApp.createBitMapIndex("e658","e");
		DBApp.selectIndex("e658", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 8, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a65, b1, c2, d1, e0, f5, g2]
		//record2: [a125, b1, c2, d1, e0, f5, g6]
		//record3: [a185, b1, c2, d1, e0, f5, g3]
		//record4: [a245, b1, c2, d1, e0, f5, g0]
		String tableTrace2 = DBApp.getLastTrace("e658");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+8));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("dsy", cols3);
		String [][] records_dsy = new String[462][cols3.length];
		for(int i=0;i<462;i++)
		{
			records_dsy[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_dsy[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("dsy", records_dsy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1]
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2]
		String[] ConditionColumns3 = {"b","f","c"};
		String[] ConditionColumnsValues3 = {"b1","f1","c1"};
		DBApp.createBitMapIndex("dsy","b");
		DBApp.selectIndex("dsy", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 77, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		String tableTrace3 = DBApp.getLastTrace("dsy");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+77));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u1", cols4);
		String [][] records_u1 = new String[480][cols4.length];
		for(int i=0;i<480;i++)
		{
			records_u1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u1", records_u1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		//record479: [a479, b1, c2, d3, e4, f5, g3]
		String[] ConditionColumns4 = {"g","f","e","c"};
		String[] ConditionColumnsValues4 = {"g3","f2","e0","c2"};
		DBApp.createBitMapIndex("u1","g");
		DBApp.createBitMapIndex("u1","e");
		DBApp.selectIndex("u1", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 2, records:
		//record0: [a80, b0, c2, d0, e0, f2, g3]
		//record1: [a290, b0, c2, d2, e0, f2, g3]
		String tableTrace4 = DBApp.getLastTrace("u1");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hw2", cols0);
		String [][] records_hw2 = new String[322][cols0.length];
		for(int i=0;i<322;i++)
		{
			records_hw2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hw2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hw2", records_hw2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record317: [a317, b1, c2, d1, e2, f5, g2]
		//record318: [a318, b0, c0, d2, e3, f0, g3]
		//record319: [a319, b1, c1, d3, e4, f1, g4]
		//record320: [a320, b0, c2, d0, e0, f2, g5]
		//record321: [a321, b1, c0, d1, e1, f3, g6]
		String[] ConditionColumns0 = {"f","b","a"};
		String[] ConditionColumnsValues0 = {"f5","b1","a311"};
		DBApp.createBitMapIndex("hw2","f");
		DBApp.selectIndex("hw2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a311, b1, c2, d3, e1, f5, g3]
		String tableTrace0 = DBApp.getLastTrace("hw2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[a, b]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("kdi", cols1);
		String [][] records_kdi = new String[445][cols1.length];
		for(int i=0;i<445;i++)
		{
			records_kdi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kdi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kdi", records_kdi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4]
		String[] ConditionColumns1 = {"i","g","k","j"};
		String[] ConditionColumnsValues1 = {"i3","g0","k10","j1"};
		DBApp.createBitMapIndex("kdi","i");
		DBApp.createBitMapIndex("kdi","g");
		DBApp.selectIndex("kdi", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		String tableTrace1 = DBApp.getLastTrace("kdi");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[j, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y8c9i", cols2);
		String [][] records_y8c9i = new String[355][cols2.length];
		for(int i=0;i<355;i++)
		{
			records_y8c9i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y8c9i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y8c9i", records_y8c9i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record350: [a350, b0, c2, d2, e0, f2, g0]
		//record351: [a351, b1, c0, d3, e1, f3, g1]
		//record352: [a352, b0, c1, d0, e2, f4, g2]
		//record353: [a353, b1, c2, d1, e3, f5, g3]
		//record354: [a354, b0, c0, d2, e4, f0, g4]
		String[] ConditionColumns2 = {"b","e","g","d","f"};
		String[] ConditionColumnsValues2 = {"b1","e0","g6","d3","f5"};
		DBApp.createBitMapIndex("y8c9i","b");
		DBApp.createBitMapIndex("y8c9i","e");
		DBApp.selectIndex("y8c9i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a335, b1, c2, d3, e0, f5, g6]
		String tableTrace2 = DBApp.getLastTrace("y8c9i");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("x0", cols0);
		String [][] records_x0 = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			records_x0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x0", records_x0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3, q15, r11, s7, t3]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0, o9, p4, q16, r12, s8, t4]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10, p5, q0, r13, s9, t5]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1, r14, s10, t6]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11, t7]
		String[] ConditionColumns0 = {"e","g","t","d","f","s","a","i","c","p","b","o","r","m","l"};
		String[] ConditionColumnsValues0 = {"e3","g6","t8","d0","f0","s10","a48","i3","c0","p0","b0","o3","r12","m9","l0"};
		DBApp.createBitMapIndex("x0","e");
		DBApp.createBitMapIndex("x0","d");
		DBApp.createBitMapIndex("x0","s");
		DBApp.createBitMapIndex("x0","a");
		DBApp.createBitMapIndex("x0","i");
		DBApp.createBitMapIndex("x0","p");
		DBApp.createBitMapIndex("x0","r");
		DBApp.createBitMapIndex("x0","l");
		DBApp.selectIndex("x0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12, s10, t8]
		String tableTrace0 = DBApp.getLastTrace("x0");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, d, e, i, l, p, r, s]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c, f, g, m, o, t]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u5e", cols1);
		String [][] records_u5e = new String[495][cols1.length];
		for(int i=0;i<495;i++)
		{
			records_u5e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u5e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u5e", records_u5e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record490: [a490, b0, c1, d2, e0, f4, g0]
		//record491: [a491, b1, c2, d3, e1, f5, g1]
		//record492: [a492, b0, c0, d0, e2, f0, g2]
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		String[] ConditionColumns1 = {"f","g","d","e","b","c"};
		String[] ConditionColumnsValues1 = {"f0","g0","d2","e3","b0","c0"};
		DBApp.createBitMapIndex("u5e","g");
		DBApp.selectIndex("u5e", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a378, b0, c0, d2, e3, f0, g0]
		String tableTrace1 = DBApp.getLastTrace("u5e");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, d, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y6on", cols2);
		String [][] records_y6on = new String[269][cols2.length];
		for(int i=0;i<269;i++)
		{
			records_y6on[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y6on[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y6on", records_y6on[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8]
		String[] ConditionColumns2 = {"d","h","e","i","c","g","f"};
		String[] ConditionColumnsValues2 = {"d3","h7","e3","i3","c0","g1","f3"};
		DBApp.createBitMapIndex("y6on","d");
		DBApp.createBitMapIndex("y6on","e");
		DBApp.createBitMapIndex("y6on","i");
		DBApp.createBitMapIndex("y6on","f");
		DBApp.selectIndex("y6on", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3]
		String tableTrace2 = DBApp.getLastTrace("y6on");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[d, e, f, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, g, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("wk5j", cols3);
		String [][] records_wk5j = new String[219][cols3.length];
		for(int i=0;i<219;i++)
		{
			records_wk5j[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wk5j[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wk5j", records_wk5j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4, o4, p6, q10]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5, p7, q11]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6, p8, q12]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7, p9, q13]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8, p10, q14]
		String[] ConditionColumns3 = {"c","n","b"};
		String[] ConditionColumnsValues3 = {"c1","n12","b0"};
		DBApp.createBitMapIndex("wk5j","b");
		DBApp.selectIndex("wk5j", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 5, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		//record1: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7, p2, q14]
		//record2: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4, p12, q5]
		//record3: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13]
		//record4: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12, o13, p0, q4]
		String tableTrace3 = DBApp.getLastTrace("wk5j");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[c, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ugd8z", cols4);
		String [][] records_ugd8z = new String[30][cols4.length];
		for(int i=0;i<30;i++)
		{
			records_ugd8z[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ugd8z[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ugd8z", records_ugd8z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		String[] ConditionColumns4 = {"b","h","m","l"};
		String[] ConditionColumnsValues4 = {"b0","h6","m1","l2"};
		DBApp.createBitMapIndex("ugd8z","h");
		DBApp.createBitMapIndex("ugd8z","m");
		DBApp.selectIndex("ugd8z", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		String tableTrace4 = DBApp.getLastTrace("ugd8z");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[h, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[b, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b991", cols0);
		String [][] records_b991 = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_b991[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b991[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b991", records_b991[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		//record416: [a416, b0, c2, d0, e1, f2, g3]
		String[] ConditionColumns0 = {"f","d","e"};
		String[] ConditionColumnsValues0 = {"f0","d0","e2"};
		DBApp.createBitMapIndex("b991","f");
		DBApp.selectIndex("b991", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a72, b0, c0, d0, e2, f0, g2]
		//record2: [a132, b0, c0, d0, e2, f0, g6]
		//record3: [a192, b0, c0, d0, e2, f0, g3]
		//record4: [a252, b0, c0, d0, e2, f0, g0]
		String tableTrace0 = DBApp.getLastTrace("b991");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("j0g", cols0);
		String [][] records_j0g = new String[359][cols0.length];
		for(int i=0;i<359;i++)
		{
			records_j0g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j0g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j0g", records_j0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3, n4, o9, p2, q14, r12, s12]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10, p3, q15, r13, s13]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11, p4, q16, r14, s14]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12, p5, q0, r15, s15]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13, p6, q1, r16, s16]
		String[] ConditionColumns0 = {"b","e","n","p","h","i","g","s","r","q","a","l","d","k","j"};
		String[] ConditionColumnsValues0 = {"b1","e2","n13","p11","h3","i0","g6","s8","r9","q10","a27","l3","d3","k5","j7"};
		DBApp.createBitMapIndex("j0g","e");
		DBApp.createBitMapIndex("j0g","p");
		DBApp.createBitMapIndex("j0g","i");
		DBApp.createBitMapIndex("j0g","g");
		DBApp.createBitMapIndex("j0g","r");
		DBApp.createBitMapIndex("j0g","a");
		DBApp.createBitMapIndex("j0g","l");
		DBApp.createBitMapIndex("j0g","d");
		DBApp.createBitMapIndex("j0g","k");
		DBApp.createBitMapIndex("j0g","j");
		DBApp.selectIndex("j0g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8]
		String tableTrace0 = DBApp.getLastTrace("j0g");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, d, e, g, i, j, k, l, p, r]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, h, n, q, s]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u4478", cols1);
		String [][] records_u4478 = new String[203][cols1.length];
		for(int i=0;i<203;i++)
		{
			records_u4478[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u4478[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u4478", records_u4478[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record198: [a198, b0, c0, d2, e3, f0, g2]
		//record199: [a199, b1, c1, d3, e4, f1, g3]
		//record200: [a200, b0, c2, d0, e0, f2, g4]
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		String[] ConditionColumns1 = {"f","g","e","d","a"};
		String[] ConditionColumnsValues1 = {"f1","g4","e0","d1","a25"};
		DBApp.createBitMapIndex("u4478","f");
		DBApp.createBitMapIndex("u4478","d");
		DBApp.selectIndex("u4478", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4]
		String tableTrace1 = DBApp.getLastTrace("u4478");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("q32bd", cols2);
		String [][] records_q32bd = new String[411][cols2.length];
		for(int i=0;i<411;i++)
		{
			records_q32bd[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q32bd[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q32bd", records_q32bd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10, m3, n0, o1, p6]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11, m4, n1, o2, p7]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0, m5, n2, o3, p8]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2, l1, m6, n3, o4, p9]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2, m7, n4, o5, p10]
		String[] ConditionColumns2 = {"d","p","f","g","n","k","m"};
		String[] ConditionColumnsValues2 = {"d3","p7","f3","g1","n1","k7","m1"};
		DBApp.createBitMapIndex("q32bd","p");
		DBApp.selectIndex("q32bd", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1, o3, p7]
		String tableTrace2 = DBApp.getLastTrace("q32bd");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, f, g, k, m, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a7", cols0);
		String [][] records_a7 = new String[195][cols0.length];
		for(int i=0;i<195;i++)
		{
			records_a7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a7", records_a7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1]
		//record191: [a191, b1, c2, d3, e1, f5, g2]
		//record192: [a192, b0, c0, d0, e2, f0, g3]
		//record193: [a193, b1, c1, d1, e3, f1, g4]
		//record194: [a194, b0, c2, d2, e4, f2, g5]
		String[] ConditionColumns0 = {"c","d","b"};
		String[] ConditionColumnsValues0 = {"c2","d2","b0"};
		DBApp.createBitMapIndex("a7","b");
		DBApp.selectIndex("a7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a14, b0, c2, d2, e4, f2, g0]
		//record2: [a26, b0, c2, d2, e1, f2, g5]
		//record3: [a38, b0, c2, d2, e3, f2, g3]
		//record4: [a50, b0, c2, d2, e0, f2, g1]
		String tableTrace0 = DBApp.getLastTrace("a7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+17));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("wq", cols1);
		String [][] records_wq = new String[170][cols1.length];
		for(int i=0;i<170;i++)
		{
			records_wq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wq", records_wq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9, n11, o0, p5]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9]
		String[] ConditionColumns1 = {"n","l","p"};
		String[] ConditionColumnsValues1 = {"n9","l7","p3"};
		DBApp.createBitMapIndex("wq","p");
		DBApp.selectIndex("wq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13, p3]
		String tableTrace1 = DBApp.getLastTrace("wq");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[l, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sp1s", cols2);
		String [][] records_sp1s = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			records_sp1s[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sp1s[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sp1s", records_sp1s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record91: [a91, b1, c1, d3, e1, f1, g0]
		//record92: [a92, b0, c2, d0, e2, f2, g1]
		//record93: [a93, b1, c0, d1, e3, f3, g2]
		//record94: [a94, b0, c1, d2, e4, f4, g3]
		//record95: [a95, b1, c2, d3, e0, f5, g4]
		String[] ConditionColumns2 = {"b","d","g","e"};
		String[] ConditionColumnsValues2 = {"b1","d1","g5","e0"};
		DBApp.createBitMapIndex("sp1s","b");
		DBApp.createBitMapIndex("sp1s","d");
		DBApp.selectIndex("sp1s", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		String tableTrace2 = DBApp.getLastTrace("sp1s");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("hpx", cols3);
		String [][] records_hpx = new String[351][cols3.length];
		for(int i=0;i<351;i++)
		{
			records_hpx[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hpx[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hpx", records_hpx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6, r4]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7, r5]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8, r6]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13, o4, p13, q9, r7]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0, o5, p14, q10, r8]
		String[] ConditionColumns3 = {"m","a","d","p","h","r","i","g","n","o","c","f","b"};
		String[] ConditionColumnsValues3 = {"m0","a78","d2","p14","h6","r6","i6","g1","n8","o3","c0","f0","b0"};
		DBApp.createBitMapIndex("hpx","m");
		DBApp.createBitMapIndex("hpx","a");
		DBApp.createBitMapIndex("hpx","h");
		DBApp.createBitMapIndex("hpx","n");
		DBApp.createBitMapIndex("hpx","c");
		DBApp.createBitMapIndex("hpx","f");
		DBApp.selectIndex("hpx", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3, p14, q10, r6]
		String tableTrace3 = DBApp.getLastTrace("hpx");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[a, c, f, h, m, n]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, d, g, i, o, p, r]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lv7", cols0);
		String [][] records_lv7 = new String[265][cols0.length];
		for(int i=0;i<265;i++)
		{
			records_lv7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lv7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lv7", records_lv7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		//record263: [a263, b1, c2, d3, e3, f5, g4]
		//record264: [a264, b0, c0, d0, e4, f0, g5]
		String[] ConditionColumns0 = {"d","g","b","e"};
		String[] ConditionColumnsValues0 = {"d0","g2","b0","e1"};
		DBApp.createBitMapIndex("lv7","d");
		DBApp.createBitMapIndex("lv7","g");
		DBApp.selectIndex("lv7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2]
		//record1: [a156, b0, c0, d0, e1, f0, g2]
		String tableTrace0 = DBApp.getLastTrace("lv7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[d, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r2", cols0);
		String [][] records_r2 = new String[131][cols0.length];
		for(int i=0;i<131;i++)
		{
			records_r2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r2", records_r2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		String[] ConditionColumns0 = {"g","l","j","h","b","c","d","e"};
		String[] ConditionColumnsValues0 = {"g2","l6","j4","h2","b0","c0","d2","e4"};
		DBApp.createBitMapIndex("r2","g");
		DBApp.createBitMapIndex("r2","l");
		DBApp.createBitMapIndex("r2","j");
		DBApp.createBitMapIndex("r2","h");
		DBApp.createBitMapIndex("r2","d");
		DBApp.createBitMapIndex("r2","e");
		DBApp.selectIndex("r2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10]
		String tableTrace0 = DBApp.getLastTrace("r2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[d, e, g, h, j, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("vpg", cols0);
		String [][] records_vpg = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_vpg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vpg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vpg", records_vpg[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		String[] ConditionColumns0 = {"i","g","f"};
		String[] ConditionColumnsValues0 = {"i1","g1","f1"};
		DBApp.createBitMapIndex("vpg","i");
		DBApp.selectIndex("vpg", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		String tableTrace0 = DBApp.getLastTrace("vpg");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("k4c", cols1);
		String [][] records_k4c = new String[276][cols1.length];
		for(int i=0;i<276;i++)
		{
			records_k4c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k4c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k4c", records_k4c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5, o1, p15]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2, p0]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3, p1]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1, n8, o4, p2]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2, n9, o5, p3]
		String[] ConditionColumns1 = {"g","p","j","i","l","d","c","b","o","k","f","n","h","m","e"};
		String[] ConditionColumnsValues1 = {"g4","p11","j3","i6","l3","d3","c0","b1","o3","k2","f3","n11","h3","m6","e3"};
		DBApp.createBitMapIndex("k4c","p");
		DBApp.createBitMapIndex("k4c","i");
		DBApp.createBitMapIndex("k4c","l");
		DBApp.createBitMapIndex("k4c","f");
		DBApp.createBitMapIndex("k4c","n");
		DBApp.createBitMapIndex("k4c","m");
		DBApp.createBitMapIndex("k4c","e");
		DBApp.selectIndex("k4c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3, p11]
		String tableTrace1 = DBApp.getLastTrace("k4c");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[e, f, i, l, m, n, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, d, g, h, j, k, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("u46", cols2);
		String [][] records_u46 = new String[419][cols2.length];
		for(int i=0;i<419;i++)
		{
			records_u46[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u46[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u46", records_u46[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8, o9, p14]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12, n9, o10, p15]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0, n10, o11, p0]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12, p1]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2, n12, o13, p2]
		String[] ConditionColumns2 = {"g","f","h"};
		String[] ConditionColumnsValues2 = {"g6","f5","h1"};
		DBApp.createBitMapIndex("u46","g");
		DBApp.selectIndex("u46", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9]
		//record1: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13, o14, p1]
		//record2: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5, m0, n13, o2, p9]
		String tableTrace2 = DBApp.getLastTrace("u46");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[f, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v6", cols0);
		String [][] records_v6 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_v6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v6", records_v6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		String[] ConditionColumns0 = {"e","b","c"};
		String[] ConditionColumnsValues0 = {"e2","b0","c0"};
		DBApp.createBitMapIndex("v6","e");
		DBApp.selectIndex("v6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a42, b0, c0, d2, e2, f0, g0]
		String tableTrace0 = DBApp.getLastTrace("v6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("lo", cols1);
		String [][] records_lo = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			records_lo[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lo[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lo", records_lo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3, q16]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6, n1, o11, p7, q3]
		String[] ConditionColumns1 = {"k","d","b","q","f","e","m","l","g","p","a","n"};
		String[] ConditionColumnsValues1 = {"k1","d3","b1","q6","f5","e3","m10","l11","g2","p7","a23","n9"};
		DBApp.createBitMapIndex("lo","f");
		DBApp.createBitMapIndex("lo","l");
		DBApp.createBitMapIndex("lo","p");
		DBApp.createBitMapIndex("lo","a");
		DBApp.selectIndex("lo", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6]
		String tableTrace1 = DBApp.getLastTrace("lo");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[a, f, l, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, d, e, g, k, m, n, q]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("cn", cols2);
		String [][] records_cn = new String[290][cols2.length];
		for(int i=0;i<290;i++)
		{
			records_cn[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cn[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cn", records_cn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0, m2]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3]
		String[] ConditionColumns2 = {"h","e","k","j","c","g"};
		String[] ConditionColumnsValues2 = {"h0","e3","k8","j8","c2","g1"};
		DBApp.createBitMapIndex("cn","k");
		DBApp.createBitMapIndex("cn","j");
		DBApp.createBitMapIndex("cn","c");
		DBApp.selectIndex("cn", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		String tableTrace2 = DBApp.getLastTrace("cn");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[c, j, k]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[e, g, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r81", cols0);
		String [][] records_r81 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_r81[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r81[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r81", records_r81[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		String[] ConditionColumns0 = {"g","c","f"};
		String[] ConditionColumnsValues0 = {"g5","c2","f5"};
		DBApp.createBitMapIndex("r81","g");
		DBApp.selectIndex("r81", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		String tableTrace0 = DBApp.getLastTrace("r81");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("kz4", cols1);
		String [][] records_kz4 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			records_kz4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kz4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kz4", records_kz4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		String[] ConditionColumns1 = {"d","c","h"};
		String[] ConditionColumnsValues1 = {"d2","c1","h2"};
		DBApp.createBitMapIndex("kz4","h");
		DBApp.selectIndex("kz4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		String tableTrace1 = DBApp.getLastTrace("kz4");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lh2", cols2);
		String [][] records_lh2 = new String[103][cols2.length];
		for(int i=0;i<103;i++)
		{
			records_lh2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lh2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lh2", records_lh2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3]
		String[] ConditionColumns2 = {"i","b","h","f","d","c","k","e","j","g"};
		String[] ConditionColumnsValues2 = {"i3","b1","h7","f3","d3","c0","k6","e4","j9","g4"};
		DBApp.createBitMapIndex("lh2","b");
		DBApp.createBitMapIndex("lh2","f");
		DBApp.createBitMapIndex("lh2","d");
		DBApp.createBitMapIndex("lh2","e");
		DBApp.selectIndex("lh2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		String tableTrace2 = DBApp.getLastTrace("lh2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, d, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, g, h, i, j, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("y6", cols0);
		String [][] records_y6 = new String[259][cols0.length];
		for(int i=0;i<259;i++)
		{
			records_y6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y6", records_y6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14, p14, q16]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0, p15, q0]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1, p0, q1]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1, q2]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2, q3]
		String[] ConditionColumns0 = {"b","l","d","p","c","m","i"};
		String[] ConditionColumnsValues0 = {"b1","l7","d3","p15","c1","m1","i7"};
		DBApp.createBitMapIndex("y6","p");
		DBApp.createBitMapIndex("y6","m");
		DBApp.createBitMapIndex("y6","i");
		DBApp.selectIndex("y6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15, q11]
		String tableTrace0 = DBApp.getLastTrace("y6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[i, m, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c, d, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dpx", cols1);
		String [][] records_dpx = new String[112][cols1.length];
		for(int i=0;i<112;i++)
		{
			records_dpx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dpx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dpx", records_dpx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record107: [a107, b1, c2, d3, e2, f5, g2]
		//record108: [a108, b0, c0, d0, e3, f0, g3]
		//record109: [a109, b1, c1, d1, e4, f1, g4]
		//record110: [a110, b0, c2, d2, e0, f2, g5]
		//record111: [a111, b1, c0, d3, e1, f3, g6]
		String[] ConditionColumns1 = {"g","d","b","f"};
		String[] ConditionColumnsValues1 = {"g6","d0","b0","f2"};
		DBApp.createBitMapIndex("dpx","f");
		DBApp.selectIndex("dpx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6]
		//record1: [a104, b0, c2, d0, e4, f2, g6]
		String tableTrace1 = DBApp.getLastTrace("dpx");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, d, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wk1", cols2);
		String [][] records_wk1 = new String[468][cols2.length];
		for(int i=0;i<468;i++)
		{
			records_wk1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wk1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wk1", records_wk1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record463: [a463, b1, c1, d3, e3, f1, g1]
		//record464: [a464, b0, c2, d0, e4, f2, g2]
		//record465: [a465, b1, c0, d1, e0, f3, g3]
		//record466: [a466, b0, c1, d2, e1, f4, g4]
		//record467: [a467, b1, c2, d3, e2, f5, g5]
		String[] ConditionColumns2 = {"g","c","e","f","d"};
		String[] ConditionColumnsValues2 = {"g3","c0","e1","f0","d2"};
		DBApp.createBitMapIndex("wk1","c");
		DBApp.createBitMapIndex("wk1","e");
		DBApp.createBitMapIndex("wk1","f");
		DBApp.selectIndex("wk1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a66, b0, c0, d2, e1, f0, g3]
		String tableTrace2 = DBApp.getLastTrace("wk1");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[c, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("h8", cols3);
		String [][] records_h8 = new String[476][cols3.length];
		for(int i=0;i<476;i++)
		{
			records_h8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h8", records_h8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7]
		String[] ConditionColumns3 = {"g","j","b","l","k"};
		String[] ConditionColumnsValues3 = {"g6","j1","b1","l1","k5"};
		DBApp.createBitMapIndex("h8","j");
		DBApp.createBitMapIndex("h8","b");
		DBApp.createBitMapIndex("h8","k");
		DBApp.selectIndex("h8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12]
		String tableTrace3 = DBApp.getLastTrace("h8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b, j, k]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[g, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f7", cols0);
		String [][] records_f7 = new String[369][cols0.length];
		for(int i=0;i<369;i++)
		{
			records_f7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f7", records_f7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4, f4, g0]
		//record365: [a365, b1, c2, d1, e0, f5, g1]
		//record366: [a366, b0, c0, d2, e1, f0, g2]
		//record367: [a367, b1, c1, d3, e2, f1, g3]
		//record368: [a368, b0, c2, d0, e3, f2, g4]
		String[] ConditionColumns0 = {"e","g","b","f"};
		String[] ConditionColumnsValues0 = {"e4","g0","b1","f5"};
		DBApp.createBitMapIndex("f7","e");
		DBApp.createBitMapIndex("f7","g");
		DBApp.selectIndex("f7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a119, b1, c2, d3, e4, f5, g0]
		//record1: [a329, b1, c2, d1, e4, f5, g0]
		String tableTrace0 = DBApp.getLastTrace("f7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m0g", cols1);
		String [][] records_m0g = new String[485][cols1.length];
		for(int i=0;i<485;i++)
		{
			records_m0g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m0g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m0g", records_m0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record480: [a480, b0, c0, d0, e0, f0, g4]
		//record481: [a481, b1, c1, d1, e1, f1, g5]
		//record482: [a482, b0, c2, d2, e2, f2, g6]
		//record483: [a483, b1, c0, d3, e3, f3, g0]
		//record484: [a484, b0, c1, d0, e4, f4, g1]
		String[] ConditionColumns1 = {"d","e","b","a","c"};
		String[] ConditionColumnsValues1 = {"d2","e3","b0","a298","c1"};
		DBApp.createBitMapIndex("m0g","e");
		DBApp.createBitMapIndex("m0g","b");
		DBApp.createBitMapIndex("m0g","a");
		DBApp.selectIndex("m0g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a298, b0, c1, d2, e3, f4, g4]
		String tableTrace1 = DBApp.getLastTrace("m0g");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[a, b, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tnbg", cols2);
		String [][] records_tnbg = new String[206][cols2.length];
		for(int i=0;i<206;i++)
		{
			records_tnbg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tnbg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tnbg", records_tnbg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		//record203: [a203, b1, c2, d3, e3, f5, g0]
		//record204: [a204, b0, c0, d0, e4, f0, g1]
		//record205: [a205, b1, c1, d1, e0, f1, g2]
		String[] ConditionColumns2 = {"e","g","d","f","b"};
		String[] ConditionColumnsValues2 = {"e4","g5","d0","f4","b0"};
		DBApp.createBitMapIndex("tnbg","f");
		DBApp.createBitMapIndex("tnbg","b");
		DBApp.selectIndex("tnbg", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a124, b0, c1, d0, e4, f4, g5]
		String tableTrace2 = DBApp.getLastTrace("tnbg");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k66q", cols3);
		String [][] records_k66q = new String[414][cols3.length];
		for(int i=0;i<414;i++)
		{
			records_k66q[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k66q[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k66q", records_k66q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6]
		String[] ConditionColumns3 = {"k","d","c","e","g"};
		String[] ConditionColumnsValues3 = {"k9","d0","c0","e3","g3"};
		DBApp.createBitMapIndex("k66q","k");
		DBApp.selectIndex("k66q", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9]
		String tableTrace3 = DBApp.getLastTrace("k66q");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[k]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[c, d, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g4", cols4);
		String [][] records_g4 = new String[244][cols4.length];
		for(int i=0;i<244;i++)
		{
			records_g4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g4", records_g4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1]
		String[] ConditionColumns4 = {"d","a","i","g"};
		String[] ConditionColumnsValues4 = {"d2","a2","i2","g2"};
		DBApp.createBitMapIndex("g4","d");
		DBApp.createBitMapIndex("g4","i");
		DBApp.selectIndex("g4", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		String tableTrace4 = DBApp.getLastTrace("g4");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[d, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[a, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sckw", cols0);
		String [][] records_sckw = new String[376][cols0.length];
		for(int i=0;i<376;i++)
		{
			records_sckw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sckw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sckw", records_sckw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record371: [a371, b1, c2, d3, e1, f5, g0]
		//record372: [a372, b0, c0, d0, e2, f0, g1]
		//record373: [a373, b1, c1, d1, e3, f1, g2]
		//record374: [a374, b0, c2, d2, e4, f2, g3]
		//record375: [a375, b1, c0, d3, e0, f3, g4]
		String[] ConditionColumns0 = {"g","d","f"};
		String[] ConditionColumnsValues0 = {"g1","d0","f0"};
		DBApp.createBitMapIndex("sckw","d");
		DBApp.selectIndex("sckw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a36, b0, c0, d0, e1, f0, g1]
		//record1: [a120, b0, c0, d0, e0, f0, g1]
		//record2: [a204, b0, c0, d0, e4, f0, g1]
		//record3: [a288, b0, c0, d0, e3, f0, g1]
		//record4: [a372, b0, c0, d0, e2, f0, g1]
		String tableTrace0 = DBApp.getLastTrace("sckw");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+5));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c6", cols0);
		String [][] records_c6 = new String[299][cols0.length];
		for(int i=0;i<299;i++)
		{
			records_c6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c6", records_c6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		//record298: [a298, b0, c1, d2, e3, f4, g4]
		String[] ConditionColumns0 = {"d","g","c","b","f"};
		String[] ConditionColumnsValues0 = {"d2","g0","c0","b0","f0"};
		DBApp.createBitMapIndex("c6","g");
		DBApp.selectIndex("c6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0]
		//record1: [a126, b0, c0, d2, e1, f0, g0]
		//record2: [a210, b0, c0, d2, e0, f0, g0]
		//record3: [a294, b0, c0, d2, e4, f0, g0]
		String tableTrace0 = DBApp.getLastTrace("c6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pw", cols1);
		String [][] records_pw = new String[358][cols1.length];
		for(int i=0;i<358;i++)
		{
			records_pw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pw", records_pw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record353: [a353, b1, c2, d1, e3, f5, g3]
		//record354: [a354, b0, c0, d2, e4, f0, g4]
		//record355: [a355, b1, c1, d3, e0, f1, g5]
		//record356: [a356, b0, c2, d0, e1, f2, g6]
		//record357: [a357, b1, c0, d1, e2, f3, g0]
		String[] ConditionColumns1 = {"f","a","d"};
		String[] ConditionColumnsValues1 = {"f3","a225","d1"};
		DBApp.createBitMapIndex("pw","f");
		DBApp.selectIndex("pw", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a225, b1, c0, d1, e0, f3, g1]
		String tableTrace1 = DBApp.getLastTrace("pw");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n974", cols2);
		String [][] records_n974 = new String[378][cols2.length];
		for(int i=0;i<378;i++)
		{
			records_n974[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n974[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n974", records_n974[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2]
		//record374: [a374, b0, c2, d2, e4, f2, g3]
		//record375: [a375, b1, c0, d3, e0, f3, g4]
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		String[] ConditionColumns2 = {"c","b","g"};
		String[] ConditionColumnsValues2 = {"c1","b0","g6"};
		DBApp.createBitMapIndex("n974","g");
		DBApp.selectIndex("n974", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a34, b0, c1, d2, e4, f4, g6]
		//record1: [a76, b0, c1, d0, e1, f4, g6]
		//record2: [a118, b0, c1, d2, e3, f4, g6]
		//record3: [a160, b0, c1, d0, e0, f4, g6]
		//record4: [a202, b0, c1, d2, e2, f4, g6]
		String tableTrace2 = DBApp.getLastTrace("n974");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+9));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qz1", cols3);
		String [][] records_qz1 = new String[197][cols3.length];
		for(int i=0;i<197;i++)
		{
			records_qz1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qz1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qz1", records_qz1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record192: [a192, b0, c0, d0, e2, f0, g3]
		//record193: [a193, b1, c1, d1, e3, f1, g4]
		//record194: [a194, b0, c2, d2, e4, f2, g5]
		//record195: [a195, b1, c0, d3, e0, f3, g6]
		//record196: [a196, b0, c1, d0, e1, f4, g0]
		String[] ConditionColumns3 = {"e","c","b","f"};
		String[] ConditionColumnsValues3 = {"e1","c2","b1","f5"};
		DBApp.createBitMapIndex("qz1","e");
		DBApp.createBitMapIndex("qz1","c");
		DBApp.selectIndex("qz1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a41, b1, c2, d1, e1, f5, g6]
		//record2: [a71, b1, c2, d3, e1, f5, g1]
		//record3: [a101, b1, c2, d1, e1, f5, g3]
		//record4: [a131, b1, c2, d3, e1, f5, g5]
		String tableTrace3 = DBApp.getLastTrace("qz1");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[c, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+7));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("k2t", cols4);
		String [][] records_k2t = new String[319][cols4.length];
		for(int i=0;i<319;i++)
		{
			records_k2t[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_k2t[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("k2t", records_k2t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2, n6, o14, p10, q8, r8, s10]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7, o0, p11, q9, r9, s11]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1, p12, q10, r10, s12]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13, q11, r11, s13]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12, s14]
		String[] ConditionColumns4 = {"d","m","o"};
		String[] ConditionColumnsValues4 = {"d3","m6","o2"};
		DBApp.createBitMapIndex("k2t","m");
		DBApp.selectIndex("k2t", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3, o2, p3, q6, r11, s18]
		String tableTrace4 = DBApp.getLastTrace("k2t");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[d, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("rj2", cols0);
		String [][] records_rj2 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_rj2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rj2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rj2", records_rj2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11]
		String[] ConditionColumns0 = {"n","o","m","b"};
		String[] ConditionColumnsValues0 = {"n0","o13","m2","b0"};
		DBApp.createBitMapIndex("rj2","m");
		DBApp.createBitMapIndex("rj2","b");
		DBApp.selectIndex("rj2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11]
		String tableTrace0 = DBApp.getLastTrace("rj2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[n, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e96", cols1);
		String [][] records_e96 = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_e96[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e96[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e96", records_e96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8]
		String[] ConditionColumns1 = {"c","i","e"};
		String[] ConditionColumnsValues1 = {"c1","i1","e2"};
		DBApp.createBitMapIndex("e96","i");
		DBApp.selectIndex("e96", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		//record1: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5]
		//record2: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6]
		String tableTrace1 = DBApp.getLastTrace("e96");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("gu", cols0);
		String [][] records_gu = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_gu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gu", records_gu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12, n3, o13, p13, q0, r7, s18, t13]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4, o14, p14, q1, r8, s0, t14]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5, o0, p15, q2, r9, s1, t15]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2, n6, o1, p0, q3, r10, s2, t16]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3, n7, o2, p1, q4, r11, s3, t17]
		String[] ConditionColumns0 = {"c","e","m","q","o","s","d","k","p","n","g","j","i","f","l","h","t","b"};
		String[] ConditionColumnsValues0 = {"c2","e3","m3","q16","o8","s2","d2","k7","p10","n10","g3","j8","i8","f2","l2","h2","t18","b0"};
		DBApp.createBitMapIndex("gu","c");
		DBApp.createBitMapIndex("gu","e");
		DBApp.createBitMapIndex("gu","q");
		DBApp.createBitMapIndex("gu","o");
		DBApp.createBitMapIndex("gu","s");
		DBApp.createBitMapIndex("gu","n");
		DBApp.createBitMapIndex("gu","g");
		DBApp.createBitMapIndex("gu","i");
		DBApp.createBitMapIndex("gu","f");
		DBApp.createBitMapIndex("gu","h");
		DBApp.createBitMapIndex("gu","b");
		DBApp.selectIndex("gu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2, m3, n10, o8, p10, q16, r8, s2, t18]
		String tableTrace0 = DBApp.getLastTrace("gu");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, c, e, f, g, h, i, n, o, q, s]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, j, k, l, m, p, t]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k318", cols1);
		String [][] records_k318 = new String[247][cols1.length];
		for(int i=0;i<247;i++)
		{
			records_k318[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k318[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k318", records_k318[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2, l4, m10]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12]
		String[] ConditionColumns1 = {"e","c","a","k","b","i","m","h","l","d","g","f"};
		String[] ConditionColumnsValues1 = {"e1","c0","a111","k1","b1","i3","m7","h7","l3","d3","g6","f3"};
		DBApp.createBitMapIndex("k318","e");
		DBApp.createBitMapIndex("k318","a");
		DBApp.createBitMapIndex("k318","k");
		DBApp.createBitMapIndex("k318","l");
		DBApp.createBitMapIndex("k318","g");
		DBApp.selectIndex("k318", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7]
		String tableTrace1 = DBApp.getLastTrace("k318");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[a, e, g, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, d, f, h, i, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u38m", cols2);
		String [][] records_u38m = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			records_u38m[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u38m[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u38m", records_u38m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record71: [a71, b1, c2, d3, e1, f5, g1]
		//record72: [a72, b0, c0, d0, e2, f0, g2]
		//record73: [a73, b1, c1, d1, e3, f1, g3]
		//record74: [a74, b0, c2, d2, e4, f2, g4]
		//record75: [a75, b1, c0, d3, e0, f3, g5]
		String[] ConditionColumns2 = {"f","b","c"};
		String[] ConditionColumnsValues2 = {"f5","b1","c2"};
		DBApp.createBitMapIndex("u38m","f");
		DBApp.selectIndex("u38m", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 12, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		String tableTrace2 = DBApp.getLastTrace("u38m");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, c]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+12));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("hwu", cols0);
		String [][] records_hwu = new String[288][cols0.length];
		for(int i=0;i<288;i++)
		{
			records_hwu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hwu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hwu", records_hwu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0, n6]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1, n7]
		String[] ConditionColumns0 = {"m","k","h","a"};
		String[] ConditionColumnsValues0 = {"m9","k4","h0","a48"};
		DBApp.createBitMapIndex("hwu","a");
		DBApp.selectIndex("hwu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6]
		String tableTrace0 = DBApp.getLastTrace("hwu");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[h, k, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("od", cols1);
		String [][] records_od = new String[199][cols1.length];
		for(int i=0;i<199;i++)
		{
			records_od[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_od[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("od", records_od[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12, o14]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13, o0]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0, o1]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2, n1, o2]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3, n2, o3]
		String[] ConditionColumns1 = {"n","h","o"};
		String[] ConditionColumnsValues1 = {"n2","h6","o11"};
		DBApp.createBitMapIndex("od","h");
		DBApp.selectIndex("od", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11]
		String tableTrace1 = DBApp.getLastTrace("od");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[n, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i1524", cols2);
		String [][] records_i1524 = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			records_i1524[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i1524[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i1524", records_i1524[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1]
		String[] ConditionColumns2 = {"d","k","f"};
		String[] ConditionColumnsValues2 = {"d2","k1","f0"};
		DBApp.createBitMapIndex("i1524","f");
		DBApp.selectIndex("i1524", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1]
		String tableTrace2 = DBApp.getLastTrace("i1524");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("n6", cols3);
		String [][] records_n6 = new String[339][cols3.length];
		for(int i=0;i<339;i++)
		{
			records_n6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n6", records_n6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12, o4, p14]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13, o5, p15]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6, p0]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7, p1]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2]
		String[] ConditionColumns3 = {"k","c","h","f","o","d","a","e","p"};
		String[] ConditionColumnsValues3 = {"k7","c2","h4","f2","o5","d0","a260","e0","p4"};
		DBApp.createBitMapIndex("n6","k");
		DBApp.createBitMapIndex("n6","h");
		DBApp.createBitMapIndex("n6","f");
		DBApp.createBitMapIndex("n6","o");
		DBApp.selectIndex("n6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0, n8, o5, p4]
		String tableTrace3 = DBApp.getLastTrace("n6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[f, h, k, o]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[a, c, d, e, p]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pq1", cols0);
		String [][] records_pq1 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_pq1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pq1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pq1", records_pq1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		String[] ConditionColumns0 = {"c","g","b","d","e","f"};
		String[] ConditionColumnsValues0 = {"c1","g6","b1","d1","e3","f1"};
		DBApp.createBitMapIndex("pq1","c");
		DBApp.createBitMapIndex("pq1","d");
		DBApp.createBitMapIndex("pq1","f");
		DBApp.selectIndex("pq1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6]
		String tableTrace0 = DBApp.getLastTrace("pq1");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[c, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z65v6", cols1);
		String [][] records_z65v6 = new String[195][cols1.length];
		for(int i=0;i<195;i++)
		{
			records_z65v6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z65v6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z65v6", records_z65v6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7]
		String[] ConditionColumns1 = {"d","b","g","f","h"};
		String[] ConditionColumnsValues1 = {"d3","b1","g2","f1","h7"};
		DBApp.createBitMapIndex("z65v6","d");
		DBApp.createBitMapIndex("z65v6","f");
		DBApp.createBitMapIndex("z65v6","h");
		DBApp.selectIndex("z65v6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2]
		String tableTrace1 = DBApp.getLastTrace("z65v6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, f, h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("pwwa0", cols2);
		String [][] records_pwwa0 = new String[179][cols2.length];
		for(int i=0;i<179;i++)
		{
			records_pwwa0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pwwa0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pwwa0", records_pwwa0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9, p14, q4]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6, n7, o10, p15, q5]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8, o11, p0, q6]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1, l9, m8, n9, o12, p1, q7]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9, n10, o13, p2, q8]
		String[] ConditionColumns2 = {"h","f","q","n","e","b","l","m","j","p","g","i","k","d","o"};
		String[] ConditionColumnsValues2 = {"h5","f1","q3","n9","e2","b1","l1","m11","j7","p5","g2","i1","k4","d1","o7"};
		DBApp.createBitMapIndex("pwwa0","q");
		DBApp.createBitMapIndex("pwwa0","e");
		DBApp.createBitMapIndex("pwwa0","b");
		DBApp.createBitMapIndex("pwwa0","p");
		DBApp.createBitMapIndex("pwwa0","i");
		DBApp.createBitMapIndex("pwwa0","o");
		DBApp.selectIndex("pwwa0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		String tableTrace2 = DBApp.getLastTrace("pwwa0");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, e, i, o, p, q]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, f, g, h, j, k, l, m, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h0v", cols3);
		String [][] records_h0v = new String[67][cols3.length];
		for(int i=0;i<67;i++)
		{
			records_h0v[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h0v[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h0v", records_h0v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6]
		//record63: [a63, b1, c0, d3, e3, f3, g0]
		//record64: [a64, b0, c1, d0, e4, f4, g1]
		//record65: [a65, b1, c2, d1, e0, f5, g2]
		//record66: [a66, b0, c0, d2, e1, f0, g3]
		String[] ConditionColumns3 = {"e","b","d"};
		String[] ConditionColumnsValues3 = {"e0","b1","d1"};
		DBApp.createBitMapIndex("h0v","b");
		DBApp.selectIndex("h0v", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a25, b1, c1, d1, e0, f1, g4]
		//record2: [a45, b1, c0, d1, e0, f3, g3]
		//record3: [a65, b1, c2, d1, e0, f5, g2]
		String tableTrace3 = DBApp.getLastTrace("h0v");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[d, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d8", cols4);
		String [][] records_d8 = new String[271][cols4.length];
		for(int i=0;i<271;i++)
		{
			records_d8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_d8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("d8", records_d8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record266: [a266, b0, c2, d2, e1, f2, g0]
		//record267: [a267, b1, c0, d3, e2, f3, g1]
		//record268: [a268, b0, c1, d0, e3, f4, g2]
		//record269: [a269, b1, c2, d1, e4, f5, g3]
		//record270: [a270, b0, c0, d2, e0, f0, g4]
		String[] ConditionColumns4 = {"b","d","e"};
		String[] ConditionColumnsValues4 = {"b1","d3","e1"};
		DBApp.createBitMapIndex("d8","b");
		DBApp.selectIndex("d8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 13, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a51, b1, c0, d3, e1, f3, g2]
		//record3: [a71, b1, c2, d3, e1, f5, g1]
		//record4: [a91, b1, c1, d3, e1, f1, g0]
		String tableTrace4 = DBApp.getLastTrace("d8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[d, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+13));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ox", cols0);
		String [][] records_ox = new String[147][cols0.length];
		for(int i=0;i<147;i++)
		{
			records_ox[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ox[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ox", records_ox[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3]
		String[] ConditionColumns0 = {"c","a","h"};
		String[] ConditionColumnsValues0 = {"c1","a127","h7"};
		DBApp.createBitMapIndex("ox","a");
		DBApp.selectIndex("ox", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6]
		String tableTrace0 = DBApp.getLastTrace("ox");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rele", cols1);
		String [][] records_rele = new String[173][cols1.length];
		for(int i=0;i<173;i++)
		{
			records_rele[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rele[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rele", records_rele[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0, g0]
		//record169: [a169, b1, c1, d1, e4, f1, g1]
		//record170: [a170, b0, c2, d2, e0, f2, g2]
		//record171: [a171, b1, c0, d3, e1, f3, g3]
		//record172: [a172, b0, c1, d0, e2, f4, g4]
		String[] ConditionColumns1 = {"d","b","f"};
		String[] ConditionColumnsValues1 = {"d2","b0","f0"};
		DBApp.createBitMapIndex("rele","d");
		DBApp.selectIndex("rele", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a18, b0, c0, d2, e3, f0, g4]
		//record2: [a30, b0, c0, d2, e0, f0, g2]
		//record3: [a42, b0, c0, d2, e2, f0, g0]
		//record4: [a54, b0, c0, d2, e4, f0, g5]
		String tableTrace1 = DBApp.getLastTrace("rele");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+14));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("a9f", cols2);
		String [][] records_a9f = new String[497][cols2.length];
		for(int i=0;i<497;i++)
		{
			records_a9f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a9f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a9f", records_a9f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11, n2, o12, p12, q16, r6, s17, t12]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12, n3, o13, p13, q0, r7, s18, t13]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4, o14, p14, q1, r8, s0, t14]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5, o0, p15, q2, r9, s1, t15]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2, n6, o1, p0, q3, r10, s2, t16]
		String[] ConditionColumns2 = {"s","l","r","g","i","q","c","t","o","b","n","f"};
		String[] ConditionColumnsValues2 = {"s14","l2","r14","g0","i5","q14","c2","t14","o14","b0","n0","f2"};
		DBApp.createBitMapIndex("a9f","s");
		DBApp.createBitMapIndex("a9f","g");
		DBApp.createBitMapIndex("a9f","c");
		DBApp.createBitMapIndex("a9f","t");
		DBApp.createBitMapIndex("a9f","n");
		DBApp.selectIndex("a9f", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14, t14]
		String tableTrace2 = DBApp.getLastTrace("a9f");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[c, g, n, s, t]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, f, i, l, o, q, r]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a7b", cols0);
		String [][] records_a7b = new String[324][cols0.length];
		for(int i=0;i<324;i++)
		{
			records_a7b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a7b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a7b", records_a7b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8]
		String[] ConditionColumns0 = {"g","f","i","e","a"};
		String[] ConditionColumnsValues0 = {"g6","f0","i6","e2","a132"};
		DBApp.createBitMapIndex("a7b","i");
		DBApp.createBitMapIndex("a7b","e");
		DBApp.createBitMapIndex("a7b","a");
		DBApp.selectIndex("a7b", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a132, b0, c0, d0, e2, f0, g6, h4, i6]
		String tableTrace0 = DBApp.getLastTrace("a7b");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, e, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[f, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ft15i", cols0);
		String [][] records_ft15i = new String[444][cols0.length];
		for(int i=0;i<444;i++)
		{
			records_ft15i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ft15i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ft15i", records_ft15i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		//record441: [a441, b1, c0, d1, e1, f3, g0]
		//record442: [a442, b0, c1, d2, e2, f4, g1]
		//record443: [a443, b1, c2, d3, e3, f5, g2]
		String[] ConditionColumns0 = {"g","f"};
		String[] ConditionColumnsValues0 = {"g5","f0"};
		DBApp.createBitMapIndex("ft15i","g");
		DBApp.createBitMapIndex("ft15i","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ft15i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a54, b0, c0, d2, e4, f0, g5]
		//record2: [a96, b0, c0, d0, e1, f0, g5]
		//record3: [a138, b0, c0, d2, e3, f0, g5]
		//record4: [a180, b0, c0, d0, e0, f0, g5]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 11);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a138","b0","c0","d2","e3","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a180","b0","c0","d0","e0","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vt7r4", cols1);
		String [][] records_vt7r4 = new String[338][cols1.length];
		for(int i=0;i<338;i++)
		{
			records_vt7r4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vt7r4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vt7r4", records_vt7r4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record333: [a333, b1, c0, d1, e3, f3, g4]
		//record334: [a334, b0, c1, d2, e4, f4, g5]
		//record335: [a335, b1, c2, d3, e0, f5, g6]
		//record336: [a336, b0, c0, d0, e1, f0, g0]
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		String[] ConditionColumns1 = {"b","a"};
		String[] ConditionColumnsValues1 = {"b1","a221"};
		DBApp.createBitMapIndex("vt7r4","b");
		DBApp.createBitMapIndex("vt7r4","a");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vt7r4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a221, b1, c2, d1, e1, f5, g4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a221","b1","c2","d1","e1","f5","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fe", cols2);
		String [][] records_fe = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			records_fe[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fe[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fe", records_fe[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10]
		String[] ConditionColumns2 = {"k","h","a","l"};
		String[] ConditionColumnsValues2 = {"k6","h6","a6","l6"};
		DBApp.createBitMapIndex("fe","k");
		DBApp.createBitMapIndex("fe","h");
		DBApp.createBitMapIndex("fe","a");
		DBApp.createBitMapIndex("fe","l");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("fe", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kr86u", cols0);
		String [][] records_kr86u = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_kr86u[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kr86u[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kr86u", records_kr86u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4]
		//record208: [a208, b0, c1, d0, e3, f4, g5]
		//record209: [a209, b1, c2, d1, e4, f5, g6]
		//record210: [a210, b0, c0, d2, e0, f0, g0]
		//record211: [a211, b1, c1, d3, e1, f1, g1]
		String[] ConditionColumns0 = {"e","c","d"};
		String[] ConditionColumnsValues0 = {"e0","c0","d3"};
		DBApp.createBitMapIndex("kr86u","e");
		DBApp.createBitMapIndex("kr86u","c");
		DBApp.createBitMapIndex("kr86u","d");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("kr86u", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1]
		//record1: [a75, b1, c0, d3, e0, f3, g5]
		//record2: [a135, b1, c0, d3, e0, f3, g2]
		//record3: [a195, b1, c0, d3, e0, f3, g6]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a75","b1","c0","d3","e0","f3","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a135","b1","c0","d3","e0","f3","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a195","b1","c0","d3","e0","f3","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qo700", cols0);
		String [][] records_qo700 = new String[432][cols0.length];
		for(int i=0;i<432;i++)
		{
			records_qo700[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qo700[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qo700", records_qo700[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2]
		String[] ConditionColumns0 = {"e","i"};
		String[] ConditionColumnsValues0 = {"e2","i8"};
		DBApp.createBitMapIndex("qo700","e");
		DBApp.createBitMapIndex("qo700","i");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qo700", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record1: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10]
		//record2: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3]
		//record3: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9]
		//record4: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 10);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a62","b0","c2","d2","e2","f2","g6","h6","i8","j2","k7","l2","m10"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a107","b1","c2","d3","e2","f5","g2","h3","i8","j7","k8","l11","m3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a152","b0","c2","d0","e2","f2","g5","h0","i8","j2","k9","l8","m9"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a197","b1","c2","d1","e2","f5","g1","h5","i8","j7","k10","l5","m2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("pwt", cols1);
		String [][] records_pwt = new String[405][cols1.length];
		for(int i=0;i<405;i++)
		{
			records_pwt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pwt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pwt", records_pwt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8, o10, p0, q9, r4]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9, o11, p1, q10, r5]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12, n10, o12, p2, q11, r6]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13, p3, q12, r7]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12, o14, p4, q13, r8]
		String[] ConditionColumns1 = {"h","a","p"};
		String[] ConditionColumnsValues1 = {"h7","a111","p15"};
		DBApp.createBitMapIndex("pwt","h");
		DBApp.createBitMapIndex("pwt","a");
		DBApp.createBitMapIndex("pwt","p");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("pwt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15, q9, r3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a111","b1","c0","d3","e1","f3","g6","h7","i3","j1","k1","l3","m7","n13","o6","p15","q9","r3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uc9", cols0);
		String [][] records_uc9 = new String[494][cols0.length];
		for(int i=0;i<494;i++)
		{
			records_uc9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uc9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uc9", records_uc9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1]
		String[] ConditionColumns0 = {"e","l","b"};
		String[] ConditionColumnsValues0 = {"e0","l11","b1"};
		DBApp.createBitMapIndex("uc9","e");
		DBApp.createBitMapIndex("uc9","l");
		DBApp.createBitMapIndex("uc9","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("uc9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record1: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11]
		//record2: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11]
		//record3: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11]
		//record4: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a95","b1","c2","d3","e0","f5","g4","h7","i5","j5","k7","l11"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a155","b1","c2","d3","e0","f5","g1","h3","i2","j5","k1","l11"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a215","b1","c2","d3","e0","f5","g5","h7","i8","j5","k6","l11"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a275","b1","c2","d3","e0","f5","g2","h3","i5","j5","k0","l11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n302", cols1);
		String [][] records_n302 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_n302[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n302[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n302", records_n302[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7]
		String[] ConditionColumns1 = {"b","g","e"};
		String[] ConditionColumnsValues1 = {"b1","g4","e1"};
		DBApp.createBitMapIndex("n302","b");
		DBApp.createBitMapIndex("n302","g");
		DBApp.createBitMapIndex("n302","e");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("n302", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j12", cols2);
		String [][] records_j12 = new String[429][cols2.length];
		for(int i=0;i<429;i++)
		{
			records_j12[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j12[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j12", records_j12[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record424: [a424, b0, c1, d0, e4, f4, g4]
		//record425: [a425, b1, c2, d1, e0, f5, g5]
		//record426: [a426, b0, c0, d2, e1, f0, g6]
		//record427: [a427, b1, c1, d3, e2, f1, g0]
		//record428: [a428, b0, c2, d0, e3, f2, g1]
		String[] ConditionColumns2 = {"g","d"};
		String[] ConditionColumnsValues2 = {"g1","d3"};
		DBApp.createBitMapIndex("j12","g");
		DBApp.createBitMapIndex("j12","d");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("j12", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 15, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1]
		//record1: [a43, b1, c1, d3, e3, f1, g1]
		//record2: [a71, b1, c2, d3, e1, f5, g1]
		//record3: [a99, b1, c0, d3, e4, f3, g1]
		//record4: [a127, b1, c1, d3, e2, f1, g1]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 15);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a71","b1","c2","d3","e1","f5","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a127","b1","c1","d3","e2","f1","g1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("t06", cols3);
		String [][] records_t06 = new String[103][cols3.length];
		for(int i=0;i<103;i++)
		{
			records_t06[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t06[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t06", records_t06[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8, p2, q13, r8]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15, r10]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5, q16, r11]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4, o12, p6, q0, r12]
		String[] ConditionColumns3 = {"o","p","l","h","k","r","d","j"};
		String[] ConditionColumnsValues3 = {"o8","p6","l2","h6","k5","r2","d2","j8"};
		DBApp.createBitMapIndex("t06","o");
		DBApp.createBitMapIndex("t06","p");
		DBApp.createBitMapIndex("t06","l");
		DBApp.createBitMapIndex("t06","h");
		DBApp.createBitMapIndex("t06","k");
		DBApp.createBitMapIndex("t06","r");
		DBApp.createBitMapIndex("t06","d");
		DBApp.createBitMapIndex("t06","j");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("t06", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8","p6","q4","r2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("v7", cols0);
		String [][] records_v7 = new String[274][cols0.length];
		for(int i=0;i<274;i++)
		{
			records_v7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v7", records_v7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3, o14, p13, q14, r17, s3]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4, o0, p14, q15, r0, s4]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5, o1, p15, q16, r1, s5]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2, p0, q0, r2, s6]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3, p1, q1, r3, s7]
		String[] ConditionColumns0 = {"r","l","j","q","h","o"};
		String[] ConditionColumnsValues0 = {"r16","l10","j0","q2","h6","o10"};
		DBApp.createBitMapIndex("v7","r");
		DBApp.createBitMapIndex("v7","l");
		DBApp.createBitMapIndex("v7","j");
		DBApp.createBitMapIndex("v7","q");
		DBApp.createBitMapIndex("v7","h");
		DBApp.createBitMapIndex("v7","o");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("v7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2, r16, s13]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7","j0","k4","l10","m5","n0","o10","p6","q2","r16","s13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jlq", cols0);
		String [][] records_jlq = new String[212][cols0.length];
		for(int i=0;i<212;i++)
		{
			records_jlq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jlq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jlq", records_jlq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11, o12]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12, o13]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13, o14]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0, o0]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3, n1, o1]
		String[] ConditionColumns0 = {"a","n","k","i","c","d","f"};
		String[] ConditionColumnsValues0 = {"a206","n10","k8","i8","c2","d2","f2"};
		DBApp.createBitMapIndex("jlq","a");
		DBApp.createBitMapIndex("jlq","n");
		DBApp.createBitMapIndex("jlq","k");
		DBApp.createBitMapIndex("jlq","i");
		DBApp.createBitMapIndex("jlq","c");
		DBApp.createBitMapIndex("jlq","d");
		DBApp.createBitMapIndex("jlq","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("jlq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10, o11]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a206","b0","c2","d2","e1","f2","g3","h6","i8","j6","k8","l2","m11","n10","o11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("o8c", cols1);
		String [][] records_o8c = new String[230][cols1.length];
		for(int i=0;i<230;i++)
		{
			records_o8c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o8c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o8c", records_o8c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9, m4, n1, o0, p1, q4]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5, n2, o1, p2, q5]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3, o2, p3, q6]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4, o3, p4, q7]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4, p5, q8]
		String[] ConditionColumns1 = {"j","m"};
		String[] ConditionColumnsValues1 = {"j4","m4"};
		DBApp.createBitMapIndex("o8c","j");
		DBApp.createBitMapIndex("o8c","m");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("o8c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record1: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8, o14, p6, q15]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4","q4"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a134","b0","c2","d2","e4","f2","g1","h6","i8","j4","k2","l2","m4","n8","o14","p6","q15"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l58", cols0);
		String [][] records_l58 = new String[362][cols0.length];
		for(int i=0;i<362;i++)
		{
			records_l58[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l58[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l58", records_l58[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record357: [a357, b1, c0, d1, e2, f3, g0]
		//record358: [a358, b0, c1, d2, e3, f4, g1]
		//record359: [a359, b1, c2, d3, e4, f5, g2]
		//record360: [a360, b0, c0, d0, e0, f0, g3]
		//record361: [a361, b1, c1, d1, e1, f1, g4]
		String[] ConditionColumns0 = {"b","d"};
		String[] ConditionColumnsValues0 = {"b0","d0"};
		DBApp.createBitMapIndex("l58","b");
		DBApp.createBitMapIndex("l58","d");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("l58", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 91, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 91);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vbyt", cols1);
		String [][] records_vbyt = new String[104][cols1.length];
		for(int i=0;i<104;i++)
		{
			records_vbyt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vbyt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vbyt", records_vbyt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4, o12]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13]
		String[] ConditionColumns1 = {"c","d","i","j","e"};
		String[] ConditionColumnsValues1 = {"c0","d3","i3","j9","e4"};
		DBApp.createBitMapIndex("vbyt","c");
		DBApp.createBitMapIndex("vbyt","d");
		DBApp.createBitMapIndex("vbyt","i");
		DBApp.createBitMapIndex("vbyt","j");
		DBApp.createBitMapIndex("vbyt","e");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vbyt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0","n11","o9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("kf748", cols2);
		String [][] records_kf748 = new String[86][cols2.length];
		for(int i=0;i<86;i++)
		{
			records_kf748[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kf748[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kf748", records_kf748[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4]
		String[] ConditionColumns2 = {"c","i","g","b"};
		String[] ConditionColumnsValues2 = {"c0","i0","g5","b0"};
		DBApp.createBitMapIndex("kf748","c");
		DBApp.createBitMapIndex("kf748","i");
		DBApp.createBitMapIndex("kf748","g");
		DBApp.createBitMapIndex("kf748","b");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("kf748", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a54, b0, c0, d2, e4, f0, g5, h6, i0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gca3i", cols0);
		String [][] records_gca3i = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			records_gca3i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gca3i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gca3i", records_gca3i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		String[] ConditionColumns0 = {"d","e","c"};
		String[] ConditionColumnsValues0 = {"d0","e1","c1"};
		DBApp.createBitMapIndex("gca3i","d");
		DBApp.createBitMapIndex("gca3i","e");
		DBApp.createBitMapIndex("gca3i","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("gca3i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h53", cols1);
		String [][] records_h53 = new String[183][cols1.length];
		for(int i=0;i<183;i++)
		{
			records_h53[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h53[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h53", records_h53[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2, l10, m9, n10, o13, p2]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11, o14, p3]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11, n12, o0, p4]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13, o1, p5]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0, o2, p6]
		String[] ConditionColumns1 = {"l","i","o"};
		String[] ConditionColumnsValues1 = {"l2","i8","o5"};
		DBApp.createBitMapIndex("h53","l");
		DBApp.createBitMapIndex("h53","i");
		DBApp.createBitMapIndex("h53","o");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("h53", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a170","b0","c2","d2","e0","f2","g2","h2","i8","j0","k5","l2","m1","n2","o5","p10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t73g", cols0);
		String [][] records_t73g = new String[155][cols0.length];
		for(int i=0;i<155;i++)
		{
			records_t73g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t73g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t73g", records_t73g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record150: [a150, b0, c0, d2, e0, f0, g3]
		//record151: [a151, b1, c1, d3, e1, f1, g4]
		//record152: [a152, b0, c2, d0, e2, f2, g5]
		//record153: [a153, b1, c0, d1, e3, f3, g6]
		//record154: [a154, b0, c1, d2, e4, f4, g0]
		String[] ConditionColumns0 = {"f","b"};
		String[] ConditionColumnsValues0 = {"f2","b0"};
		DBApp.createBitMapIndex("t73g","f");
		DBApp.createBitMapIndex("t73g","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("t73g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 26, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a20, b0, c2, d0, e0, f2, g6]
		//record4: [a26, b0, c2, d2, e1, f2, g5]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 26);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x2i17", cols1);
		String [][] records_x2i17 = new String[171][cols1.length];
		for(int i=0;i<171;i++)
		{
			records_x2i17[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x2i17[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x2i17", records_x2i17[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5]
		String[] ConditionColumns1 = {"h","i","l","g","f","o"};
		String[] ConditionColumnsValues1 = {"h5","i6","l9","g6","f3","o9"};
		DBApp.createBitMapIndex("x2i17","h");
		DBApp.createBitMapIndex("x2i17","i");
		DBApp.createBitMapIndex("x2i17","l");
		DBApp.createBitMapIndex("x2i17","g");
		DBApp.createBitMapIndex("x2i17","f");
		DBApp.createBitMapIndex("x2i17","o");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("x2i17", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5","i6","j9","k3","l9","m4","n13","o9"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("h4", cols2);
		String [][] records_h4 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			records_h4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h4", records_h4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8, t8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9, t9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10, t10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11, t11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		String[] ConditionColumns2 = {"q","g"};
		String[] ConditionColumnsValues2 = {"q12","g5"};
		DBApp.createBitMapIndex("h4","q");
		DBApp.createBitMapIndex("h4","g");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("h4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12","q12","r12","s12","t12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r7966", cols0);
		String [][] records_r7966 = new String[292][cols0.length];
		for(int i=0;i<292;i++)
		{
			records_r7966[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r7966[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r7966", records_r7966[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0, m2]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5]
		String[] ConditionColumns0 = {"c","h"};
		String[] ConditionColumnsValues0 = {"c2","h0"};
		DBApp.createBitMapIndex("r7966","c");
		DBApp.createBitMapIndex("r7966","h");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("r7966", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record1: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record2: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4]
		//record3: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2]
		//record4: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 12);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a80","b0","c2","d0","e0","f2","g3","h0","i8","j0","k3","l8","m2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6","h0","i5","j4","k5","l8","m0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("z4p", cols0);
		String [][] records_z4p = new String[472][cols0.length];
		for(int i=0;i<472;i++)
		{
			records_z4p[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z4p[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z4p", records_z4p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12, n5, o2, p3, q8]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6, o3, p4, q9]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4, p5, q10]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5, p6, q11]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6, p7, q12]
		String[] ConditionColumns0 = {"e","k","i","h","b","f","d","q"};
		String[] ConditionColumnsValues0 = {"e3","k8","i2","h5","b1","f5","d1","q3"};
		DBApp.createBitMapIndex("z4p","e");
		DBApp.createBitMapIndex("z4p","k");
		DBApp.createBitMapIndex("z4p","i");
		DBApp.createBitMapIndex("z4p","h");
		DBApp.createBitMapIndex("z4p","b");
		DBApp.createBitMapIndex("z4p","f");
		DBApp.createBitMapIndex("z4p","d");
		DBApp.createBitMapIndex("z4p","q");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("z4p", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5, o8, p13, q3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a173","b1","c2","d1","e3","f5","g5","h5","i2","j3","k8","l5","m4","n5","o8","p13","q3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o5", cols0);
		String [][] records_o5 = new String[412][cols0.length];
		for(int i=0;i<412;i++)
		{
			records_o5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o5", records_o5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record407: [a407, b1, c2, d3, e2, f5, g1]
		//record408: [a408, b0, c0, d0, e3, f0, g2]
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		String[] ConditionColumns0 = {"e","f","c"};
		String[] ConditionColumnsValues0 = {"e1","f1","c1"};
		DBApp.createBitMapIndex("o5","e");
		DBApp.createBitMapIndex("o5","f");
		DBApp.createBitMapIndex("o5","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("o5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a61, b1, c1, d1, e1, f1, g5]
		//record3: [a91, b1, c1, d3, e1, f1, g0]
		//record4: [a121, b1, c1, d1, e1, f1, g2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 14);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a91","b1","c1","d3","e1","f1","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a121","b1","c1","d1","e1","f1","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("l2", cols0);
		String [][] records_l2 = new String[134][cols0.length];
		for(int i=0;i<134;i++)
		{
			records_l2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l2", records_l2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0, n4, o10, p2]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1, n5, o11, p3]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2, n6, o12, p4]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3, n7, o13, p5]
		String[] ConditionColumns0 = {"j","e","k"};
		String[] ConditionColumnsValues0 = {"j2","e2","k8"};
		DBApp.createBitMapIndex("l2","j");
		DBApp.createBitMapIndex("l2","e");
		DBApp.createBitMapIndex("l2","k");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("l2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7, p4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10","o7","p4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("qa", cols1);
		String [][] records_qa = new String[490][cols1.length];
		for(int i=0;i<490;i++)
		{
			records_qa[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qa[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qa", records_qa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1]
		String[] ConditionColumns1 = {"c","d","g"};
		String[] ConditionColumnsValues1 = {"c2","d1","g4"};
		DBApp.createBitMapIndex("qa","c");
		DBApp.createBitMapIndex("qa","d");
		DBApp.createBitMapIndex("qa","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("qa", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a53, b1, c2, d1, e3, f5, g4, h5]
		//record1: [a137, b1, c2, d1, e2, f5, g4, h1]
		//record2: [a221, b1, c2, d1, e1, f5, g4, h5]
		//record3: [a305, b1, c2, d1, e0, f5, g4, h1]
		//record4: [a389, b1, c2, d1, e4, f5, g4, h5]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a137","b1","c2","d1","e2","f5","g4","h1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a221","b1","c2","d1","e1","f5","g4","h5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a305","b1","c2","d1","e0","f5","g4","h1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a389","b1","c2","d1","e4","f5","g4","h5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("y21i5", cols2);
		String [][] records_y21i5 = new String[242][cols2.length];
		for(int i=0;i<242;i++)
		{
			records_y21i5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y21i5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y21i5", records_y21i5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13, o12, p13]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13, p14]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14, p15]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6, n2, o0, p0]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7, n3, o1, p1]
		String[] ConditionColumns2 = {"f","k"};
		String[] ConditionColumnsValues2 = {"f0","k10"};
		DBApp.createBitMapIndex("y21i5","f");
		DBApp.createBitMapIndex("y21i5","k");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("y21i5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9, p6]
		//record1: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8, o0, p8]
		//record2: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6, p10]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 3);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2","n12","o9","p6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a120","b0","c0","d0","e0","f0","g1","h0","i3","j0","k10","l0","m3","n8","o0","p8"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a186","b0","c0","d2","e1","f0","g4","h2","i6","j6","k10","l6","m4","n4","o6","p10"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e7662", cols3);
		String [][] records_e7662 = new String[393][cols3.length];
		for(int i=0;i<393;i++)
		{
			records_e7662[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e7662[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e7662", records_e7662[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2]
		String[] ConditionColumns3 = {"d","g"};
		String[] ConditionColumnsValues3 = {"d3","g4"};
		DBApp.createBitMapIndex("e7662","d");
		DBApp.createBitMapIndex("e7662","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("e7662", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 14, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record1: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		//record2: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7]
		//record3: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5]
		//record4: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 14);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a95","b1","c2","d3","e0","f5","g4","h7","i5","j5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a123","b1","c0","d3","e3","f3","g4","h3","i6","j3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ybb", cols4);
		String [][] records_ybb = new String[382][cols4.length];
		for(int i=0;i<382;i++)
		{
			records_ybb[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ybb[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ybb", records_ybb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1]
		String[] ConditionColumns4 = {"d","e","g"};
		String[] ConditionColumnsValues4 = {"d2","e0","g3"};
		DBApp.createBitMapIndex("ybb","d");
		DBApp.createBitMapIndex("ybb","e");
		DBApp.createBitMapIndex("ybb","g");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("ybb", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 3, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record1: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0]
		//record2: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect4.size(), 3);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a150","b0","c0","d2","e0","f0","g3","h6","i6","j0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a290","b0","c2","d2","e0","f2","g3","h2","i2","j0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("sp8ue", cols0);
		String [][] records_sp8ue = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_sp8ue[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sp8ue[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sp8ue", records_sp8ue[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		String[] ConditionColumns0 = {"h","b"};
		String[] ConditionColumnsValues0 = {"h0","b0"};
		DBApp.createBitMapIndex("sp8ue","h");
		DBApp.createBitMapIndex("sp8ue","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("sp8ue", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 3);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fg", cols1);
		String [][] records_fg = new String[258][cols1.length];
		for(int i=0;i<258;i++)
		{
			records_fg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_fg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("fg", records_fg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record253: [a253, b1, c1, d1, e3, f1, g1]
		//record254: [a254, b0, c2, d2, e4, f2, g2]
		//record255: [a255, b1, c0, d3, e0, f3, g3]
		//record256: [a256, b0, c1, d0, e1, f4, g4]
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		String[] ConditionColumns1 = {"b","f"};
		String[] ConditionColumnsValues1 = {"b1","f1"};
		DBApp.createBitMapIndex("fg","b");
		DBApp.createBitMapIndex("fg","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("fg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 43, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 43);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xe9", cols2);
		String [][] records_xe9 = new String[308][cols2.length];
		for(int i=0;i<308;i++)
		{
			records_xe9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xe9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xe9", records_xe9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4, n9, o3]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5, n10, o4]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6, n11, o5]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7, n12, o6]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10, l7, m8, n13, o7]
		String[] ConditionColumns2 = {"k","g","o","b","f"};
		String[] ConditionColumnsValues2 = {"k9","g6","o9","b0","f0"};
		DBApp.createBitMapIndex("xe9","k");
		DBApp.createBitMapIndex("xe9","g");
		DBApp.createBitMapIndex("xe9","o");
		DBApp.createBitMapIndex("xe9","b");
		DBApp.createBitMapIndex("xe9","f");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("xe9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a174","b0","c0","d2","e4","f0","g6","h6","i3","j4","k9","l6","m5","n6","o9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e735", cols3);
		String [][] records_e735 = new String[206][cols3.length];
		for(int i=0;i<206;i++)
		{
			records_e735[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e735[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e735", records_e735[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		//record203: [a203, b1, c2, d3, e3, f5, g0]
		//record204: [a204, b0, c0, d0, e4, f0, g1]
		//record205: [a205, b1, c1, d1, e0, f1, g2]
		String[] ConditionColumns3 = {"d","e"};
		String[] ConditionColumnsValues3 = {"d1","e1"};
		DBApp.createBitMapIndex("e735","d");
		DBApp.createBitMapIndex("e735","e");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("e735", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 11, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a21, b1, c0, d1, e1, f3, g0]
		//record2: [a41, b1, c2, d1, e1, f5, g6]
		//record3: [a61, b1, c1, d1, e1, f1, g5]
		//record4: [a81, b1, c0, d1, e1, f3, g4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 11);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1","f3","g4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lfk", cols4);
		String [][] records_lfk = new String[318][cols4.length];
		for(int i=0;i<318;i++)
		{
			records_lfk[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_lfk[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("lfk", records_lfk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1, g5]
		//record314: [a314, b0, c2, d2, e4, f2, g6]
		//record315: [a315, b1, c0, d3, e0, f3, g0]
		//record316: [a316, b0, c1, d0, e1, f4, g1]
		//record317: [a317, b1, c2, d1, e2, f5, g2]
		String[] ConditionColumns4 = {"e","d"};
		String[] ConditionColumnsValues4 = {"e1","d2"};
		DBApp.createBitMapIndex("lfk","e");
		DBApp.createBitMapIndex("lfk","d");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("lfk", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 16, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a26, b0, c2, d2, e1, f2, g5]
		//record2: [a46, b0, c1, d2, e1, f4, g4]
		//record3: [a66, b0, c0, d2, e1, f0, g3]
		//record4: [a86, b0, c2, d2, e1, f2, g2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect4.size(), 16);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f1e7q", cols0);
		String [][] records_f1e7q = new String[483][cols0.length];
		for(int i=0;i<483;i++)
		{
			records_f1e7q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f1e7q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f1e7q", records_f1e7q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		//record479: [a479, b1, c2, d3, e4, f5, g3]
		//record480: [a480, b0, c0, d0, e0, f0, g4]
		//record481: [a481, b1, c1, d1, e1, f1, g5]
		//record482: [a482, b0, c2, d2, e2, f2, g6]
		String[] ConditionColumns0 = {"a","e"};
		String[] ConditionColumnsValues0 = {"a50","e0"};
		DBApp.createBitMapIndex("f1e7q","a");
		DBApp.createBitMapIndex("f1e7q","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("f1e7q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a50, b0, c2, d2, e0, f2, g1]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("j8v", cols1);
		String [][] records_j8v = new String[451][cols1.length];
		for(int i=0;i<451;i++)
		{
			records_j8v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j8v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j8v", records_j8v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2, m4, n12, o11, p14, q4, r14, s9]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13, o12, p15, q5, r15, s10]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0, o13, p0, q6, r16, s11]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1, o14, p1, q7, r17, s12]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2, o0, p2, q8, r0, s13]
		String[] ConditionColumns1 = {"e","b","k","i","r"};
		String[] ConditionColumnsValues1 = {"e3","b0","k0","i4","r4"};
		DBApp.createBitMapIndex("j8v","e");
		DBApp.createBitMapIndex("j8v","b");
		DBApp.createBitMapIndex("j8v","k");
		DBApp.createBitMapIndex("j8v","i");
		DBApp.createBitMapIndex("j8v","r");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("j8v", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2, n12, o13, p2, q10, r4, s0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a418","b0","c1","d2","e3","f4","g5","h2","i4","j8","k0","l10","m2","n12","o13","p2","q10","r4","s0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hhg86", cols2);
		String [][] records_hhg86 = new String[261][cols2.length];
		for(int i=0;i<261;i++)
		{
			records_hhg86[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hhg86[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hhg86", records_hhg86[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record256: [a256, b0, c1, d0, e1, f4, g4]
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		String[] ConditionColumns2 = {"d","f"};
		String[] ConditionColumnsValues2 = {"d0","f0"};
		DBApp.createBitMapIndex("hhg86","d");
		DBApp.createBitMapIndex("hhg86","f");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("hhg86", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 22, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a12, b0, c0, d0, e2, f0, g5]
		//record2: [a24, b0, c0, d0, e4, f0, g3]
		//record3: [a36, b0, c0, d0, e1, f0, g1]
		//record4: [a48, b0, c0, d0, e3, f0, g6]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 22);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("l2wat", cols3);
		String [][] records_l2wat = new String[92][cols3.length];
		for(int i=0;i<92;i++)
		{
			records_l2wat[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l2wat[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l2wat", records_l2wat[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1]
		String[] ConditionColumns3 = {"c","g","i"};
		String[] ConditionColumnsValues3 = {"c1","g0","i7"};
		DBApp.createBitMapIndex("l2wat","c");
		DBApp.createBitMapIndex("l2wat","g");
		DBApp.createBitMapIndex("l2wat","i");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("l2wat", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record1: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 2);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7","j0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b7il5", cols0);
		String [][] records_b7il5 = new String[171][cols0.length];
		for(int i=0;i<171;i++)
		{
			records_b7il5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b7il5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b7il5", records_b7il5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2]
		String[] ConditionColumns0 = {"n","k","c","j"};
		String[] ConditionColumnsValues0 = {"n9","k1","c2","j3"};
		DBApp.createBitMapIndex("b7il5","n");
		DBApp.createBitMapIndex("b7il5","k");
		DBApp.createBitMapIndex("b7il5","c");
		DBApp.createBitMapIndex("b7il5","j");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("b7il5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nv", cols0);
		String [][] records_nv = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			records_nv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nv", records_nv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4]
		String[] ConditionColumns0 = {"f","j"};
		String[] ConditionColumnsValues0 = {"f2","j2"};
		DBApp.createBitMapIndex("nv","f");
		DBApp.createBitMapIndex("nv","j");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("nv", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2]
		//record2: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 3);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a62","b0","c2","d2","e2","f2","g6","h6","i8","j2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b1twn", cols1);
		String [][] records_b1twn = new String[382][cols1.length];
		for(int i=0;i<382;i++)
		{
			records_b1twn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1twn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1twn", records_b1twn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5, m0, n13]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8, k4, l6, m1, n0]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7, m2, n1]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6, l8, m3, n2]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3]
		String[] ConditionColumns1 = {"h","n","k","i"};
		String[] ConditionColumnsValues1 = {"h0","n8","k10","i3"};
		DBApp.createBitMapIndex("b1twn","h");
		DBApp.createBitMapIndex("b1twn","n");
		DBApp.createBitMapIndex("b1twn","k");
		DBApp.createBitMapIndex("b1twn","i");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("b1twn", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a120","b0","c0","d0","e0","f0","g1","h0","i3","j0","k10","l0","m3","n8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rpzw3", cols0);
		String [][] records_rpzw3 = new String[160][cols0.length];
		for(int i=0;i<160;i++)
		{
			records_rpzw3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rpzw3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rpzw3", records_rpzw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1]
		//record156: [a156, b0, c0, d0, e1, f0, g2]
		//record157: [a157, b1, c1, d1, e2, f1, g3]
		//record158: [a158, b0, c2, d2, e3, f2, g4]
		//record159: [a159, b1, c0, d3, e4, f3, g5]
		String[] ConditionColumns0 = {"c","b"};
		String[] ConditionColumnsValues0 = {"c0","b1"};
		DBApp.createBitMapIndex("rpzw3","c");
		DBApp.createBitMapIndex("rpzw3","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("rpzw3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 27, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a9, b1, c0, d1, e4, f3, g2]
		//record2: [a15, b1, c0, d3, e0, f3, g1]
		//record3: [a21, b1, c0, d1, e1, f3, g0]
		//record4: [a27, b1, c0, d3, e2, f3, g6]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 27);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z6", cols1);
		String [][] records_z6 = new String[252][cols1.length];
		for(int i=0;i<252;i++)
		{
			records_z6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z6", records_z6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3]
		String[] ConditionColumns1 = {"d","c","h"};
		String[] ConditionColumnsValues1 = {"d0","c0","h0"};
		DBApp.createBitMapIndex("z6","d");
		DBApp.createBitMapIndex("z6","c");
		DBApp.createBitMapIndex("z6","h");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("z6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record2: [a48, b0, c0, d0, e3, f0, g6, h0]
		//record3: [a72, b0, c0, d0, e2, f0, g2, h0]
		//record4: [a96, b0, c0, d0, e1, f0, g5, h0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 11);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6","h0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2","h0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5","h0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q24", cols2);
		String [][] records_q24 = new String[268][cols2.length];
		for(int i=0;i<268;i++)
		{
			records_q24[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q24[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q24", records_q24[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record263: [a263, b1, c2, d3, e3, f5, g4]
		//record264: [a264, b0, c0, d0, e4, f0, g5]
		//record265: [a265, b1, c1, d1, e0, f1, g6]
		//record266: [a266, b0, c2, d2, e1, f2, g0]
		//record267: [a267, b1, c0, d3, e2, f3, g1]
		String[] ConditionColumns2 = {"f","g","e"};
		String[] ConditionColumnsValues2 = {"f5","g2","e3"};
		DBApp.createBitMapIndex("q24","f");
		DBApp.createBitMapIndex("q24","g");
		DBApp.createBitMapIndex("q24","e");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("q24", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2]
		//record1: [a233, b1, c2, d1, e3, f5, g2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 2);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a233","b1","c2","d1","e3","f5","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m8tp8", cols3);
		String [][] records_m8tp8 = new String[219][cols3.length];
		for(int i=0;i<219;i++)
		{
			records_m8tp8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m8tp8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m8tp8", records_m8tp8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		//record216: [a216, b0, c0, d0, e1, f0, g6]
		//record217: [a217, b1, c1, d1, e2, f1, g0]
		//record218: [a218, b0, c2, d2, e3, f2, g1]
		String[] ConditionColumns3 = {"f","d"};
		String[] ConditionColumnsValues3 = {"f5","d1"};
		DBApp.createBitMapIndex("m8tp8","f");
		DBApp.createBitMapIndex("m8tp8","d");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("m8tp8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 18, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a17, b1, c2, d1, e2, f5, g3]
		//record2: [a29, b1, c2, d1, e4, f5, g1]
		//record3: [a41, b1, c2, d1, e1, f5, g6]
		//record4: [a53, b1, c2, d1, e3, f5, g4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 18);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u1u93", cols0);
		String [][] records_u1u93 = new String[215][cols0.length];
		for(int i=0;i<215;i++)
		{
			records_u1u93[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u1u93[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u1u93", records_u1u93[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		String[] ConditionColumns0 = {"e","a","g","f"};
		String[] ConditionColumnsValues0 = {"e1","a181","g6","f1"};
		DBApp.createBitMapIndex("u1u93","e");
		DBApp.createBitMapIndex("u1u93","a");
		DBApp.createBitMapIndex("u1u93","g");
		DBApp.createBitMapIndex("u1u93","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("u1u93", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a181, b1, c1, d1, e1, f1, g6, h5]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a181","b1","c1","d1","e1","f1","g6","h5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("of", cols1);
		String [][] records_of = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			records_of[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_of[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("of", records_of[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		String[] ConditionColumns1 = {"b","i","g","h"};
		String[] ConditionColumnsValues1 = {"b1","i3","g3","h3"};
		DBApp.createBitMapIndex("of","b");
		DBApp.createBitMapIndex("of","i");
		DBApp.createBitMapIndex("of","g");
		DBApp.createBitMapIndex("of","h");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("of", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yvar", cols0);
		String [][] records_yvar = new String[400][cols0.length];
		for(int i=0;i<400;i++)
		{
			records_yvar[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yvar[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yvar", records_yvar[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record395: [a395, b1, c2, d3, e0, f5, g3]
		//record396: [a396, b0, c0, d0, e1, f0, g4]
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		String[] ConditionColumns0 = {"c","g"};
		String[] ConditionColumnsValues0 = {"c0","g0"};
		DBApp.createBitMapIndex("yvar","c");
		DBApp.createBitMapIndex("yvar","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("yvar", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 20, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a21, b1, c0, d1, e1, f3, g0]
		//record2: [a42, b0, c0, d2, e2, f0, g0]
		//record3: [a63, b1, c0, d3, e3, f3, g0]
		//record4: [a84, b0, c0, d0, e4, f0, g0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 20);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a84","b0","c0","d0","e4","f0","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n11go", cols1);
		String [][] records_n11go = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			records_n11go[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n11go[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n11go", records_n11go[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8]
		String[] ConditionColumns1 = {"m","h"};
		String[] ConditionColumnsValues1 = {"m5","h7"};
		DBApp.createBitMapIndex("n11go","m");
		DBApp.createBitMapIndex("n11go","h");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("n11go", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ll", cols0);
		String [][] records_ll = new String[147][cols0.length];
		for(int i=0;i<147;i++)
		{
			records_ll[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ll[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ll", records_ll[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record142: [a142, b0, c1, d2, e2, f4, g2]
		//record143: [a143, b1, c2, d3, e3, f5, g3]
		//record144: [a144, b0, c0, d0, e4, f0, g4]
		//record145: [a145, b1, c1, d1, e0, f1, g5]
		//record146: [a146, b0, c2, d2, e1, f2, g6]
		String[] ConditionColumns0 = {"g","f","e"};
		String[] ConditionColumnsValues0 = {"g3","f1","e0"};
		DBApp.createBitMapIndex("ll","g");
		DBApp.createBitMapIndex("ll","f");
		DBApp.createBitMapIndex("ll","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ll", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a115, b1, c1, d3, e0, f1, g3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a115","b1","c1","d3","e0","f1","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s0", cols1);
		String [][] records_s0 = new String[468][cols1.length];
		for(int i=0;i<468;i++)
		{
			records_s0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s0", records_s0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7]
		String[] ConditionColumns1 = {"b","j","c"};
		String[] ConditionColumnsValues1 = {"b0","j2","c1"};
		DBApp.createBitMapIndex("s0","b");
		DBApp.createBitMapIndex("s0","j");
		DBApp.createBitMapIndex("s0","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("s0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 15, records:
		//record0: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record1: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2]
		//record2: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2]
		//record3: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2]
		//record4: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 15);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a82","b0","c1","d2","e2","f4","g5","h2","i1","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a112","b0","c1","d0","e2","f4","g0","h0","i4","j2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a142","b0","c1","d2","e2","f4","g2","h6","i7","j2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ghl1q", cols2);
		String [][] records_ghl1q = new String[113][cols2.length];
		for(int i=0;i<113;i++)
		{
			records_ghl1q[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ghl1q[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ghl1q", records_ghl1q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12, q6, r0, s13]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13, q7, r1, s14]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14, q8, r2, s15]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15, q9, r3, s16]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7, p0, q10, r4, s17]
		String[] ConditionColumns2 = {"n","p"};
		String[] ConditionColumnsValues2 = {"n0","p6"};
		DBApp.createBitMapIndex("ghl1q","n");
		DBApp.createBitMapIndex("ghl1q","p");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("ghl1q", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2, r16, s13]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7","j0","k4","l10","m5","n0","o10","p6","q2","r16","s13"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hu9", cols3);
		String [][] records_hu9 = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			records_hu9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hu9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hu9", records_hu9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		String[] ConditionColumns3 = {"f","h","i","g"};
		String[] ConditionColumnsValues3 = {"f5","h5","i2","g1"};
		DBApp.createBitMapIndex("hu9","f");
		DBApp.createBitMapIndex("hu9","h");
		DBApp.createBitMapIndex("hu9","i");
		DBApp.createBitMapIndex("hu9","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("hu9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexed_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rkm3x", cols0);
		String [][] records_rkm3x = new String[132][cols0.length];
		for(int i=0;i<132;i++)
		{
			records_rkm3x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rkm3x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rkm3x", records_rkm3x[i]);
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
		String[] ConditionColumns0 = {"e","g"};
		String[] ConditionColumnsValues0 = {"e0","g3"};
		DBApp.createBitMapIndex("rkm3x","e");
		DBApp.createBitMapIndex("rkm3x","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("rkm3x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		//record1: [a45, b1, c0, d1, e0, f3, g3]
		//record2: [a80, b0, c2, d0, e0, f2, g3]
		//record3: [a115, b1, c1, d3, e0, f1, g3]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size(), 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a80","b0","c2","d0","e0","f2","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a115","b1","c1","d3","e0","f1","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k1h", cols1);
		String [][] records_k1h = new String[133][cols1.length];
		for(int i=0;i<133;i++)
		{
			records_k1h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k1h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k1h", records_k1h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		//record129: [a129, b1, c0, d1, e4, f3, g3]
		//record130: [a130, b0, c1, d2, e0, f4, g4]
		//record131: [a131, b1, c2, d3, e1, f5, g5]
		//record132: [a132, b0, c0, d0, e2, f0, g6]
		String[] ConditionColumns1 = {"e","b","c"};
		String[] ConditionColumnsValues1 = {"e0","b0","c1"};
		DBApp.createBitMapIndex("k1h","e");
		DBApp.createBitMapIndex("k1h","b");
		DBApp.createBitMapIndex("k1h","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("k1h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		//record1: [a40, b0, c1, d0, e0, f4, g5]
		//record2: [a70, b0, c1, d2, e0, f4, g0]
		//record3: [a100, b0, c1, d0, e0, f4, g2]
		//record4: [a130, b0, c1, d2, e0, f4, g4]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect1.size(), 5);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a100","b0","c1","d0","e0","f4","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a130","b0","c1","d2","e0","f4","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l9xyw", cols2);
		String [][] records_l9xyw = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			records_l9xyw[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l9xyw[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l9xyw", records_l9xyw[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		String[] ConditionColumns2 = {"f","a","b"};
		String[] ConditionColumnsValues2 = {"f0","a0","b0"};
		DBApp.createBitMapIndex("l9xyw","f");
		DBApp.createBitMapIndex("l9xyw","a");
		DBApp.createBitMapIndex("l9xyw","b");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("l9xyw", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("i14", cols3);
		String [][] records_i14 = new String[216][cols3.length];
		for(int i=0;i<216;i++)
		{
			records_i14[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_i14[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("i14", records_i14[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3, n1, o1, p3, q7, r13, s2, t11]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8, m4, n2, o2, p4, q8, r14, s3, t12]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5, n3, o3, p5, q9, r15, s4, t13]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4, o4, p6, q10, r16, s5, t14]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5, p7, q11, r17, s6, t15]
		String[] ConditionColumns3 = {"o","s","f","e","h","n"};
		String[] ConditionColumnsValues3 = {"o10","s17","f1","e0","h7","n13"};
		DBApp.createBitMapIndex("i14","o");
		DBApp.createBitMapIndex("i14","s");
		DBApp.createBitMapIndex("i14","f");
		DBApp.createBitMapIndex("i14","e");
		DBApp.createBitMapIndex("i14","h");
		DBApp.createBitMapIndex("i14","n");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("i14", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10, p7, q4, r1, s17, t15]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3","n13","o10","p7","q4","r1","s17","t15"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mk803", cols4);
		String [][] records_mk803 = new String[403][cols4.length];
		for(int i=0;i<403;i++)
		{
			records_mk803[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mk803[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mk803", records_mk803[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		//record400: [a400, b0, c1, d0, e0, f4, g1]
		//record401: [a401, b1, c2, d1, e1, f5, g2]
		//record402: [a402, b0, c0, d2, e2, f0, g3]
		String[] ConditionColumns4 = {"f","b","c"};
		String[] ConditionColumnsValues4 = {"f3","b1","c0"};
		DBApp.createBitMapIndex("mk803","f");
		DBApp.createBitMapIndex("mk803","b");
		DBApp.createBitMapIndex("mk803","c");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("mk803", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 67, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a9, b1, c0, d1, e4, f3, g2]
		//record2: [a15, b1, c0, d3, e0, f3, g1]
		//record3: [a21, b1, c0, d1, e1, f3, g0]
		//record4: [a27, b1, c0, d3, e2, f3, g6]
		assertEquals("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect4.size(), 67);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h44", cols0);
		String [][] records_h44 = new String[234][cols0.length];
		for(int i=0;i<234;i++)
		{
			records_h44[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h44[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h44", records_h44[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4, p5]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6, o5, p6]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7, o6, p7]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9]
		String[] ConditionColumns0 = {"k","o"};
		String[] ConditionColumnsValues0 = {"k2","o12"};
		DBApp.createBitMapIndex("h44","k");
		DBApp.createBitMapIndex("h44","o");
		DBApp.selectIndex("h44", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1, o12, p9]
		//record1: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12, p14]
		String tableTrace0 = DBApp.getLastTrace("h44");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x6v", cols1);
		String [][] records_x6v = new String[444][cols1.length];
		for(int i=0;i<444;i++)
		{
			records_x6v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x6v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x6v", records_x6v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		//record441: [a441, b1, c0, d1, e1, f3, g0]
		//record442: [a442, b0, c1, d2, e2, f4, g1]
		//record443: [a443, b1, c2, d3, e3, f5, g2]
		String[] ConditionColumns1 = {"f","d"};
		String[] ConditionColumnsValues1 = {"f0","d2"};
		DBApp.createBitMapIndex("x6v","f");
		DBApp.createBitMapIndex("x6v","d");
		DBApp.selectIndex("x6v", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 37, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a18, b0, c0, d2, e3, f0, g4]
		//record2: [a30, b0, c0, d2, e0, f0, g2]
		//record3: [a42, b0, c0, d2, e2, f0, g0]
		//record4: [a54, b0, c0, d2, e4, f0, g5]
		String tableTrace1 = DBApp.getLastTrace("x6v");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+37));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zoq", cols2);
		String [][] records_zoq = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			records_zoq[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zoq[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zoq", records_zoq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		String[] ConditionColumns2 = {"f","b","d"};
		String[] ConditionColumnsValues2 = {"f0","b0","d2"};
		DBApp.createBitMapIndex("zoq","f");
		DBApp.createBitMapIndex("zoq","b");
		DBApp.createBitMapIndex("zoq","d");
		DBApp.selectIndex("zoq", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a18, b0, c0, d2, e3, f0, g4]
		String tableTrace2 = DBApp.getLastTrace("zoq");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m3hey", cols3);
		String [][] records_m3hey = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			records_m3hey[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m3hey[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m3hey", records_m3hey[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		String[] ConditionColumns3 = {"h","c","k","j","i"};
		String[] ConditionColumnsValues3 = {"h2","c1","k10","j0","i1"};
		DBApp.createBitMapIndex("m3hey","h");
		DBApp.createBitMapIndex("m3hey","c");
		DBApp.createBitMapIndex("m3hey","k");
		DBApp.createBitMapIndex("m3hey","j");
		DBApp.createBitMapIndex("m3hey","i");
		DBApp.selectIndex("m3hey", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		String tableTrace3 = DBApp.getLastTrace("m3hey");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ijb", cols4);
		String [][] records_ijb = new String[55][cols4.length];
		for(int i=0;i<55;i++)
		{
			records_ijb[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ijb[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ijb", records_ijb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record50: [a50, b0, c2, d2, e0, f2, g1]
		//record51: [a51, b1, c0, d3, e1, f3, g2]
		//record52: [a52, b0, c1, d0, e2, f4, g3]
		//record53: [a53, b1, c2, d1, e3, f5, g4]
		//record54: [a54, b0, c0, d2, e4, f0, g5]
		String[] ConditionColumns4 = {"c","e"};
		String[] ConditionColumnsValues4 = {"c1","e1"};
		DBApp.createBitMapIndex("ijb","c");
		DBApp.createBitMapIndex("ijb","e");
		DBApp.selectIndex("ijb", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 4, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a31, b1, c1, d3, e1, f1, g3]
		//record3: [a46, b0, c1, d2, e1, f4, g4]
		String tableTrace4 = DBApp.getLastTrace("ijb");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dp346", cols0);
		String [][] records_dp346 = new String[240][cols0.length];
		for(int i=0;i<240;i++)
		{
			records_dp346[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dp346[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dp346", records_dp346[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4]
		//record236: [a236, b0, c2, d0, e1, f2, g5]
		//record237: [a237, b1, c0, d1, e2, f3, g6]
		//record238: [a238, b0, c1, d2, e3, f4, g0]
		//record239: [a239, b1, c2, d3, e4, f5, g1]
		String[] ConditionColumns0 = {"g","b","d"};
		String[] ConditionColumnsValues0 = {"g6","b1","d1"};
		DBApp.createBitMapIndex("dp346","g");
		DBApp.createBitMapIndex("dp346","b");
		DBApp.createBitMapIndex("dp346","d");
		DBApp.selectIndex("dp346", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6]
		//record1: [a41, b1, c2, d1, e1, f5, g6]
		//record2: [a69, b1, c0, d1, e4, f3, g6]
		//record3: [a97, b1, c1, d1, e2, f1, g6]
		//record4: [a125, b1, c2, d1, e0, f5, g6]
		String tableTrace0 = DBApp.getLastTrace("dp346");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+9));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y35", cols1);
		String [][] records_y35 = new String[113][cols1.length];
		for(int i=0;i<113;i++)
		{
			records_y35[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y35[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y35", records_y35[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2]
		String[] ConditionColumns1 = {"a","f","b","i","c"};
		String[] ConditionColumnsValues1 = {"a72","f0","b0","i0","c0"};
		DBApp.createBitMapIndex("y35","a");
		DBApp.createBitMapIndex("y35","f");
		DBApp.createBitMapIndex("y35","b");
		DBApp.createBitMapIndex("y35","i");
		DBApp.createBitMapIndex("y35","c");
		DBApp.selectIndex("y35", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2]
		String tableTrace1 = DBApp.getLastTrace("y35");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s5", cols2);
		String [][] records_s5 = new String[223][cols2.length];
		for(int i=0;i<223;i++)
		{
			records_s5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s5", records_s5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record218: [a218, b0, c2, d2, e3, f2, g1]
		//record219: [a219, b1, c0, d3, e4, f3, g2]
		//record220: [a220, b0, c1, d0, e0, f4, g3]
		//record221: [a221, b1, c2, d1, e1, f5, g4]
		//record222: [a222, b0, c0, d2, e2, f0, g5]
		String[] ConditionColumns2 = {"f","b","c"};
		String[] ConditionColumnsValues2 = {"f2","b0","c2"};
		DBApp.createBitMapIndex("s5","f");
		DBApp.createBitMapIndex("s5","b");
		DBApp.createBitMapIndex("s5","c");
		DBApp.selectIndex("s5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 37, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a20, b0, c2, d0, e0, f2, g6]
		//record4: [a26, b0, c2, d2, e1, f2, g5]
		String tableTrace2 = DBApp.getLastTrace("s5");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+37));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("b69", cols3);
		String [][] records_b69 = new String[190][cols3.length];
		for(int i=0;i<190;i++)
		{
			records_b69[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b69[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b69", records_b69[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5, p9, q15, r5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6, p10, q16, r6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5, o7, p11, q0, r7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6, n6, o8, p12, q1, r8]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7, o9, p13, q2, r9]
		String[] ConditionColumns3 = {"b","o","d"};
		String[] ConditionColumnsValues3 = {"b1","o9","d1"};
		DBApp.createBitMapIndex("b69","b");
		DBApp.createBitMapIndex("b69","o");
		DBApp.createBitMapIndex("b69","d");
		DBApp.selectIndex("b69", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record1: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1, r15]
		//record2: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1, q10, r3]
		//record3: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7, o9, p13, q2, r9]
		String tableTrace3 = DBApp.getLastTrace("b69");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("c2q", cols4);
		String [][] records_c2q = new String[337][cols4.length];
		for(int i=0;i<337;i++)
		{
			records_c2q[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_c2q[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("c2q", records_c2q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6]
		String[] ConditionColumns4 = {"i","h","j","c"};
		String[] ConditionColumnsValues4 = {"i7","h3","j9","c1"};
		DBApp.createBitMapIndex("c2q","i");
		DBApp.createBitMapIndex("c2q","h");
		DBApp.createBitMapIndex("c2q","j");
		DBApp.createBitMapIndex("c2q","c");
		DBApp.selectIndex("c2q", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9]
		String tableTrace4 = DBApp.getLastTrace("c2q");
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
	public void TableSelectionIndexConditionAllIndexedTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("m0f", cols0);
		String [][] records_m0f = new String[420][cols0.length];
		for(int i=0;i<420;i++)
		{
			records_m0f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m0f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m0f", records_m0f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3]
		String[] ConditionColumns0 = {"e","a","c"};
		String[] ConditionColumnsValues0 = {"e1","a281","c2"};
		DBApp.createBitMapIndex("m0f","e");
		DBApp.createBitMapIndex("m0f","a");
		DBApp.createBitMapIndex("m0f","c");
		DBApp.selectIndex("m0f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a281, b1, c2, d1, e1, f5, g1, h1]
		String tableTrace0 = DBApp.getLastTrace("m0f");
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
	public void TableSelectionIndexConditionAllIndexedTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m88", cols0);
		String [][] records_m88 = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			records_m88[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m88[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m88", records_m88[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11]
		String[] ConditionColumns0 = {"j","f"};
		String[] ConditionColumnsValues0 = {"j4","f4"};
		DBApp.createBitMapIndex("m88","j");
		DBApp.createBitMapIndex("m88","f");
		DBApp.selectIndex("m88", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record2: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12]
		String tableTrace0 = DBApp.getLastTrace("m88");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("lpf", cols1);
		String [][] records_lpf = new String[427][cols1.length];
		for(int i=0;i<427;i++)
		{
			records_lpf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lpf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lpf", records_lpf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2, o2, p6, q14]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3, m7, n3, o3, p7, q15]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8, n4, o4, p8, q16]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9, n5, o5, p9, q0]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6, o6, p10, q1]
		String[] ConditionColumns1 = {"i","o","e","l","f"};
		String[] ConditionColumnsValues1 = {"i0","o9","e4","l3","f3"};
		DBApp.createBitMapIndex("lpf","i");
		DBApp.createBitMapIndex("lpf","o");
		DBApp.createBitMapIndex("lpf","e");
		DBApp.createBitMapIndex("lpf","l");
		DBApp.createBitMapIndex("lpf","f");
		DBApp.selectIndex("lpf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14]
		//record1: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9, k4, l3, m6, n13, o9, p7, q7]
		String tableTrace1 = DBApp.getLastTrace("lpf");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("evg5", cols0);
		String [][] records_evg5 = new String[375][cols0.length];
		for(int i=0;i<375;i++)
		{
			records_evg5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_evg5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("evg5", records_evg5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record370: [a370, b0, c1, d2, e0, f4, g6]
		//record371: [a371, b1, c2, d3, e1, f5, g0]
		//record372: [a372, b0, c0, d0, e2, f0, g1]
		//record373: [a373, b1, c1, d1, e3, f1, g2]
		//record374: [a374, b0, c2, d2, e4, f2, g3]
		String[] ConditionColumns0 = {"d","e"};
		String[] ConditionColumnsValues0 = {"d1","e4"};
		DBApp.createBitMapIndex("evg5","d");
		DBApp.createBitMapIndex("evg5","e");
		DBApp.selectIndex("evg5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2]
		//record1: [a29, b1, c2, d1, e4, f5, g1]
		//record2: [a49, b1, c1, d1, e4, f1, g0]
		//record3: [a69, b1, c0, d1, e4, f3, g6]
		//record4: [a89, b1, c2, d1, e4, f5, g5]
		String tableTrace0 = DBApp.getLastTrace("evg5");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+19));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p77e1", cols1);
		String [][] records_p77e1 = new String[328][cols1.length];
		for(int i=0;i<328;i++)
		{
			records_p77e1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p77e1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p77e1", records_p77e1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		//record324: [a324, b0, c0, d0, e4, f0, g2]
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		String[] ConditionColumns1 = {"f","d"};
		String[] ConditionColumnsValues1 = {"f5","d1"};
		DBApp.createBitMapIndex("p77e1","f");
		DBApp.createBitMapIndex("p77e1","d");
		DBApp.selectIndex("p77e1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a17, b1, c2, d1, e2, f5, g3]
		//record2: [a29, b1, c2, d1, e4, f5, g1]
		//record3: [a41, b1, c2, d1, e1, f5, g6]
		//record4: [a53, b1, c2, d1, e3, f5, g4]
		String tableTrace1 = DBApp.getLastTrace("p77e1");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+27));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p1r0", cols2);
		String [][] records_p1r0 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_p1r0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p1r0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p1r0", records_p1r0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		String[] ConditionColumns2 = {"e","d"};
		String[] ConditionColumnsValues2 = {"e1","d1"};
		DBApp.createBitMapIndex("p1r0","e");
		DBApp.createBitMapIndex("p1r0","d");
		DBApp.selectIndex("p1r0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a21, b1, c0, d1, e1, f3, g0]
		String tableTrace2 = DBApp.getLastTrace("p1r0");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pb377", cols0);
		String [][] records_pb377 = new String[251][cols0.length];
		for(int i=0;i<251;i++)
		{
			records_pb377[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pb377[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pb377", records_pb377[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1]
		//record247: [a247, b1, c1, d3, e2, f1, g2]
		//record248: [a248, b0, c2, d0, e3, f2, g3]
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		String[] ConditionColumns0 = {"g","b","f"};
		String[] ConditionColumnsValues0 = {"g5","b0","f4"};
		DBApp.createBitMapIndex("pb377","g");
		DBApp.createBitMapIndex("pb377","b");
		DBApp.createBitMapIndex("pb377","f");
		DBApp.selectIndex("pb377", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5]
		//record1: [a82, b0, c1, d2, e2, f4, g5]
		//record2: [a124, b0, c1, d0, e4, f4, g5]
		//record3: [a166, b0, c1, d2, e1, f4, g5]
		//record4: [a208, b0, c1, d0, e3, f4, g5]
		String tableTrace0 = DBApp.getLastTrace("pb377");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+6));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y75y7", cols1);
		String [][] records_y75y7 = new String[264][cols1.length];
		for(int i=0;i<264;i++)
		{
			records_y75y7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y75y7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y75y7", records_y75y7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		//record263: [a263, b1, c2, d3, e3, f5, g4]
		String[] ConditionColumns1 = {"a","g","b"};
		String[] ConditionColumnsValues1 = {"a98","g0","b0"};
		DBApp.createBitMapIndex("y75y7","a");
		DBApp.createBitMapIndex("y75y7","g");
		DBApp.createBitMapIndex("y75y7","b");
		DBApp.selectIndex("y75y7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a98, b0, c2, d2, e3, f2, g0]
		String tableTrace1 = DBApp.getLastTrace("y75y7");
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
	public void TableSelectionIndexConditionAllIndexedTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("q6", cols0);
		String [][] records_q6 = new String[321][cols0.length];
		for(int i=0;i<321;i++)
		{
			records_q6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q6", records_q6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1]
		String[] ConditionColumns0 = {"d","i","b","e"};
		String[] ConditionColumnsValues0 = {"d0","i0","b0","e4"};
		DBApp.createBitMapIndex("q6","d");
		DBApp.createBitMapIndex("q6","i");
		DBApp.createBitMapIndex("q6","b");
		DBApp.createBitMapIndex("q6","e");
		DBApp.selectIndex("q6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1]
		String tableTrace0 = DBApp.getLastTrace("q6");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qqr", cols1);
		String [][] records_qqr = new String[407][cols1.length];
		for(int i=0;i<407;i++)
		{
			records_qqr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qqr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qqr", records_qqr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10]
		String[] ConditionColumns1 = {"g","c"};
		String[] ConditionColumnsValues1 = {"g6","c2"};
		DBApp.createBitMapIndex("qqr","g");
		DBApp.createBitMapIndex("qqr","c");
		DBApp.selectIndex("qqr", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 19, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record1: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		//record2: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2]
		//record3: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11]
		//record4: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8]
		String tableTrace1 = DBApp.getLastTrace("qqr");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+19));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qgs", cols2);
		String [][] records_qgs = new String[189][cols2.length];
		for(int i=0;i<189;i++)
		{
			records_qgs[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qgs[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qgs", records_qgs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2]
		//record185: [a185, b1, c2, d1, e0, f5, g3]
		//record186: [a186, b0, c0, d2, e1, f0, g4]
		//record187: [a187, b1, c1, d3, e2, f1, g5]
		//record188: [a188, b0, c2, d0, e3, f2, g6]
		String[] ConditionColumns2 = {"g","e"};
		String[] ConditionColumnsValues2 = {"g2","e1"};
		DBApp.createBitMapIndex("qgs","g");
		DBApp.createBitMapIndex("qgs","e");
		DBApp.selectIndex("qgs", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2]
		//record1: [a51, b1, c0, d3, e1, f3, g2]
		//record2: [a86, b0, c2, d2, e1, f2, g2]
		//record3: [a121, b1, c1, d1, e1, f1, g2]
		//record4: [a156, b0, c0, d0, e1, f0, g2]
		String tableTrace2 = DBApp.getLastTrace("qgs");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vw", cols3);
		String [][] records_vw = new String[415][cols3.length];
		for(int i=0;i<415;i++)
		{
			records_vw[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vw[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vw", records_vw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7]
		String[] ConditionColumns3 = {"h","b","i"};
		String[] ConditionColumnsValues3 = {"h5","b1","i4"};
		DBApp.createBitMapIndex("vw","h");
		DBApp.createBitMapIndex("vw","b");
		DBApp.createBitMapIndex("vw","i");
		DBApp.selectIndex("vw", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 6, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record1: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8]
		//record2: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3]
		//record3: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9]
		//record4: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4]
		String tableTrace3 = DBApp.getLastTrace("vw");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("y1", cols0);
		String [][] records_y1 = new String[437][cols0.length];
		for(int i=0;i<437;i++)
		{
			records_y1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y1", records_y1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7, r0, s14, t12]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8, r1, s15, t13]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2, q9, r2, s16, t14]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0, p3, q10, r3, s17, t15]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2, o1, p4, q11, r4, s18, t16]
		String[] ConditionColumns0 = {"h","i"};
		String[] ConditionColumnsValues0 = {"h0","i5"};
		DBApp.createBitMapIndex("y1","h");
		DBApp.createBitMapIndex("y1","i");
		DBApp.selectIndex("y1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13, t12]
		//record1: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14, p8, q2, r14, s9, t4]
		//record2: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8, o11, p0, q6, r14, s5, t16]
		//record3: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10, o8, p8, q10, r14, s1, t8]
		//record4: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0, q14, r14, s16, t0]
		String tableTrace0 = DBApp.getLastTrace("y1");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+6));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p09h0", cols1);
		String [][] records_p09h0 = new String[208][cols1.length];
		for(int i=0;i<208;i++)
		{
			records_p09h0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p09h0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p09h0", records_p09h0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7]
		String[] ConditionColumns1 = {"j","i"};
		String[] ConditionColumnsValues1 = {"j5","i6"};
		DBApp.createBitMapIndex("p09h0","j");
		DBApp.createBitMapIndex("p09h0","i");
		DBApp.selectIndex("p09h0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record1: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5]
		//record2: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5]
		String tableTrace1 = DBApp.getLastTrace("p09h0");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("kn80", cols2);
		String [][] records_kn80 = new String[435][cols2.length];
		for(int i=0;i<435;i++)
		{
			records_kn80[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kn80[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kn80", records_kn80[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10, p14, q5]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15, q6]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2, q9]
		String[] ConditionColumns2 = {"q","k","m","o","d","j","g","h"};
		String[] ConditionColumnsValues2 = {"q10","k3","m3","o11","d2","j6","g6","h2"};
		DBApp.createBitMapIndex("kn80","q");
		DBApp.createBitMapIndex("kn80","k");
		DBApp.createBitMapIndex("kn80","m");
		DBApp.createBitMapIndex("kn80","o");
		DBApp.createBitMapIndex("kn80","d");
		DBApp.createBitMapIndex("kn80","j");
		DBApp.createBitMapIndex("kn80","g");
		DBApp.createBitMapIndex("kn80","h");
		DBApp.selectIndex("kn80", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10]
		String tableTrace2 = DBApp.getLastTrace("kn80");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionAllIndexedTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z1q", cols0);
		String [][] records_z1q = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_z1q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z1q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z1q", records_z1q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record147: [a147, b1, c0, d3, e2, f3, g0]
		//record148: [a148, b0, c1, d0, e3, f4, g1]
		//record149: [a149, b1, c2, d1, e4, f5, g2]
		//record150: [a150, b0, c0, d2, e0, f0, g3]
		//record151: [a151, b1, c1, d3, e1, f1, g4]
		String[] ConditionColumns0 = {"c","f"};
		String[] ConditionColumnsValues0 = {"c2","f5"};
		DBApp.createBitMapIndex("z1q","c");
		DBApp.createBitMapIndex("z1q","f");
		DBApp.selectIndex("z1q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		String tableTrace0 = DBApp.getLastTrace("z1q");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+25));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ces", cols1);
		String [][] records_ces = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			records_ces[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ces[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ces", records_ces[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		String[] ConditionColumns1 = {"f","d"};
		String[] ConditionColumnsValues1 = {"f5","d1"};
		DBApp.createBitMapIndex("ces","f");
		DBApp.createBitMapIndex("ces","d");
		DBApp.selectIndex("ces", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a17, b1, c2, d1, e2, f5, g3]
		String tableTrace1 = DBApp.getLastTrace("ces");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n2t5e", cols2);
		String [][] records_n2t5e = new String[252][cols2.length];
		for(int i=0;i<252;i++)
		{
			records_n2t5e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n2t5e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n2t5e", records_n2t5e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record247: [a247, b1, c1, d3, e2, f1, g2]
		//record248: [a248, b0, c2, d0, e3, f2, g3]
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		//record251: [a251, b1, c2, d3, e1, f5, g6]
		String[] ConditionColumns2 = {"e","c","g"};
		String[] ConditionColumnsValues2 = {"e4","c0","g6"};
		DBApp.createBitMapIndex("n2t5e","e");
		DBApp.createBitMapIndex("n2t5e","c");
		DBApp.createBitMapIndex("n2t5e","g");
		DBApp.selectIndex("n2t5e", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6]
		//record1: [a174, b0, c0, d2, e4, f0, g6]
		String tableTrace2 = DBApp.getLastTrace("n2t5e");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(557);
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