package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_09
{

	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("owl", cols0);
		String [][] records_owl = new String[427][cols0.length];
		for(int i=0;i<427;i++)
		{
			records_owl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_owl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("owl", records_owl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3]
		String[] ConditionColumns0 = {"i","b","f"};
		String[] ConditionColumnsValues0 = {"i2","b0","f2"};
		DBApp.selectIndex("owl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 24, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record3: [a56, b0, c2, d0, e1, f2, g0, h0, i2]
		//record4: [a74, b0, c2, d2, e4, f2, g4, h2, i2]
		String tableTrace0 = DBApp.getLastTrace("owl");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+24));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("nl3ah", cols1);
		String [][] records_nl3ah = new String[441][cols1.length];
		for(int i=0;i<441;i++)
		{
			records_nl3ah[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nl3ah[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nl3ah", records_nl3ah[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6]
		String[] ConditionColumns1 = {"d","f","g","a","i"};
		String[] ConditionColumnsValues1 = {"d2","f4","g3","a346","i4"};
		DBApp.selectIndex("nl3ah", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10]
		String tableTrace1 = DBApp.getLastTrace("nl3ah");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("r31", cols2);
		String [][] records_r31 = new String[202][cols2.length];
		for(int i=0;i<202;i++)
		{
			records_r31[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r31[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r31", records_r31[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1]
		//record200: [a200, b0, c2, d0, e0, f2, g4, h0, i2, j0, k2]
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3]
		String[] ConditionColumns2 = {"b","h"};
		String[] ConditionColumnsValues2 = {"b1","h5"};
		DBApp.selectIndex("r31", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 25, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		String tableTrace2 = DBApp.getLastTrace("r31");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+25));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m6", cols3);
		String [][] records_m6 = new String[406][cols3.length];
		for(int i=0;i<406;i++)
		{
			records_m6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m6", records_m6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record401: [a401, b1, c2, d1, e1, f5, g2]
		//record402: [a402, b0, c0, d2, e2, f0, g3]
		//record403: [a403, b1, c1, d3, e3, f1, g4]
		//record404: [a404, b0, c2, d0, e4, f2, g5]
		//record405: [a405, b1, c0, d1, e0, f3, g6]
		String[] ConditionColumns3 = {"e","g","f"};
		String[] ConditionColumnsValues3 = {"e3","g0","f0"};
		DBApp.selectIndex("m6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a168, b0, c0, d0, e3, f0, g0]
		//record1: [a378, b0, c0, d2, e3, f0, g0]
		String tableTrace3 = DBApp.getLastTrace("m6");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("w3", cols0);
		String [][] records_w3 = new String[149][cols0.length];
		for(int i=0;i<149;i++)
		{
			records_w3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w3", records_w3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4, o9, p0, q8, r0, s11]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10, p1, q9, r1, s12]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10, r2, s13]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12, p3, q11, r3, s14]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13, p4, q12, r4, s15]
		String[] ConditionColumns0 = {"l","c","o","m","q","k"};
		String[] ConditionColumnsValues0 = {"l5","c2","o8","m9","q11","k3"};
		DBApp.selectIndex("w3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9, n1, o8, p1, q11, r5, s18]
		String tableTrace0 = DBApp.getLastTrace("w3");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("zr", cols1);
		String [][] records_zr = new String[313][cols1.length];
		for(int i=0;i<313;i++)
		{
			records_zr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zr", records_zr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8, m9, n0, o8, p4, q2, r2, s4, t8]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10, n1, o9, p5, q3, r3, s5, t9]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11, n2, o10, p6, q4, r4, s6, t10]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12, n3, o11, p7, q5, r5, s7, t11]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0, n4, o12, p8, q6, r6, s8, t12]
		String[] ConditionColumns1 = {"c","i","t","e","h","s"};
		String[] ConditionColumnsValues1 = {"c2","i5","t3","e3","h3","s13"};
		DBApp.selectIndex("zr", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5, l11, m8, n7, o8, p11, q16, r5, s13, t3]
		String tableTrace1 = DBApp.getLastTrace("zr");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n62", cols0);
		String [][] records_n62 = new String[432][cols0.length];
		for(int i=0;i<432;i++)
		{
			records_n62[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n62[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n62", records_n62[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		DBApp.selectIndex("n62", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 72, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0]
		String tableTrace0 = DBApp.getLastTrace("n62");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+72));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bur", cols0);
		String [][] records_bur = new String[188][cols0.length];
		for(int i=0;i<188;i++)
		{
			records_bur[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bur[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bur", records_bur[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3]
		String[] ConditionColumns0 = {"c","e"};
		String[] ConditionColumnsValues0 = {"c0","e4"};
		DBApp.selectIndex("bur", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record2: [a39, b1, c0, d3, e4, f3, g4, h7]
		//record3: [a54, b0, c0, d2, e4, f0, g5, h6]
		//record4: [a69, b1, c0, d1, e4, f3, g6, h5]
		String tableTrace0 = DBApp.getLastTrace("bur");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+12));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x55", cols1);
		String [][] records_x55 = new String[149][cols1.length];
		for(int i=0;i<149;i++)
		{
			records_x55[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x55[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x55", records_x55[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4]
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		DBApp.selectIndex("x55", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 49, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		String tableTrace1 = DBApp.getLastTrace("x55");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+49));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("uenb", cols0);
		String [][] records_uenb = new String[260][cols0.length];
		for(int i=0;i<260;i++)
		{
			records_uenb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uenb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uenb", records_uenb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7, o4]
		String[] ConditionColumns0 = {"k","a","c","f"};
		String[] ConditionColumnsValues0 = {"k8","a96","c0","f0"};
		DBApp.createBitMapIndex("uenb","k");
		DBApp.createBitMapIndex("uenb","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("uenb", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5","h0","i6","j6","k8","l0","m5","n12","o6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("k6f", cols1);
		String [][] records_k6f = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			records_k6f[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k6f[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k6f", records_k6f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10, o5, p0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6, p1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7, p2]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3]
		String[] ConditionColumns1 = {"d","o","c","i","p","m","e","n","j"};
		String[] ConditionColumnsValues1 = {"d1","o9","c0","i6","p5","m4","e4","n13","j9"};
		DBApp.createBitMapIndex("k6f","d");
		DBApp.createBitMapIndex("k6f","o");
		DBApp.createBitMapIndex("k6f","i");
		DBApp.createBitMapIndex("k6f","p");
		DBApp.createBitMapIndex("k6f","n");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("k6f", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5","i6","j9","k3","l9","m4","n13","o9","p5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("aj4", cols2);
		String [][] records_aj4 = new String[350][cols2.length];
		for(int i=0;i<350;i++)
		{
			records_aj4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_aj4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("aj4", records_aj4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5]
		String[] ConditionColumns2 = {"e","d","c","f","g"};
		String[] ConditionColumnsValues2 = {"e1","d3","c2","f5","g2"};
		DBApp.createBitMapIndex("aj4","d");
		DBApp.createBitMapIndex("aj4","g");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("aj4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a191, b1, c2, d3, e1, f5, g2, h7]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a191","b1","c2","d3","e1","f5","g2","h7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p7k", cols0);
		String [][] records_p7k = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			records_p7k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p7k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p7k", records_p7k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7]
		String[] ConditionColumns0 = {"d","e","f","g","j","k","c","a"};
		String[] ConditionColumnsValues0 = {"d3","e2","f3","g6","j7","k5","c0","a27"};
		DBApp.createBitMapIndex("p7k","f");
		DBApp.createBitMapIndex("p7k","g");
		DBApp.createBitMapIndex("p7k","k");
		DBApp.createBitMapIndex("p7k","c");
		DBApp.createBitMapIndex("p7k","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("p7k", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("f164", cols1);
		String [][] records_f164 = new String[410][cols1.length];
		for(int i=0;i<410;i++)
		{
			records_f164[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f164[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f164", records_f164[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2, l1]
		String[] ConditionColumns1 = {"i","e","f","b","j","l","k","h"};
		String[] ConditionColumnsValues1 = {"i1","e1","f4","b0","j6","l10","k6","h2"};
		DBApp.createBitMapIndex("f164","i");
		DBApp.createBitMapIndex("f164","e");
		DBApp.createBitMapIndex("f164","f");
		DBApp.createBitMapIndex("f164","j");
		DBApp.createBitMapIndex("f164","h");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("f164", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a226","b0","c1","d2","e1","f4","g2","h2","i1","j6","k6","l10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kw3", cols2);
		String [][] records_kw3 = new String[188][cols2.length];
		for(int i=0;i<188;i++)
		{
			records_kw3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kw3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kw3", records_kw3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7]
		String[] ConditionColumns2 = {"g","d","c","b","j","e","i","h"};
		String[] ConditionColumnsValues2 = {"g0","d2","c1","b0","j4","e4","i1","h2"};
		DBApp.createBitMapIndex("kw3","g");
		DBApp.createBitMapIndex("kw3","d");
		DBApp.createBitMapIndex("kw3","j");
		DBApp.createBitMapIndex("kw3","e");
		DBApp.createBitMapIndex("kw3","i");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("kw3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a154","b0","c1","d2","e4","f4","g0","h2","i1","j4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jsm", cols3);
		String [][] records_jsm = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			records_jsm[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jsm[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jsm", records_jsm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		//record49: [a49, b1, c1, d1, e4, f1, g0]
		//record50: [a50, b0, c2, d2, e0, f2, g1]
		String[] ConditionColumns3 = {"e","c","d"};
		String[] ConditionColumnsValues3 = {"e0","c2","d0"};
		DBApp.createBitMapIndex("jsm","e");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("jsm", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("blj", cols4);
		String [][] records_blj = new String[102][cols4.length];
		for(int i=0;i<102;i++)
		{
			records_blj[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_blj[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("blj", records_blj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2]
		String[] ConditionColumns4 = {"h","f","b"};
		String[] ConditionColumnsValues4 = {"h5","f5","b1"};
		DBApp.createBitMapIndex("blj","f");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("blj", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 5, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record2: [a53, b1, c2, d1, e3, f5, g4, h5, i8]
		//record3: [a77, b1, c2, d1, e2, f5, g0, h5, i5]
		//record4: [a101, b1, c2, d1, e1, f5, g3, h5, i2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 5);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a77","b1","c2","d1","e2","f5","g0","h5","i5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a101","b1","c2","d1","e1","f5","g3","h5","i2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("t49", cols0);
		String [][] records_t49 = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_t49[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t49[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t49", records_t49[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7, p12, q2, r10]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5, o8, p13, q3, r11]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9, p14, q4, r12]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6, n7, o10, p15, q5, r13]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8, o11, p0, q6, r14]
		String[] ConditionColumns0 = {"d","j","a","p","f","n","m","o","r","h","e"};
		String[] ConditionColumnsValues0 = {"d3","j9","a59","p11","f5","n3","m7","o14","r5","h3","e4"};
		DBApp.createBitMapIndex("t49","d");
		DBApp.createBitMapIndex("t49","a");
		DBApp.createBitMapIndex("t49","p");
		DBApp.createBitMapIndex("t49","n");
		DBApp.createBitMapIndex("t49","o");
		DBApp.createBitMapIndex("t49","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("t49", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7, n3, o14, p11, q8, r5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a59","b1","c2","d3","e4","f5","g3","h3","i5","j9","k4","l11","m7","n3","o14","p11","q8","r5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("px", cols0);
		String [][] records_px = new String[438][cols0.length];
		for(int i=0;i<438;i++)
		{
			records_px[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_px[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("px", records_px[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8, r1, s15, t13]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2, q9, r2, s16, t14]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0, p3, q10, r3, s17, t15]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2, o1, p4, q11, r4, s18, t16]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2, p5, q12, r5, s0, t17]
		String[] ConditionColumns0 = {"q","g","f","d","h","r","n","i"};
		String[] ConditionColumnsValues0 = {"q12","g6","f0","d0","h0","r0","n6","i0"};
		DBApp.createBitMapIndex("px","f");
		DBApp.createBitMapIndex("px","d");
		DBApp.createBitMapIndex("px","h");
		DBApp.createBitMapIndex("px","r");
		DBApp.createBitMapIndex("px","i");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("px", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6, p8, q12, r0, s7, t16]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a216","b0","c0","d0","e1","f0","g6","h0","i0","j6","k7","l0","m8","n6","o6","p8","q12","r0","s7","t16"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o85t1", cols1);
		String [][] records_o85t1 = new String[355][cols1.length];
		for(int i=0;i<355;i++)
		{
			records_o85t1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o85t1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o85t1", records_o85t1[i]);
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
		String[] ConditionColumns1 = {"b","f","d","c"};
		String[] ConditionColumnsValues1 = {"b1","f3","d3","c0"};
		DBApp.createBitMapIndex("o85t1","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("o85t1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 30, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a15, b1, c0, d3, e0, f3, g1]
		//record2: [a27, b1, c0, d3, e2, f3, g6]
		//record3: [a39, b1, c0, d3, e4, f3, g4]
		//record4: [a51, b1, c0, d3, e1, f3, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 30);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ab", cols0);
		String [][] records_ab = new String[263][cols0.length];
		for(int i=0;i<263;i++)
		{
			records_ab[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ab[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ab", records_ab[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		//record259: [a259, b1, c1, d3, e4, f1, g0]
		//record260: [a260, b0, c2, d0, e0, f2, g1]
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		String[] ConditionColumns0 = {"e","d","f"};
		String[] ConditionColumnsValues0 = {"e1","d1","f3"};
		DBApp.createBitMapIndex("ab","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ab", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0]
		//record1: [a81, b1, c0, d1, e1, f3, g4]
		//record2: [a141, b1, c0, d1, e1, f3, g1]
		//record3: [a201, b1, c0, d1, e1, f3, g5]
		//record4: [a261, b1, c0, d1, e1, f3, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 5);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1","f3","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a141","b1","c0","d1","e1","f3","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a201","b1","c0","d1","e1","f3","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a261","b1","c0","d1","e1","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i4d3", cols1);
		String [][] records_i4d3 = new String[345][cols1.length];
		for(int i=0;i<345;i++)
		{
			records_i4d3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i4d3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i4d3", records_i4d3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		//record342: [a342, b0, c0, d2, e2, f0, g6]
		//record343: [a343, b1, c1, d3, e3, f1, g0]
		//record344: [a344, b0, c2, d0, e4, f2, g1]
		String[] ConditionColumns1 = {"d","e","g"};
		String[] ConditionColumnsValues1 = {"d1","e3","g3"};
		DBApp.createBitMapIndex("i4d3","d");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("i4d3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a73, b1, c1, d1, e3, f1, g3]
		//record1: [a213, b1, c0, d1, e3, f3, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a73","b1","c1","d1","e3","f1","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a213","b1","c0","d1","e3","f3","g3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b7g4", cols2);
		String [][] records_b7g4 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			records_b7g4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b7g4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b7g4", records_b7g4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		String[] ConditionColumns2 = {"c","b","a","e"};
		String[] ConditionColumnsValues2 = {"c1","b0","a22","e2"};
		DBApp.createBitMapIndex("b7g4","e");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("b7g4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a22, b0, c1, d2, e2, f4, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("a2nz4", cols0);
		String [][] records_a2nz4 = new String[371][cols0.length];
		for(int i=0;i<371;i++)
		{
			records_a2nz4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a2nz4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a2nz4", records_a2nz4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10]
		String[] ConditionColumns0 = {"k","g","a","i"};
		String[] ConditionColumnsValues0 = {"k3","g5","a278","i8"};
		DBApp.createBitMapIndex("a2nz4","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("a2nz4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a278","b0","c2","d2","e3","f2","g5","h6","i8","j8","k3","l2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a5h67", cols1);
		String [][] records_a5h67 = new String[127][cols1.length];
		for(int i=0;i<127;i++)
		{
			records_a5h67[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a5h67[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a5h67", records_a5h67[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2, g3]
		//record123: [a123, b1, c0, d3, e3, f3, g4]
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		String[] ConditionColumns1 = {"b","f","e"};
		String[] ConditionColumnsValues1 = {"b0","f4","e1"};
		DBApp.createBitMapIndex("a5h67","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("a5h67", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 4, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2]
		//record1: [a46, b0, c1, d2, e1, f4, g4]
		//record2: [a76, b0, c1, d0, e1, f4, g6]
		//record3: [a106, b0, c1, d2, e1, f4, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 4);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a76","b0","c1","d0","e1","f4","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a106","b0","c1","d2","e1","f4","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a4k", cols2);
		String [][] records_a4k = new String[143][cols2.length];
		for(int i=0;i<143;i++)
		{
			records_a4k[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a4k[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a4k", records_a4k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		//record140: [a140, b0, c2, d0, e0, f2, g0]
		//record141: [a141, b1, c0, d1, e1, f3, g1]
		//record142: [a142, b0, c1, d2, e2, f4, g2]
		String[] ConditionColumns2 = {"b","d","e"};
		String[] ConditionColumnsValues2 = {"b0","d2","e1"};
		DBApp.createBitMapIndex("a4k","b");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("a4k", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a26, b0, c2, d2, e1, f2, g5]
		//record2: [a46, b0, c1, d2, e1, f4, g4]
		//record3: [a66, b0, c0, d2, e1, f0, g3]
		//record4: [a86, b0, c2, d2, e1, f2, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 7);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("w6", cols3);
		String [][] records_w6 = new String[274][cols3.length];
		for(int i=0;i<274;i++)
		{
			records_w6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_w6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("w6", records_w6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3, o14, p13, q14, r17, s3, t9]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4, o0, p14, q15, r0, s4, t10]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5, o1, p15, q16, r1, s5, t11]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2, p0, q0, r2, s6, t12]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3, p1, q1, r3, s7, t13]
		String[] ConditionColumns3 = {"o","t","h","d","p","r","s","j","f","l","e","m","k"};
		String[] ConditionColumnsValues3 = {"o5","t10","h6","d2","p14","r2","s15","j0","f2","l2","e0","m6","k0"};
		DBApp.createBitMapIndex("w6","o");
		DBApp.createBitMapIndex("w6","p");
		DBApp.createBitMapIndex("w6","f");
		DBApp.createBitMapIndex("w6","l");
		DBApp.createBitMapIndex("w6","e");
		DBApp.createBitMapIndex("w6","m");
		DBApp.createBitMapIndex("w6","k");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("w6", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14, q8, r2, s15, t10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a110","b0","c2","d2","e0","f2","g5","h6","i2","j0","k0","l2","m6","n12","o5","p14","q8","r2","s15","t10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s628", cols0);
		String [][] records_s628 = new String[363][cols0.length];
		for(int i=0;i<363;i++)
		{
			records_s628[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s628[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s628", records_s628[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record358: [a358, b0, c1, d2, e3, f4, g1]
		//record359: [a359, b1, c2, d3, e4, f5, g2]
		//record360: [a360, b0, c0, d0, e0, f0, g3]
		//record361: [a361, b1, c1, d1, e1, f1, g4]
		//record362: [a362, b0, c2, d2, e2, f2, g5]
		String[] ConditionColumns0 = {"d","g","a"};
		String[] ConditionColumnsValues0 = {"d3","g5","a159"};
		DBApp.createBitMapIndex("s628","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("s628", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a159, b1, c0, d3, e4, f3, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a159","b1","c0","d3","e4","f3","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g9", cols1);
		String [][] records_g9 = new String[182][cols1.length];
		for(int i=0;i<182;i++)
		{
			records_g9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g9", records_g9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1]
		String[] ConditionColumns1 = {"d","f","h","c","e","i"};
		String[] ConditionColumnsValues1 = {"d0","f0","h0","c0","e4","i6"};
		DBApp.createBitMapIndex("g9","d");
		DBApp.createBitMapIndex("g9","h");
		DBApp.createBitMapIndex("g9","i");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("g9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h8uja", cols2);
		String [][] records_h8uja = new String[435][cols2.length];
		for(int i=0;i<435;i++)
		{
			records_h8uja[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h8uja[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h8uja", records_h8uja[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3]
		//record431: [a431, b1, c2, d3, e1, f5, g4]
		//record432: [a432, b0, c0, d0, e2, f0, g5]
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		String[] ConditionColumns2 = {"c","e","a"};
		String[] ConditionColumnsValues2 = {"c2","e4","a104"};
		DBApp.createBitMapIndex("h8uja","c");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("h8uja", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a104, b0, c2, d0, e4, f2, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q8", cols3);
		String [][] records_q8 = new String[144][cols3.length];
		for(int i=0;i<144;i++)
		{
			records_q8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q8", records_q8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8]
		String[] ConditionColumns3 = {"c","f","h"};
		String[] ConditionColumnsValues3 = {"c0","f0","h6"};
		DBApp.createBitMapIndex("q8","f");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("q8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 6, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record2: [a54, b0, c0, d2, e4, f0, g5, h6, i0]
		//record3: [a78, b0, c0, d2, e3, f0, g1, h6, i6]
		//record4: [a102, b0, c0, d2, e2, f0, g4, h6, i3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 6);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a78","b0","c0","d2","e3","f0","g1","h6","i6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a102","b0","c0","d2","e2","f0","g4","h6","i3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qaur", cols0);
		String [][] records_qaur = new String[431][cols0.length];
		for(int i=0;i<431;i++)
		{
			records_qaur[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qaur[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qaur", records_qaur[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6, o6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7, o7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8, o8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10]
		String[] ConditionColumns0 = {"i","m","a","n","e","l","b","d","o","j"};
		String[] ConditionColumnsValues0 = {"i3","m7","a345","n9","e0","l9","b1","d1","o0","j5"};
		DBApp.createBitMapIndex("qaur","m");
		DBApp.createBitMapIndex("qaur","a");
		DBApp.createBitMapIndex("qaur","n");
		DBApp.createBitMapIndex("qaur","b");
		DBApp.createBitMapIndex("qaur","o");
		DBApp.createBitMapIndex("qaur","j");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qaur", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a345","b1","c0","d1","e0","f3","g2","h1","i3","j5","k4","l9","m7","n9","o0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("x09h5", cols0);
		String [][] records_x09h5 = new String[472][cols0.length];
		for(int i=0;i<472;i++)
		{
			records_x09h5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x09h5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x09h5", records_x09h5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9]
		String[] ConditionColumns0 = {"d","g","c","k","e","a","b","i","j","h"};
		String[] ConditionColumnsValues0 = {"d1","g4","c0","k10","e2","a417","b1","i3","j7","h1"};
		DBApp.createBitMapIndex("x09h5","k");
		DBApp.createBitMapIndex("x09h5","j");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("x09h5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a417","b1","c0","d1","e2","f3","g4","h1","i3","j7","k10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("v1v31", cols0);
		String [][] records_v1v31 = new String[104][cols0.length];
		for(int i=0;i<104;i++)
		{
			records_v1v31[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v1v31[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v1v31", records_v1v31[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9, s4]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15, r10, s5]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5, q16, r11, s6]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4, o12, p6, q0, r12, s7]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13, p7, q1, r13, s8]
		String[] ConditionColumns0 = {"j","f","p","m","s","q","b","o","d","g"};
		String[] ConditionColumnsValues0 = {"j1","f1","p1","m1","s1","q1","b1","o1","d1","g1"};
		DBApp.createBitMapIndex("v1v31","f");
		DBApp.createBitMapIndex("v1v31","p");
		DBApp.createBitMapIndex("v1v31","s");
		DBApp.createBitMapIndex("v1v31","b");
		DBApp.createBitMapIndex("v1v31","o");
		DBApp.createBitMapIndex("v1v31","d");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("v1v31", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1","p1","q1","r1","s1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dg", cols1);
		String [][] records_dg = new String[292][cols1.length];
		for(int i=0;i<292;i++)
		{
			records_dg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dg", records_dg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record287: [a287, b1, c2, d3, e2, f5, g0]
		//record288: [a288, b0, c0, d0, e3, f0, g1]
		//record289: [a289, b1, c1, d1, e4, f1, g2]
		//record290: [a290, b0, c2, d2, e0, f2, g3]
		//record291: [a291, b1, c0, d3, e1, f3, g4]
		String[] ConditionColumns1 = {"b","g","d"};
		String[] ConditionColumnsValues1 = {"b1","g2","d3"};
		DBApp.createBitMapIndex("dg","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("dg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 10, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2]
		//record1: [a51, b1, c0, d3, e1, f3, g2]
		//record2: [a79, b1, c1, d3, e4, f1, g2]
		//record3: [a107, b1, c2, d3, e2, f5, g2]
		//record4: [a135, b1, c0, d3, e0, f3, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 10);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a79","b1","c1","d3","e4","f1","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a107","b1","c2","d3","e2","f5","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a135","b1","c0","d3","e0","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("lr", cols2);
		String [][] records_lr = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			records_lr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lr", records_lr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5, p15, q10, r5, s0, t15]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6, p0, q11, r6, s1, t16]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7, p1, q12, r7, s2, t17]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8, p2, q13, r8, s3, t18]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9, s4, t19]
		String[] ConditionColumns2 = {"j","f","d","q","m","g","o","k","s","t","i"};
		String[] ConditionColumnsValues2 = {"j6","f0","d0","q11","m5","g5","o6","k8","s1","t16","i6"};
		DBApp.createBitMapIndex("lr","j");
		DBApp.createBitMapIndex("lr","d");
		DBApp.createBitMapIndex("lr","q");
		DBApp.createBitMapIndex("lr","k");
		DBApp.createBitMapIndex("lr","t");
		DBApp.createBitMapIndex("lr","i");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("lr", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6, p0, q11, r6, s1, t16]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5","h0","i6","j6","k8","l0","m5","n12","o6","p0","q11","r6","s1","t16"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("hg4", cols3);
		String [][] records_hg4 = new String[172][cols3.length];
		for(int i=0;i<172;i++)
		{
			records_hg4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hg4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hg4", records_hg4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3, o6, p11]
		String[] ConditionColumns3 = {"m","n","i","d","f","g","k","a"};
		String[] ConditionColumnsValues3 = {"m12","n7","i5","d1","f5","g0","k0","a77"};
		DBApp.createBitMapIndex("hg4","n");
		DBApp.createBitMapIndex("hg4","i");
		DBApp.createBitMapIndex("hg4","d");
		DBApp.createBitMapIndex("hg4","f");
		DBApp.createBitMapIndex("hg4","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("hg4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7, o2, p13]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a77","b1","c2","d1","e2","f5","g0","h5","i5","j7","k0","l5","m12","n7","o2","p13"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g25", cols4);
		String [][] records_g25 = new String[39][cols4.length];
		for(int i=0;i<39;i++)
		{
			records_g25[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g25[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g25", records_g25[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6]
		String[] ConditionColumns4 = {"f","e","h","g","a"};
		String[] ConditionColumnsValues4 = {"f2","e2","h2","g2","a2"};
		DBApp.createBitMapIndex("g25","e");
		DBApp.createBitMapIndex("g25","h");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("g25", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("z4om", cols0);
		String [][] records_z4om = new String[329][cols0.length];
		for(int i=0;i<329;i++)
		{
			records_z4om[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z4om[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z4om", records_z4om[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9, p4, q1, r0]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3, o10, p5, q2, r1]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4, o11, p6, q3, r2]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12, p7, q4, r3]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6, o13, p8, q5, r4]
		String[] ConditionColumns0 = {"q","k","d","p","j","e","h","n","g","i","o","c","m"};
		String[] ConditionColumnsValues0 = {"q10","k0","d0","p12","j4","e4","h4","n2","g2","i8","o14","c2","m5"};
		DBApp.createBitMapIndex("z4om","k");
		DBApp.createBitMapIndex("z4om","p");
		DBApp.createBitMapIndex("z4om","j");
		DBApp.createBitMapIndex("z4om","h");
		DBApp.createBitMapIndex("z4om","n");
		DBApp.createBitMapIndex("z4om","o");
		DBApp.createBitMapIndex("z4om","m");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("z4om", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5","n2","o14","p12","q10","r8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g6k", cols1);
		String [][] records_g6k = new String[205][cols1.length];
		for(int i=0;i<205;i++)
		{
			records_g6k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g6k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g6k", records_g6k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record200: [a200, b0, c2, d0, e0, f2, g4]
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		//record203: [a203, b1, c2, d3, e3, f5, g0]
		//record204: [a204, b0, c0, d0, e4, f0, g1]
		String[] ConditionColumns1 = {"g","a","f","d"};
		String[] ConditionColumnsValues1 = {"g4","a116","f2","d0"};
		DBApp.createBitMapIndex("g6k","g");
		DBApp.createBitMapIndex("g6k","a");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("g6k", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a116, b0, c2, d0, e1, f2, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a116","b0","c2","d0","e1","f2","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("eu6", cols0);
		String [][] records_eu6 = new String[143][cols0.length];
		for(int i=0;i<143;i++)
		{
			records_eu6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eu6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eu6", records_eu6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0, o5, p12, q4]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1, o6, p13, q5]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2, o7, p14, q6]
		String[] ConditionColumns0 = {"p","m","h","j","l","o"};
		String[] ConditionColumnsValues0 = {"p0","m8","h0","j2","l4","o7"};
		DBApp.createBitMapIndex("eu6","o");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("eu6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7, p0, q10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a112","b0","c1","d0","e2","f4","g0","h0","i4","j2","k2","l4","m8","n0","o7","p0","q10"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vgz", cols1);
		String [][] records_vgz = new String[365][cols1.length];
		for(int i=0;i<365;i++)
		{
			records_vgz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vgz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vgz", records_vgz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4]
		String[] ConditionColumns1 = {"h","c","e","a","d","b"};
		String[] ConditionColumnsValues1 = {"h2","c1","e1","a346","d2","b0"};
		DBApp.createBitMapIndex("vgz","c");
		DBApp.createBitMapIndex("vgz","a");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vgz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a346, b0, c1, d2, e1, f4, g3, h2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a346","b0","c1","d2","e1","f4","g3","h2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j6", cols0);
		String [][] records_j6 = new String[280][cols0.length];
		for(int i=0;i<280;i++)
		{
			records_j6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j6", records_j6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record275: [a275, b1, c2, d3, e0, f5, g2]
		//record276: [a276, b0, c0, d0, e1, f0, g3]
		//record277: [a277, b1, c1, d1, e2, f1, g4]
		//record278: [a278, b0, c2, d2, e3, f2, g5]
		//record279: [a279, b1, c0, d3, e4, f3, g6]
		String[] ConditionColumns0 = {"g","b","c","a"};
		String[] ConditionColumnsValues0 = {"g1","b0","c2","a134"};
		DBApp.createBitMapIndex("j6","c");
		DBApp.createBitMapIndex("j6","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("j6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a134, b0, c2, d2, e4, f2, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a134","b0","c2","d2","e4","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kh949", cols1);
		String [][] records_kh949 = new String[438][cols1.length];
		for(int i=0;i<438;i++)
		{
			records_kh949[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kh949[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kh949", records_kh949[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		String[] ConditionColumns1 = {"d","c","a","g","e"};
		String[] ConditionColumnsValues1 = {"d1","c1","a253","g1","e3"};
		DBApp.createBitMapIndex("kh949","d");
		DBApp.createBitMapIndex("kh949","a");
		DBApp.createBitMapIndex("kh949","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("kh949", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a253, b1, c1, d1, e3, f1, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a253","b1","c1","d1","e3","f1","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qor", cols2);
		String [][] records_qor = new String[153][cols2.length];
		for(int i=0;i<153;i++)
		{
			records_qor[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qor[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qor", records_qor[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9]
		String[] ConditionColumns2 = {"c","f","g"};
		String[] ConditionColumnsValues2 = {"c0","f0","g5"};
		DBApp.createBitMapIndex("qor","f");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("qor", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record1: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10]
		//record2: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8]
		//record3: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 4);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5","h0","i6","j6","k8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a138","b0","c0","d2","e3","f0","g5","h2","i3","j8","k6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z3277", cols3);
		String [][] records_z3277 = new String[395][cols3.length];
		for(int i=0;i<395;i++)
		{
			records_z3277[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z3277[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z3277", records_z3277[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record390: [a390, b0, c0, d2, e0, f0, g5]
		//record391: [a391, b1, c1, d3, e1, f1, g6]
		//record392: [a392, b0, c2, d0, e2, f2, g0]
		//record393: [a393, b1, c0, d1, e3, f3, g1]
		//record394: [a394, b0, c1, d2, e4, f4, g2]
		String[] ConditionColumns3 = {"e","c","g","f","b","d","a"};
		String[] ConditionColumnsValues3 = {"e1","c1","g2","f4","b0","d0","a16"};
		DBApp.createBitMapIndex("z3277","e");
		DBApp.createBitMapIndex("z3277","c");
		DBApp.createBitMapIndex("z3277","f");
		DBApp.createBitMapIndex("z3277","b");
		DBApp.createBitMapIndex("z3277","a");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("z3277", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("i50", cols0);
		String [][] records_i50 = new String[383][cols0.length];
		for(int i=0;i<383;i++)
		{
			records_i50[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i50[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i50", records_i50[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8, k4, l6, m1, n0, o3, p10, q4]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7, m2, n1, o4, p11, q5]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6, l8, m3, n2, o5, p12, q6]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3, o6, p13, q7]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4, o7, p14, q8]
		String[] ConditionColumns0 = {"n","d","l","e","q","g","i","o","b","c","h"};
		String[] ConditionColumnsValues0 = {"n4","d0","l4","e0","q0","g4","i7","o10","b0","c1","h4"};
		DBApp.createBitMapIndex("i50","n");
		DBApp.createBitMapIndex("i50","d");
		DBApp.createBitMapIndex("i50","q");
		DBApp.createBitMapIndex("i50","o");
		DBApp.createBitMapIndex("i50","b");
		DBApp.createBitMapIndex("i50","h");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("i50", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4, q0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a340","b0","c1","d0","e0","f4","g4","h4","i7","j0","k10","l4","m2","n4","o10","p4","q0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tud", cols1);
		String [][] records_tud = new String[155][cols1.length];
		for(int i=0;i<155;i++)
		{
			records_tud[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tud[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tud", records_tud[i]);
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
		String[] ConditionColumns1 = {"e","f","c","d"};
		String[] ConditionColumnsValues1 = {"e3","f5","c2","d3"};
		DBApp.createBitMapIndex("tud","e");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("tud", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2]
		//record1: [a83, b1, c2, d3, e3, f5, g6]
		//record2: [a143, b1, c2, d3, e3, f5, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a143","b1","c2","d3","e3","f5","g3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("o5h6", cols2);
		String [][] records_o5h6 = new String[134][cols2.length];
		for(int i=0;i<134;i++)
		{
			records_o5h6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o5h6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o5h6", records_o5h6[i]);
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
		String[] ConditionColumns2 = {"f","p","k","c","h","j","b","e","m","l","n","d","o"};
		String[] ConditionColumnsValues2 = {"f1","p3","k1","c1","h3","j7","b1","e2","m2","l7","n11","d3","o7"};
		DBApp.createBitMapIndex("o5h6","f");
		DBApp.createBitMapIndex("o5h6","p");
		DBApp.createBitMapIndex("o5h6","c");
		DBApp.createBitMapIndex("o5h6","j");
		DBApp.createBitMapIndex("o5h6","b");
		DBApp.createBitMapIndex("o5h6","e");
		DBApp.createBitMapIndex("o5h6","m");
		DBApp.createBitMapIndex("o5h6","d");
		DBApp.createBitMapIndex("o5h6","o");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("o5h6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7","k1","l7","m2","n11","o7","p3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f2", cols3);
		String [][] records_f2 = new String[410][cols3.length];
		for(int i=0;i<410;i++)
		{
			records_f2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f2", records_f2[i]);
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
		String[] ConditionColumns3 = {"e","g","c"};
		String[] ConditionColumnsValues3 = {"e2","g4","c0"};
		DBApp.createBitMapIndex("f2","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("f2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 3, records:
		//record0: [a102, b0, c0, d2, e2, f0, g4]
		//record1: [a207, b1, c0, d3, e2, f3, g4]
		//record2: [a312, b0, c0, d0, e2, f0, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 3);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a102","b0","c0","d2","e2","f0","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a207","b1","c0","d3","e2","f3","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a312","b0","c0","d0","e2","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f23", cols4);
		String [][] records_f23 = new String[54][cols4.length];
		for(int i=0;i<54;i++)
		{
			records_f23[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_f23[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("f23", records_f23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11]
		String[] ConditionColumns4 = {"g","k","a","j","b","m","i","l","n","c"};
		String[] ConditionColumnsValues4 = {"g3","k10","a10","j0","b0","m10","i1","l10","n10","c1"};
		DBApp.createBitMapIndex("f23","k");
		DBApp.createBitMapIndex("f23","a");
		DBApp.createBitMapIndex("f23","i");
		DBApp.createBitMapIndex("f23","l");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("f23", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p2e8", cols0);
		String [][] records_p2e8 = new String[369][cols0.length];
		for(int i=0;i<369;i++)
		{
			records_p2e8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p2e8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p2e8", records_p2e8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5, m1, n1, o5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6, m2, n2, o6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7, m3, n3, o7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8, m4, n4, o8]
		String[] ConditionColumns0 = {"k","h","l","o","n","b","e","i","j","d","m"};
		String[] ConditionColumnsValues0 = {"k10","h1","l5","o5","n9","b1","e0","i2","j5","d1","m0"};
		DBApp.createBitMapIndex("p2e8","h");
		DBApp.createBitMapIndex("p2e8","l");
		DBApp.createBitMapIndex("p2e8","n");
		DBApp.createBitMapIndex("p2e8","m");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("p2e8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2","h1","i2","j5","k10","l5","m0","n9","o5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("gq", cols1);
		String [][] records_gq = new String[381][cols1.length];
		for(int i=0;i<381;i++)
		{
			records_gq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gq", records_gq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0]
		String[] ConditionColumns1 = {"h","f","c","a","e","g","b"};
		String[] ConditionColumnsValues1 = {"h4","f2","c2","a356","e1","g6","b0"};
		DBApp.createBitMapIndex("gq","h");
		DBApp.createBitMapIndex("gq","e");
		DBApp.createBitMapIndex("gq","g");
		DBApp.createBitMapIndex("gq","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("gq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a356","b0","c2","d0","e1","f2","g6","h4","i5","j6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l2u7", cols2);
		String [][] records_l2u7 = new String[440][cols2.length];
		for(int i=0;i<440;i++)
		{
			records_l2u7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l2u7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l2u7", records_l2u7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		//record438: [a438, b0, c0, d2, e3, f0, g4]
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		String[] ConditionColumns2 = {"a","c","e"};
		String[] ConditionColumnsValues2 = {"a175","c1","e0"};
		DBApp.createBitMapIndex("l2u7","c");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("l2u7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a175, b1, c1, d3, e0, f1, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a175","b1","c1","d3","e0","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tj14", cols3);
		String [][] records_tj14 = new String[101][cols3.length];
		for(int i=0;i<101;i++)
		{
			records_tj14[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tj14[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tj14", records_tj14[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0]
		String[] ConditionColumns3 = {"i","d","a","g","e","f","h","c","b","j"};
		String[] ConditionColumnsValues3 = {"i8","d1","a89","g5","e4","f5","h1","c2","b1","j9"};
		DBApp.createBitMapIndex("tj14","d");
		DBApp.createBitMapIndex("tj14","a");
		DBApp.createBitMapIndex("tj14","g");
		DBApp.createBitMapIndex("tj14","e");
		DBApp.createBitMapIndex("tj14","b");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("tj14", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a89","b1","c2","d1","e4","f5","g5","h1","i8","j9"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q5", cols4);
		String [][] records_q5 = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			records_q5[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q5[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q5", records_q5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7]
		String[] ConditionColumns4 = {"g","c","d"};
		String[] ConditionColumnsValues4 = {"g4","c2","d3"};
		DBApp.createBitMapIndex("q5","d");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("q5", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y2", cols0);
		String [][] records_y2 = new String[253][cols0.length];
		for(int i=0;i<253;i++)
		{
			records_y2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y2", records_y2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10, o8]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11, o9]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12, o10]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0, o12]
		String[] ConditionColumns0 = {"d","h","l","j","f","i","g","n","m","k","c"};
		String[] ConditionColumnsValues0 = {"d2","h6","l10","j8","f4","i4","g0","n0","m4","k7","c1"};
		DBApp.createBitMapIndex("y2","h");
		DBApp.createBitMapIndex("y2","j");
		DBApp.createBitMapIndex("y2","g");
		DBApp.createBitMapIndex("y2","m");
		DBApp.createBitMapIndex("y2","k");
		DBApp.createBitMapIndex("y2","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("y2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a238","b0","c1","d2","e3","f4","g0","h6","i4","j8","k7","l10","m4","n0","o13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i02", cols0);
		String [][] records_i02 = new String[365][cols0.length];
		for(int i=0;i<365;i++)
		{
			records_i02[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i02[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i02", records_i02[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3, m12]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0]
		String[] ConditionColumns0 = {"c","i","g","h","m","e","a","b","j"};
		String[] ConditionColumnsValues0 = {"c2","i2","g3","h2","m4","e0","a290","b0","j0"};
		DBApp.createBitMapIndex("i02","i");
		DBApp.createBitMapIndex("i02","g");
		DBApp.createBitMapIndex("i02","h");
		DBApp.createBitMapIndex("i02","e");
		DBApp.createBitMapIndex("i02","a");
		DBApp.createBitMapIndex("i02","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("i02", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a290","b0","c2","d2","e0","f2","g3","h2","i2","j0","k4","l2","m4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i88i", cols1);
		String [][] records_i88i = new String[401][cols1.length];
		for(int i=0;i<401;i++)
		{
			records_i88i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i88i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i88i", records_i88i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0]
		String[] ConditionColumns1 = {"e","j","a","c","h","b"};
		String[] ConditionColumnsValues1 = {"e0","j0","a120","c0","h0","b0"};
		DBApp.createBitMapIndex("i88i","e");
		DBApp.createBitMapIndex("i88i","j");
		DBApp.createBitMapIndex("i88i","a");
		DBApp.createBitMapIndex("i88i","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("i88i", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a120","b0","c0","d0","e0","f0","g1","h0","i3","j0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n49", cols2);
		String [][] records_n49 = new String[438][cols2.length];
		for(int i=0;i<438;i++)
		{
			records_n49[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n49[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n49", records_n49[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		String[] ConditionColumns2 = {"a","e","f","d","b","c"};
		String[] ConditionColumnsValues2 = {"a322","e2","f4","d2","b0","c1"};
		DBApp.createBitMapIndex("n49","e");
		DBApp.createBitMapIndex("n49","d");
		DBApp.createBitMapIndex("n49","c");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("n49", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a322, b0, c1, d2, e2, f4, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a322","b0","c1","d2","e2","f4","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("bjz25", cols0);
		String [][] records_bjz25 = new String[474][cols0.length];
		for(int i=0;i<474;i++)
		{
			records_bjz25[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bjz25[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bjz25", records_bjz25[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4, p5, q10, r1, s13, t9]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5, p6, q11, r2, s14, t10]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6, p7, q12, r3, s15, t11]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7, p8, q13, r4, s16, t12]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8, p9, q14, r5, s17, t13]
		String[] ConditionColumns0 = {"s","n","g","f","b","j","m","k","a","c","q","t"};
		String[] ConditionColumnsValues0 = {"s1","n12","g5","f2","b0","j2","m11","k10","a362","c2","q5","t2"};
		DBApp.createBitMapIndex("bjz25","s");
		DBApp.createBitMapIndex("bjz25","g");
		DBApp.createBitMapIndex("bjz25","f");
		DBApp.createBitMapIndex("bjz25","b");
		DBApp.createBitMapIndex("bjz25","k");
		DBApp.createBitMapIndex("bjz25","a");
		DBApp.createBitMapIndex("bjz25","q");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("bjz25", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11, n12, o2, p10, q5, r2, s1, t2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a362","b0","c2","d2","e2","f2","g5","h2","i2","j2","k10","l2","m11","n12","o2","p10","q5","r2","s1","t2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("nq60", cols1);
		String [][] records_nq60 = new String[220][cols1.length];
		for(int i=0;i<220;i++)
		{
			records_nq60[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nq60[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nq60", records_nq60[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9]
		String[] ConditionColumns1 = {"b","a","e","h","n","f","i","m","k","j"};
		String[] ConditionColumnsValues1 = {"b1","a67","e2","h3","n11","f1","i4","m2","k1","j7"};
		DBApp.createBitMapIndex("nq60","b");
		DBApp.createBitMapIndex("nq60","e");
		DBApp.createBitMapIndex("nq60","h");
		DBApp.createBitMapIndex("nq60","f");
		DBApp.createBitMapIndex("nq60","i");
		DBApp.createBitMapIndex("nq60","k");
		DBApp.createBitMapIndex("nq60","j");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("nq60", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4","h3","i4","j7","k1","l7","m2","n11"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m2095", cols2);
		String [][] records_m2095 = new String[365][cols2.length];
		for(int i=0;i<365;i++)
		{
			records_m2095[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m2095[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m2095", records_m2095[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record360: [a360, b0, c0, d0, e0, f0, g3]
		//record361: [a361, b1, c1, d1, e1, f1, g4]
		//record362: [a362, b0, c2, d2, e2, f2, g5]
		//record363: [a363, b1, c0, d3, e3, f3, g6]
		//record364: [a364, b0, c1, d0, e4, f4, g0]
		String[] ConditionColumns2 = {"e","c","g"};
		String[] ConditionColumnsValues2 = {"e0","c0","g2"};
		DBApp.createBitMapIndex("m2095","e");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("m2095", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a30, b0, c0, d2, e0, f0, g2]
		//record1: [a135, b1, c0, d3, e0, f3, g2]
		//record2: [a240, b0, c0, d0, e0, f0, g2]
		//record3: [a345, b1, c0, d1, e0, f3, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 4);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a135","b1","c0","d3","e0","f3","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a240","b0","c0","d0","e0","f0","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a345","b1","c0","d1","e0","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("zkdre", cols3);
		String [][] records_zkdre = new String[357][cols3.length];
		for(int i=0;i<357;i++)
		{
			records_zkdre[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zkdre[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zkdre", records_zkdre[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5]
		String[] ConditionColumns3 = {"g","f","b","h","d"};
		String[] ConditionColumnsValues3 = {"g6","f5","b1","h1","d1"};
		DBApp.createBitMapIndex("zkdre","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("zkdre", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		//record1: [a209, b1, c2, d1, e4, f5, g6, h1, i2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 2);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a209","b1","c2","d1","e4","f5","g6","h1","i2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oe", cols0);
		String [][] records_oe = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_oe[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oe[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oe", records_oe[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		String[] ConditionColumns0 = {"d","g","a"};
		String[] ConditionColumnsValues0 = {"d1","g5","a5"};
		DBApp.createBitMapIndex("oe","d");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("oe", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f0", cols0);
		String [][] records_f0 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			records_f0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f0", records_f0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6]
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10]
		String[] ConditionColumns0 = {"f","c","k","j","d","a","i"};
		String[] ConditionColumnsValues0 = {"f4","c1","k7","j0","d0","a40","i4"};
		DBApp.createBitMapIndex("f0","f");
		DBApp.createBitMapIndex("f0","k");
		DBApp.createBitMapIndex("f0","j");
		DBApp.createBitMapIndex("f0","a");
		DBApp.createBitMapIndex("f0","i");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("f0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dl", cols1);
		String [][] records_dl = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			records_dl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dl", records_dl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		//record89: [a89, b1, c2, d1, e4, f5, g5]
		//record90: [a90, b0, c0, d2, e0, f0, g6]
		String[] ConditionColumns1 = {"d","a","e"};
		String[] ConditionColumnsValues1 = {"d3","a51","e1"};
		DBApp.createBitMapIndex("dl","e");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("dl", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a51, b1, c0, d3, e1, f3, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("t7y42", cols2);
		String [][] records_t7y42 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			records_t7y42[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_t7y42[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("t7y42", records_t7y42[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10, o5, p0, q12]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6, p1, q13]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7, p2, q14]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3, q15]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0, o9, p4, q16]
		String[] ConditionColumns2 = {"l","m","e","n","h","f","o","p"};
		String[] ConditionColumnsValues2 = {"l2","m0","e1","n12","h2","f2","o11","p10"};
		DBApp.createBitMapIndex("t7y42","l");
		DBApp.createBitMapIndex("t7y42","m");
		DBApp.createBitMapIndex("t7y42","e");
		DBApp.createBitMapIndex("t7y42","o");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("t7y42", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12","o11","p10","q9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ri4i", cols0);
		String [][] records_ri4i = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			records_ri4i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ri4i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ri4i", records_ri4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3]
		String[] ConditionColumns0 = {"j","c","b","h","m","n","k","g","l","i","d","f","e"};
		String[] ConditionColumnsValues0 = {"j8","c2","b0","h6","m12","n10","k5","g3","l2","i2","d2","f2","e3"};
		DBApp.createBitMapIndex("ri4i","j");
		DBApp.createBitMapIndex("ri4i","b");
		DBApp.createBitMapIndex("ri4i","m");
		DBApp.createBitMapIndex("ri4i","l");
		DBApp.createBitMapIndex("ri4i","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ri4i", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d4o1", cols0);
		String [][] records_d4o1 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_d4o1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d4o1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d4o1", records_d4o1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		String[] ConditionColumns0 = {"c","d","f"};
		String[] ConditionColumnsValues0 = {"c2","d2","f2"};
		DBApp.createBitMapIndex("d4o1","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("d4o1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a14, b0, c2, d2, e4, f2, g0]
		//record2: [a26, b0, c2, d2, e1, f2, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 3);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vk", cols1);
		String [][] records_vk = new String[129][cols1.length];
		for(int i=0;i<129;i++)
		{
			records_vk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vk", records_vk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0]
		String[] ConditionColumns1 = {"g","e","f","h","d","c","b"};
		String[] ConditionColumnsValues1 = {"g2","e0","f0","h6","d2","c0","b0"};
		DBApp.createBitMapIndex("vk","g");
		DBApp.createBitMapIndex("vk","f");
		DBApp.createBitMapIndex("vk","d");
		DBApp.createBitMapIndex("vk","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vk", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a30, b0, c0, d2, e0, f0, g2, h6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o58", cols2);
		String [][] records_o58 = new String[429][cols2.length];
		for(int i=0;i<429;i++)
		{
			records_o58[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o58[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o58", records_o58[i]);
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
		String[] ConditionColumns2 = {"e","c","g","a"};
		String[] ConditionColumnsValues2 = {"e4","c0","g5","a369"};
		DBApp.createBitMapIndex("o58","a");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("o58", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a369, b1, c0, d1, e4, f3, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a369","b1","c0","d1","e4","f3","g5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fa9r2", cols3);
		String [][] records_fa9r2 = new String[480][cols3.length];
		for(int i=0;i<480;i++)
		{
			records_fa9r2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_fa9r2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("fa9r2", records_fa9r2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2]
		String[] ConditionColumns3 = {"h","d","a","c","f"};
		String[] ConditionColumnsValues3 = {"h5","d1","a69","c0","f3"};
		DBApp.createBitMapIndex("fa9r2","h");
		DBApp.createBitMapIndex("fa9r2","d");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("fa9r2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5","i6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qf72", cols0);
		String [][] records_qf72 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			records_qf72[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qf72[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qf72", records_qf72[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10, o4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8]
		String[] ConditionColumns0 = {"n","k","g","j","e","b","f","o"};
		String[] ConditionColumnsValues0 = {"n9","k10","g2","j5","e0","b1","f5","o5"};
		DBApp.createBitMapIndex("qf72","g");
		DBApp.createBitMapIndex("qf72","o");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qf72", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2","h1","i2","j5","k10","l5","m0","n9","o5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x6", cols1);
		String [][] records_x6 = new String[138][cols1.length];
		for(int i=0;i<138;i++)
		{
			records_x6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x6", records_x6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record133: [a133, b1, c1, d1, e3, f1, g0]
		//record134: [a134, b0, c2, d2, e4, f2, g1]
		//record135: [a135, b1, c0, d3, e0, f3, g2]
		//record136: [a136, b0, c1, d0, e1, f4, g3]
		//record137: [a137, b1, c2, d1, e2, f5, g4]
		String[] ConditionColumns1 = {"g","a","c","e","d"};
		String[] ConditionColumnsValues1 = {"g6","a111","c0","e1","d3"};
		DBApp.createBitMapIndex("x6","g");
		DBApp.createBitMapIndex("x6","c");
		DBApp.createBitMapIndex("x6","d");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("x6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a111, b1, c0, d3, e1, f3, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a111","b1","c0","d3","e1","f3","g6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ygf", cols2);
		String [][] records_ygf = new String[354][cols2.length];
		for(int i=0;i<354;i++)
		{
			records_ygf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ygf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ygf", records_ygf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2]
		String[] ConditionColumns2 = {"m","l","c","i"};
		String[] ConditionColumnsValues2 = {"m3","l11","c2","i2"};
		DBApp.createBitMapIndex("ygf","c");
		DBApp.createBitMapIndex("ygf","i");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("ygf", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11, m3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a263","b1","c2","d3","e3","f5","g4","h7","i2","j3","k10","l11","m3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("iqbi", cols3);
		String [][] records_iqbi = new String[332][cols3.length];
		for(int i=0;i<332;i++)
		{
			records_iqbi[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_iqbi[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("iqbi", records_iqbi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		//record330: [a330, b0, c0, d2, e0, f0, g1]
		//record331: [a331, b1, c1, d3, e1, f1, g2]
		String[] ConditionColumns3 = {"g","c","b"};
		String[] ConditionColumnsValues3 = {"g0","c0","b1"};
		DBApp.createBitMapIndex("iqbi","g");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("iqbi", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 8, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0]
		//record1: [a63, b1, c0, d3, e3, f3, g0]
		//record2: [a105, b1, c0, d1, e0, f3, g0]
		//record3: [a147, b1, c0, d3, e2, f3, g0]
		//record4: [a189, b1, c0, d1, e4, f3, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 8);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a105","b1","c0","d1","e0","f3","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a147","b1","c0","d3","e2","f3","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a189","b1","c0","d1","e4","f3","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nbzb", cols0);
		String [][] records_nbzb = new String[459][cols0.length];
		for(int i=0;i<459;i++)
		{
			records_nbzb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nbzb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nbzb", records_nbzb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record454: [a454, b0, c1, d2, e4, f4, g6]
		//record455: [a455, b1, c2, d3, e0, f5, g0]
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		//record458: [a458, b0, c2, d2, e3, f2, g3]
		String[] ConditionColumns0 = {"c","f","a"};
		String[] ConditionColumnsValues0 = {"c1","f4","a334"};
		DBApp.createBitMapIndex("nbzb","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("nbzb", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a334, b0, c1, d2, e4, f4, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a334","b0","c1","d2","e4","f4","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zf8a", cols0);
		String [][] records_zf8a = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			records_zf8a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zf8a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zf8a", records_zf8a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		String[] ConditionColumns0 = {"b","f","a"};
		String[] ConditionColumnsValues0 = {"b1","f1","a1"};
		DBApp.createBitMapIndex("zf8a","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("zf8a", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eqq7p", cols1);
		String [][] records_eqq7p = new String[163][cols1.length];
		for(int i=0;i<163;i++)
		{
			records_eqq7p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_eqq7p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("eqq7p", records_eqq7p[i]);
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
		String[] ConditionColumns1 = {"f","b","c","e","d"};
		String[] ConditionColumnsValues1 = {"f1","b1","c1","e3","d1"};
		DBApp.createBitMapIndex("eqq7p","f");
		DBApp.createBitMapIndex("eqq7p","c");
		DBApp.createBitMapIndex("eqq7p","e");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("eqq7p", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6]
		//record1: [a73, b1, c1, d1, e3, f1, g3]
		//record2: [a133, b1, c1, d1, e3, f1, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a73","b1","c1","d1","e3","f1","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a133","b1","c1","d1","e3","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vnjf9", cols2);
		String [][] records_vnjf9 = new String[238][cols2.length];
		for(int i=0;i<238;i++)
		{
			records_vnjf9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vnjf9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vnjf9", records_vnjf9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9, q12, r17, s5, t13]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10, o9, p10, q13, r0, s6, t14]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10, p11, q14, r1, s7, t15]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11, p12, q15, r2, s8, t16]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13, o12, p13, q16, r3, s9, t17]
		String[] ConditionColumns2 = {"k","q","i","c","t"};
		String[] ConditionColumnsValues2 = {"k3","q5","i7","c1","t4"};
		DBApp.createBitMapIndex("vnjf9","k");
		DBApp.createBitMapIndex("vnjf9","q");
		DBApp.createBitMapIndex("vnjf9","c");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("vnjf9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4, p12, q5, r16, s10, t4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a124","b0","c1","d0","e4","f4","g5","h4","i7","j4","k3","l4","m7","n12","o4","p12","q5","r16","s10","t4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z4i", cols3);
		String [][] records_z4i = new String[381][cols3.length];
		for(int i=0;i<381;i++)
		{
			records_z4i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z4i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z4i", records_z4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8, k4]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6]
		String[] ConditionColumns3 = {"h","g","c","k"};
		String[] ConditionColumnsValues3 = {"h0","g0","c0","k6"};
		DBApp.createBitMapIndex("z4i","h");
		DBApp.createBitMapIndex("z4i","c");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("z4i", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a336","b0","c0","d0","e1","f0","g0","h0","i3","j6","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qs0", cols0);
		String [][] records_qs0 = new String[244][cols0.length];
		for(int i=0;i<244;i++)
		{
			records_qs0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qs0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qs0", records_qs0[i]);
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
		String[] ConditionColumns0 = {"c","a","h"};
		String[] ConditionColumnsValues0 = {"c0","a69","h5"};
		DBApp.createBitMapIndex("qs0","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qs0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a69","b1","c0","d1","e4","f3","g6","h5","i6","j9","k3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ern", cols1);
		String [][] records_ern = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			records_ern[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ern[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ern", records_ern[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9]
		String[] ConditionColumns1 = {"c","b","g"};
		String[] ConditionColumnsValues1 = {"c1","b0","g2"};
		DBApp.createBitMapIndex("ern","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ern", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record1: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a58","b0","c1","d2","e3","f4","g2","h2","i4","j8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mu4bi", cols2);
		String [][] records_mu4bi = new String[364][cols2.length];
		for(int i=0;i<364;i++)
		{
			records_mu4bi[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mu4bi[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mu4bi", records_mu4bi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3]
		String[] ConditionColumns2 = {"b","c","f","d"};
		String[] ConditionColumnsValues2 = {"b0","c1","f4","d2"};
		DBApp.createBitMapIndex("mu4bi","c");
		DBApp.createBitMapIndex("mu4bi","d");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("mu4bi", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 30, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record1: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record2: [a34, b0, c1, d2, e4, f4, g6, h2]
		//record3: [a46, b0, c1, d2, e1, f4, g4, h6]
		//record4: [a58, b0, c1, d2, e3, f4, g2, h2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 30);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a58","b0","c1","d2","e3","f4","g2","h2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("l1", cols3);
		String [][] records_l1 = new String[249][cols3.length];
		for(int i=0;i<249;i++)
		{
			records_l1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l1", records_l1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0]
		String[] ConditionColumns3 = {"h","e","g"};
		String[] ConditionColumnsValues3 = {"h2","e3","g0"};
		DBApp.createBitMapIndex("l1","h");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("l1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a98, b0, c2, d2, e3, f2, g0, h2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a98","b0","c2","d2","e3","f2","g0","h2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d8f", cols0);
		String [][] records_d8f = new String[220][cols0.length];
		for(int i=0;i<220;i++)
		{
			records_d8f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d8f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d8f", records_d8f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		//record216: [a216, b0, c0, d0, e1, f0, g6]
		//record217: [a217, b1, c1, d1, e2, f1, g0]
		//record218: [a218, b0, c2, d2, e3, f2, g1]
		//record219: [a219, b1, c0, d3, e4, f3, g2]
		String[] ConditionColumns0 = {"g","c","e"};
		String[] ConditionColumnsValues0 = {"g0","c1","e0"};
		DBApp.createBitMapIndex("d8f","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("d8f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a70, b0, c1, d2, e0, f4, g0]
		//record1: [a175, b1, c1, d3, e0, f1, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a175","b1","c1","d3","e0","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n6", cols1);
		String [][] records_n6 = new String[240][cols1.length];
		for(int i=0;i<240;i++)
		{
			records_n6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n6", records_n6[i]);
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
		String[] ConditionColumns1 = {"b","g","c","f","e","d"};
		String[] ConditionColumnsValues1 = {"b1","g4","c1","f1","e1","d3"};
		DBApp.createBitMapIndex("n6","b");
		DBApp.createBitMapIndex("n6","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("n6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a151, b1, c1, d3, e1, f1, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a151","b1","c1","d3","e1","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("mw8v", cols2);
		String [][] records_mw8v = new String[140][cols2.length];
		for(int i=0;i<140;i++)
		{
			records_mw8v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mw8v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mw8v", records_mw8v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5, n9, o0, p7, q16]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3]
		String[] ConditionColumns2 = {"q","m","g","j","o","h","k","a","l","e","b","c","p","n","f"};
		String[] ConditionColumnsValues2 = {"q3","m9","g6","j9","o4","h3","k7","a139","l7","e4","b1","c1","p11","n13","f1"};
		DBApp.createBitMapIndex("mw8v","q");
		DBApp.createBitMapIndex("mw8v","m");
		DBApp.createBitMapIndex("mw8v","g");
		DBApp.createBitMapIndex("mw8v","j");
		DBApp.createBitMapIndex("mw8v","o");
		DBApp.createBitMapIndex("mw8v","k");
		DBApp.createBitMapIndex("mw8v","l");
		DBApp.createBitMapIndex("mw8v","e");
		DBApp.createBitMapIndex("mw8v","b");
		DBApp.createBitMapIndex("mw8v","c");
		DBApp.createBitMapIndex("mw8v","n");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("mw8v", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a139","b1","c1","d3","e4","f1","g6","h3","i4","j9","k7","l7","m9","n13","o4","p11","q3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("j0jzs", cols3);
		String [][] records_j0jzs = new String[114][cols3.length];
		for(int i=0;i<114;i++)
		{
			records_j0jzs[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j0jzs[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j0jzs", records_j0jzs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9, n1]
		String[] ConditionColumns3 = {"h","j","g","d","m","c","k"};
		String[] ConditionColumnsValues3 = {"h7","j5","g6","d3","m3","c1","k0"};
		DBApp.createBitMapIndex("j0jzs","j");
		DBApp.createBitMapIndex("j0jzs","m");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("j0jzs", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3","n13"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("th", cols4);
		String [][] records_th = new String[118][cols4.length];
		for(int i=0;i<118;i++)
		{
			records_th[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_th[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("th", records_th[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record113: [a113, b1, c2, d1, e3, f5, g1]
		//record114: [a114, b0, c0, d2, e4, f0, g2]
		//record115: [a115, b1, c1, d3, e0, f1, g3]
		//record116: [a116, b0, c2, d0, e1, f2, g4]
		//record117: [a117, b1, c0, d1, e2, f3, g5]
		String[] ConditionColumns4 = {"f","g","b","e"};
		String[] ConditionColumnsValues4 = {"f1","g1","b1","e1"};
		DBApp.createBitMapIndex("th","f");
		DBApp.createBitMapIndex("th","e");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("th", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ai7qq", cols0);
		String [][] records_ai7qq = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			records_ai7qq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ai7qq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ai7qq", records_ai7qq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8]
		String[] ConditionColumns0 = {"l","k","g","i","b"};
		String[] ConditionColumnsValues0 = {"l4","k5","g2","i7","b0"};
		DBApp.createBitMapIndex("ai7qq","l");
		DBApp.createBitMapIndex("ai7qq","g");
		DBApp.createBitMapIndex("ai7qq","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ai7qq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("e5s92", cols1);
		String [][] records_e5s92 = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_e5s92[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e5s92[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e5s92", records_e5s92[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		String[] ConditionColumns1 = {"e","c","a","j","h"};
		String[] ConditionColumnsValues1 = {"e2","c0","a12","j2","h4"};
		DBApp.createBitMapIndex("e5s92","e");
		DBApp.createBitMapIndex("e5s92","h");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("e5s92", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mjo", cols2);
		String [][] records_mjo = new String[154][cols2.length];
		for(int i=0;i<154;i++)
		{
			records_mjo[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mjo[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mjo", records_mjo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8, l7, m8]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9, m10]
		String[] ConditionColumns2 = {"m","l","d"};
		String[] ConditionColumnsValues2 = {"m3","l6","d2"};
		DBApp.createBitMapIndex("mjo","l");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("mjo", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ry3", cols3);
		String [][] records_ry3 = new String[405][cols3.length];
		for(int i=0;i<405;i++)
		{
			records_ry3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ry3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ry3", records_ry3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8, o10, p0, q9, r4, s1, t0]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9, o11, p1, q10, r5, s2, t1]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12, n10, o12, p2, q11, r6, s3, t2]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13, p3, q12, r7, s4, t3]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12, o14, p4, q13, r8, s5, t4]
		String[] ConditionColumns3 = {"g","s","e","d","p","o","a","j","t","m","n","c","f","q","h","l","r","k","b"};
		String[] ConditionColumnsValues3 = {"g5","s7","e2","d0","p4","o7","a292","j2","t12","m6","n12","c1","f4","q3","h4","l4","r4","k6","b0"};
		DBApp.createBitMapIndex("ry3","g");
		DBApp.createBitMapIndex("ry3","s");
		DBApp.createBitMapIndex("ry3","e");
		DBApp.createBitMapIndex("ry3","d");
		DBApp.createBitMapIndex("ry3","p");
		DBApp.createBitMapIndex("ry3","o");
		DBApp.createBitMapIndex("ry3","t");
		DBApp.createBitMapIndex("ry3","n");
		DBApp.createBitMapIndex("ry3","c");
		DBApp.createBitMapIndex("ry3","f");
		DBApp.createBitMapIndex("ry3","h");
		DBApp.createBitMapIndex("ry3","l");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("ry3", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12, o7, p4, q3, r4, s7, t12]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a292","b0","c1","d0","e2","f4","g5","h4","i4","j2","k6","l4","m6","n12","o7","p4","q3","r4","s7","t12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("lmav4", cols0);
		String [][] records_lmav4 = new String[98][cols0.length];
		for(int i=0;i<98;i++)
		{
			records_lmav4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lmav4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lmav4", records_lmav4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13]
		String[] ConditionColumns0 = {"a","i","h","n","e"};
		String[] ConditionColumnsValues0 = {"a52","i7","h4","n10","e2"};
		DBApp.createBitMapIndex("lmav4","h");
		DBApp.createBitMapIndex("lmav4","n");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("lmav4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nih6m", cols1);
		String [][] records_nih6m = new String[264][cols1.length];
		for(int i=0;i<264;i++)
		{
			records_nih6m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nih6m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nih6m", records_nih6m[i]);
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
		String[] ConditionColumns1 = {"f","b","d","g","c","e","a"};
		String[] ConditionColumnsValues1 = {"f1","b1","d1","g0","c1","e4","a49"};
		DBApp.createBitMapIndex("nih6m","f");
		DBApp.createBitMapIndex("nih6m","b");
		DBApp.createBitMapIndex("nih6m","d");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("nih6m", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a49, b1, c1, d1, e4, f1, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("or", cols2);
		String [][] records_or = new String[268][cols2.length];
		for(int i=0;i<268;i++)
		{
			records_or[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_or[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("or", records_or[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11, m3, n11, o8, p7, q8, r11, s16, t3]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9, p8, q9, r12, s17, t4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10, p9, q10, r13, s18, t5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11, p10, q11, r14, s0, t6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12, p11, q12, r15, s1, t7]
		String[] ConditionColumns2 = {"t","f","b","a","r","m","k","l","o","g","c","i","n","h","s","p","j","d","e","q"};
		String[] ConditionColumnsValues2 = {"t1","f3","b1","a141","r15","m11","k9","l9","o6","g1","c0","i6","n1","h5","s8","p13","j1","d1","e1","q5"};
		DBApp.createBitMapIndex("or","f");
		DBApp.createBitMapIndex("or","b");
		DBApp.createBitMapIndex("or","a");
		DBApp.createBitMapIndex("or","r");
		DBApp.createBitMapIndex("or","m");
		DBApp.createBitMapIndex("or","k");
		DBApp.createBitMapIndex("or","g");
		DBApp.createBitMapIndex("or","s");
		DBApp.createBitMapIndex("or","p");
		DBApp.createBitMapIndex("or","q");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("or", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1, o6, p13, q5, r15, s8, t1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a141","b1","c0","d1","e1","f3","g1","h5","i6","j1","k9","l9","m11","n1","o6","p13","q5","r15","s8","t1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j6i", cols3);
		String [][] records_j6i = new String[380][cols3.length];
		for(int i=0;i<380;i++)
		{
			records_j6i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_j6i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("j6i", records_j6i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0, j8]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9]
		String[] ConditionColumns3 = {"d","j","f"};
		String[] ConditionColumnsValues3 = {"d2","j8","f2"};
		DBApp.createBitMapIndex("j6i","d");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("j6i", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 6, records:
		//record0: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		//record1: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8]
		//record2: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8]
		//record3: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8]
		//record4: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 6);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a98","b0","c2","d2","e3","f2","g0","h2","i8","j8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a158","b0","c2","d2","e3","f2","g4","h6","i5","j8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a218","b0","c2","d2","e3","f2","g1","h2","i2","j8"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a278","b0","c2","d2","e3","f2","g5","h6","i8","j8"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hvv6", cols4);
		String [][] records_hvv6 = new String[218][cols4.length];
		for(int i=0;i<218;i++)
		{
			records_hvv6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_hvv6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("hvv6", records_hvv6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record213: [a213, b1, c0, d1, e3, f3, g3]
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		//record216: [a216, b0, c0, d0, e1, f0, g6]
		//record217: [a217, b1, c1, d1, e2, f1, g0]
		String[] ConditionColumns4 = {"a","e","f","g","c","b","d"};
		String[] ConditionColumnsValues4 = {"a27","e2","f3","g6","c0","b1","d3"};
		DBApp.createBitMapIndex("hvv6","a");
		DBApp.createBitMapIndex("hvv6","e");
		DBApp.createBitMapIndex("hvv6","g");
		DBApp.createBitMapIndex("hvv6","b");
		DBApp.createBitMapIndex("hvv6","d");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("hvv6", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h4c0", cols0);
		String [][] records_h4c0 = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_h4c0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h4c0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h4c0", records_h4c0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		String[] ConditionColumns0 = {"b","g","c","a","f","e","d"};
		String[] ConditionColumnsValues0 = {"b0","g0","c1","a28","f4","e3","d0"};
		DBApp.createBitMapIndex("h4c0","c");
		DBApp.createBitMapIndex("h4c0","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("h4c0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("oi408", cols0);
		String [][] records_oi408 = new String[130][cols0.length];
		for(int i=0;i<130;i++)
		{
			records_oi408[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oi408[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oi408", records_oi408[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3]
		String[] ConditionColumns0 = {"b","i","e","h","n","f","g","j","c"};
		String[] ConditionColumnsValues0 = {"b1","i5","e3","h7","n9","f5","g2","j3","c2"};
		DBApp.createBitMapIndex("oi408","i");
		DBApp.createBitMapIndex("oi408","h");
		DBApp.createBitMapIndex("oi408","n");
		DBApp.createBitMapIndex("oi408","f");
		DBApp.createBitMapIndex("oi408","j");
		DBApp.createBitMapIndex("oi408","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("oi408", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("xmg", cols1);
		String [][] records_xmg = new String[109][cols1.length];
		for(int i=0;i<109;i++)
		{
			records_xmg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xmg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xmg", records_xmg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14, p8, q2]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9, m1, n7, o0, p9, q3]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10, m2, n8, o1, p10, q4]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11, q5]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12, q6]
		String[] ConditionColumns1 = {"m","f","n","p","o","l","q","a"};
		String[] ConditionColumnsValues1 = {"m8","f2","n2","p6","o11","l2","q1","a86"};
		DBApp.createBitMapIndex("xmg","p");
		DBApp.createBitMapIndex("xmg","q");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("xmg", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2","h6","i5","j6","k9","l2","m8","n2","o11","p6","q1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k0", cols0);
		String [][] records_k0 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_k0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k0", records_k0[i]);
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
		String[] ConditionColumns0 = {"e","g","f","c","a","d"};
		String[] ConditionColumnsValues0 = {"e0","g0","f5","c2","a35","d3"};
		DBApp.createBitMapIndex("k0","c");
		DBApp.createBitMapIndex("k0","d");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("k0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ez", cols1);
		String [][] records_ez = new String[442][cols1.length];
		for(int i=0;i<442;i++)
		{
			records_ez[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ez[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ez", records_ez[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		//record438: [a438, b0, c0, d2, e3, f0, g4]
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		//record441: [a441, b1, c0, d1, e1, f3, g0]
		String[] ConditionColumns1 = {"f","b","c","g","e","d"};
		String[] ConditionColumnsValues1 = {"f0","b0","c0","g0","e4","d2"};
		DBApp.createBitMapIndex("ez","f");
		DBApp.createBitMapIndex("ez","c");
		DBApp.createBitMapIndex("ez","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ez", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a294, b0, c0, d2, e4, f0, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a294","b0","c0","d2","e4","f0","g0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ht8", cols2);
		String [][] records_ht8 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			records_ht8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ht8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ht8", records_ht8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record80: [a80, b0, c2, d0, e0, f2, g3]
		//record81: [a81, b1, c0, d1, e1, f3, g4]
		//record82: [a82, b0, c1, d2, e2, f4, g5]
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		String[] ConditionColumns2 = {"d","a","e","b"};
		String[] ConditionColumnsValues2 = {"d3","a83","e3","b1"};
		DBApp.createBitMapIndex("ht8","d");
		DBApp.createBitMapIndex("ht8","e");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("ht8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a83, b1, c2, d3, e3, f5, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("c3w1", cols3);
		String [][] records_c3w1 = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			records_c3w1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c3w1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c3w1", records_c3w1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4]
		String[] ConditionColumns3 = {"s","n","h"};
		String[] ConditionColumnsValues3 = {"s17","n8","h4"};
		DBApp.createBitMapIndex("c3w1","n");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("c3w1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8","o6","p4","q2","r0","s17"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o53", cols4);
		String [][] records_o53 = new String[241][cols4.length];
		for(int i=0;i<241;i++)
		{
			records_o53[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o53[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o53", records_o53[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record236: [a236, b0, c2, d0, e1, f2, g5]
		//record237: [a237, b1, c0, d1, e2, f3, g6]
		//record238: [a238, b0, c1, d2, e3, f4, g0]
		//record239: [a239, b1, c2, d3, e4, f5, g1]
		//record240: [a240, b0, c0, d0, e0, f0, g2]
		String[] ConditionColumns4 = {"c","f","d"};
		String[] ConditionColumnsValues4 = {"c0","f0","d0"};
		DBApp.createBitMapIndex("o53","f");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("o53", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 21, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a12, b0, c0, d0, e2, f0, g5]
		//record2: [a24, b0, c0, d0, e4, f0, g3]
		//record3: [a36, b0, c0, d0, e1, f0, g1]
		//record4: [a48, b0, c0, d0, e3, f0, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 21);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fq5n6", cols0);
		String [][] records_fq5n6 = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			records_fq5n6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fq5n6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fq5n6", records_fq5n6[i]);
		}
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		String[] ConditionColumns0 = {"e","a","g","f"};
		String[] ConditionColumnsValues0 = {"e2","a2","g2","f2"};
		DBApp.createBitMapIndex("fq5n6","a");
		DBApp.createBitMapIndex("fq5n6","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("fq5n6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yfx", cols1);
		String [][] records_yfx = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_yfx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yfx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yfx", records_yfx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		String[] ConditionColumns1 = {"e","a","b","d","f","c"};
		String[] ConditionColumnsValues1 = {"e0","a15","b1","d3","f3","c0"};
		DBApp.createBitMapIndex("yfx","b");
		DBApp.createBitMapIndex("yfx","d");
		DBApp.createBitMapIndex("yfx","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("yfx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hyba", cols0);
		String [][] records_hyba = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_hyba[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hyba[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hyba", records_hyba[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		String[] ConditionColumns0 = {"a","g","f"};
		String[] ConditionColumnsValues0 = {"a6","g6","f0"};
		DBApp.createBitMapIndex("hyba","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("hyba", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("r69", cols1);
		String [][] records_r69 = new String[437][cols1.length];
		for(int i=0;i<437;i++)
		{
			records_r69[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r69[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r69", records_r69[i]);
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
		String[] ConditionColumns1 = {"d","k","l","h","g","f","o"};
		String[] ConditionColumnsValues1 = {"d2","k9","l6","h2","g5","f0","o6"};
		DBApp.createBitMapIndex("r69","k");
		DBApp.createBitMapIndex("r69","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("r69", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7, n12, o6, p2, q0, r0, s2, t6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a306","b0","c0","d2","e1","f0","g5","h2","i0","j6","k9","l6","m7","n12","o6","p2","q0","r0","s2","t6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("zjm82", cols0);
		String [][] records_zjm82 = new String[473][cols0.length];
		for(int i=0;i<473;i++)
		{
			records_zjm82[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zjm82[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zjm82", records_zjm82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6, o3]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7]
		String[] ConditionColumns0 = {"g","f","j"};
		String[] ConditionColumnsValues0 = {"g3","f5","j1"};
		DBApp.createBitMapIndex("zjm82","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("zjm82", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11]
		//record1: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12, n3, o11]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a101","b1","c2","d1","e1","f5","g3","h5","i2","j1","k2","l5","m10","n3","o11"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a311","b1","c2","d3","e1","f5","g3","h7","i5","j1","k3","l11","m12","n3","o11"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bt9", cols0);
		String [][] records_bt9 = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_bt9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bt9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bt9", records_bt9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2]
		//record136: [a136, b0, c1, d0, e1, f4, g3]
		//record137: [a137, b1, c2, d1, e2, f5, g4]
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		String[] ConditionColumns0 = {"f","d","b"};
		String[] ConditionColumnsValues0 = {"f2","d0","b0"};
		DBApp.createBitMapIndex("bt9","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("bt9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1]
		//record1: [a20, b0, c2, d0, e0, f2, g6]
		//record2: [a32, b0, c2, d0, e2, f2, g4]
		//record3: [a44, b0, c2, d0, e4, f2, g2]
		//record4: [a56, b0, c2, d0, e1, f2, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 11);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ymmrt", cols1);
		String [][] records_ymmrt = new String[486][cols1.length];
		for(int i=0;i<486;i++)
		{
			records_ymmrt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ymmrt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ymmrt", records_ymmrt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record481: [a481, b1, c1, d1, e1, f1, g5]
		//record482: [a482, b0, c2, d2, e2, f2, g6]
		//record483: [a483, b1, c0, d3, e3, f3, g0]
		//record484: [a484, b0, c1, d0, e4, f4, g1]
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		String[] ConditionColumns1 = {"e","c","d","g","b"};
		String[] ConditionColumnsValues1 = {"e1","c1","d3","g4","b1"};
		DBApp.createBitMapIndex("ymmrt","d");
		DBApp.createBitMapIndex("ymmrt","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ymmrt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a151, b1, c1, d3, e1, f1, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a151","b1","c1","d3","e1","f1","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wyv", cols2);
		String [][] records_wyv = new String[473][cols2.length];
		for(int i=0;i<473;i++)
		{
			records_wyv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wyv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wyv", records_wyv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record468: [a468, b0, c0, d0, e3, f0, g6]
		//record469: [a469, b1, c1, d1, e4, f1, g0]
		//record470: [a470, b0, c2, d2, e0, f2, g1]
		//record471: [a471, b1, c0, d3, e1, f3, g2]
		//record472: [a472, b0, c1, d0, e2, f4, g3]
		String[] ConditionColumns2 = {"e","c","f","a","g"};
		String[] ConditionColumnsValues2 = {"e3","c1","f1","a103","g5"};
		DBApp.createBitMapIndex("wyv","e");
		DBApp.createBitMapIndex("wyv","f");
		DBApp.createBitMapIndex("wyv","a");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("wyv", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a103, b1, c1, d3, e3, f1, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a103","b1","c1","d3","e3","f1","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("d97", cols0);
		String [][] records_d97 = new String[261][cols0.length];
		for(int i=0;i<261;i++)
		{
			records_d97[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d97[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d97", records_d97[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0]
		String[] ConditionColumns0 = {"b","h","k"};
		String[] ConditionColumnsValues0 = {"b0","h2","k0"};
		DBApp.createBitMapIndex("d97","h");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("d97", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1]
		//record1: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10, m11]
		//record2: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 3);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3","h2","i3","j6","k0","l6","m1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a154","b0","c1","d2","e4","f4","g0","h2","i1","j4","k0","l10","m11"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a242","b0","c2","d2","e2","f2","g4","h2","i8","j2","k0","l2","m8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("f7c", cols1);
		String [][] records_f7c = new String[358][cols1.length];
		for(int i=0;i<358;i++)
		{
			records_f7c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f7c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f7c", records_f7c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2, n3, o8, p1, q13, r11, s11]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3, n4, o9, p2, q14, r12, s12]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10, p3, q15, r13, s13]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11, p4, q16, r14, s14]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12, p5, q0, r15, s15]
		String[] ConditionColumns1 = {"q","f","n"};
		String[] ConditionColumnsValues1 = {"q13","f5","n13"};
		DBApp.createBitMapIndex("f7c","q");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("f7c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11, p11, q13, r17, s4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a251","b1","c2","d3","e1","f5","g6","h3","i8","j1","k9","l11","m4","n13","o11","p11","q13","r17","s4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yt0", cols2);
		String [][] records_yt0 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			records_yt0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yt0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yt0", records_yt0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7]
		String[] ConditionColumns2 = {"h","f","c","b","e"};
		String[] ConditionColumnsValues2 = {"h7","f1","c1","b1","e1"};
		DBApp.createBitMapIndex("yt0","h");
		DBApp.createBitMapIndex("yt0","c");
		DBApp.createBitMapIndex("yt0","b");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("yt0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3, h7]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b6ab2", cols3);
		String [][] records_b6ab2 = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			records_b6ab2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b6ab2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b6ab2", records_b6ab2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4]
		String[] ConditionColumns3 = {"d","e","g","b","f","h"};
		String[] ConditionColumnsValues3 = {"d3","e0","g1","b1","f3","h7"};
		DBApp.createBitMapIndex("b6ab2","g");
		DBApp.createBitMapIndex("b6ab2","b");
		DBApp.createBitMapIndex("b6ab2","h");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("b6ab2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mw", cols0);
		String [][] records_mw = new String[362][cols0.length];
		for(int i=0;i<362;i++)
		{
			records_mw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mw", records_mw[i]);
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
		String[] ConditionColumns0 = {"a","f","e"};
		String[] ConditionColumnsValues0 = {"a354","f0","e4"};
		DBApp.createBitMapIndex("mw","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("mw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a354, b0, c0, d2, e4, f0, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a354","b0","c0","d2","e4","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hz6", cols1);
		String [][] records_hz6 = new String[190][cols1.length];
		for(int i=0;i<190;i++)
		{
			records_hz6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hz6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hz6", records_hz6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record185: [a185, b1, c2, d1, e0, f5, g3]
		//record186: [a186, b0, c0, d2, e1, f0, g4]
		//record187: [a187, b1, c1, d3, e2, f1, g5]
		//record188: [a188, b0, c2, d0, e3, f2, g6]
		//record189: [a189, b1, c0, d1, e4, f3, g0]
		String[] ConditionColumns1 = {"b","c","f","g","d","e"};
		String[] ConditionColumnsValues1 = {"b0","c0","f0","g1","d0","e1"};
		DBApp.createBitMapIndex("hz6","c");
		DBApp.createBitMapIndex("hz6","f");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("hz6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a36, b0, c0, d0, e1, f0, g1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("m7", cols2);
		String [][] records_m7 = new String[480][cols2.length];
		for(int i=0;i<480;i++)
		{
			records_m7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m7", records_m7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2]
		String[] ConditionColumns2 = {"i","d","a","g","b","e","h"};
		String[] ConditionColumnsValues2 = {"i8","d1","a197","g1","b1","e2","h5"};
		DBApp.createBitMapIndex("m7","i");
		DBApp.createBitMapIndex("m7","g");
		DBApp.createBitMapIndex("m7","b");
		DBApp.createBitMapIndex("m7","e");
		DBApp.createBitMapIndex("m7","h");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("m7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a197, b1, c2, d1, e2, f5, g1, h5, i8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a197","b1","c2","d1","e2","f5","g1","h5","i8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lx", cols3);
		String [][] records_lx = new String[403][cols3.length];
		for(int i=0;i<403;i++)
		{
			records_lx[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lx[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lx", records_lx[i]);
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
		String[] ConditionColumns3 = {"e","a","b","f"};
		String[] ConditionColumnsValues3 = {"e2","a357","b1","f3"};
		DBApp.createBitMapIndex("lx","e");
		DBApp.createBitMapIndex("lx","b");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("lx", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a357, b1, c0, d1, e2, f3, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a357","b1","c0","d1","e2","f3","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sef", cols0);
		String [][] records_sef = new String[372][cols0.length];
		for(int i=0;i<372;i++)
		{
			records_sef[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sef[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sef", records_sef[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record367: [a367, b1, c1, d3, e2, f1, g3]
		//record368: [a368, b0, c2, d0, e3, f2, g4]
		//record369: [a369, b1, c0, d1, e4, f3, g5]
		//record370: [a370, b0, c1, d2, e0, f4, g6]
		//record371: [a371, b1, c2, d3, e1, f5, g0]
		String[] ConditionColumns0 = {"e","d","f"};
		String[] ConditionColumnsValues0 = {"e2","d1","f1"};
		DBApp.createBitMapIndex("sef","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("sef", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a37, b1, c1, d1, e2, f1, g2]
		//record1: [a97, b1, c1, d1, e2, f1, g6]
		//record2: [a157, b1, c1, d1, e2, f1, g3]
		//record3: [a217, b1, c1, d1, e2, f1, g0]
		//record4: [a277, b1, c1, d1, e2, f1, g4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a97","b1","c1","d1","e2","f1","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a157","b1","c1","d1","e2","f1","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a217","b1","c1","d1","e2","f1","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a277","b1","c1","d1","e2","f1","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("z3ly", cols0);
		String [][] records_z3ly = new String[149][cols0.length];
		for(int i=0;i<149;i++)
		{
			records_z3ly[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z3ly[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z3ly", records_z3ly[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4, o9, p0, q8, r0, s11, t4]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10, p1, q9, r1, s12, t5]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10, r2, s13, t6]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12, p3, q11, r3, s14, t7]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13, p4, q12, r4, s15, t8]
		String[] ConditionColumns0 = {"e","l","i","p","o","d","q","f","r","s","c","j","n","t"};
		String[] ConditionColumnsValues0 = {"e1","l4","i1","p8","o1","d0","q0","f4","r10","s3","c1","j6","n10","t16"};
		DBApp.createBitMapIndex("z3ly","l");
		DBApp.createBitMapIndex("z3ly","i");
		DBApp.createBitMapIndex("z3ly","p");
		DBApp.createBitMapIndex("z3ly","d");
		DBApp.createBitMapIndex("z3ly","f");
		DBApp.createBitMapIndex("z3ly","r");
		DBApp.createBitMapIndex("z3ly","j");
		DBApp.createBitMapIndex("z3ly","n");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("z3ly", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0, r10, s3, t16]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a136","b0","c1","d0","e1","f4","g3","h0","i1","j6","k4","l4","m6","n10","o1","p8","q0","r10","s3","t16"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f27", cols0);
		String [][] records_f27 = new String[203][cols0.length];
		for(int i=0;i<203;i++)
		{
			records_f27[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f27[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f27", records_f27[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3, n2, o3]
		//record199: [a199, b1, c1, d3, e4, f1, g3, h7, i1, j9, k1, l7, m4, n3, o4]
		//record200: [a200, b0, c2, d0, e0, f2, g4, h0, i2, j0, k2, l8, m5, n4, o5]
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3, l9, m6, n5, o6]
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2, i4, j2, k4, l10, m7, n6, o7]
		String[] ConditionColumns0 = {"f","h","k","d","c","a","o","i","g","m","b","l","e"};
		String[] ConditionColumnsValues0 = {"f5","h1","k7","d1","c2","a161","o11","i8","g0","m5","b1","l5","e1"};
		DBApp.createBitMapIndex("f27","f");
		DBApp.createBitMapIndex("f27","d");
		DBApp.createBitMapIndex("f27","c");
		DBApp.createBitMapIndex("f27","o");
		DBApp.createBitMapIndex("f27","g");
		DBApp.createBitMapIndex("f27","b");
		DBApp.createBitMapIndex("f27","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("f27", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a161","b1","c2","d1","e1","f5","g0","h1","i8","j1","k7","l5","m5","n7","o11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q7", cols1);
		String [][] records_q7 = new String[478][cols1.length];
		for(int i=0;i<478;i++)
		{
			records_q7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q7", records_q7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record473: [a473, b1, c2, d1, e3, f5, g4]
		//record474: [a474, b0, c0, d2, e4, f0, g5]
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		String[] ConditionColumns1 = {"g","e","c","b"};
		String[] ConditionColumnsValues1 = {"g2","e2","c0","b0"};
		DBApp.createBitMapIndex("q7","g");
		DBApp.createBitMapIndex("q7","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("q7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a72, b0, c0, d0, e2, f0, g2]
		//record1: [a282, b0, c0, d2, e2, f0, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a282","b0","c0","d2","e2","f0","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("fa", cols2);
		String [][] records_fa = new String[284][cols2.length];
		for(int i=0;i<284;i++)
		{
			records_fa[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fa[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fa", records_fa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record279: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9, k4, l3, m6, n13, o9, p7, q7, r9, s13, t19]
		//record280: [a280, b0, c1, d0, e0, f4, g0, h0, i1, j0, k5, l4, m7, n0, o10, p8, q8, r10, s14, t0]
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5, m8, n1, o11, p9, q9, r11, s15, t1]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9, n2, o12, p10, q10, r12, s16, t2]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3, o13, p11, q11, r13, s17, t3]
		String[] ConditionColumns2 = {"b","a","n","l","k","r","j","g","h","o","d","f"};
		String[] ConditionColumnsValues2 = {"b0","a28","n0","l4","k6","r10","j8","g0","h4","o13","d0","f4"};
		DBApp.createBitMapIndex("fa","l");
		DBApp.createBitMapIndex("fa","k");
		DBApp.createBitMapIndex("fa","r");
		DBApp.createBitMapIndex("fa","g");
		DBApp.createBitMapIndex("fa","o");
		DBApp.createBitMapIndex("fa","d");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("fa", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9, t8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13","p12","q11","r10","s9","t8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("r262", cols3);
		String [][] records_r262 = new String[312][cols3.length];
		for(int i=0;i<312;i++)
		{
			records_r262[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r262[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r262", records_r262[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3]
		String[] ConditionColumns3 = {"k","j","f","b"};
		String[] ConditionColumnsValues3 = {"k6","j8","f4","b0"};
		DBApp.createBitMapIndex("r262","b");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("r262", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s6", cols0);
		String [][] records_s6 = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_s6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s6", records_s6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6, l8]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11]
		String[] ConditionColumns0 = {"k","e","g","i","j","c","l","h"};
		String[] ConditionColumnsValues0 = {"k8","e2","g2","i8","j7","c2","l11","h3"};
		DBApp.createBitMapIndex("s6","k");
		DBApp.createBitMapIndex("s6","e");
		DBApp.createBitMapIndex("s6","g");
		DBApp.createBitMapIndex("s6","c");
		DBApp.createBitMapIndex("s6","l");
		DBApp.createBitMapIndex("s6","h");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("s6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a107","b1","c2","d3","e2","f5","g2","h3","i8","j7","k8","l11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("o6ia", cols1);
		String [][] records_o6ia = new String[27][cols1.length];
		for(int i=0;i<27;i++)
		{
			records_o6ia[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o6ia[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o6ia", records_o6ia[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		String[] ConditionColumns1 = {"c","m","f","o","b","q","e","k","j","r","d","n"};
		String[] ConditionColumnsValues1 = {"c2","m0","f2","o11","b0","q9","e1","k4","j6","r8","d2","n12"};
		DBApp.createBitMapIndex("o6ia","f");
		DBApp.createBitMapIndex("o6ia","b");
		DBApp.createBitMapIndex("o6ia","q");
		DBApp.createBitMapIndex("o6ia","j");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("o6ia", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12","o11","p10","q9","r8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w1n", cols0);
		String [][] records_w1n = new String[489][cols0.length];
		for(int i=0;i<489;i++)
		{
			records_w1n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w1n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w1n", records_w1n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5, m4]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7]
		String[] ConditionColumns0 = {"e","l","g"};
		String[] ConditionColumnsValues0 = {"e4","l6","g5"};
		DBApp.createBitMapIndex("w1n","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("w1n", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2]
		//record1: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a474","b0","c0","d2","e4","f0","g5","h2","i6","j4","k1","l6","m6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("m947n", cols1);
		String [][] records_m947n = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			records_m947n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m947n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m947n", records_m947n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9]
		String[] ConditionColumns1 = {"g","b","h"};
		String[] ConditionColumnsValues1 = {"g4","b1","h5"};
		DBApp.createBitMapIndex("m947n","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("m947n", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1","n11","o8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w9", cols0);
		String [][] records_w9 = new String[359][cols0.length];
		for(int i=0;i<359;i++)
		{
			records_w9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w9", records_w9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3, n4, o9]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13]
		String[] ConditionColumns0 = {"b","g","o","e"};
		String[] ConditionColumnsValues0 = {"b0","g6","o12","e2"};
		DBApp.createBitMapIndex("w9","b");
		DBApp.createBitMapIndex("w9","o");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("w9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2, n6, o12]
		//record1: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4, n6, o12]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a132","b0","c0","d0","e2","f0","g6","h4","i6","j2","k0","l0","m2","n6","o12"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a342","b0","c0","d2","e2","f0","g6","h6","i0","j2","k1","l6","m4","n6","o12"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uv1", cols1);
		String [][] records_uv1 = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			records_uv1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uv1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uv1", records_uv1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record54: [a54, b0, c0, d2, e4, f0, g5]
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		//record56: [a56, b0, c2, d0, e1, f2, g0]
		//record57: [a57, b1, c0, d1, e2, f3, g1]
		//record58: [a58, b0, c1, d2, e3, f4, g2]
		String[] ConditionColumns1 = {"g","b","c"};
		String[] ConditionColumnsValues1 = {"g6","b0","c0"};
		DBApp.createBitMapIndex("uv1","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("uv1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a48, b0, c0, d0, e3, f0, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("qjm7", cols0);
		String [][] records_qjm7 = new String[374][cols0.length];
		for(int i=0;i<374;i++)
		{
			records_qjm7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qjm7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qjm7", records_qjm7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5, n5, o9, p1, q12, r9]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6, n6, o10, p2, q13, r10]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7, n7, o11, p3, q14, r11]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8, n8, o12, p4, q15, r12]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9, n9, o13, p5, q16, r13]
		String[] ConditionColumns0 = {"k","n","g","r","l","a","p","d","o","j","q"};
		String[] ConditionColumnsValues0 = {"k3","n8","g1","r10","l10","a190","p14","d2","o10","j0","q3"};
		DBApp.createBitMapIndex("qjm7","n");
		DBApp.createBitMapIndex("qjm7","r");
		DBApp.createBitMapIndex("qjm7","o");
		DBApp.createBitMapIndex("qjm7","j");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qjm7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8, o10, p14, q3, r10]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a190","b0","c1","d2","e0","f4","g1","h6","i1","j0","k3","l10","m8","n8","o10","p14","q3","r10"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s262", cols1);
		String [][] records_s262 = new String[173][cols1.length];
		for(int i=0;i<173;i++)
		{
			records_s262[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s262[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s262", records_s262[i]);
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
		String[] ConditionColumns1 = {"d","c","f"};
		String[] ConditionColumnsValues1 = {"d3","c1","f1"};
		DBApp.createBitMapIndex("s262","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("s262", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0]
		//record1: [a19, b1, c1, d3, e4, f1, g5]
		//record2: [a31, b1, c1, d3, e1, f1, g3]
		//record3: [a43, b1, c1, d3, e3, f1, g1]
		//record4: [a55, b1, c1, d3, e0, f1, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 14);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o1pu0", cols2);
		String [][] records_o1pu0 = new String[258][cols2.length];
		for(int i=0;i<258;i++)
		{
			records_o1pu0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o1pu0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o1pu0", records_o1pu0[i]);
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
		String[] ConditionColumns2 = {"c","b","e","f","d"};
		String[] ConditionColumnsValues2 = {"c2","b0","e1","f2","d2"};
		DBApp.createBitMapIndex("o1pu0","c");
		DBApp.createBitMapIndex("o1pu0","d");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("o1pu0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5]
		//record1: [a86, b0, c2, d2, e1, f2, g2]
		//record2: [a146, b0, c2, d2, e1, f2, g6]
		//record3: [a206, b0, c2, d2, e1, f2, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 4);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a146","b0","c2","d2","e1","f2","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a206","b0","c2","d2","e1","f2","g3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("a0v", cols3);
		String [][] records_a0v = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			records_a0v[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_a0v[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("a0v", records_a0v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9]
		String[] ConditionColumns3 = {"f","b","l","o","d","c","n"};
		String[] ConditionColumnsValues3 = {"f3","b1","l9","o6","d1","c0","n7"};
		DBApp.createBitMapIndex("a0v","b");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("a0v", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q81", cols4);
		String [][] records_q81 = new String[171][cols4.length];
		for(int i=0;i<171;i++)
		{
			records_q81[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q81[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q81", records_q81[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5]
		//record167: [a167, b1, c2, d3, e2, f5, g6]
		//record168: [a168, b0, c0, d0, e3, f0, g0]
		//record169: [a169, b1, c1, d1, e4, f1, g1]
		//record170: [a170, b0, c2, d2, e0, f2, g2]
		String[] ConditionColumns4 = {"e","c","g","b","d","f"};
		String[] ConditionColumnsValues4 = {"e4","c2","g3","b0","d0","f2"};
		DBApp.createBitMapIndex("q81","d");
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("q81", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a164, b0, c2, d0, e4, f2, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a164","b0","c2","d0","e4","f2","g3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("x10", cols0);
		String [][] records_x10 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			records_x10[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x10[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x10", records_x10[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15, q11, r7, s3, t19]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10, o5, p0, q12, r8, s4, t0]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6, p1, q13, r9, s5, t1]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7, p2, q14, r10, s6, t2]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3, q15, r11, s7, t3]
		String[] ConditionColumns0 = {"e","r","c","h","g","a","j","n","i","p","k"};
		String[] ConditionColumnsValues0 = {"e4","r10","c1","h0","g1","a64","j4","n8","i1","p0","k9"};
		DBApp.createBitMapIndex("x10","e");
		DBApp.createBitMapIndex("x10","r");
		DBApp.createBitMapIndex("x10","c");
		DBApp.createBitMapIndex("x10","h");
		DBApp.createBitMapIndex("x10","g");
		DBApp.createBitMapIndex("x10","n");
		DBApp.createBitMapIndex("x10","p");
		DBApp.createBitMapIndex("x10","k");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("x10", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0, q13, r10, s7, t4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a64","b0","c1","d0","e4","f4","g1","h0","i1","j4","k9","l4","m12","n8","o4","p0","q13","r10","s7","t4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("w21", cols1);
		String [][] records_w21 = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			records_w21[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w21[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w21", records_w21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		String[] ConditionColumns1 = {"f","a","k","j","h","c","e"};
		String[] ConditionColumnsValues1 = {"f1","a13","k2","j3","h5","c1","e3"};
		DBApp.createBitMapIndex("w21","f");
		DBApp.createBitMapIndex("w21","k");
		DBApp.createBitMapIndex("w21","j");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("w21", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eme", cols2);
		String [][] records_eme = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			records_eme[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eme[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eme", records_eme[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		String[] ConditionColumns2 = {"f","a","g","d","e"};
		String[] ConditionColumnsValues2 = {"f3","a3","g3","d3","e3"};
		DBApp.createBitMapIndex("eme","f");
		DBApp.createBitMapIndex("eme","g");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("eme", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("q3t", cols0);
		String [][] records_q3t = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			records_q3t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q3t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q3t", records_q3t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6, t6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7, t7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8, t8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9, t9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10, t10]
		String[] ConditionColumns0 = {"m","c","t","h","l","a","g","s","r","j","e"};
		String[] ConditionColumnsValues0 = {"m4","c1","t4","h4","l4","a4","g4","s4","r4","j4","e4"};
		DBApp.createBitMapIndex("q3t","m");
		DBApp.createBitMapIndex("q3t","t");
		DBApp.createBitMapIndex("q3t","h");
		DBApp.createBitMapIndex("q3t","g");
		DBApp.createBitMapIndex("q3t","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("q3t", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4","q4","r4","s4","t4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z27", cols0);
		String [][] records_z27 = new String[141][cols0.length];
		for(int i=0;i<141;i++)
		{
			records_z27[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z27[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z27", records_z27[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3]
		//record137: [a137, b1, c2, d1, e2, f5, g4]
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		//record140: [a140, b0, c2, d0, e0, f2, g0]
		String[] ConditionColumns0 = {"e","d","g"};
		String[] ConditionColumnsValues0 = {"e3","d1","g5"};
		DBApp.createBitMapIndex("z27","g");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("z27", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gv3d", cols0);
		String [][] records_gv3d = new String[168][cols0.length];
		for(int i=0;i<168;i++)
		{
			records_gv3d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gv3d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gv3d", records_gv3d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7]
		String[] ConditionColumns0 = {"a","b","c","h","f"};
		String[] ConditionColumnsValues0 = {"a17","b1","c2","h1","f5"};
		DBApp.createBitMapIndex("gv3d","h");
		DBApp.createBitMapIndex("gv3d","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("gv3d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("drv2h", cols1);
		String [][] records_drv2h = new String[164][cols1.length];
		for(int i=0;i<164;i++)
		{
			records_drv2h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_drv2h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("drv2h", records_drv2h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9]
		String[] ConditionColumns1 = {"j","d","k","n","g"};
		String[] ConditionColumnsValues1 = {"j4","d0","k5","n6","g6"};
		DBApp.createBitMapIndex("drv2h","d");
		DBApp.createBitMapIndex("drv2h","k");
		DBApp.createBitMapIndex("drv2h","n");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("drv2h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6","h0","i5","j4","k5","l8","m0","n6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("cy4wk", cols2);
		String [][] records_cy4wk = new String[304][cols2.length];
		for(int i=0;i<304;i++)
		{
			records_cy4wk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cy4wk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cy4wk", records_cy4wk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2, l11, m0]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0, m1]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1, m2]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4]
		String[] ConditionColumns2 = {"m","a","j","g","c","l"};
		String[] ConditionColumnsValues2 = {"m9","a282","j2","g2","c0","l6"};
		DBApp.createBitMapIndex("cy4wk","c");
		DBApp.createBitMapIndex("cy4wk","l");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("cy4wk", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a282","b0","c0","d2","e2","f0","g2","h2","i3","j2","k7","l6","m9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gkb", cols3);
		String [][] records_gkb = new String[257][cols3.length];
		for(int i=0;i<257;i++)
		{
			records_gkb[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gkb[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gkb", records_gkb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4]
		String[] ConditionColumns3 = {"a","d","c"};
		String[] ConditionColumnsValues3 = {"a23","d3","c2"};
		DBApp.createBitMapIndex("gkb","a");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("gkb", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("hw", cols0);
		String [][] records_hw = new String[346][cols0.length];
		for(int i=0;i<346;i++)
		{
			records_hw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hw", records_hw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3, n5, o11, p5, q1]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4, n6, o12, p6, q2]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5, n7, o13, p7, q3]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14, p8, q4]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9, q5]
		String[] ConditionColumns0 = {"d","b","h","k","j","f","a","n","o","g","m","p"};
		String[] ConditionColumnsValues0 = {"d3","b1","h3","k0","j7","f1","a187","n5","o7","g5","m5","p11"};
		DBApp.createBitMapIndex("hw","b");
		DBApp.createBitMapIndex("hw","n");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("hw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5, o7, p11, q0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a187","b1","c1","d3","e2","f1","g5","h3","i7","j7","k0","l7","m5","n5","o7","p11","q0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("bkx", cols1);
		String [][] records_bkx = new String[437][cols1.length];
		for(int i=0;i<437;i++)
		{
			records_bkx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bkx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bkx", records_bkx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0, p3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2, o1, p4]
		String[] ConditionColumns1 = {"k","b","g","h","p","d","e","n","o","c","m","l","f"};
		String[] ConditionColumnsValues1 = {"k9","b0","g5","h2","p2","d2","e1","n12","o6","c0","m7","l6","f0"};
		DBApp.createBitMapIndex("bkx","g");
		DBApp.createBitMapIndex("bkx","h");
		DBApp.createBitMapIndex("bkx","p");
		DBApp.createBitMapIndex("bkx","d");
		DBApp.createBitMapIndex("bkx","e");
		DBApp.createBitMapIndex("bkx","o");
		DBApp.createBitMapIndex("bkx","m");
		DBApp.createBitMapIndex("bkx","l");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("bkx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7, n12, o6, p2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a306","b0","c0","d2","e1","f0","g5","h2","i0","j6","k9","l6","m7","n12","o6","p2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a7438", cols2);
		String [][] records_a7438 = new String[251][cols2.length];
		for(int i=0;i<251;i++)
		{
			records_a7438[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a7438[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a7438", records_a7438[i]);
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
		String[] ConditionColumns2 = {"a","b","g","d","f","e","c"};
		String[] ConditionColumnsValues2 = {"a42","b0","g0","d2","f0","e2","c0"};
		DBApp.createBitMapIndex("a7438","a");
		DBApp.createBitMapIndex("a7438","e");
		DBApp.createBitMapIndex("a7438","c");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("a7438", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a42, b0, c0, d2, e2, f0, g0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("z8794", cols3);
		String [][] records_z8794 = new String[418][cols3.length];
		for(int i=0;i<418;i++)
		{
			records_z8794[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_z8794[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("z8794", records_z8794[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10, n7, o8, p13, q5, r17]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8, o9, p14, q6, r0]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12, n9, o10, p15, q7, r1]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0, n10, o11, p0, q8, r2]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12, p1, q9, r3]
		String[] ConditionColumns3 = {"b","i","l","c","k","g","r","q","m","p","o","f","n","h","j"};
		String[] ConditionColumnsValues3 = {"b0","i7","l10","c1","k9","g2","r16","q3","m4","p10","o4","f4","n2","h2","j4"};
		DBApp.createBitMapIndex("z8794","b");
		DBApp.createBitMapIndex("z8794","i");
		DBApp.createBitMapIndex("z8794","l");
		DBApp.createBitMapIndex("z8794","c");
		DBApp.createBitMapIndex("z8794","g");
		DBApp.createBitMapIndex("z8794","r");
		DBApp.createBitMapIndex("z8794","q");
		DBApp.createBitMapIndex("z8794","p");
		DBApp.createBitMapIndex("z8794","o");
		DBApp.createBitMapIndex("z8794","f");
		DBApp.createBitMapIndex("z8794","h");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("z8794", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2, o4, p10, q3, r16]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a394","b0","c1","d2","e4","f4","g2","h2","i7","j4","k9","l10","m4","n2","o4","p10","q3","r16"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bl", cols0);
		String [][] records_bl = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_bl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bl", records_bl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		String[] ConditionColumns0 = {"f","d","e","b","g","c","a"};
		String[] ConditionColumnsValues0 = {"f5","d3","e3","b1","g6","c2","a83"};
		DBApp.createBitMapIndex("bl","f");
		DBApp.createBitMapIndex("bl","d");
		DBApp.createBitMapIndex("bl","e");
		DBApp.createBitMapIndex("bl","a");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("bl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a83, b1, c2, d3, e3, f5, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("w3", cols0);
		String [][] records_w3 = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_w3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w3", records_w3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0]
		String[] ConditionColumns0 = {"f","n","i"};
		String[] ConditionColumnsValues0 = {"f2","n0","i5"};
		DBApp.createBitMapIndex("w3","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("w3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14","p14","q14","r14"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nbn74", cols1);
		String [][] records_nbn74 = new String[105][cols1.length];
		for(int i=0;i<105;i++)
		{
			records_nbn74[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nbn74[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nbn74", records_nbn74[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record100: [a100, b0, c1, d0, e0, f4, g2]
		//record101: [a101, b1, c2, d1, e1, f5, g3]
		//record102: [a102, b0, c0, d2, e2, f0, g4]
		//record103: [a103, b1, c1, d3, e3, f1, g5]
		//record104: [a104, b0, c2, d0, e4, f2, g6]
		String[] ConditionColumns1 = {"g","d","f"};
		String[] ConditionColumnsValues1 = {"g5","d1","f5"};
		DBApp.createBitMapIndex("nbn74","g");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("nbn74", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a89, b1, c2, d1, e4, f5, g5]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a89","b1","c2","d1","e4","f5","g5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xn6", cols2);
		String [][] records_xn6 = new String[424][cols2.length];
		for(int i=0;i<424;i++)
		{
			records_xn6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xn6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xn6", records_xn6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record419: [a419, b1, c2, d3, e4, f5, g6]
		//record420: [a420, b0, c0, d0, e0, f0, g0]
		//record421: [a421, b1, c1, d1, e1, f1, g1]
		//record422: [a422, b0, c2, d2, e2, f2, g2]
		//record423: [a423, b1, c0, d3, e3, f3, g3]
		String[] ConditionColumns2 = {"e","c","a","b","d","f"};
		String[] ConditionColumnsValues2 = {"e2","c1","a247","b1","d3","f1"};
		DBApp.createBitMapIndex("xn6","b");
		DBApp.createBitMapIndex("xn6","f");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("xn6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a247, b1, c1, d3, e2, f1, g2]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a247","b1","c1","d3","e2","f1","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bk4a8", cols3);
		String [][] records_bk4a8 = new String[425][cols3.length];
		for(int i=0;i<425;i++)
		{
			records_bk4a8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bk4a8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bk4a8", records_bk4a8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record420: [a420, b0, c0, d0, e0, f0, g0]
		//record421: [a421, b1, c1, d1, e1, f1, g1]
		//record422: [a422, b0, c2, d2, e2, f2, g2]
		//record423: [a423, b1, c0, d3, e3, f3, g3]
		//record424: [a424, b0, c1, d0, e4, f4, g4]
		String[] ConditionColumns3 = {"e","f","b","g"};
		String[] ConditionColumnsValues3 = {"e1","f0","b0","g3"};
		DBApp.createBitMapIndex("bk4a8","f");
		DBApp.createBitMapIndex("bk4a8","b");
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("bk4a8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 2, records:
		//record0: [a66, b0, c0, d2, e1, f0, g3]
		//record1: [a276, b0, c0, d0, e1, f0, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect3.size(), 2);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a276","b0","c0","d0","e1","f0","g3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("p83", cols0);
		String [][] records_p83 = new String[442][cols0.length];
		for(int i=0;i<442;i++)
		{
			records_p83[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p83[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p83", records_p83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2, p5, q12]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4, o3, p6, q13]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5, o4, p7, q14]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6, o5, p8, q15]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7, o6, p9, q16]
		String[] ConditionColumns0 = {"e","n","i","h","c","a"};
		String[] ConditionColumnsValues0 = {"e4","n9","i0","h1","c0","a9"};
		DBApp.createBitMapIndex("p83","e");
		DBApp.createBitMapIndex("p83","n");
		DBApp.createBitMapIndex("p83","c");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("p83", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9","p9","q9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j2x", cols1);
		String [][] records_j2x = new String[427][cols1.length];
		for(int i=0;i<427;i++)
		{
			records_j2x[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j2x[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j2x", records_j2x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record422: [a422, b0, c2, d2, e2, f2, g2]
		//record423: [a423, b1, c0, d3, e3, f3, g3]
		//record424: [a424, b0, c1, d0, e4, f4, g4]
		//record425: [a425, b1, c2, d1, e0, f5, g5]
		//record426: [a426, b0, c0, d2, e1, f0, g6]
		String[] ConditionColumns1 = {"f","a","g","d","c","e"};
		String[] ConditionColumnsValues1 = {"f4","a286","g6","d2","c1","e1"};
		DBApp.createBitMapIndex("j2x","d");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("j2x", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a286, b0, c1, d2, e1, f4, g6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a286","b0","c1","d2","e1","f4","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("f6", cols0);
		String [][] records_f6 = new String[415][cols0.length];
		for(int i=0;i<415;i++)
		{
			records_f6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f6", records_f6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2, m7, n4, o5, p10]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3, m8, n5, o6, p11]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9, n6, o7, p12]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10, n7, o8, p13]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8, o9, p14]
		String[] ConditionColumns0 = {"c","l","m","d","j","o","i","h","f","e","g","n"};
		String[] ConditionColumnsValues0 = {"c2","l5","m8","d1","j1","o11","i2","h1","f5","e1","g1","n1"};
		DBApp.createBitMapIndex("f6","l");
		DBApp.createBitMapIndex("f6","m");
		DBApp.createBitMapIndex("f6","j");
		DBApp.createBitMapIndex("f6","e");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("f6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5, m8, n1, o11, p9]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a281","b1","c2","d1","e1","f5","g1","h1","i2","j1","k6","l5","m8","n1","o11","p9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s920", cols1);
		String [][] records_s920 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			records_s920[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s920[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s920", records_s920[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		//record49: [a49, b1, c1, d1, e4, f1, g0]
		//record50: [a50, b0, c2, d2, e0, f2, g1]
		String[] ConditionColumns1 = {"e","a","c","f","b"};
		String[] ConditionColumnsValues1 = {"e0","a10","c1","f4","b0"};
		DBApp.createBitMapIndex("s920","f");
		DBApp.createBitMapIndex("s920","b");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("s920", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k928", cols2);
		String [][] records_k928 = new String[176][cols2.length];
		for(int i=0;i<176;i++)
		{
			records_k928[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k928[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k928", records_k928[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record171: [a171, b1, c0, d3, e1, f3, g3]
		//record172: [a172, b0, c1, d0, e2, f4, g4]
		//record173: [a173, b1, c2, d1, e3, f5, g5]
		//record174: [a174, b0, c0, d2, e4, f0, g6]
		//record175: [a175, b1, c1, d3, e0, f1, g0]
		String[] ConditionColumns2 = {"f","e","c"};
		String[] ConditionColumnsValues2 = {"f5","e3","c2"};
		DBApp.createBitMapIndex("k928","f");
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("k928", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2]
		//record1: [a53, b1, c2, d1, e3, f5, g4]
		//record2: [a83, b1, c2, d3, e3, f5, g6]
		//record3: [a113, b1, c2, d1, e3, f5, g1]
		//record4: [a143, b1, c2, d3, e3, f5, g3]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect2.size(), 6);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a113","b1","c2","d1","e3","f5","g1"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a143","b1","c2","d3","e3","f5","g3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexed_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("cpt", cols0);
		String [][] records_cpt = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_cpt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cpt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cpt", records_cpt[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		String[] ConditionColumns0 = {"g","c","b","f"};
		String[] ConditionColumnsValues0 = {"g0","c0","b0","f0"};
		DBApp.createBitMapIndex("cpt","g");
		DBApp.createBitMapIndex("cpt","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("cpt", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ko20g", cols1);
		String [][] records_ko20g = new String[294][cols1.length];
		for(int i=0;i<294;i++)
		{
			records_ko20g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ko20g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ko20g", records_ko20g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3, n9, o4]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4, n10, o5]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5, n11, o6]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12, o7]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7, n13, o8]
		String[] ConditionColumns1 = {"j","f","h","m","o","a","b","k","c","d"};
		String[] ConditionColumnsValues1 = {"j6","f0","h4","m3","o6","a276","b0","k1","c0","d0"};
		DBApp.createBitMapIndex("ko20g","f");
		DBApp.createBitMapIndex("ko20g","h");
		DBApp.createBitMapIndex("ko20g","o");
		DBApp.createBitMapIndex("ko20g","b");
		DBApp.createBitMapIndex("ko20g","k");
		DBApp.createBitMapIndex("ko20g","c");
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ko20g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3, n10, o6]
		assertEquals("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a276","b0","c0","d0","e1","f0","g3","h4","i6","j6","k1","l0","m3","n10","o6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("o40k4", cols0);
		String [][] records_o40k4 = new String[358][cols0.length];
		for(int i=0;i<358;i++)
		{
			records_o40k4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o40k4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o40k4", records_o40k4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2, n3, o8, p1, q13, r11, s11, t13]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3, n4, o9, p2, q14, r12, s12, t14]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10, p3, q15, r13, s13, t15]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11, p4, q16, r14, s14, t16]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12, p5, q0, r15, s15, t17]
		String[] ConditionColumns0 = {"f","h","p","q","t","o"};
		String[] ConditionColumnsValues0 = {"f4","h0","p8","q5","t8","o13"};
		DBApp.createBitMapIndex("o40k4","f");
		DBApp.createBitMapIndex("o40k4","h");
		DBApp.createBitMapIndex("o40k4","p");
		DBApp.createBitMapIndex("o40k4","o");
		DBApp.selectIndex("o40k4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6, o13, p8, q5, r4, s5, t8]
		String tableTrace0 = DBApp.getLastTrace("o40k4");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[f, h, o, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[q, t]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y9l", cols1);
		String [][] records_y9l = new String[137][cols1.length];
		for(int i=0;i<137;i++)
		{
			records_y9l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y9l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y9l", records_y9l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6]
		String[] ConditionColumns1 = {"j","i","f","c","m","l","h","d","g","k","b","e"};
		String[] ConditionColumnsValues1 = {"j9","i5","f5","c2","m7","l11","h3","d3","g3","k4","b1","e4"};
		DBApp.createBitMapIndex("y9l","f");
		DBApp.createBitMapIndex("y9l","l");
		DBApp.createBitMapIndex("y9l","h");
		DBApp.createBitMapIndex("y9l","d");
		DBApp.createBitMapIndex("y9l","k");
		DBApp.selectIndex("y9l", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7]
		String tableTrace1 = DBApp.getLastTrace("y9l");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, f, h, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, e, g, i, j, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("d442n", cols0);
		String [][] records_d442n = new String[113][cols0.length];
		for(int i=0;i<113;i++)
		{
			records_d442n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d442n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d442n", records_d442n[i]);
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
		String[] ConditionColumns0 = {"e","q","g","l","m"};
		String[] ConditionColumnsValues0 = {"e1","q4","g0","l9","m8"};
		DBApp.createBitMapIndex("d442n","q");
		DBApp.createBitMapIndex("d442n","g");
		DBApp.createBitMapIndex("d442n","l");
		DBApp.selectIndex("d442n", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3]
		String tableTrace0 = DBApp.getLastTrace("d442n");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g, l, q]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[e, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("os", cols0);
		String [][] records_os = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_os[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_os[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("os", records_os[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4]
		String[] ConditionColumns0 = {"h","d","i","f","q","c","b","m","k","p","n","j","g","l","o"};
		String[] ConditionColumnsValues0 = {"h1","d1","i7","f1","q8","c1","b1","m12","k3","p9","n11","j5","g4","l1","o10"};
		DBApp.createBitMapIndex("os","d");
		DBApp.createBitMapIndex("os","i");
		DBApp.createBitMapIndex("os","f");
		DBApp.createBitMapIndex("os","q");
		DBApp.createBitMapIndex("os","c");
		DBApp.createBitMapIndex("os","b");
		DBApp.createBitMapIndex("os","m");
		DBApp.createBitMapIndex("os","j");
		DBApp.createBitMapIndex("os","l");
		DBApp.selectIndex("os", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		String tableTrace0 = DBApp.getLastTrace("os");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, c, d, f, i, j, l, m, q]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[g, h, k, n, o, p]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("t0", cols0);
		String [][] records_t0 = new String[427][cols0.length];
		for(int i=0;i<427;i++)
		{
			records_t0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t0", records_t0[i]);
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
		String[] ConditionColumns0 = {"b","d","f","g","h","m","j","p","c","e"};
		String[] ConditionColumnsValues0 = {"b0","d0","f4","g6","h0","m3","j8","p8","c1","e3"};
		DBApp.createBitMapIndex("t0","b");
		DBApp.createBitMapIndex("t0","d");
		DBApp.createBitMapIndex("t0","g");
		DBApp.createBitMapIndex("t0","h");
		DBApp.createBitMapIndex("t0","e");
		DBApp.selectIndex("t0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6, o13, p8, q5]
		String tableTrace0 = DBApp.getLastTrace("t0");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, d, e, g, h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f, j, m, p]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k06r", cols1);
		String [][] records_k06r = new String[248][cols1.length];
		for(int i=0;i<248;i++)
		{
			records_k06r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k06r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k06r", records_k06r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4]
		String[] ConditionColumns1 = {"h","g","d","c","b","f"};
		String[] ConditionColumnsValues1 = {"h4","g6","d0","c0","b0","f0"};
		DBApp.createBitMapIndex("k06r","g");
		DBApp.createBitMapIndex("k06r","d");
		DBApp.selectIndex("k06r", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a132, b0, c0, d0, e2, f0, g6, h4, i6]
		String tableTrace1 = DBApp.getLastTrace("k06r");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, c, f, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("j19", cols2);
		String [][] records_j19 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_j19[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j19[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j19", records_j19[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		String[] ConditionColumns2 = {"g","d","i"};
		String[] ConditionColumnsValues2 = {"g2","d3","i5"};
		DBApp.createBitMapIndex("j19","i");
		DBApp.selectIndex("j19", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		String tableTrace2 = DBApp.getLastTrace("j19");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xb6", cols0);
		String [][] records_xb6 = new String[246][cols0.length];
		for(int i=0;i<246;i++)
		{
			records_xb6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xb6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xb6", records_xb6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5]
		String[] ConditionColumns0 = {"j","a","h","i","b","d","c","f","e","g"};
		String[] ConditionColumnsValues0 = {"j2","a52","h4","i7","b0","d0","c1","f4","e2","g3"};
		DBApp.createBitMapIndex("xb6","a");
		DBApp.createBitMapIndex("xb6","h");
		DBApp.createBitMapIndex("xb6","f");
		DBApp.createBitMapIndex("xb6","e");
		DBApp.selectIndex("xb6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2]
		String tableTrace0 = DBApp.getLastTrace("xb6");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, e, f, h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, c, d, g, i, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("uw7", cols1);
		String [][] records_uw7 = new String[307][cols1.length];
		for(int i=0;i<307;i++)
		{
			records_uw7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uw7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uw7", records_uw7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3, n8, o2, p14, q13, r14, s17]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4, n9, o3, p15, q14, r15, s18]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5, n10, o4, p0, q15, r16, s0]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6, n11, o5, p1, q16, r17, s1]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7, n12, o6, p2, q0, r0, s2]
		String[] ConditionColumns1 = {"c","g","q","d","h","j","e","i","l","r","o","s","f","m","p","n"};
		String[] ConditionColumnsValues1 = {"c0","g4","q7","d0","h4","j8","e3","i3","l0","r12","o3","s0","f0","m7","p4","n4"};
		DBApp.createBitMapIndex("uw7","g");
		DBApp.createBitMapIndex("uw7","h");
		DBApp.createBitMapIndex("uw7","e");
		DBApp.createBitMapIndex("uw7","l");
		DBApp.createBitMapIndex("uw7","o");
		DBApp.createBitMapIndex("uw7","s");
		DBApp.createBitMapIndex("uw7","p");
		DBApp.selectIndex("uw7", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4, o3, p4, q7, r12, s0]
		String tableTrace1 = DBApp.getLastTrace("uw7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[e, g, h, l, o, p, s]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, d, f, i, j, m, n, q, r]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ppl", cols0);
		String [][] records_ppl = new String[239][cols0.length];
		for(int i=0;i<239;i++)
		{
			records_ppl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ppl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ppl", records_ppl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record234: [a234, b0, c0, d2, e4, f0, g3]
		//record235: [a235, b1, c1, d3, e0, f1, g4]
		//record236: [a236, b0, c2, d0, e1, f2, g5]
		//record237: [a237, b1, c0, d1, e2, f3, g6]
		//record238: [a238, b0, c1, d2, e3, f4, g0]
		String[] ConditionColumns0 = {"b","g","e","c"};
		String[] ConditionColumnsValues0 = {"b0","g5","e1","c2"};
		DBApp.createBitMapIndex("ppl","b");
		DBApp.createBitMapIndex("ppl","e");
		DBApp.selectIndex("ppl", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5]
		//record1: [a236, b0, c2, d0, e1, f2, g5]
		String tableTrace0 = DBApp.getLastTrace("ppl");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h89", cols1);
		String [][] records_h89 = new String[277][cols1.length];
		for(int i=0;i<277;i++)
		{
			records_h89[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h89[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h89", records_h89[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6]
		String[] ConditionColumns1 = {"g","j","c","e","d","h","b","i","f"};
		String[] ConditionColumnsValues1 = {"g3","j4","c0","e4","d0","h0","b0","i6","f0"};
		DBApp.createBitMapIndex("h89","j");
		DBApp.createBitMapIndex("h89","c");
		DBApp.createBitMapIndex("h89","e");
		DBApp.createBitMapIndex("h89","d");
		DBApp.createBitMapIndex("h89","i");
		DBApp.selectIndex("h89", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		String tableTrace1 = DBApp.getLastTrace("h89");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, d, e, i, j]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, f, g, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("i4n", cols0);
		String [][] records_i4n = new String[458][cols0.length];
		for(int i=0;i<458;i++)
		{
			records_i4n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i4n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i4n", records_i4n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5, o3, p5, q11, r3]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6, q12, r4]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7, o5, p7, q13, r5]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1, n8, o6, p8, q14, r6]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1, m2, n9, o7, p9, q15, r7]
		String[] ConditionColumns0 = {"i","f","d"};
		String[] ConditionColumnsValues0 = {"i8","f5","d3"};
		DBApp.createBitMapIndex("i4n","f");
		DBApp.selectIndex("i4n", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		//record1: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6, n1, o11, p7, q3, r17]
		//record2: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11, q5, r17]
		//record3: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3, o8, p15, q7, r17]
		//record4: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11, o14, p3, q9, r17]
		String tableTrace0 = DBApp.getLastTrace("i4n");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, i]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+12));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o5vh", cols1);
		String [][] records_o5vh = new String[151][cols1.length];
		for(int i=0;i<151;i++)
		{
			records_o5vh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o5vh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o5vh", records_o5vh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7]
		String[] ConditionColumns1 = {"d","g","a","j","i","e"};
		String[] ConditionColumnsValues1 = {"d3","g1","a15","j5","i6","e0"};
		DBApp.createBitMapIndex("o5vh","g");
		DBApp.createBitMapIndex("o5vh","i");
		DBApp.selectIndex("o5vh", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		String tableTrace1 = DBApp.getLastTrace("o5vh");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[g, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, d, e, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("avk5", cols2);
		String [][] records_avk5 = new String[256][cols2.length];
		for(int i=0;i<256;i++)
		{
			records_avk5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_avk5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("avk5", records_avk5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8]
		String[] ConditionColumns2 = {"d","b","m"};
		String[] ConditionColumnsValues2 = {"d1","b1","m9"};
		DBApp.createBitMapIndex("avk5","d");
		DBApp.selectIndex("avk5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record1: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9]
		//record2: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9]
		//record3: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9]
		//record4: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9]
		String tableTrace2 = DBApp.getLastTrace("avk5");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[b, m]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+5));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("j486a", cols0);
		String [][] records_j486a = new String[338][cols0.length];
		for(int i=0;i<338;i++)
		{
			records_j486a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j486a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j486a", records_j486a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12]
		String[] ConditionColumns0 = {"l","m","e","h","i","a","f","k","d","c","b","j","g"};
		String[] ConditionColumnsValues0 = {"l2","m5","e2","h2","i5","a122","f2","k1","d2","c2","b0","j2","g3"};
		DBApp.createBitMapIndex("j486a","m");
		DBApp.createBitMapIndex("j486a","e");
		DBApp.createBitMapIndex("j486a","h");
		DBApp.createBitMapIndex("j486a","a");
		DBApp.createBitMapIndex("j486a","c");
		DBApp.createBitMapIndex("j486a","b");
		DBApp.selectIndex("j486a", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5]
		String tableTrace0 = DBApp.getLastTrace("j486a");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, b, c, e, h, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, f, g, i, j, k, l]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k7b", cols1);
		String [][] records_k7b = new String[237][cols1.length];
		for(int i=0;i<237;i++)
		{
			records_k7b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k7b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k7b", records_k7b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12]
		String[] ConditionColumns1 = {"d","h","f"};
		String[] ConditionColumnsValues1 = {"d2","h6","f0"};
		DBApp.createBitMapIndex("k7b","d");
		DBApp.selectIndex("k7b", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 10, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record2: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		//record3: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8]
		//record4: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4]
		String tableTrace1 = DBApp.getLastTrace("k7b");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[f, h]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+10));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j0g", cols0);
		String [][] records_j0g = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_j0g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j0g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j0g", records_j0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		String[] ConditionColumns0 = {"c","f","e"};
		String[] ConditionColumnsValues0 = {"c2","f5","e0"};
		DBApp.createBitMapIndex("j0g","e");
		DBApp.selectIndex("j0g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		String tableTrace0 = DBApp.getLastTrace("j0g");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jm2", cols0);
		String [][] records_jm2 = new String[367][cols0.length];
		for(int i=0;i<367;i++)
		{
			records_jm2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jm2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jm2", records_jm2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record362: [a362, b0, c2, d2, e2, f2, g5]
		//record363: [a363, b1, c0, d3, e3, f3, g6]
		//record364: [a364, b0, c1, d0, e4, f4, g0]
		//record365: [a365, b1, c2, d1, e0, f5, g1]
		//record366: [a366, b0, c0, d2, e1, f0, g2]
		String[] ConditionColumns0 = {"g","b","d"};
		String[] ConditionColumnsValues0 = {"g4","b1","d3"};
		DBApp.createBitMapIndex("jm2","g");
		DBApp.selectIndex("jm2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a39, b1, c0, d3, e4, f3, g4]
		//record2: [a67, b1, c1, d3, e2, f1, g4]
		//record3: [a95, b1, c2, d3, e0, f5, g4]
		//record4: [a123, b1, c0, d3, e3, f3, g4]
		String tableTrace0 = DBApp.getLastTrace("jm2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[b, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+13));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e19", cols0);
		String [][] records_e19 = new String[163][cols0.length];
		for(int i=0;i<163;i++)
		{
			records_e19[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e19[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e19", records_e19[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2]
		String[] ConditionColumns0 = {"f","e","h","a","b","g","j"};
		String[] ConditionColumnsValues0 = {"f3","e0","h3","a75","b1","g5","j5"};
		DBApp.createBitMapIndex("e19","f");
		DBApp.createBitMapIndex("e19","b");
		DBApp.createBitMapIndex("e19","g");
		DBApp.selectIndex("e19", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5]
		String tableTrace0 = DBApp.getLastTrace("e19");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[a, e, h, j]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("k77o0", cols1);
		String [][] records_k77o0 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			records_k77o0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k77o0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k77o0", records_k77o0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		String[] ConditionColumns1 = {"b","j","k","g","h","e","f","c"};
		String[] ConditionColumnsValues1 = {"b1","j1","k10","g0","h5","e1","f3","c0"};
		DBApp.createBitMapIndex("k77o0","j");
		DBApp.createBitMapIndex("k77o0","g");
		DBApp.createBitMapIndex("k77o0","e");
		DBApp.createBitMapIndex("k77o0","c");
		DBApp.selectIndex("k77o0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		String tableTrace1 = DBApp.getLastTrace("k77o0");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, e, g, j]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[b, f, h, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("j4y", cols2);
		String [][] records_j4y = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			records_j4y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j4y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j4y", records_j4y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9, p6, q3, r0]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10, p7, q4, r1]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0, o11, p8, q5, r2]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1, o12, p9, q6, r3]
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3, l10, m6, n2, o13, p10, q7, r4]
		String[] ConditionColumns2 = {"h","m","c","f","o","l","q","r","g","p","k","i","j","b","n"};
		String[] ConditionColumnsValues2 = {"h7","m5","c1","f1","o1","l7","q14","r13","g3","p15","k9","i4","j1","b1","n3"};
		DBApp.createBitMapIndex("j4y","h");
		DBApp.createBitMapIndex("j4y","m");
		DBApp.createBitMapIndex("j4y","l");
		DBApp.createBitMapIndex("j4y","r");
		DBApp.createBitMapIndex("j4y","g");
		DBApp.createBitMapIndex("j4y","p");
		DBApp.createBitMapIndex("j4y","j");
		DBApp.createBitMapIndex("j4y","b");
		DBApp.createBitMapIndex("j4y","n");
		DBApp.selectIndex("j4y", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		String tableTrace2 = DBApp.getLastTrace("j4y");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[b, g, h, j, l, m, n, p, r]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, f, i, k, o, q]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("q7", cols0);
		String [][] records_q7 = new String[111][cols0.length];
		for(int i=0;i<111;i++)
		{
			records_q7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q7", records_q7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10, m2, n8, o1, p10]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14]
		String[] ConditionColumns0 = {"l","p","d","k","i","g","a","j","n","h","e","o"};
		String[] ConditionColumnsValues0 = {"l1","p5","d1","k8","i4","g1","a85","j5","n1","h5","e0","o10"};
		DBApp.createBitMapIndex("q7","g");
		DBApp.createBitMapIndex("q7","a");
		DBApp.createBitMapIndex("q7","h");
		DBApp.createBitMapIndex("q7","e");
		DBApp.createBitMapIndex("q7","o");
		DBApp.selectIndex("q7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10, p5]
		String tableTrace0 = DBApp.getLastTrace("q7");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, e, g, h, o]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, i, j, k, l, n, p]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l63", cols0);
		String [][] records_l63 = new String[458][cols0.length];
		for(int i=0;i<458;i++)
		{
			records_l63[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l63[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l63", records_l63[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record453: [a453, b1, c0, d1, e3, f3, g5]
		//record454: [a454, b0, c1, d2, e4, f4, g6]
		//record455: [a455, b1, c2, d3, e0, f5, g0]
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		String[] ConditionColumns0 = {"f","d","c"};
		String[] ConditionColumnsValues0 = {"f4","d2","c1"};
		DBApp.createBitMapIndex("l63","d");
		DBApp.selectIndex("l63", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 38, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		//record1: [a22, b0, c1, d2, e2, f4, g1]
		//record2: [a34, b0, c1, d2, e4, f4, g6]
		//record3: [a46, b0, c1, d2, e1, f4, g4]
		//record4: [a58, b0, c1, d2, e3, f4, g2]
		String tableTrace0 = DBApp.getLastTrace("l63");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+38));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ub0m9", cols1);
		String [][] records_ub0m9 = new String[429][cols1.length];
		for(int i=0;i<429;i++)
		{
			records_ub0m9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ub0m9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ub0m9", records_ub0m9[i]);
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
		String[] ConditionColumns1 = {"f","g","b","c","a","d"};
		String[] ConditionColumnsValues1 = {"f5","g2","b1","c2","a149","d1"};
		DBApp.createBitMapIndex("ub0m9","g");
		DBApp.createBitMapIndex("ub0m9","c");
		DBApp.selectIndex("ub0m9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a149, b1, c2, d1, e4, f5, g2]
		String tableTrace1 = DBApp.getLastTrace("ub0m9");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, d, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("az85", cols2);
		String [][] records_az85 = new String[335][cols2.length];
		for(int i=0;i<335;i++)
		{
			records_az85[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_az85[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("az85", records_az85[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record330: [a330, b0, c0, d2, e0, f0, g1]
		//record331: [a331, b1, c1, d3, e1, f1, g2]
		//record332: [a332, b0, c2, d0, e2, f2, g3]
		//record333: [a333, b1, c0, d1, e3, f3, g4]
		//record334: [a334, b0, c1, d2, e4, f4, g5]
		String[] ConditionColumns2 = {"a","g","d"};
		String[] ConditionColumnsValues2 = {"a192","g3","d0"};
		DBApp.createBitMapIndex("az85","a");
		DBApp.selectIndex("az85", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a192, b0, c0, d0, e2, f0, g3]
		String tableTrace2 = DBApp.getLastTrace("az85");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[d, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gzn", cols3);
		String [][] records_gzn = new String[499][cols3.length];
		for(int i=0;i<499;i++)
		{
			records_gzn[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gzn[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gzn", records_gzn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		//record497: [a497, b1, c2, d1, e2, f5, g0]
		//record498: [a498, b0, c0, d2, e3, f0, g1]
		String[] ConditionColumns3 = {"g","d","e","a","b","f"};
		String[] ConditionColumnsValues3 = {"g4","d1","e3","a193","b1","f1"};
		DBApp.createBitMapIndex("gzn","g");
		DBApp.createBitMapIndex("gzn","a");
		DBApp.createBitMapIndex("gzn","f");
		DBApp.selectIndex("gzn", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a193, b1, c1, d1, e3, f1, g4]
		String tableTrace3 = DBApp.getLastTrace("gzn");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[a, f, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, d, e]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pvw", cols4);
		String [][] records_pvw = new String[87][cols4.length];
		for(int i=0;i<87;i++)
		{
			records_pvw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_pvw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("pvw", records_pvw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record82: [a82, b0, c1, d2, e2, f4, g5]
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		String[] ConditionColumns4 = {"e","c","d","f","g"};
		String[] ConditionColumnsValues4 = {"e2","c2","d3","f5","g5"};
		DBApp.createBitMapIndex("pvw","d");
		DBApp.createBitMapIndex("pvw","f");
		DBApp.selectIndex("pvw", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5]
		String tableTrace4 = DBApp.getLastTrace("pvw");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[d, f]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[c, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("f45", cols0);
		String [][] records_f45 = new String[158][cols0.length];
		for(int i=0;i<158;i++)
		{
			records_f45[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f45[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f45", records_f45[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4]
		String[] ConditionColumns0 = {"a","h","f","e","d"};
		String[] ConditionColumnsValues0 = {"a17","h1","f5","e2","d1"};
		DBApp.createBitMapIndex("f45","a");
		DBApp.createBitMapIndex("f45","h");
		DBApp.selectIndex("f45", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		String tableTrace0 = DBApp.getLastTrace("f45");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[a, h]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[d, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o9l", cols0);
		String [][] records_o9l = new String[210][cols0.length];
		for(int i=0;i<210;i++)
		{
			records_o9l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o9l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o9l", records_o9l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2]
		//record206: [a206, b0, c2, d2, e1, f2, g3]
		//record207: [a207, b1, c0, d3, e2, f3, g4]
		//record208: [a208, b0, c1, d0, e3, f4, g5]
		//record209: [a209, b1, c2, d1, e4, f5, g6]
		String[] ConditionColumns0 = {"c","e","d"};
		String[] ConditionColumnsValues0 = {"c1","e4","d3"};
		DBApp.createBitMapIndex("o9l","e");
		DBApp.selectIndex("o9l", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5]
		//record1: [a79, b1, c1, d3, e4, f1, g2]
		//record2: [a139, b1, c1, d3, e4, f1, g6]
		//record3: [a199, b1, c1, d3, e4, f1, g3]
		String tableTrace0 = DBApp.getLastTrace("o9l");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ck8", cols1);
		String [][] records_ck8 = new String[291][cols1.length];
		for(int i=0;i<291;i++)
		{
			records_ck8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ck8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ck8", records_ck8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0, n6, o1, p14, q14, r16, s1]
		//record287: [a287, b1, c2, d3, e2, f5, g0, h7, i8, j7, k1, l11, m1, n7, o2, p15, q15, r17, s2]
		//record288: [a288, b0, c0, d0, e3, f0, g1, h0, i0, j8, k2, l0, m2, n8, o3, p0, q16, r0, s3]
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3, n9, o4, p1, q0, r1, s4]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4, n10, o5, p2, q1, r2, s5]
		String[] ConditionColumns1 = {"n","h","j","c","l","o"};
		String[] ConditionColumnsValues1 = {"n13","h1","j5","c1","l1","o10"};
		DBApp.createBitMapIndex("ck8","n");
		DBApp.createBitMapIndex("ck8","h");
		DBApp.createBitMapIndex("ck8","j");
		DBApp.createBitMapIndex("ck8","c");
		DBApp.selectIndex("ck8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10, p9, q10, r13, s18]
		String tableTrace1 = DBApp.getLastTrace("ck8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, h, j, n]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[l, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w64", cols2);
		String [][] records_w64 = new String[426][cols2.length];
		for(int i=0;i<426;i++)
		{
			records_w64[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w64[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w64", records_w64[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1, m5, n1, o1]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2, m6, n2, o2]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3, m7, n3, o3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8, n4, o4]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9, n5, o5]
		String[] ConditionColumns2 = {"d","h","n","c","l","o","f"};
		String[] ConditionColumnsValues2 = {"d0","h0","n6","c2","l8","o14","f2"};
		DBApp.createBitMapIndex("w64","d");
		DBApp.createBitMapIndex("w64","n");
		DBApp.createBitMapIndex("w64","c");
		DBApp.createBitMapIndex("w64","l");
		DBApp.createBitMapIndex("w64","f");
		DBApp.selectIndex("w64", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14]
		String tableTrace2 = DBApp.getLastTrace("w64");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[c, d, f, l, n]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[h, o]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h167g", cols0);
		String [][] records_h167g = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_h167g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h167g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h167g", records_h167g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		//record497: [a497, b1, c2, d1, e2, f5, g0]
		String[] ConditionColumns0 = {"b","g","e"};
		String[] ConditionColumnsValues0 = {"b1","g4","e1"};
		DBApp.createBitMapIndex("h167g","b");
		DBApp.selectIndex("h167g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 7, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a81, b1, c0, d1, e1, f3, g4]
		//record2: [a151, b1, c1, d3, e1, f1, g4]
		//record3: [a221, b1, c2, d1, e1, f5, g4]
		//record4: [a291, b1, c0, d3, e1, f3, g4]
		String tableTrace0 = DBApp.getLastTrace("h167g");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[e, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+7));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("f9e", cols1);
		String [][] records_f9e = new String[326][cols1.length];
		for(int i=0;i<326;i++)
		{
			records_f9e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f9e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f9e", records_f9e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1]
		String[] ConditionColumns1 = {"k","b","g","i"};
		String[] ConditionColumnsValues1 = {"k3","b1","g6","i6"};
		DBApp.createBitMapIndex("f9e","b");
		DBApp.createBitMapIndex("f9e","i");
		DBApp.selectIndex("f9e", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9]
		String tableTrace1 = DBApp.getLastTrace("f9e");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[b, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[g, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("rp82", cols0);
		String [][] records_rp82 = new String[355][cols0.length];
		for(int i=0;i<355;i++)
		{
			records_rp82[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rp82[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rp82", records_rp82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0, o5, p14, q10, r8, s8]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3, m0, n1, o6, p15, q11, r9, s9]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4, m1, n2, o7, p0, q12, r10, s10]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5, m2, n3, o8, p1, q13, r11, s11]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6, m3, n4, o9, p2, q14, r12, s12]
		String[] ConditionColumns0 = {"i","c","d","k","m","j","p","o","e"};
		String[] ConditionColumnsValues0 = {"i7","c1","d0","k1","m11","j2","p8","o7","e2"};
		DBApp.createBitMapIndex("rp82","k");
		DBApp.createBitMapIndex("rp82","m");
		DBApp.createBitMapIndex("rp82","j");
		DBApp.selectIndex("rp82", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8, q11, r16, s4]
		String tableTrace0 = DBApp.getLastTrace("rp82");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[j, k, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, d, e, i, o, p]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y9", cols1);
		String [][] records_y9 = new String[484][cols1.length];
		for(int i=0;i<484;i++)
		{
			records_y9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y9", records_y9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record479: [a479, b1, c2, d3, e4, f5, g3]
		//record480: [a480, b0, c0, d0, e0, f0, g4]
		//record481: [a481, b1, c1, d1, e1, f1, g5]
		//record482: [a482, b0, c2, d2, e2, f2, g6]
		//record483: [a483, b1, c0, d3, e3, f3, g0]
		String[] ConditionColumns1 = {"f","b","c"};
		String[] ConditionColumnsValues1 = {"f1","b1","c1"};
		DBApp.createBitMapIndex("y9","b");
		DBApp.selectIndex("y9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 81, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		String tableTrace1 = DBApp.getLastTrace("y9");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+81));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("o64", cols0);
		String [][] records_o64 = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			records_o64[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o64[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o64", records_o64[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		String[] ConditionColumns0 = {"k","a","m","h","c","e","d"};
		String[] ConditionColumnsValues0 = {"k4","a4","m4","h4","c1","e4","d0"};
		DBApp.createBitMapIndex("o64","m");
		DBApp.createBitMapIndex("o64","c");
		DBApp.selectIndex("o64", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		String tableTrace0 = DBApp.getLastTrace("o64");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[c, m]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[a, d, e, h, k]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tx3", cols1);
		String [][] records_tx3 = new String[368][cols1.length];
		for(int i=0;i<368;i++)
		{
			records_tx3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tx3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tx3", records_tx3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record363: [a363, b1, c0, d3, e3, f3, g6]
		//record364: [a364, b0, c1, d0, e4, f4, g0]
		//record365: [a365, b1, c2, d1, e0, f5, g1]
		//record366: [a366, b0, c0, d2, e1, f0, g2]
		//record367: [a367, b1, c1, d3, e2, f1, g3]
		String[] ConditionColumns1 = {"g","f","e","b","c","d","a"};
		String[] ConditionColumnsValues1 = {"g6","f0","e1","b0","c0","d2","a6"};
		DBApp.createBitMapIndex("tx3","g");
		DBApp.createBitMapIndex("tx3","e");
		DBApp.createBitMapIndex("tx3","d");
		DBApp.selectIndex("tx3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		String tableTrace1 = DBApp.getLastTrace("tx3");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[d, e, g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[a, b, c, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m39", cols2);
		String [][] records_m39 = new String[140][cols2.length];
		for(int i=0;i<140;i++)
		{
			records_m39[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m39[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m39", records_m39[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9]
		String[] ConditionColumns2 = {"d","i","b","g","h","c","a"};
		String[] ConditionColumnsValues2 = {"d1","i6","b1","g5","h1","c0","a33"};
		DBApp.createBitMapIndex("m39","d");
		DBApp.createBitMapIndex("m39","b");
		DBApp.createBitMapIndex("m39","c");
		DBApp.createBitMapIndex("m39","a");
		DBApp.selectIndex("m39", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		String tableTrace2 = DBApp.getLastTrace("m39");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a, b, c, d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[g, h, i]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t335", cols3);
		String [][] records_t335 = new String[490][cols3.length];
		for(int i=0;i<490;i++)
		{
			records_t335[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_t335[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("t335", records_t335[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		//record486: [a486, b0, c0, d2, e1, f0, g3]
		//record487: [a487, b1, c1, d3, e2, f1, g4]
		//record488: [a488, b0, c2, d0, e3, f2, g5]
		//record489: [a489, b1, c0, d1, e4, f3, g6]
		String[] ConditionColumns3 = {"f","b","g"};
		String[] ConditionColumnsValues3 = {"f4","b0","g3"};
		DBApp.createBitMapIndex("t335","g");
		DBApp.selectIndex("t335", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 12, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		//record1: [a52, b0, c1, d0, e2, f4, g3]
		//record2: [a94, b0, c1, d2, e4, f4, g3]
		//record3: [a136, b0, c1, d0, e1, f4, g3]
		//record4: [a178, b0, c1, d2, e3, f4, g3]
		String tableTrace3 = DBApp.getLastTrace("t335");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[g]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[b, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+12));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x4", cols4);
		String [][] records_x4 = new String[355][cols4.length];
		for(int i=0;i<355;i++)
		{
			records_x4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x4", records_x4[i]);
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
		String[] ConditionColumns4 = {"b","c","g","e","a"};
		String[] ConditionColumnsValues4 = {"b0","c1","g3","e2","a262"};
		DBApp.createBitMapIndex("x4","b");
		DBApp.createBitMapIndex("x4","e");
		DBApp.createBitMapIndex("x4","a");
		DBApp.selectIndex("x4", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a262, b0, c1, d2, e2, f4, g3]
		String tableTrace4 = DBApp.getLastTrace("x4");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[a, b, e]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[c, g]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionSomeIndexedTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i7bhu", cols0);
		String [][] records_i7bhu = new String[297][cols0.length];
		for(int i=0;i<297;i++)
		{
			records_i7bhu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i7bhu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i7bhu", records_i7bhu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record292: [a292, b0, c1, d0, e2, f4, g5]
		//record293: [a293, b1, c2, d1, e3, f5, g6]
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		String[] ConditionColumns0 = {"f","e","d","c"};
		String[] ConditionColumnsValues0 = {"f3","e1","d3","c0"};
		DBApp.createBitMapIndex("i7bhu","d");
		DBApp.selectIndex("i7bhu", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 5, records:
		//record0: [a51, b1, c0, d3, e1, f3, g2]
		//record1: [a111, b1, c0, d3, e1, f3, g6]
		//record2: [a171, b1, c0, d3, e1, f3, g3]
		//record3: [a231, b1, c0, d3, e1, f3, g0]
		//record4: [a291, b1, c0, d3, e1, f3, g4]
		String tableTrace0 = DBApp.getLastTrace("i7bhu");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains("[c, e, f]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("tz", cols1);
		String [][] records_tz = new String[378][cols1.length];
		for(int i=0;i<378;i++)
		{
			records_tz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tz", records_tz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9, n9, o13, p5, q16, r13, s12]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10, n10, o14, p6, q0, r14, s13]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3, m11, n11, o0, p7, q1, r15, s14]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2, l4, m12, n12, o1, p8, q2, r16, s15]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1, i8, j7, k3, l5, m0, n13, o2, p9, q3, r17, s16]
		String[] ConditionColumns1 = {"e","f","n","i","c","h"};
		String[] ConditionColumnsValues1 = {"e3","f4","n8","i4","c1","h4"};
		DBApp.createBitMapIndex("tz","i");
		DBApp.createBitMapIndex("tz","c");
		DBApp.selectIndex("tz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13, p4, q12, r4, s15]
		String tableTrace1 = DBApp.getLastTrace("tz");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace1.contains("[c, i]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains("[e, f, h, n]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("yb", cols2);
		String [][] records_yb = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			records_yb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yb", records_yb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15, t15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16, t16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17, t17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18, t18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0, t19]
		String[] ConditionColumns2 = {"a","s","c","e","b","j","d","p","g","i","m"};
		String[] ConditionColumnsValues2 = {"a6","s6","c0","e1","b0","j6","d2","p6","g6","i6","m6"};
		DBApp.createBitMapIndex("yb","a");
		DBApp.createBitMapIndex("yb","b");
		DBApp.createBitMapIndex("yb","d");
		DBApp.createBitMapIndex("yb","p");
		DBApp.createBitMapIndex("yb","m");
		DBApp.selectIndex("yb", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6, t6]
		String tableTrace2 = DBApp.getLastTrace("yb");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace2.contains("[a, b, d, m, p]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains("[c, e, g, i, j, s]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("px5", cols3);
		String [][] records_px5 = new String[103][cols3.length];
		for(int i=0;i<103;i++)
		{
			records_px5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_px5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("px5", records_px5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record98: [a98, b0, c2, d2, e3, f2, g0]
		//record99: [a99, b1, c0, d3, e4, f3, g1]
		//record100: [a100, b0, c1, d0, e0, f4, g2]
		//record101: [a101, b1, c2, d1, e1, f5, g3]
		//record102: [a102, b0, c0, d2, e2, f0, g4]
		String[] ConditionColumns3 = {"b","d","c"};
		String[] ConditionColumnsValues3 = {"b0","d0","c0"};
		DBApp.createBitMapIndex("px5","b");
		DBApp.selectIndex("px5", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a12, b0, c0, d0, e2, f0, g5]
		//record2: [a24, b0, c0, d0, e4, f0, g3]
		//record3: [a36, b0, c0, d0, e1, f0, g1]
		//record4: [a48, b0, c0, d0, e3, f0, g6]
		String tableTrace3 = DBApp.getLastTrace("px5");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace3.contains("[b]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains("[c, d]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+9));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j3g2", cols4);
		String [][] records_j3g2 = new String[382][cols4.length];
		for(int i=0;i<382;i++)
		{
			records_j3g2[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j3g2[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j3g2", records_j3g2[i]);
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
		String[] ConditionColumns4 = {"i","g","j","c","d"};
		String[] ConditionColumnsValues4 = {"i0","g4","j7","c0","d3"};
		DBApp.createBitMapIndex("j3g2","j");
		DBApp.createBitMapIndex("j3g2","d");
		DBApp.selectIndex("j3g2", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7]
		String tableTrace4 = DBApp.getLastTrace("j3g2");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace4.contains("[d, j]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains("[c, g, i]"));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1006);
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