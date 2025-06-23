package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_08
{

	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("au9", cols0);
		String [][] records_au9 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			records_au9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_au9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("au9", records_au9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6, n1, o11, p7]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7, n2, o12, p8]
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1, m8, n3, o13, p9]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4, o14, p10]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("au9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 12, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 12);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3","p3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9","p9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0","p15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6","p5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3","m1","n13","o12","p11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f4", cols1);
		String [][] records_f4 = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			records_f4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f4", records_f4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3]
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("f4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 24);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lhd", cols0);
		String [][] records_lhd = new String[329][cols0.length];
		for(int i=0;i<329;i++)
		{
			records_lhd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lhd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lhd", records_lhd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2]
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("lhd", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 110, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 110);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v8", cols1);
		String [][] records_v8 = new String[286][cols1.length];
		for(int i=0;i<286;i++)
		{
			records_v8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v8", records_v8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10]
		String[] ConditionColumns1 = {"k","e","h","g","f"};
		String[] ConditionColumnsValues1 = {"k10","e1","h4","g6","f4"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("v8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a76","b0","c1","d0","e1","f4","g6","h4","i4","j6","k10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ve8", cols2);
		String [][] records_ve8 = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			records_ve8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ve8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ve8", records_ve8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("ve8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 27, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 27);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d9y8l", cols0);
		String [][] records_d9y8l = new String[492][cols0.length];
		for(int i=0;i<492;i++)
		{
			records_d9y8l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d9y8l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d9y8l", records_d9y8l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1, o11]
		String[] ConditionColumns0 = {"k","c","b","f","d","i","e"};
		String[] ConditionColumnsValues0 = {"k6","c0","b1","f3","d3","i3","e4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("d9y8l", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0","n11","o9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t9c", cols0);
		String [][] records_t9c = new String[316][cols0.length];
		for(int i=0;i<316;i++)
		{
			records_t9c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t9c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t9c", records_t9c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record311: [a311, b1, c2, d3, e1, f5, g3]
		//record312: [a312, b0, c0, d0, e2, f0, g4]
		//record313: [a313, b1, c1, d1, e3, f1, g5]
		//record314: [a314, b0, c2, d2, e4, f2, g6]
		//record315: [a315, b1, c0, d3, e0, f3, g0]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("t9c", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 105, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 105);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("z2q", cols1);
		String [][] records_z2q = new String[386][cols1.length];
		for(int i=0;i<386;i++)
		{
			records_z2q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z2q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z2q", records_z2q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3, o6, p13, q7]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4, o7, p14, q8]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6, o9, p0, q10]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10, p1, q11]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i6"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("z2q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 43, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		//record3: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 43);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6","o6","p6","q6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0","p15","q15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9","p8","q7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7","n5","o3","p1","q16"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3","n0","o12","p10","q8"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q04y", cols2);
		String [][] records_q04y = new String[321][cols2.length];
		for(int i=0;i<321;i++)
		{
			records_q04y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q04y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q04y", records_q04y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record316: [a316, b0, c1, d0, e1, f4, g1]
		//record317: [a317, b1, c2, d1, e2, f5, g2]
		//record318: [a318, b0, c0, d2, e3, f0, g3]
		//record319: [a319, b1, c1, d3, e4, f1, g4]
		//record320: [a320, b0, c2, d0, e0, f2, g5]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("q04y", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 81, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 81);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s9", cols3);
		String [][] records_s9 = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			records_s9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s9", records_s9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		String[] ConditionColumns3 = {"c","m","g","e","b"};
		String[] ConditionColumnsValues3 = {"c0","m6","g6","e1","b0"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("s9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6","o6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("aj", cols0);
		String [][] records_aj = new String[121][cols0.length];
		for(int i=0;i<121;i++)
		{
			records_aj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_aj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("aj", records_aj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7, i2, j9, k9]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10]
		String[] ConditionColumns0 = {"c","k","f"};
		String[] ConditionColumnsValues0 = {"c1","k2","f1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("aj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record1: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a79","b1","c1","d3","e4","f1","g2","h7","i7","j9","k2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q2po", cols1);
		String [][] records_q2po = new String[104][cols1.length];
		for(int i=0;i<104;i++)
		{
			records_q2po[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q2po[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q2po", records_q2po[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7]
		String[] ConditionColumns1 = {"e","a","h","l","k"};
		String[] ConditionColumnsValues1 = {"e4","a14","h6","l2","k3"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("q2po", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fko", cols2);
		String [][] records_fko = new String[489][cols2.length];
		for(int i=0;i<489;i++)
		{
			records_fko[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fko[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fko", records_fko[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record484: [a484, b0, c1, d0, e4, f4, g1]
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		//record486: [a486, b0, c0, d2, e1, f0, g3]
		//record487: [a487, b1, c1, d3, e2, f1, g4]
		//record488: [a488, b0, c2, d0, e3, f2, g5]
		String[] ConditionColumns2 = {"c","b","f"};
		String[] ConditionColumnsValues2 = {"c1","b0","f4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("fko", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 81, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 81);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o05", cols3);
		String [][] records_o05 = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			records_o05[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o05[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o05", records_o05[i]);
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
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("o05", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 8, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record3: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record4: [a17, b1, c2, d1, e2, f5, g3, h1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 8);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("nf9", cols0);
		String [][] records_nf9 = new String[120][cols0.length];
		for(int i=0;i<120;i++)
		{
			records_nf9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nf9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nf9", records_nf9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("nf9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 40, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 40);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u4dh", cols1);
		String [][] records_u4dh = new String[115][cols1.length];
		for(int i=0;i<115;i++)
		{
			records_u4dh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u4dh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u4dh", records_u4dh[i]);
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
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("u4dh", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 19, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 19);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("xj", cols0);
		String [][] records_xj = new String[350][cols0.length];
		for(int i=0;i<350;i++)
		{
			records_xj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xj", records_xj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9, q5, r3, s3]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6, r4, s4]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7, r5, s5]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8, r6, s6]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13, o4, p13, q9, r7, s7]
		String[] ConditionColumns0 = {"s"};
		String[] ConditionColumnsValues0 = {"s11"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("xj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 18, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11]
		//record2: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4, p1, q15, r13, s11]
		//record3: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0, r14, s11]
		//record4: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 18);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11","p11","q11","r11","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4","n2","o0","p14","q13","r12","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10","n7","o4","p1","q15","r13","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a68","b0","c2","d0","e3","f2","g5","h4","i5","j8","k2","l8","m3","n12","o8","p4","q0","r14","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a87","b1","c0","d3","e2","f3","g3","h7","i6","j7","k10","l3","m9","n3","o12","p7","q2","r15","s11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mc", cols1);
		String [][] records_mc = new String[106][cols1.length];
		for(int i=0;i<106;i++)
		{
			records_mc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mc", records_mc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record101: [a101, b1, c2, d1, e1, f5, g3]
		//record102: [a102, b0, c0, d2, e2, f0, g4]
		//record103: [a103, b1, c1, d3, e3, f1, g5]
		//record104: [a104, b0, c2, d0, e4, f2, g6]
		//record105: [a105, b1, c0, d1, e0, f3, g0]
		String[] ConditionColumns1 = {"b","d"};
		String[] ConditionColumnsValues1 = {"b1","d1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("mc", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 27, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a9, b1, c0, d1, e4, f3, g2]
		//record3: [a13, b1, c1, d1, e3, f1, g6]
		//record4: [a17, b1, c2, d1, e2, f5, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 27);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zlhi", cols0);
		String [][] records_zlhi = new String[146][cols0.length];
		for(int i=0;i<146;i++)
		{
			records_zlhi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zlhi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zlhi", records_zlhi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5]
		String[] ConditionColumns0 = {"m","h","b"};
		String[] ConditionColumnsValues0 = {"m1","h6","b0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("zlhi", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record1: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1, n6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a118","b0","c1","d2","e3","f4","g6","h6","i1","j8","k8","l10","m1","n6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ei688", cols1);
		String [][] records_ei688 = new String[477][cols1.length];
		for(int i=0;i<477;i++)
		{
			records_ei688[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ei688[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ei688", records_ei688[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record472: [a472, b0, c1, d0, e2, f4, g3]
		//record473: [a473, b1, c2, d1, e3, f5, g4]
		//record474: [a474, b0, c0, d2, e4, f0, g5]
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		String[] ConditionColumns1 = {"d","c","e"};
		String[] ConditionColumnsValues1 = {"d3","c1","e0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ei688", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a55, b1, c1, d3, e0, f1, g6]
		//record1: [a115, b1, c1, d3, e0, f1, g3]
		//record2: [a175, b1, c1, d3, e0, f1, g0]
		//record3: [a235, b1, c1, d3, e0, f1, g4]
		//record4: [a295, b1, c1, d3, e0, f1, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 8);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a115","b1","c1","d3","e0","f1","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a175","b1","c1","d3","e0","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a235","b1","c1","d3","e0","f1","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a295","b1","c1","d3","e0","f1","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g96v", cols2);
		String [][] records_g96v = new String[203][cols2.length];
		for(int i=0;i<203;i++)
		{
			records_g96v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g96v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g96v", records_g96v[i]);
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
		String[] ConditionColumns2 = {"f","e","b"};
		String[] ConditionColumnsValues2 = {"f1","e2","b1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("g96v", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0]
		//record1: [a37, b1, c1, d1, e2, f1, g2]
		//record2: [a67, b1, c1, d3, e2, f1, g4]
		//record3: [a97, b1, c1, d1, e2, f1, g6]
		//record4: [a127, b1, c1, d3, e2, f1, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 7);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a67","b1","c1","d3","e2","f1","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a97","b1","c1","d1","e2","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a127","b1","c1","d3","e2","f1","g1"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("en2", cols3);
		String [][] records_en2 = new String[394][cols3.length];
		for(int i=0;i<394;i++)
		{
			records_en2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_en2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("en2", records_en2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1]
		String[] ConditionColumns3 = {"a","c"};
		String[] ConditionColumnsValues3 = {"a32","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("en2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a32, b0, c2, d0, e2, f2, g4, h0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o8", cols4);
		String [][] records_o8 = new String[479][cols4.length];
		for(int i=0;i<479;i++)
		{
			records_o8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o8", records_o8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record474: [a474, b0, c0, d2, e4, f0, g5]
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		String[] ConditionColumns4 = {"d","f","b"};
		String[] ConditionColumnsValues4 = {"d0","f0","b0"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("o8", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 40, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a12, b0, c0, d0, e2, f0, g5]
		//record2: [a24, b0, c0, d0, e4, f0, g3]
		//record3: [a36, b0, c0, d0, e1, f0, g1]
		//record4: [a48, b0, c0, d0, e3, f0, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 40);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a48","b0","c0","d0","e3","f0","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jxd", cols0);
		String [][] records_jxd = new String[265][cols0.length];
		for(int i=0;i<265;i++)
		{
			records_jxd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jxd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jxd", records_jxd[i]);
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
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("jxd", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 132, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 132);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("vuv", cols0);
		String [][] records_vuv = new String[491][cols0.length];
		for(int i=0;i<491;i++)
		{
			records_vuv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vuv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vuv", records_vuv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5, n10, o6, p6, q10, r0, s11]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7, p7, q11, r1, s12]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8, p8, q12, r2, s13]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9, p9, q13, r3, s14]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10, p10, q14, r4, s15]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("vuv", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 70, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 70);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4","q4","r4","s4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11","p11","q11","r11","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3","p2","q1","r0","s18"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10","p9","q8","r7","s6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2","p0","q15","r14","s13"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("m96", cols1);
		String [][] records_m96 = new String[434][cols1.length];
		for(int i=0;i<434;i++)
		{
			records_m96[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m96[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m96", records_m96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9, p13, q4]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10, p14, q5]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15, q6]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8]
		String[] ConditionColumns1 = {"b","c","p","m","o"};
		String[] ConditionColumnsValues1 = {"b0","c0","p14","m0","o3"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("m96", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3, p14, q10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a78","b0","c0","d2","e3","f0","g1","h6","i6","j8","k1","l6","m0","n8","o3","p14","q10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i04r", cols0);
		String [][] records_i04r = new String[324][cols0.length];
		for(int i=0;i<324;i++)
		{
			records_i04r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i04r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i04r", records_i04r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1, g4]
		//record320: [a320, b0, c2, d0, e0, f2, g5]
		//record321: [a321, b1, c0, d1, e1, f3, g6]
		//record322: [a322, b0, c1, d2, e2, f4, g0]
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("i04r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 46, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a18, b0, c0, d2, e3, f0, g4]
		//record3: [a25, b1, c1, d1, e0, f1, g4]
		//record4: [a32, b0, c2, d0, e2, f2, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 46);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p6d", cols1);
		String [][] records_p6d = new String[384][cols1.length];
		for(int i=0;i<384;i++)
		{
			records_p6d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p6d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p6d", records_p6d[i]);
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
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f5"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("p6d", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 64, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 64);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pm", cols2);
		String [][] records_pm = new String[469][cols2.length];
		for(int i=0;i<469;i++)
		{
			records_pm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pm", records_pm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record464: [a464, b0, c2, d0, e4, f2, g2]
		//record465: [a465, b1, c0, d1, e0, f3, g3]
		//record466: [a466, b0, c1, d2, e1, f4, g4]
		//record467: [a467, b1, c2, d3, e2, f5, g5]
		//record468: [a468, b0, c0, d0, e3, f0, g6]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("pm", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 67, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a21, b1, c0, d1, e1, f3, g0]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 67);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k2ec", cols3);
		String [][] records_k2ec = new String[294][cols3.length];
		for(int i=0;i<294;i++)
		{
			records_k2ec[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k2ec[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k2ec", records_k2ec[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3, n9]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4, n10]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5, n11]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7, n13]
		String[] ConditionColumns3 = {"a","m","l","h","j"};
		String[] ConditionColumnsValues3 = {"a279","m6","l3","h7","j9"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("k2ec", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9, k4, l3, m6, n13]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a279","b1","c0","d3","e4","f3","g6","h7","i0","j9","k4","l3","m6","n13"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qgl", cols4);
		String [][] records_qgl = new String[466][cols4.length];
		for(int i=0;i<466;i++)
		{
			records_qgl[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_qgl[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("qgl", records_qgl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2, j1, k10, l5, m6, n13]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0, l6, m7, n0]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7, m8, n1]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8, m9, n2]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9, m10, n3]
		String[] ConditionColumns4 = {"l"};
		String[] ConditionColumnsValues4 = {"l4"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("qgl", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 39, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 39);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("c00", cols0);
		String [][] records_c00 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			records_c00[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c00[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c00", records_c00[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j5"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("c00", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 9);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("gn", cols1);
		String [][] records_gn = new String[451][cols1.length];
		for(int i=0;i<451;i++)
		{
			records_gn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gn", records_gn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2, m4, n12, o11, p14]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13, o12, p15]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0, o13, p0]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1, o14, p1]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2, o0, p2]
		String[] ConditionColumns1 = {"o"};
		String[] ConditionColumnsValues1 = {"o10"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("gn", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 30, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9]
		//record2: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8]
		//record3: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10, p7]
		//record4: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 30);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10","o10","p10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10","p9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10","p8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a55","b1","c1","d3","e0","f1","g6","h7","i1","j5","k0","l7","m3","n13","o10","p7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7","j0","k4","l10","m5","n0","o10","p6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("hs8", cols2);
		String [][] records_hs8 = new String[78][cols2.length];
		for(int i=0;i<78;i++)
		{
			records_hs8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hs8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hs8", records_hs8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1, m8, n3]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3, m10, n5]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11, n6]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7]
		String[] ConditionColumns2 = {"a","b","l","i","g","k","n"};
		String[] ConditionColumnsValues2 = {"a19","b1","l7","i1","g5","k8","n5"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("hs8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("u2", cols3);
		String [][] records_u2 = new String[173][cols3.length];
		for(int i=0;i<173;i++)
		{
			records_u2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u2", records_u2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3, o6, p11]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7, p12]
		String[] ConditionColumns3 = {"h","j"};
		String[] ConditionColumnsValues3 = {"h5","j1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("u2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record1: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1, p13]
		//record2: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5]
		//record3: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1, o6, p13]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 4);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6","p5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5","h5","i7","j1","k6","l1","m9","n5","o1","p13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a101","b1","c2","d1","e1","f5","g3","h5","i2","j1","k2","l5","m10","n3","o11","p5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a141","b1","c0","d1","e1","f3","g1","h5","i6","j1","k9","l9","m11","n1","o6","p13"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("bj", cols0);
		String [][] records_bj = new String[360][cols0.length];
		for(int i=0;i<360;i++)
		{
			records_bj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bj", records_bj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5, o10, p3, q15, r13]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6, o11, p4, q16, r14]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12, p5, q0, r15]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13, p6, q1, r16]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9, o14, p7, q2, r17]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("bj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 45, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 45);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4","q4","r4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12","q12","r12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5","p4","q3","r2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13","p12","q11","r10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8","o6","p4","q2","r0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("jn8", cols1);
		String [][] records_jn8 = new String[348][cols1.length];
		for(int i=0;i<348;i++)
		{
			records_jn8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jn8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jn8", records_jn8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5, n7, o13, p7]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14, p8]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("jn8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 50, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 50);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1","p1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8","p8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0","p15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8","o7","p6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3","n1","o14","p13"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i9", cols2);
		String [][] records_i9 = new String[65][cols2.length];
		for(int i=0;i<65;i++)
		{
			records_i9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1]
		String[] ConditionColumns2 = {"d","b","i"};
		String[] ConditionColumnsValues2 = {"d1","b1","i6"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("i9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("hl6", cols0);
		String [][] records_hl6 = new String[150][cols0.length];
		for(int i=0;i<150;i++)
		{
			records_hl6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hl6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hl6", records_hl6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10, p1, q9, r1, s12]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10, r2, s13]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12, p3, q11, r3, s14]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13, p4, q12, r4, s15]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6, n9, o14, p5, q13, r5, s16]
		String[] ConditionColumns0 = {"k","q"};
		String[] ConditionColumnsValues0 = {"k6","q10"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("hl6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1, p13, q10, r7, s4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5","h5","i7","j1","k6","l1","m9","n5","o1","p13","q10","r7","s4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e63n4", cols1);
		String [][] records_e63n4 = new String[200][cols1.length];
		for(int i=0;i<200;i++)
		{
			records_e63n4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e63n4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e63n4", records_e63n4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record195: [a195, b1, c0, d3, e0, f3, g6]
		//record196: [a196, b0, c1, d0, e1, f4, g0]
		//record197: [a197, b1, c2, d1, e2, f5, g1]
		//record198: [a198, b0, c0, d2, e3, f0, g2]
		//record199: [a199, b1, c1, d3, e4, f1, g3]
		String[] ConditionColumns1 = {"e","f"};
		String[] ConditionColumnsValues1 = {"e2","f0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("e63n4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a42, b0, c0, d2, e2, f0, g0]
		//record2: [a72, b0, c0, d0, e2, f0, g2]
		//record3: [a102, b0, c0, d2, e2, f0, g4]
		//record4: [a132, b0, c0, d0, e2, f0, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 7);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a102","b0","c0","d2","e2","f0","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a132","b0","c0","d0","e2","f0","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("df", cols0);
		String [][] records_df = new String[442][cols0.length];
		for(int i=0;i<442;i++)
		{
			records_df[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_df[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("df", records_df[i]);
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
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("df", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 74, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a24, b0, c0, d0, e4, f0, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 74);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("u61g", cols1);
		String [][] records_u61g = new String[351][cols1.length];
		for(int i=0;i<351;i++)
		{
			records_u61g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u61g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u61g", records_u61g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6, r4, s4, t6]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7, r5, s5, t7]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8, r6, s6, t8]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13, o4, p13, q9, r7, s7, t9]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2, m12, n0, o5, p14, q10, r8, s8, t10]
		String[] ConditionColumns1 = {"m","c"};
		String[] ConditionColumnsValues1 = {"m10","c1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("u61g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10, t10]
		//record1: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4, p1, q15, r13, s11, t9]
		//record2: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4, o13, p8, q3, r16, s12, t8]
		//record3: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7, p15, q8, r1, s13, t7]
		//record4: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13, r4, s14, t6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 9);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10","o10","p10","q10","r10","s10","t10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10","n7","o4","p1","q15","r13","s11","t9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a88","b0","c1","d0","e3","f4","g4","h0","i7","j8","k0","l4","m10","n4","o13","p8","q3","r16","s12","t8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a127","b1","c1","d3","e2","f1","g1","h7","i1","j7","k6","l7","m10","n1","o7","p15","q8","r1","s13","t7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a166","b0","c1","d2","e1","f4","g5","h6","i4","j6","k1","l10","m10","n12","o1","p6","q13","r4","s14","t6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ax75", cols2);
		String [][] records_ax75 = new String[240][cols2.length];
		for(int i=0;i<240;i++)
		{
			records_ax75[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ax75[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ax75", records_ax75[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("ax75", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 120, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 120);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o3", cols0);
		String [][] records_o3 = new String[205][cols0.length];
		for(int i=0;i<205;i++)
		{
			records_o3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o3", records_o3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record200: [a200, b0, c2, d0, e0, f2, g4, h0, i2, j0, k2]
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1, i3, j1, k3]
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2, i4, j2, k4]
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5, j3, k5]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4, k6]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k9"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("o3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 18, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record4: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 18);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("nk6v", cols1);
		String [][] records_nk6v = new String[60][cols1.length];
		for(int i=0;i<60;i++)
		{
			records_nk6v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nk6v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nk6v", records_nk6v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2]
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3]
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("nk6v", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r1n", cols2);
		String [][] records_r1n = new String[347][cols2.length];
		for(int i=0;i<347;i++)
		{
			records_r1n[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r1n[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r1n", records_r1n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8]
		String[] ConditionColumns2 = {"l","j","k"};
		String[] ConditionColumnsValues2 = {"l10","j0","k10"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("r1n", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xbknj", cols3);
		String [][] records_xbknj = new String[204][cols3.length];
		for(int i=0;i<204;i++)
		{
			records_xbknj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xbknj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xbknj", records_xbknj[i]);
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
		String[] ConditionColumns3 = {"c","d"};
		String[] ConditionColumnsValues3 = {"c1","d0"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("xbknj", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 17, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a28, b0, c1, d0, e3, f4, g0]
		//record3: [a40, b0, c1, d0, e0, f4, g5]
		//record4: [a52, b0, c1, d0, e2, f4, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 17);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v8f", cols4);
		String [][] records_v8f = new String[230][cols4.length];
		for(int i=0;i<230;i++)
		{
			records_v8f[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_v8f[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("v8f", records_v8f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9]
		String[] ConditionColumns4 = {"g","c"};
		String[] ConditionColumnsValues4 = {"g6","c2"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("v8f", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 10, records:
		//record0: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record1: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1]
		//record2: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2]
		//record3: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3]
		//record4: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 10);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a62","b0","c2","d2","e2","f2","g6","h6","i8","j2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6","h3","i2","j3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a104","b0","c2","d0","e4","f2","g6","h0","i5","j4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dg", cols0);
		String [][] records_dg = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_dg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dg", records_dg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0]
		String[] ConditionColumns0 = {"m","e","a","c"};
		String[] ConditionColumnsValues0 = {"m9","e0","a100","c1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("dg", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a100","b0","c1","d0","e0","f4","g2","h4","i1","j0","k1","l4","m9"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n0se6", cols1);
		String [][] records_n0se6 = new String[238][cols1.length];
		for(int i=0;i<238;i++)
		{
			records_n0se6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n0se6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n0se6", records_n0se6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("n0se6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record4: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 24);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9","l6","m3","n0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("m516", cols2);
		String [][] records_m516 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			records_m516[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m516[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m516", records_m516[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10, s8, t6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11, s9, t7]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12, s10, t8]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4, p1, q15, r13, s11, t9]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5, p2, q16, r14, s12, t10]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("m516", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 9, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7, t7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13, t13]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0, t19]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6, t5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 9);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1","p1","q1","r1","s1","t1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7","o7","p7","q7","r7","s7","t7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13","p13","q13","r13","s13","t13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5","o4","p3","q2","r1","s0","t19"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10","p9","q8","r7","s6","t5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("yxj", cols0);
		String [][] records_yxj = new String[266][cols0.length];
		for(int i=0;i<266;i++)
		{
			records_yxj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yxj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yxj", records_yxj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a252"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("yxj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a252","b0","c0","d0","e2","f0","g0","h4","i0","j2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j724t", cols1);
		String [][] records_j724t = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_j724t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j724t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j724t", records_j724t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("j724t", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a17, b1, c2, d1, e2, f5, g3]
		//record4: [a22, b0, c1, d2, e2, f4, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 7);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a55", cols2);
		String [][] records_a55 = new String[223][cols2.length];
		for(int i=0;i<223;i++)
		{
			records_a55[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a55[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a55", records_a55[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("a55", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 111, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 111);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f4", cols3);
		String [][] records_f4 = new String[132][cols3.length];
		for(int i=0;i<132;i++)
		{
			records_f4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f4", records_f4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		String[] ConditionColumns3 = {"j"};
		String[] ConditionColumnsValues3 = {"j4"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("f4", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 13, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record4: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 13);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("gi9p7", cols0);
		String [][] records_gi9p7 = new String[429][cols0.length];
		for(int i=0;i<429;i++)
		{
			records_gi9p7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gi9p7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gi9p7", records_gi9p7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4, m8]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i7"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("gi9p7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 47, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record4: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 47);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w1139", cols1);
		String [][] records_w1139 = new String[431][cols1.length];
		for(int i=0;i<431;i++)
		{
			records_w1139[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w1139[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w1139", records_w1139[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("w1139", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 54, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10]
		//record4: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 54);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("eup", cols2);
		String [][] records_eup = new String[365][cols2.length];
		for(int i=0;i<365;i++)
		{
			records_eup[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eup[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eup", records_eup[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9, n10, o0, p8, q3]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11, o1, p9, q4]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11, n12, o2, p10, q5]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3, m12, n13, o3, p11, q6]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4, p12, q7]
		String[] ConditionColumns2 = {"p"};
		String[] ConditionColumnsValues2 = {"p4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("eup", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 23, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record2: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2]
		//record3: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7, p4, q1]
		//record4: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 23);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4","q4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5","p4","q3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8","o6","p4","q2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10","o7","p4","q1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a68","b0","c2","d0","e3","f2","g5","h4","i5","j8","k2","l8","m3","n12","o8","p4","q0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mx", cols0);
		String [][] records_mx = new String[258][cols0.length];
		for(int i=0;i<258;i++)
		{
			records_mx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mx", records_mx[i]);
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
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("mx", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 129, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 129);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("va", cols0);
		String [][] records_va = new String[171][cols0.length];
		for(int i=0;i<171;i++)
		{
			records_va[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_va[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("va", records_va[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		DBApp.selectIndex("va", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 57, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record2: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record3: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record4: [a13, b1, c1, d1, e3, f1, g6, h5]
		String tableTrace0 = DBApp.getLastTrace("va");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+57));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fm70", cols0);
		String [][] records_fm70 = new String[169][cols0.length];
		for(int i=0;i<169;i++)
		{
			records_fm70[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fm70[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fm70", records_fm70[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9, n11, o0]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m9"};
		DBApp.selectIndex("fm70", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record1: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record3: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3]
		//record4: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1]
		String tableTrace0 = DBApp.getLastTrace("fm70");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+13));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("or5e9", cols1);
		String [][] records_or5e9 = new String[220][cols1.length];
		for(int i=0;i<220;i++)
		{
			records_or5e9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_or5e9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("or5e9", records_or5e9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g5"};
		DBApp.selectIndex("or5e9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 31, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		String tableTrace1 = DBApp.getLastTrace("or5e9");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+31));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r0", cols2);
		String [][] records_r0 = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			records_r0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r0", records_r0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0]
		String[] ConditionColumns2 = {"c","d","g"};
		String[] ConditionColumnsValues2 = {"c2","d1","g6"};
		DBApp.selectIndex("r0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1]
		String tableTrace2 = DBApp.getLastTrace("r0");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s66", cols3);
		String [][] records_s66 = new String[353][cols3.length];
		for(int i=0;i<353;i++)
		{
			records_s66[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s66[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s66", records_s66[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5]
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0]
		String[] ConditionColumns3 = {"f","c"};
		String[] ConditionColumnsValues3 = {"f1","c1"};
		DBApp.selectIndex("s66", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 59, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1]
		String tableTrace3 = DBApp.getLastTrace("s66");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+59));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e0q", cols0);
		String [][] records_e0q = new String[487][cols0.length];
		for(int i=0;i<487;i++)
		{
			records_e0q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e0q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e0q", records_e0q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		DBApp.selectIndex("e0q", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 162, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		String tableTrace0 = DBApp.getLastTrace("e0q");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+162));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h7b3", cols1);
		String [][] records_h7b3 = new String[194][cols1.length];
		for(int i=0;i<194;i++)
		{
			records_h7b3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h7b3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h7b3", records_h7b3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7, o9, p13]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8, o10, p14]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4, l11, m9, n9, o11, p15]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10, n10, o12, p0]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11, o13, p1]
		String[] ConditionColumns1 = {"k","n","g","a","d"};
		String[] ConditionColumnsValues1 = {"k2","n10","g3","a24","d0"};
		DBApp.selectIndex("h7b3", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		String tableTrace1 = DBApp.getLastTrace("h7b3");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("asy", cols2);
		String [][] records_asy = new String[478][cols2.length];
		for(int i=0;i<478;i++)
		{
			records_asy[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_asy[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("asy", records_asy[i]);
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
		String[] ConditionColumns2 = {"b","c","f"};
		String[] ConditionColumnsValues2 = {"b1","c1","f1"};
		DBApp.selectIndex("asy", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 80, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		String tableTrace2 = DBApp.getLastTrace("asy");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+80));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pmv", cols0);
		String [][] records_pmv = new String[352][cols0.length];
		for(int i=0;i<352;i++)
		{
			records_pmv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pmv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pmv", records_pmv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record347: [a347, b1, c2, d3, e2, f5, g4]
		//record348: [a348, b0, c0, d0, e3, f0, g5]
		//record349: [a349, b1, c1, d1, e4, f1, g6]
		//record350: [a350, b0, c2, d2, e0, f2, g0]
		//record351: [a351, b1, c0, d3, e1, f3, g1]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d2"};
		DBApp.selectIndex("pmv", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 88, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		String tableTrace0 = DBApp.getLastTrace("pmv");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+88));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p4u", cols0);
		String [][] records_p4u = new String[383][cols0.length];
		for(int i=0;i<383;i++)
		{
			records_p4u[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p4u[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p4u", records_p4u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		String[] ConditionColumns0 = {"a","g","b"};
		String[] ConditionColumnsValues0 = {"a122","g3","b0"};
		DBApp.selectIndex("p4u", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a122, b0, c2, d2, e2, f2, g3]
		String tableTrace0 = DBApp.getLastTrace("p4u");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w5", cols1);
		String [][] records_w5 = new String[101][cols1.length];
		for(int i=0;i<101;i++)
		{
			records_w5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w5", records_w5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record96: [a96, b0, c0, d0, e1, f0, g5]
		//record97: [a97, b1, c1, d1, e2, f1, g6]
		//record98: [a98, b0, c2, d2, e3, f2, g0]
		//record99: [a99, b1, c0, d3, e4, f3, g1]
		//record100: [a100, b0, c1, d0, e0, f4, g2]
		String[] ConditionColumns1 = {"d","f"};
		String[] ConditionColumnsValues1 = {"d2","f4"};
		DBApp.selectIndex("w5", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3]
		//record1: [a22, b0, c1, d2, e2, f4, g1]
		//record2: [a34, b0, c1, d2, e4, f4, g6]
		//record3: [a46, b0, c1, d2, e1, f4, g4]
		//record4: [a58, b0, c1, d2, e3, f4, g2]
		String tableTrace1 = DBApp.getLastTrace("w5");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+8));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k91", cols2);
		String [][] records_k91 = new String[339][cols2.length];
		for(int i=0;i<339;i++)
		{
			records_k91[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k91[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k91", records_k91[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record334: [a334, b0, c1, d2, e4, f4, g5]
		//record335: [a335, b1, c2, d3, e0, f5, g6]
		//record336: [a336, b0, c0, d0, e1, f0, g0]
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		DBApp.selectIndex("k91", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 84, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		String tableTrace2 = DBApp.getLastTrace("k91");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+84));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b7", cols0);
		String [][] records_b7 = new String[342][cols0.length];
		for(int i=0;i<342;i++)
		{
			records_b7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b7", records_b7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a305"};
		DBApp.selectIndex("b7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a305, b1, c2, d1, e0, f5, g4]
		String tableTrace0 = DBApp.getLastTrace("b7");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("g4lrf", cols1);
		String [][] records_g4lrf = new String[164][cols1.length];
		for(int i=0;i<164;i++)
		{
			records_g4lrf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g4lrf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g4lrf", records_g4lrf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9, p15]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10, p0]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11, p1]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8, o12, p2]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13, p3]
		String[] ConditionColumns1 = {"n"};
		String[] ConditionColumnsValues1 = {"n13"};
		DBApp.selectIndex("g4lrf", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record1: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		//record2: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9]
		//record3: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10, p7]
		//record4: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5]
		String tableTrace1 = DBApp.getLastTrace("g4lrf");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+11));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ij2", cols2);
		String [][] records_ij2 = new String[207][cols2.length];
		for(int i=0;i<207;i++)
		{
			records_ij2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ij2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ij2", records_ij2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record202: [a202, b0, c1, d2, e2, f4, g6]
		//record203: [a203, b1, c2, d3, e3, f5, g0]
		//record204: [a204, b0, c0, d0, e4, f0, g1]
		//record205: [a205, b1, c1, d1, e0, f1, g2]
		//record206: [a206, b0, c2, d2, e1, f2, g3]
		String[] ConditionColumns2 = {"g","f","b"};
		String[] ConditionColumnsValues2 = {"g5","f1","b1"};
		DBApp.selectIndex("ij2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5]
		//record1: [a61, b1, c1, d1, e1, f1, g5]
		//record2: [a103, b1, c1, d3, e3, f1, g5]
		//record3: [a145, b1, c1, d1, e0, f1, g5]
		//record4: [a187, b1, c1, d3, e2, f1, g5]
		String tableTrace2 = DBApp.getLastTrace("ij2");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l20", cols3);
		String [][] records_l20 = new String[142][cols3.length];
		for(int i=0;i<142;i++)
		{
			records_l20[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l20[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l20", records_l20[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record137: [a137, b1, c2, d1, e2, f5, g4]
		//record138: [a138, b0, c0, d2, e3, f0, g5]
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		//record140: [a140, b0, c2, d0, e0, f2, g0]
		//record141: [a141, b1, c0, d1, e1, f3, g1]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c0"};
		DBApp.selectIndex("l20", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 48, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		String tableTrace3 = DBApp.getLastTrace("l20");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+48));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mv9", cols4);
		String [][] records_mv9 = new String[248][cols4.length];
		for(int i=0;i<248;i++)
		{
			records_mv9[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mv9[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mv9", records_mv9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record243: [a243, b1, c0, d3, e3, f3, g5]
		//record244: [a244, b0, c1, d0, e4, f4, g6]
		//record245: [a245, b1, c2, d1, e0, f5, g0]
		//record246: [a246, b0, c0, d2, e1, f0, g1]
		//record247: [a247, b1, c1, d3, e2, f1, g2]
		String[] ConditionColumns4 = {"d"};
		String[] ConditionColumnsValues4 = {"d0"};
		DBApp.selectIndex("mv9", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 62, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		String tableTrace4 = DBApp.getLastTrace("mv9");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+62));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vn0a", cols0);
		String [][] records_vn0a = new String[423][cols0.length];
		for(int i=0;i<423;i++)
		{
			records_vn0a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vn0a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vn0a", records_vn0a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record418: [a418, b0, c1, d2, e3, f4, g5]
		//record419: [a419, b1, c2, d3, e4, f5, g6]
		//record420: [a420, b0, c0, d0, e0, f0, g0]
		//record421: [a421, b1, c1, d1, e1, f1, g1]
		//record422: [a422, b0, c2, d2, e2, f2, g2]
		String[] ConditionColumns0 = {"e","c"};
		String[] ConditionColumnsValues0 = {"e1","c1"};
		DBApp.selectIndex("vn0a", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a31, b1, c1, d3, e1, f1, g3]
		//record3: [a46, b0, c1, d2, e1, f4, g4]
		//record4: [a61, b1, c1, d1, e1, f1, g5]
		String tableTrace0 = DBApp.getLastTrace("vn0a");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+29));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h0", cols1);
		String [][] records_h0 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			records_h0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h0", records_h0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record75: [a75, b1, c0, d3, e0, f3, g5]
		//record76: [a76, b0, c1, d0, e1, f4, g6]
		//record77: [a77, b1, c2, d1, e2, f5, g0]
		//record78: [a78, b0, c0, d2, e3, f0, g1]
		//record79: [a79, b1, c1, d3, e4, f1, g2]
		String[] ConditionColumns1 = {"g","b","a"};
		String[] ConditionColumnsValues1 = {"g6","b0","a6"};
		DBApp.selectIndex("h0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		String tableTrace1 = DBApp.getLastTrace("h0");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ig98", cols2);
		String [][] records_ig98 = new String[140][cols2.length];
		for(int i=0;i<140;i++)
		{
			records_ig98[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ig98[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ig98", records_ig98[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5, n9, o0, p7, q16, r9]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0, r10]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1, r11]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2, r12]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3, r13]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g3"};
		DBApp.selectIndex("ig98", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 20, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		String tableTrace2 = DBApp.getLastTrace("ig98");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+20));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("xrz0", cols0);
		String [][] records_xrz0 = new String[171][cols0.length];
		for(int i=0;i<171;i++)
		{
			records_xrz0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xrz0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xrz0", records_xrz0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13, r4, s14]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7, q14, r5, s15]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8, q15, r6, s16]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9, q16, r7, s17]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10, q0, r8, s18]
		String[] ConditionColumns0 = {"s","h","m","b","o","l"};
		String[] ConditionColumnsValues0 = {"s9","h3","m6","b1","o3","l3"};
		DBApp.selectIndex("xrz0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3, p11, q4, r15, s9]
		String tableTrace0 = DBApp.getLastTrace("xrz0");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mcc0o", cols1);
		String [][] records_mcc0o = new String[375][cols1.length];
		for(int i=0;i<375;i++)
		{
			records_mcc0o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mcc0o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mcc0o", records_mcc0o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10]
		String[] ConditionColumns1 = {"k"};
		String[] ConditionColumnsValues1 = {"k4"};
		DBApp.selectIndex("mcc0o", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 34, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record2: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record3: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record4: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9]
		String tableTrace1 = DBApp.getLastTrace("mcc0o");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+34));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ocm", cols0);
		String [][] records_ocm = new String[175][cols0.length];
		for(int i=0;i<175;i++)
		{
			records_ocm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ocm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ocm", records_ocm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3, o6]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5, o8]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9]
		String[] ConditionColumns0 = {"o","d","e","g","b","c"};
		String[] ConditionColumnsValues0 = {"o3","d3","e3","g3","b1","c0"};
		DBApp.selectIndex("ocm", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		String tableTrace0 = DBApp.getLastTrace("ocm");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("xa", cols1);
		String [][] records_xa = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			records_xa[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xa[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xa", records_xa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15, q11, r7, s3]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10, o5, p0, q12, r8, s4]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6, p1, q13, r9, s5]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7, p2, q14, r10, s6]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3, q15, r11, s7]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		DBApp.selectIndex("xa", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 42, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8]
		String tableTrace1 = DBApp.getLastTrace("xa");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+42));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a3851", cols2);
		String [][] records_a3851 = new String[428][cols2.length];
		for(int i=0;i<428;i++)
		{
			records_a3851[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a3851[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a3851", records_a3851[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		DBApp.selectIndex("a3851", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 143, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		String tableTrace2 = DBApp.getLastTrace("a3851");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+143));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e4", cols0);
		String [][] records_e4 = new String[239][cols0.length];
		for(int i=0;i<239;i++)
		{
			records_e4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e4", records_e4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f1"};
		DBApp.selectIndex("e4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 40, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		String tableTrace0 = DBApp.getLastTrace("e4");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+40));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vd1", cols1);
		String [][] records_vd1 = new String[86][cols1.length];
		for(int i=0;i<86;i++)
		{
			records_vd1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vd1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vd1", records_vd1[i]);
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
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m6"};
		DBApp.selectIndex("vd1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		//record4: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8, k3, l10, m6]
		String tableTrace1 = DBApp.getLastTrace("vd1");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+7));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j1j", cols2);
		String [][] records_j1j = new String[473][cols2.length];
		for(int i=0;i<473;i++)
		{
			records_j1j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j1j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j1j", records_j1j[i]);
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
		String[] ConditionColumns2 = {"b","c","g"};
		String[] ConditionColumnsValues2 = {"b1","c0","g6"};
		DBApp.selectIndex("j1j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 11, records:
		//record0: [a27, b1, c0, d3, e2, f3, g6]
		//record1: [a69, b1, c0, d1, e4, f3, g6]
		//record2: [a111, b1, c0, d3, e1, f3, g6]
		//record3: [a153, b1, c0, d1, e3, f3, g6]
		//record4: [a195, b1, c0, d3, e0, f3, g6]
		String tableTrace2 = DBApp.getLastTrace("j1j");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+11));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("l0", cols0);
		String [][] records_l0 = new String[325][cols0.length];
		for(int i=0;i<325;i++)
		{
			records_l0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l0", records_l0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13, o6, p1]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0, o7, p2]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1, o8, p3]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9, p4]
		String[] ConditionColumns0 = {"e","f"};
		String[] ConditionColumnsValues0 = {"e0","f5"};
		DBApp.selectIndex("l0", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 11, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record1: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3]
		//record2: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5, p1]
		//record3: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5, p15]
		//record4: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5, p13]
		String tableTrace0 = DBApp.getLastTrace("l0");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+11));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ku3h", cols0);
		String [][] records_ku3h = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			records_ku3h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ku3h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ku3h", records_ku3h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16, t16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17, t17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18, t18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0, t19]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1, t0]
		String[] ConditionColumns0 = {"h","s","b","c","k","o","e","a","r"};
		String[] ConditionColumnsValues0 = {"h4","s12","b0","c0","k1","o12","e2","a12","r12"};
		DBApp.selectIndex("ku3h", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		String tableTrace0 = DBApp.getLastTrace("ku3h");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("sku8", cols1);
		String [][] records_sku8 = new String[169][cols1.length];
		for(int i=0;i<169;i++)
		{
			records_sku8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sku8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sku8", records_sku8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14, p4, q11]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9, n11, o0, p5, q12]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7, q14]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8, q15]
		String[] ConditionColumns1 = {"p"};
		String[] ConditionColumnsValues1 = {"p5"};
		DBApp.selectIndex("sku8", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 11, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		//record3: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8, p5, q2]
		//record4: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1]
		String tableTrace1 = DBApp.getLastTrace("sku8");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+11));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dt9t", cols2);
		String [][] records_dt9t = new String[436][cols2.length];
		for(int i=0;i<436;i++)
		{
			records_dt9t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dt9t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dt9t", records_dt9t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record431: [a431, b1, c2, d3, e1, f5, g4]
		//record432: [a432, b0, c0, d0, e2, f0, g5]
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		DBApp.selectIndex("dt9t", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 72, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		String tableTrace2 = DBApp.getLastTrace("dt9t");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+72));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uqng", cols3);
		String [][] records_uqng = new String[137][cols3.length];
		for(int i=0;i<137;i++)
		{
			records_uqng[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_uqng[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("uqng", records_uqng[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record132: [a132, b0, c0, d0, e2, f0, g6]
		//record133: [a133, b1, c1, d1, e3, f1, g0]
		//record134: [a134, b0, c2, d2, e4, f2, g1]
		//record135: [a135, b1, c0, d3, e0, f3, g2]
		//record136: [a136, b0, c1, d0, e1, f4, g3]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d3"};
		DBApp.selectIndex("uqng", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 34, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		String tableTrace3 = DBApp.getLastTrace("uqng");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+34));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g3j1", cols0);
		String [][] records_g3j1 = new String[216][cols0.length];
		for(int i=0;i<216;i++)
		{
			records_g3j1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g3j1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g3j1", records_g3j1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1]
		//record212: [a212, b0, c2, d0, e2, f2, g2]
		//record213: [a213, b1, c0, d1, e3, f3, g3]
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		//record215: [a215, b1, c2, d3, e0, f5, g5]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		DBApp.selectIndex("g3j1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 72, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a11, b1, c2, d3, e1, f5, g4]
		//record4: [a14, b0, c2, d2, e4, f2, g0]
		String tableTrace0 = DBApp.getLastTrace("g3j1");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+72));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m1i4", cols1);
		String [][] records_m1i4 = new String[267][cols1.length];
		for(int i=0;i<267;i++)
		{
			records_m1i4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m1i4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m1i4", records_m1i4[i]);
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
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		DBApp.selectIndex("m1i4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 67, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		String tableTrace1 = DBApp.getLastTrace("m1i4");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+67));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("wz1v", cols0);
		String [][] records_wz1v = new String[259][cols0.length];
		for(int i=0;i<259;i++)
		{
			records_wz1v[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wz1v[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wz1v", records_wz1v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14, p14, q16, r2, s7, t14]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0, p15, q0, r3, s8, t15]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1, p0, q1, r4, s9, t16]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1, q2, r5, s10, t17]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2, q3, r6, s11, t18]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f3"};
		DBApp.selectIndex("wz1v", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 43, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9, t9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15, t15]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2, t1]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8, t7]
		String tableTrace0 = DBApp.getLastTrace("wz1v");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+43));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("u4ik", cols1);
		String [][] records_u4ik = new String[171][cols1.length];
		for(int i=0;i<171;i++)
		{
			records_u4ik[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u4ik[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u4ik", records_u4ik[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13, r4]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7, q14, r5]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8, q15, r6]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9, q16, r7]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10, q0, r8]
		String[] ConditionColumns1 = {"l","q"};
		String[] ConditionColumnsValues1 = {"l10","q13"};
		DBApp.selectIndex("u4ik", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6, k1, l10, m10, n12, o1, p6, q13, r4]
		String tableTrace1 = DBApp.getLastTrace("u4ik");
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
	public void TableSelectionIndexConditionNonIndexedTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ktd", cols0);
		String [][] records_ktd = new String[161][cols0.length];
		for(int i=0;i<161;i++)
		{
			records_ktd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ktd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ktd", records_ktd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7]
		String[] ConditionColumns0 = {"i","g","a"};
		String[] ConditionColumnsValues0 = {"i2","g2","a2"};
		DBApp.selectIndex("ktd", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		String tableTrace0 = DBApp.getLastTrace("ktd");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("o0h", cols1);
		String [][] records_o0h = new String[129][cols1.length];
		for(int i=0;i<129;i++)
		{
			records_o0h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o0h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o0h", records_o0h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4, p12, q5, r16]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5, p13, q6, r17]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6, p14, q7, r0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7, p15, q8, r1]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2, o8, p0, q9, r2]
		String[] ConditionColumns1 = {"o"};
		String[] ConditionColumnsValues1 = {"o0"};
		DBApp.selectIndex("o0h", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 9, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9]
		//record4: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0, m8, n4, o0, p12, q9, r6]
		String tableTrace1 = DBApp.getLastTrace("o0h");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+9));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w0", cols2);
		String [][] records_w0 = new String[298][cols2.length];
		for(int i=0;i<298;i++)
		{
			records_w0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w0", records_w0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7, m9]
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8, m10]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9, m11]
		String[] ConditionColumns2 = {"m","b","f","e","c","k"};
		String[] ConditionColumnsValues2 = {"m9","b1","f1","e2","c1","k8"};
		DBApp.selectIndex("w0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9]
		String tableTrace2 = DBApp.getLastTrace("w0");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("hr", cols0);
		String [][] records_hr = new String[208][cols0.length];
		for(int i=0;i<208;i++)
		{
			records_hr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hr", records_hr[i]);
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
		String[] ConditionColumns0 = {"c","n","q","j"};
		String[] ConditionColumnsValues0 = {"c1","n10","q1","j2"};
		DBApp.selectIndex("hr", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7, p4, q1]
		String tableTrace0 = DBApp.getLastTrace("hr");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("t55", cols1);
		String [][] records_t55 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_t55[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t55[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t55", records_t55[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		String[] ConditionColumns1 = {"d","g","i","c","q","k","e","j"};
		String[] ConditionColumnsValues1 = {"d1","g5","i5","c2","q5","k5","e0","j5"};
		DBApp.selectIndex("t55", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		String tableTrace1 = DBApp.getLastTrace("t55");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c241j", cols2);
		String [][] records_c241j = new String[259][cols2.length];
		for(int i=0;i<259;i++)
		{
			records_c241j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c241j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c241j", records_c241j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2]
		//record255: [a255, b1, c0, d3, e0, f3, g3]
		//record256: [a256, b0, c1, d0, e1, f4, g4]
		//record257: [a257, b1, c2, d1, e2, f5, g5]
		//record258: [a258, b0, c0, d2, e3, f0, g6]
		String[] ConditionColumns2 = {"f","d","b"};
		String[] ConditionColumnsValues2 = {"f3","d3","b1"};
		DBApp.selectIndex("c241j", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 22, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a15, b1, c0, d3, e0, f3, g1]
		//record2: [a27, b1, c0, d3, e2, f3, g6]
		//record3: [a39, b1, c0, d3, e4, f3, g4]
		//record4: [a51, b1, c0, d3, e1, f3, g2]
		String tableTrace2 = DBApp.getLastTrace("c241j");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+22));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("q8el", cols3);
		String [][] records_q8el = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_q8el[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q8el[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q8el", records_q8el[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		String[] ConditionColumns3 = {"c","m","g","k","b","d"};
		String[] ConditionColumnsValues3 = {"c0","m2","g1","k4","b1","d3"};
		DBApp.selectIndex("q8el", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		String tableTrace3 = DBApp.getLastTrace("q8el");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("i1d2", cols0);
		String [][] records_i1d2 = new String[444][cols0.length];
		for(int i=0;i<444;i++)
		{
			records_i1d2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i1d2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i1d2", records_i1d2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5, o4, p7]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6, o5, p8]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7, o6, p9]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0, n8, o7, p10]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1, n9, o8, p11]
		String[] ConditionColumns0 = {"g","c","m","d","b"};
		String[] ConditionColumnsValues0 = {"g2","c2","m7","d2","b0"};
		DBApp.selectIndex("i1d2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14, p14]
		String tableTrace0 = DBApp.getLastTrace("i1d2");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("f1432", cols1);
		String [][] records_f1432 = new String[229][cols1.length];
		for(int i=0;i<229;i++)
		{
			records_f1432[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f1432[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f1432", records_f1432[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3, n0, o14, p0, q3, r8, s15]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9, m4, n1, o0, p1, q4, r9, s16]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5, n2, o1, p2, q5, r10, s17]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3, o2, p3, q6, r11, s18]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4, o3, p4, q7, r12, s0]
		String[] ConditionColumns1 = {"k","b","n","e","r","h","o","m","q"};
		String[] ConditionColumnsValues1 = {"k5","b0","n6","e4","r14","h0","o14","m0","q2"};
		DBApp.selectIndex("f1432", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14, p8, q2, r14, s9]
		String tableTrace1 = DBApp.getLastTrace("f1432");
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
	public void TableSelectionIndexConditionNonIndexedTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("u6", cols0);
		String [][] records_u6 = new String[481][cols0.length];
		for(int i=0;i<481;i++)
		{
			records_u6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u6", records_u6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a300"};
		DBApp.selectIndex("u6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a300, b0, c0, d0, e0, f0, g6, h4, i3]
		String tableTrace0 = DBApp.getLastTrace("u6");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g1p0r", cols0);
		String [][] records_g1p0r = new String[143][cols0.length];
		for(int i=0;i<143;i++)
		{
			records_g1p0r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g1p0r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g1p0r", records_g1p0r[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e0"};
		DBApp.selectIndex("g1p0r", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a20, b0, c2, d0, e0, f2, g6]
		String tableTrace0 = DBApp.getLastTrace("g1p0r");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+29));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dhb2i", cols1);
		String [][] records_dhb2i = new String[185][cols1.length];
		for(int i=0;i<185;i++)
		{
			records_dhb2i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dhb2i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dhb2i", records_dhb2i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0]
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2]
		String[] ConditionColumns1 = {"k","c","l","i","m","j"};
		String[] ConditionColumnsValues1 = {"k10","c0","l0","i3","m3","j0"};
		DBApp.selectIndex("dhb2i", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3]
		String tableTrace1 = DBApp.getLastTrace("dhb2i");
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
	public void TableSelectionIndexConditionNonIndexedTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z67cj", cols0);
		String [][] records_z67cj = new String[251][cols0.length];
		for(int i=0;i<251;i++)
		{
			records_z67cj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z67cj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z67cj", records_z67cj[i]);
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
		String[] ConditionColumns0 = {"a","c"};
		String[] ConditionColumnsValues0 = {"a129","c0"};
		DBApp.selectIndex("z67cj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a129, b1, c0, d1, e4, f3, g3]
		String tableTrace0 = DBApp.getLastTrace("z67cj");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q56", cols1);
		String [][] records_q56 = new String[235][cols1.length];
		for(int i=0;i<235;i++)
		{
			records_q56[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q56[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q56", records_q56[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f3"};
		DBApp.selectIndex("q56", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record3: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		//record4: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		String tableTrace1 = DBApp.getLastTrace("q56");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+39));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e7i", cols2);
		String [][] records_e7i = new String[63][cols2.length];
		for(int i=0;i<63;i++)
		{
			records_e7i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e7i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e7i", records_e7i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record58: [a58, b0, c1, d2, e3, f4, g2, h2, i4, j8]
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9]
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		DBApp.selectIndex("e7i", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 31, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record3: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record4: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		String tableTrace2 = DBApp.getLastTrace("e7i");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+31));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("e4", cols0);
		String [][] records_e4 = new String[102][cols0.length];
		for(int i=0;i<102;i++)
		{
			records_e4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e4", records_e4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7, p1, q12, r7, s2]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8, p2, q13, r8, s3]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9, s4]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15, r10, s5]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5, q16, r11, s6]
		String[] ConditionColumns0 = {"n","i","h","s","q","j","g"};
		String[] ConditionColumnsValues0 = {"n12","i3","h4","s12","q12","j2","g5"};
		DBApp.selectIndex("e4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		String tableTrace0 = DBApp.getLastTrace("e4");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("yf2", cols1);
		String [][] records_yf2 = new String[223][cols1.length];
		for(int i=0;i<223;i++)
		{
			records_yf2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yf2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yf2", records_yf2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8, p10, q14, r2, s9]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9, o9, p11, q15, r3, s10]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10, o10, p12, q16, r4, s11]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0, n11, o11, p13, q0, r5, s12]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12, p14, q1, r6, s13]
		String[] ConditionColumns1 = {"d","q","b","n"};
		String[] ConditionColumnsValues1 = {"d3","q2","b1","n3"};
		DBApp.selectIndex("yf2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11]
		String tableTrace1 = DBApp.getLastTrace("yf2");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kwn4", cols2);
		String [][] records_kwn4 = new String[320][cols2.length];
		for(int i=0;i<320;i++)
		{
			records_kwn4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kwn4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kwn4", records_kwn4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7]
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b0"};
		DBApp.selectIndex("kwn4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 160, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0]
		String tableTrace2 = DBApp.getLastTrace("kwn4");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+160));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b9", cols3);
		String [][] records_b9 = new String[214][cols3.length];
		for(int i=0;i<214;i++)
		{
			records_b9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3]
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c2"};
		DBApp.selectIndex("b9", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 71, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		String tableTrace3 = DBApp.getLastTrace("b9");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+71));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v7e", cols4);
		String [][] records_v7e = new String[170][cols4.length];
		for(int i=0;i<170;i++)
		{
			records_v7e[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_v7e[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("v7e", records_v7e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7]
		String[] ConditionColumns4 = {"h","c"};
		String[] ConditionColumnsValues4 = {"h6","c2"};
		DBApp.selectIndex("v7e", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 7, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record1: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record2: [a62, b0, c2, d2, e2, f2, g6, h6, i8]
		//record3: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		//record4: [a110, b0, c2, d2, e0, f2, g5, h6, i2]
		String tableTrace4 = DBApp.getLastTrace("v7e");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("lk7", cols0);
		String [][] records_lk7 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			records_lk7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lk7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lk7", records_lk7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4, o13, p8, q3]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11, n5, o14, p9, q4]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6, m12, n6, o0, p10, q5]
		//record91: [a91, b1, c1, d3, e1, f1, g0, h3, i1, j1, k3, l7, m0, n7, o1, p11, q6]
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4, l8, m1, n8, o2, p12, q7]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j5"};
		DBApp.selectIndex("lk7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 9, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11]
		String tableTrace0 = DBApp.getLastTrace("lk7");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+9));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("eh", cols1);
		String [][] records_eh = new String[346][cols1.length];
		for(int i=0;i<346;i++)
		{
			records_eh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_eh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("eh", records_eh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7]
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i1"};
		DBApp.selectIndex("eh", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record3: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record4: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1]
		String tableTrace1 = DBApp.getLastTrace("eh");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+39));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ps2k5", cols2);
		String [][] records_ps2k5 = new String[125][cols2.length];
		for(int i=0;i<125;i++)
		{
			records_ps2k5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ps2k5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ps2k5", records_ps2k5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3]
		String[] ConditionColumns2 = {"a"};
		String[] ConditionColumnsValues2 = {"a41"};
		DBApp.selectIndex("ps2k5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8]
		String tableTrace2 = DBApp.getLastTrace("ps2k5");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u37ii", cols3);
		String [][] records_u37ii = new String[235][cols3.length];
		for(int i=0;i<235;i++)
		{
			records_u37ii[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u37ii[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u37ii", records_u37ii[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2]
		String[] ConditionColumns3 = {"g"};
		String[] ConditionColumnsValues3 = {"g3"};
		DBApp.selectIndex("u37ii", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 34, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record3: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record4: [a31, b1, c1, d3, e1, f1, g3, h7]
		String tableTrace3 = DBApp.getLastTrace("u37ii");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+34));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("p9fx5", cols0);
		String [][] records_p9fx5 = new String[404][cols0.length];
		for(int i=0;i<404;i++)
		{
			records_p9fx5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p9fx5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p9fx5", records_p9fx5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g5"};
		DBApp.selectIndex("p9fx5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 57, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1]
		String tableTrace0 = DBApp.getLastTrace("p9fx5");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+57));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("w37", cols1);
		String [][] records_w37 = new String[294][cols1.length];
		for(int i=0;i<294;i++)
		{
			records_w37[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w37[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w37", records_w37[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record289: [a289, b1, c1, d1, e4, f1, g2, h1, i1, j9, k3, l1, m3, n9]
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4, n10]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5, n11]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7, n13]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h7"};
		DBApp.selectIndex("w37", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 36, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		String tableTrace1 = DBApp.getLastTrace("w37");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+36));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k81", cols2);
		String [][] records_k81 = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_k81[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k81[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k81", records_k81[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7]
		String[] ConditionColumns2 = {"h"};
		String[] ConditionColumnsValues2 = {"h2"};
		DBApp.selectIndex("k81", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2]
		String tableTrace2 = DBApp.getLastTrace("k81");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("iulg", cols3);
		String [][] records_iulg = new String[388][cols3.length];
		for(int i=0;i<388;i++)
		{
			records_iulg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_iulg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("iulg", records_iulg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2]
		String[] ConditionColumns3 = {"h","c"};
		String[] ConditionColumnsValues3 = {"h0","c1"};
		DBApp.selectIndex("iulg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 16, records:
		//record0: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record1: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		//record2: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9]
		//record3: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0]
		//record4: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2]
		String tableTrace3 = DBApp.getLastTrace("iulg");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+16));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("a7sf", cols4);
		String [][] records_a7sf = new String[168][cols4.length];
		for(int i=0;i<168;i++)
		{
			records_a7sf[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_a7sf[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("a7sf", records_a7sf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5]
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4, j6]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7]
		String[] ConditionColumns4 = {"i","h","f"};
		String[] ConditionColumnsValues4 = {"i0","h0","f0"};
		DBApp.selectIndex("a7sf", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 3, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2]
		//record2: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4]
		String tableTrace4 = DBApp.getLastTrace("a7sf");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rti", cols0);
		String [][] records_rti = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			records_rti[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rti[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rti", records_rti[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a8"};
		DBApp.selectIndex("rti", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		String tableTrace0 = DBApp.getLastTrace("rti");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hz", cols1);
		String [][] records_hz = new String[266][cols1.length];
		for(int i=0;i<266;i++)
		{
			records_hz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hz", records_hz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record261: [a261, b1, c0, d1, e1, f3, g2]
		//record262: [a262, b0, c1, d2, e2, f4, g3]
		//record263: [a263, b1, c2, d3, e3, f5, g4]
		//record264: [a264, b0, c0, d0, e4, f0, g5]
		//record265: [a265, b1, c1, d1, e0, f1, g6]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		DBApp.selectIndex("hz", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 133, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		String tableTrace1 = DBApp.getLastTrace("hz");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+133));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("cjn", cols2);
		String [][] records_cjn = new String[310][cols2.length];
		for(int i=0;i<310;i++)
		{
			records_cjn[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cjn[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cjn", records_cjn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record305: [a305, b1, c2, d1, e0, f5, g4]
		//record306: [a306, b0, c0, d2, e1, f0, g5]
		//record307: [a307, b1, c1, d3, e2, f1, g6]
		//record308: [a308, b0, c2, d0, e3, f2, g0]
		//record309: [a309, b1, c0, d1, e4, f3, g1]
		String[] ConditionColumns2 = {"f","g","e"};
		String[] ConditionColumnsValues2 = {"f5","g4","e2"};
		DBApp.selectIndex("cjn", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a137, b1, c2, d1, e2, f5, g4]
		String tableTrace2 = DBApp.getLastTrace("cjn");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("t4", cols0);
		String [][] records_t4 = new String[320][cols0.length];
		for(int i=0;i<320;i++)
		{
			records_t4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t4", records_t4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7, o0, p11, q9, r9, s11, t15]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1, p12, q10, r10, s12, t16]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13, q11, r11, s13, t17]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12, s14, t18]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13, s15, t19]
		String[] ConditionColumns0 = {"p","d","g","r","a","j","c"};
		String[] ConditionColumnsValues0 = {"p5","d1","g2","r1","a37","j7","c1"};
		DBApp.selectIndex("t4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18, t17]
		String tableTrace0 = DBApp.getLastTrace("t4");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("tr8", cols0);
		String [][] records_tr8 = new String[476][cols0.length];
		for(int i=0;i<476;i++)
		{
			records_tr8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tr8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tr8", records_tr8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6, n12, o9]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13, o10]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f2"};
		DBApp.selectIndex("tr8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 79, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		String tableTrace0 = DBApp.getLastTrace("tr8");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+79));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("oo95", cols0);
		String [][] records_oo95 = new String[389][cols0.length];
		for(int i=0;i<389;i++)
		{
			records_oo95[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oo95[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oo95", records_oo95[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6, o9]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1, l2, m9, n8, o11]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3, m10, n9, o12]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11, n10, o13]
		String[] ConditionColumns0 = {"f","j","b","h","n","k"};
		String[] ConditionColumnsValues0 = {"f3","j5","b1","h5","n11","k0"};
		DBApp.selectIndex("oo95", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9, n11, o0]
		String tableTrace0 = DBApp.getLastTrace("oo95");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a03", cols0);
		String [][] records_a03 = new String[205][cols0.length];
		for(int i=0;i<205;i++)
		{
			records_a03[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a03[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a03", records_a03[i]);
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
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a86"};
		DBApp.selectIndex("a03", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a86, b0, c2, d2, e1, f2, g2]
		String tableTrace0 = DBApp.getLastTrace("a03");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("w0", cols1);
		String [][] records_w0 = new String[193][cols1.length];
		for(int i=0;i<193;i++)
		{
			records_w0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w0", records_w0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6, n6, o8, p12, q1]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7, o9, p13, q2]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8, o10, p14, q3]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4, l11, m9, n9, o11, p15, q4]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10, n10, o12, p0, q5]
		String[] ConditionColumns1 = {"j"};
		String[] ConditionColumnsValues1 = {"j0"};
		DBApp.selectIndex("w0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13]
		//record4: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		String tableTrace1 = DBApp.getLastTrace("w0");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+20));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("d8w", cols2);
		String [][] records_d8w = new String[69][cols2.length];
		for(int i=0;i<69;i++)
		{
			records_d8w[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d8w[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d8w", records_d8w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		DBApp.selectIndex("d8w", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 23, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		String tableTrace2 = DBApp.getLastTrace("d8w");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+23));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vqd6v", cols3);
		String [][] records_vqd6v = new String[126][cols3.length];
		for(int i=0;i<126;i++)
		{
			records_vqd6v[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vqd6v[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vqd6v", records_vqd6v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record121: [a121, b1, c1, d1, e1, f1, g2]
		//record122: [a122, b0, c2, d2, e2, f2, g3]
		//record123: [a123, b1, c0, d3, e3, f3, g4]
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e4"};
		DBApp.selectIndex("vqd6v", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 25, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a9, b1, c0, d1, e4, f3, g2]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a24, b0, c0, d0, e4, f0, g3]
		String tableTrace3 = DBApp.getLastTrace("vqd6v");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+25));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("wun", cols0);
		String [][] records_wun = new String[331][cols0.length];
		for(int i=0;i<331;i++)
		{
			records_wun[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wun[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wun", records_wun[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8]
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		DBApp.selectIndex("wun", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 56, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		String tableTrace0 = DBApp.getLastTrace("wun");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+56));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m6n1o", cols1);
		String [][] records_m6n1o = new String[393][cols1.length];
		for(int i=0;i<393;i++)
		{
			records_m6n1o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m6n1o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m6n1o", records_m6n1o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3]
		//record389: [a389, b1, c2, d1, e4, f5, g4]
		//record390: [a390, b0, c0, d2, e0, f0, g5]
		//record391: [a391, b1, c1, d3, e1, f1, g6]
		//record392: [a392, b0, c2, d0, e2, f2, g0]
		String[] ConditionColumns1 = {"a","f","b"};
		String[] ConditionColumnsValues1 = {"a64","f4","b0"};
		DBApp.selectIndex("m6n1o", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a64, b0, c1, d0, e4, f4, g1]
		String tableTrace1 = DBApp.getLastTrace("m6n1o");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("pyh6", cols2);
		String [][] records_pyh6 = new String[388][cols2.length];
		for(int i=0;i<388;i++)
		{
			records_pyh6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pyh6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pyh6", records_pyh6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6, o9, p0, q10]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10, p1, q11]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1, l2, m9, n8, o11, p2, q12]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3, m10, n9, o12, p3, q13]
		String[] ConditionColumns2 = {"p"};
		String[] ConditionColumnsValues2 = {"p14"};
		DBApp.selectIndex("pyh6", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 24, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13]
		//record2: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12]
		//record3: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6, o2, p14, q11]
		//record4: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3, p14, q10]
		String tableTrace2 = DBApp.getLastTrace("pyh6");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+24));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k65g", cols3);
		String [][] records_k65g = new String[443][cols3.length];
		for(int i=0;i<443;i++)
		{
			records_k65g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k65g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k65g", records_k65g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0]
		String[] ConditionColumns3 = {"k","i","e","c","d"};
		String[] ConditionColumnsValues3 = {"k7","i8","e1","c2","d1"};
		DBApp.selectIndex("k65g", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5]
		String tableTrace3 = DBApp.getLastTrace("k65g");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u65", cols0);
		String [][] records_u65 = new String[396][cols0.length];
		for(int i=0;i<396;i++)
		{
			records_u65[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u65[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u65", records_u65[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10]
		String[] ConditionColumns0 = {"b","k","i","h"};
		String[] ConditionColumnsValues0 = {"b0","k4","i0","h4"};
		DBApp.selectIndex("u65", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4]
		String tableTrace0 = DBApp.getLastTrace("u65");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f769", cols1);
		String [][] records_f769 = new String[97][cols1.length];
		for(int i=0;i<97;i++)
		{
			records_f769[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f769[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f769", records_f769[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4, l8, m1, n8]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12]
		String[] ConditionColumns1 = {"k","n"};
		String[] ConditionColumnsValues1 = {"k5","n7"};
		DBApp.selectIndex("f769", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7]
		String tableTrace1 = DBApp.getLastTrace("f769");
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
	public void TableSelectionIndexConditionNonIndexedTrace_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n4", cols0);
		String [][] records_n4 = new String[443][cols0.length];
		for(int i=0;i<443;i++)
		{
			records_n4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n4", records_n4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0, n8]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j0"};
		DBApp.selectIndex("n4", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 45, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record4: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		String tableTrace0 = DBApp.getLastTrace("n4");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+45));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q19u9", cols1);
		String [][] records_q19u9 = new String[189][cols1.length];
		for(int i=0;i<189;i++)
		{
			records_q19u9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q19u9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q19u9", records_q19u9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		DBApp.selectIndex("q19u9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 95, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		String tableTrace1 = DBApp.getLastTrace("q19u9");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+95));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("fls", cols2);
		String [][] records_fls = new String[461][cols2.length];
		for(int i=0;i<461;i++)
		{
			records_fls[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fls[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fls", records_fls[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1, n8, o6, p8, q14, r6]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1, m2, n9, o7, p9, q15, r7]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2, m3, n10, o8, p10, q16, r8]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0, j9, k8, l3, m4, n11, o9, p11, q0, r9]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1, j0, k9, l4, m5, n12, o10, p12, q1, r10]
		String[] ConditionColumns2 = {"f","o","r","e","p"};
		String[] ConditionColumnsValues2 = {"f1","o13","r7","e3","p3"};
		DBApp.selectIndex("fls", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13, p3, q12, r7]
		String tableTrace2 = DBApp.getLastTrace("fls");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("aj1jf", cols0);
		String [][] records_aj1jf = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_aj1jf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_aj1jf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("aj1jf", records_aj1jf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		DBApp.selectIndex("aj1jf", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		String tableTrace0 = DBApp.getLastTrace("aj1jf");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("oq", cols1);
		String [][] records_oq = new String[360][cols1.length];
		for(int i=0;i<360;i++)
		{
			records_oq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_oq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("oq", records_oq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4, n5]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5, n6]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9]
		String[] ConditionColumns1 = {"n"};
		String[] ConditionColumnsValues1 = {"n5"};
		DBApp.selectIndex("oq", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 26, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record2: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		//record3: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5]
		//record4: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5]
		String tableTrace1 = DBApp.getLastTrace("oq");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+26));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("v8", cols2);
		String [][] records_v8 = new String[303][cols2.length];
		for(int i=0;i<303;i++)
		{
			records_v8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v8", records_v8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1]
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5]
		String[] ConditionColumns2 = {"i"};
		String[] ConditionColumnsValues2 = {"i0"};
		DBApp.selectIndex("v8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 34, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record4: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		String tableTrace2 = DBApp.getLastTrace("v8");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+34));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("g41", cols0);
		String [][] records_g41 = new String[267][cols0.length];
		for(int i=0;i<267;i++)
		{
			records_g41[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g41[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g41", records_g41[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10, m2, n10, o7, p6, q7, r10, s15]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11, m3, n11, o8, p7, q8, r11, s16]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9, p8, q9, r12, s17]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10, p9, q10, r13, s18]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11, p10, q11, r14, s0]
		String[] ConditionColumns0 = {"p"};
		String[] ConditionColumnsValues0 = {"p5"};
		DBApp.selectIndex("g41", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 17, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18]
		//record3: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8, p5, q2, r17, s15]
		//record4: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1, r15, s12]
		String tableTrace0 = DBApp.getLastTrace("g41");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+17));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("uz64v", cols1);
		String [][] records_uz64v = new String[359][cols1.length];
		for(int i=0;i<359;i++)
		{
			records_uz64v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uz64v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uz64v", records_uz64v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4]
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8]
		String[] ConditionColumns1 = {"e","g","h"};
		String[] ConditionColumnsValues1 = {"e4","g4","h6"};
		DBApp.selectIndex("uz64v", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4]
		String tableTrace1 = DBApp.getLastTrace("uz64v");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lb8b", cols2);
		String [][] records_lb8b = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			records_lb8b[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lb8b[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lb8b", records_lb8b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e3"};
		DBApp.selectIndex("lb8b", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		String tableTrace2 = DBApp.getLastTrace("lb8b");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gg", cols3);
		String [][] records_gg = new String[157][cols3.length];
		for(int i=0;i<157;i++)
		{
			records_gg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gg", records_gg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record152: [a152, b0, c2, d0, e2, f2, g5]
		//record153: [a153, b1, c0, d1, e3, f3, g6]
		//record154: [a154, b0, c1, d2, e4, f4, g0]
		//record155: [a155, b1, c2, d3, e0, f5, g1]
		//record156: [a156, b0, c0, d0, e1, f0, g2]
		String[] ConditionColumns3 = {"g","f","b"};
		String[] ConditionColumnsValues3 = {"g5","f0","b0"};
		DBApp.selectIndex("gg", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 4, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5]
		//record1: [a54, b0, c0, d2, e4, f0, g5]
		//record2: [a96, b0, c0, d0, e1, f0, g5]
		//record3: [a138, b0, c0, d2, e3, f0, g5]
		String tableTrace3 = DBApp.getLastTrace("gg");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o8l", cols4);
		String [][] records_o8l = new String[86][cols4.length];
		for(int i=0;i<86;i++)
		{
			records_o8l[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o8l[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o8l", records_o8l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4]
		//record82: [a82, b0, c1, d2, e2, f4, g5]
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		String[] ConditionColumns4 = {"b"};
		String[] ConditionColumnsValues4 = {"b1"};
		DBApp.selectIndex("o8l", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 43, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		String tableTrace4 = DBApp.getLastTrace("o8l");
		Arrays.sort(ConditionColumns4);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace4.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace4.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace4.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace4.contains(Arrays.toString(ConditionColumns4)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace4.contains("Final count: "+43));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("tw9ud", cols0);
		String [][] records_tw9ud = new String[323][cols0.length];
		for(int i=0;i<323;i++)
		{
			records_tw9ud[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tw9ud[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tw9ud", records_tw9ud[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f1"};
		DBApp.selectIndex("tw9ud", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 54, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		String tableTrace0 = DBApp.getLastTrace("tw9ud");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+54));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("d9", cols1);
		String [][] records_d9 = new String[303][cols1.length];
		for(int i=0;i<303;i++)
		{
			records_d9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d9", records_d9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1, l10, m12, n4, o13, p10, q9]
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2, l11, m0, n5, o14, p11, q10]
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0, m1, n6, o0, p12, q11]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1, m2, n7, o1, p13, q12]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3, n8, o2, p14, q13]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		DBApp.selectIndex("d9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 76, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16]
		String tableTrace1 = DBApp.getLastTrace("d9");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+76));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i74", cols0);
		String [][] records_i74 = new String[135][cols0.length];
		for(int i=0;i<135;i++)
		{
			records_i74[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i74[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i74", records_i74[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0, n4, o10]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1, n5, o11]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2, n6, o12]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3, n7, o13]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8, o14]
		String[] ConditionColumns0 = {"b","g"};
		String[] ConditionColumnsValues0 = {"b1","g3"};
		DBApp.selectIndex("i74", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 10, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record3: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		//record4: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7, n3, o14]
		String tableTrace0 = DBApp.getLastTrace("i74");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+10));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mfs", cols1);
		String [][] records_mfs = new String[232][cols1.length];
		for(int i=0;i<232;i++)
		{
			records_mfs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mfs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mfs", records_mfs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a161"};
		DBApp.selectIndex("mfs", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5]
		String tableTrace1 = DBApp.getLastTrace("mfs");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h7u", cols2);
		String [][] records_h7u = new String[247][cols2.length];
		for(int i=0;i<247;i++)
		{
			records_h7u[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h7u[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h7u", records_h7u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8, n4, o2, p2]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9, n5, o3, p3]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2, l4, m10, n6, o4, p4]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11, n7, o5, p5]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8, o6, p6]
		String[] ConditionColumns2 = {"n"};
		String[] ConditionColumnsValues2 = {"n6"};
		DBApp.selectIndex("h7u", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 18, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4]
		//record2: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2]
		//record3: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0]
		//record4: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6, o2, p14]
		String tableTrace2 = DBApp.getLastTrace("h7u");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+18));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("an6xm", cols3);
		String [][] records_an6xm = new String[214][cols3.length];
		for(int i=0;i<214;i++)
		{
			records_an6xm[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_an6xm[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("an6xm", records_an6xm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record209: [a209, b1, c2, d1, e4, f5, g6]
		//record210: [a210, b0, c0, d2, e0, f0, g0]
		//record211: [a211, b1, c1, d3, e1, f1, g1]
		//record212: [a212, b0, c2, d0, e2, f2, g2]
		//record213: [a213, b1, c0, d1, e3, f3, g3]
		String[] ConditionColumns3 = {"f","e"};
		String[] ConditionColumnsValues3 = {"f5","e4"};
		DBApp.selectIndex("an6xm", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1]
		//record1: [a59, b1, c2, d3, e4, f5, g3]
		//record2: [a89, b1, c2, d1, e4, f5, g5]
		//record3: [a119, b1, c2, d3, e4, f5, g0]
		//record4: [a149, b1, c2, d1, e4, f5, g2]
		String tableTrace3 = DBApp.getLastTrace("an6xm");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s59", cols0);
		String [][] records_s59 = new String[304][cols0.length];
		for(int i=0;i<304;i++)
		{
			records_s59[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s59[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s59", records_s59[i]);
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
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g2"};
		DBApp.selectIndex("s59", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 44, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record4: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		String tableTrace0 = DBApp.getLastTrace("s59");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+44));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("l4uk", cols1);
		String [][] records_l4uk = new String[112][cols1.length];
		for(int i=0;i<112;i++)
		{
			records_l4uk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l4uk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l4uk", records_l4uk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11, q5, r17, s12]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12, q6, r0, s13]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13, q7, r1, s14]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14, q8, r2, s15]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15, q9, r3, s16]
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m12"};
		DBApp.selectIndex("l4uk", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 8, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6, p3, q0, r15, s13]
		//record4: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0, q13, r10, s7]
		String tableTrace1 = DBApp.getLastTrace("l4uk");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+8));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w56rt", cols0);
		String [][] records_w56rt = new String[364][cols0.length];
		for(int i=0;i<364;i++)
		{
			records_w56rt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w56rt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w56rt", records_w56rt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record359: [a359, b1, c2, d3, e4, f5, g2]
		//record360: [a360, b0, c0, d0, e0, f0, g3]
		//record361: [a361, b1, c1, d1, e1, f1, g4]
		//record362: [a362, b0, c2, d2, e2, f2, g5]
		//record363: [a363, b1, c0, d3, e3, f3, g6]
		String[] ConditionColumns0 = {"f","b","e"};
		String[] ConditionColumnsValues0 = {"f2","b0","e2"};
		DBApp.selectIndex("w56rt", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a32, b0, c2, d0, e2, f2, g4]
		//record2: [a62, b0, c2, d2, e2, f2, g6]
		//record3: [a92, b0, c2, d0, e2, f2, g1]
		//record4: [a122, b0, c2, d2, e2, f2, g3]
		String tableTrace0 = DBApp.getLastTrace("w56rt");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+13));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("dbu70", cols1);
		String [][] records_dbu70 = new String[485][cols1.length];
		for(int i=0;i<485;i++)
		{
			records_dbu70[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dbu70[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dbu70", records_dbu70[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12, n4, o0, p0, q4, r12]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0, n5, o1, p1, q5, r13]
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9, l2, m1, n6, o2, p2, q6, r14]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3, m2, n7, o3, p3, q7, r15]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3, n8, o4, p4, q8, r16]
		String[] ConditionColumns1 = {"r","c","a"};
		String[] ConditionColumnsValues1 = {"r3","c0","a417"};
		DBApp.selectIndex("dbu70", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12, p1, q9, r3]
		String tableTrace1 = DBApp.getLastTrace("dbu70");
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
	public void TableSelectionIndexConditionNonIndexedTrace_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("im4f", cols0);
		String [][] records_im4f = new String[138][cols0.length];
		for(int i=0;i<138;i++)
		{
			records_im4f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_im4f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("im4f", records_im4f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3, n7, o13, p5, q14, r7]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8, o14, p6, q15, r8]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5, n9, o0, p7, q16, r9]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0, r10]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1, r11]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f4"};
		DBApp.selectIndex("im4f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 23, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record2: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record4: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		String tableTrace0 = DBApp.getLastTrace("im4f");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+23));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rz88", cols0);
		String [][] records_rz88 = new String[166][cols0.length];
		for(int i=0;i<166;i++)
		{
			records_rz88[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rz88[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rz88", records_rz88[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8, o12]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14]
		//record165: [a165, b1, c0, d1, e0, f3, g4, h5, i3, j5, k0, l9, m9, n11, o0]
		String[] ConditionColumns0 = {"f","o","n","j","k"};
		String[] ConditionColumnsValues0 = {"f3","o6","n7","j1","k10"};
		DBApp.selectIndex("rz88", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		String tableTrace0 = DBApp.getLastTrace("rz88");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("mte", cols0);
		String [][] records_mte = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			records_mte[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mte[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mte", records_mte[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g3"};
		DBApp.selectIndex("mte", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		String tableTrace0 = DBApp.getLastTrace("mte");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("nrjn2", cols1);
		String [][] records_nrjn2 = new String[175][cols1.length];
		for(int i=0;i<175;i++)
		{
			records_nrjn2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nrjn2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nrjn2", records_nrjn2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3, o6]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5, o8]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9]
		String[] ConditionColumns1 = {"l","j","i","e","g","b","f"};
		String[] ConditionColumnsValues1 = {"l0","j6","i3","e1","g2","b0","f0"};
		DBApp.selectIndex("nrjn2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6]
		String tableTrace1 = DBApp.getLastTrace("nrjn2");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("x91h1", cols2);
		String [][] records_x91h1 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			records_x91h1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x91h1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x91h1", records_x91h1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g2"};
		DBApp.selectIndex("x91h1", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		String tableTrace2 = DBApp.getLastTrace("x91h1");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dn", cols0);
		String [][] records_dn = new String[343][cols0.length];
		for(int i=0;i<343;i++)
		{
			records_dn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dn", records_dn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l7"};
		DBApp.selectIndex("dn", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record2: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		//record4: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7]
		String tableTrace0 = DBApp.getLastTrace("dn");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+28));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s2tz1", cols1);
		String [][] records_s2tz1 = new String[171][cols1.length];
		for(int i=0;i<171;i++)
		{
			records_s2tz1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s2tz1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s2tz1", records_s2tz1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record166: [a166, b0, c1, d2, e1, f4, g5, h6, i4]
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i5"};
		DBApp.selectIndex("s2tz1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 19, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record3: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		//record4: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		String tableTrace1 = DBApp.getLastTrace("s2tz1");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+19));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("aeap0", cols2);
		String [][] records_aeap0 = new String[379][cols2.length];
		for(int i=0;i<379;i++)
		{
			records_aeap0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_aeap0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("aeap0", records_aeap0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0]
		//record377: [a377, b1, c2, d1, e2, f5, g6, h1]
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2]
		String[] ConditionColumns2 = {"g","f","e"};
		String[] ConditionColumnsValues2 = {"g4","f5","e0"};
		DBApp.selectIndex("aeap0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a95, b1, c2, d3, e0, f5, g4, h7]
		//record1: [a305, b1, c2, d1, e0, f5, g4, h1]
		String tableTrace2 = DBApp.getLastTrace("aeap0");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qc1", cols3);
		String [][] records_qc1 = new String[145][cols3.length];
		for(int i=0;i<145;i++)
		{
			records_qc1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qc1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qc1", records_qc1[i]);
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
		String[] ConditionColumns3 = {"l"};
		String[] ConditionColumnsValues3 = {"l6"};
		DBApp.selectIndex("qc1", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 12, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6]
		String tableTrace3 = DBApp.getLastTrace("qc1");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+12));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("c9", cols0);
		String [][] records_c9 = new String[113][cols0.length];
		for(int i=0;i<113;i++)
		{
			records_c9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c9", records_c9[i]);
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
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c0"};
		DBApp.selectIndex("c9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 38, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record3: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9]
		//record4: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		String tableTrace0 = DBApp.getLastTrace("c9");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+38));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tk", cols0);
		String [][] records_tk = new String[129][cols0.length];
		for(int i=0;i<129;i++)
		{
			records_tk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tk", records_tk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record124: [a124, b0, c1, d0, e4, f4, g5]
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		DBApp.selectIndex("tk", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 22, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a24, b0, c0, d0, e4, f0, g3]
		String tableTrace0 = DBApp.getLastTrace("tk");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+22));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("p39", cols0);
		String [][] records_p39 = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_p39[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p39[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p39", records_p39[i]);
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
		String[] ConditionColumns0 = {"g","h","j","c"};
		String[] ConditionColumnsValues0 = {"g5","h7","j3","c1"};
		DBApp.selectIndex("p39", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13, p7, q1, r13]
		String tableTrace0 = DBApp.getLastTrace("p39");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m3w0", cols1);
		String [][] records_m3w0 = new String[267][cols1.length];
		for(int i=0;i<267;i++)
		{
			records_m3w0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m3w0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m3w0", records_m3w0[i]);
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
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c0"};
		DBApp.selectIndex("m3w0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 89, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		String tableTrace1 = DBApp.getLastTrace("m3w0");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+89));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d96", cols0);
		String [][] records_d96 = new String[213][cols0.length];
		for(int i=0;i<213;i++)
		{
			records_d96[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d96[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d96", records_d96[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j0"};
		DBApp.selectIndex("d96", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 22, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record4: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		String tableTrace0 = DBApp.getLastTrace("d96");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+22));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x5q", cols1);
		String [][] records_x5q = new String[340][cols1.length];
		for(int i=0;i<340;i++)
		{
			records_x5q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x5q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x5q", records_x5q[i]);
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
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		DBApp.selectIndex("x5q", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 170, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		String tableTrace1 = DBApp.getLastTrace("x5q");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+170));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qcm", cols2);
		String [][] records_qcm = new String[114][cols2.length];
		for(int i=0;i<114;i++)
		{
			records_qcm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qcm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qcm", records_qcm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5]
		String[] ConditionColumns2 = {"e","i"};
		String[] ConditionColumnsValues2 = {"e1","i5"};
		DBApp.selectIndex("qcm", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 2, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		//record1: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		String tableTrace2 = DBApp.getLastTrace("qcm");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i7237", cols0);
		String [][] records_i7237 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			records_i7237[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i7237[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i7237", records_i7237[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		String[] ConditionColumns0 = {"j"};
		String[] ConditionColumnsValues0 = {"j7"};
		DBApp.selectIndex("i7237", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record1: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record2: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		String tableTrace0 = DBApp.getLastTrace("i7237");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("sh1", cols1);
		String [][] records_sh1 = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			records_sh1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sh1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sh1", records_sh1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		String[] ConditionColumns1 = {"l","h","f","n","e","k","a"};
		String[] ConditionColumnsValues1 = {"l2","h2","f2","n2","e2","k2","a2"};
		DBApp.selectIndex("sh1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		String tableTrace1 = DBApp.getLastTrace("sh1");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e7", cols2);
		String [][] records_e7 = new String[189][cols2.length];
		for(int i=0;i<189;i++)
		{
			records_e7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e7", records_e7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8]
		String[] ConditionColumns2 = {"f","g"};
		String[] ConditionColumnsValues2 = {"f5","g6"};
		DBApp.selectIndex("e7", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 4, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		//record1: [a83, b1, c2, d3, e3, f5, g6, h3, i2]
		//record2: [a125, b1, c2, d1, e0, f5, g6, h5, i8]
		//record3: [a167, b1, c2, d3, e2, f5, g6, h7, i5]
		String tableTrace2 = DBApp.getLastTrace("e7");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+4));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xf72", cols3);
		String [][] records_xf72 = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			records_xf72[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xf72[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xf72", records_xf72[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1]
		//record93: [a93, b1, c0, d1, e3, f3, g2]
		//record94: [a94, b0, c1, d2, e4, f4, g3]
		//record95: [a95, b1, c2, d3, e0, f5, g4]
		//record96: [a96, b0, c0, d0, e1, f0, g5]
		String[] ConditionColumns3 = {"a"};
		String[] ConditionColumnsValues3 = {"a9"};
		DBApp.selectIndex("xf72", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2]
		String tableTrace3 = DBApp.getLastTrace("xf72");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p63", cols0);
		String [][] records_p63 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			records_p63[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p63[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p63", records_p63[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g6"};
		DBApp.selectIndex("p63", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 3, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a20, b0, c2, d0, e0, f2, g6]
		String tableTrace0 = DBApp.getLastTrace("p63");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vv20", cols1);
		String [][] records_vv20 = new String[201][cols1.length];
		for(int i=0;i<201;i++)
		{
			records_vv20[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vv20[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vv20", records_vv20[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record196: [a196, b0, c1, d0, e1, f4, g0]
		//record197: [a197, b1, c2, d1, e2, f5, g1]
		//record198: [a198, b0, c0, d2, e3, f0, g2]
		//record199: [a199, b1, c1, d3, e4, f1, g3]
		//record200: [a200, b0, c2, d0, e0, f2, g4]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		DBApp.selectIndex("vv20", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 50, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a9, b1, c0, d1, e4, f3, g2]
		//record3: [a13, b1, c1, d1, e3, f1, g6]
		//record4: [a17, b1, c2, d1, e2, f5, g3]
		String tableTrace1 = DBApp.getLastTrace("vv20");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+50));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("hx5", cols0);
		String [][] records_hx5 = new String[281][cols0.length];
		for(int i=0;i<281;i++)
		{
			records_hx5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hx5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hx5", records_hx5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5]
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6]
		//record279: [a279, b1, c0, d3, e4, f3, g6, h7]
		//record280: [a280, b0, c1, d0, e0, f4, g0, h0]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a125"};
		DBApp.selectIndex("hx5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a125, b1, c2, d1, e0, f5, g6, h5]
		String tableTrace0 = DBApp.getLastTrace("hx5");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l2y", cols0);
		String [][] records_l2y = new String[146][cols0.length];
		for(int i=0;i<146;i++)
		{
			records_l2y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l2y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l2y", records_l2y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1, o6]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2, o7]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3, o8]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4, o9]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10]
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g1"};
		DBApp.selectIndex("l2y", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 21, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record3: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		String tableTrace0 = DBApp.getLastTrace("l2y");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+21));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("e68", cols1);
		String [][] records_e68 = new String[387][cols1.length];
		for(int i=0;i<387;i++)
		{
			records_e68[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e68[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e68", records_e68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4, o7, p14, q8, r4, s2]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9, r5, s3]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6, o9, p0, q10, r6, s4]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10, p1, q11, r7, s5]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1, l2, m9, n8, o11, p2, q12, r8, s6]
		String[] ConditionColumns1 = {"d","g","f","m","c"};
		String[] ConditionColumnsValues1 = {"d2","g2","f2","m0","c2"};
		DBApp.selectIndex("e68", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2, q15, r14, s15]
		String tableTrace1 = DBApp.getLastTrace("e68");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("whp9", cols2);
		String [][] records_whp9 = new String[411][cols2.length];
		for(int i=0;i<411;i++)
		{
			records_whp9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_whp9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("whp9", records_whp9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10, m3, n0, o1, p6, q15, r10, s7, t6]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11, m4, n1, o2, p7, q16, r11, s8, t7]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1, l0, m5, n2, o3, p8, q0, r12, s9, t8]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2, l1, m6, n3, o4, p9, q1, r13, s10, t9]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2, m7, n4, o5, p10, q2, r14, s11, t10]
		String[] ConditionColumns2 = {"k","h","j","e"};
		String[] ConditionColumnsValues2 = {"k10","h1","j3","e3"};
		DBApp.selectIndex("whp9", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9, m10, n13, o3, p9, q0, r9, s1, t13]
		String tableTrace2 = DBApp.getLastTrace("whp9");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v2f", cols0);
		String [][] records_v2f = new String[342][cols0.length];
		for(int i=0;i<342;i++)
		{
			records_v2f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v2f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v2f", records_v2f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		String[] ConditionColumns0 = {"d","f"};
		String[] ConditionColumnsValues0 = {"d3","f5"};
		DBApp.selectIndex("v2f", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a23, b1, c2, d3, e3, f5, g2]
		//record2: [a35, b1, c2, d3, e0, f5, g0]
		//record3: [a47, b1, c2, d3, e2, f5, g5]
		//record4: [a59, b1, c2, d3, e4, f5, g3]
		String tableTrace0 = DBApp.getLastTrace("v2f");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+28));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qcq61", cols1);
		String [][] records_qcq61 = new String[151][cols1.length];
		for(int i=0;i<151;i++)
		{
			records_qcq61[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qcq61[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qcq61", records_qcq61[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record146: [a146, b0, c2, d2, e1, f2, g6]
		//record147: [a147, b1, c0, d3, e2, f3, g0]
		//record148: [a148, b0, c1, d0, e3, f4, g1]
		//record149: [a149, b1, c2, d1, e4, f5, g2]
		//record150: [a150, b0, c0, d2, e0, f0, g3]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		DBApp.selectIndex("qcq61", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 25, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a19, b1, c1, d3, e4, f1, g5]
		//record4: [a25, b1, c1, d1, e0, f1, g4]
		String tableTrace1 = DBApp.getLastTrace("qcq61");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+25));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ct4", cols2);
		String [][] records_ct4 = new String[105][cols2.length];
		for(int i=0;i<105;i++)
		{
			records_ct4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ct4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ct4", records_ct4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5, q16]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4, o12, p6, q0]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13, p7, q1]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6, o14, p8, q2]
		String[] ConditionColumns2 = {"o"};
		String[] ConditionColumnsValues2 = {"o5"};
		DBApp.selectIndex("ct4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 7, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record2: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1]
		//record3: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5, p2, q16]
		//record4: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5, p1, q14]
		String tableTrace2 = DBApp.getLastTrace("ct4");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexedTrace_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("plb7g", cols0);
		String [][] records_plb7g = new String[456][cols0.length];
		for(int i=0;i<456;i++)
		{
			records_plb7g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_plb7g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("plb7g", records_plb7g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3, o1, p3, q9, r1, s14]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4, o2, p4, q10, r2, s15]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5, o3, p5, q11, r3, s16]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6, q12, r4, s17]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7, o5, p7, q13, r5, s18]
		String[] ConditionColumns0 = {"e","k","h","l"};
		String[] ConditionColumnsValues0 = {"e0","k10","h4","l4"};
		DBApp.selectIndex("plb7g", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4, q0, r16, s17]
		String tableTrace0 = DBApp.getLastTrace("plb7g");
		Arrays.sort(ConditionColumns0);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ov", cols1);
		String [][] records_ov = new String[248][cols1.length];
		for(int i=0;i<248;i++)
		{
			records_ov[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ov[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ov", records_ov[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7]
		String[] ConditionColumns1 = {"g","h"};
		String[] ConditionColumnsValues1 = {"g4","h3"};
		DBApp.selectIndex("ov", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 5, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record1: [a67, b1, c1, d3, e2, f1, g4, h3]
		//record2: [a123, b1, c0, d3, e3, f3, g4, h3]
		//record3: [a179, b1, c2, d3, e4, f5, g4, h3]
		//record4: [a235, b1, c1, d3, e0, f1, g4, h3]
		String tableTrace1 = DBApp.getLastTrace("ov");
		Arrays.sort(ConditionColumns1);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace1.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace1.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace1.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace1.contains(Arrays.toString(ConditionColumns1)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace1.contains("Final count: "+5));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("vq3x5", cols2);
		String [][] records_vq3x5 = new String[161][cols2.length];
		for(int i=0;i<161;i++)
		{
			records_vq3x5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vq3x5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vq3x5", records_vq3x5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6, p12, q3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7, p13, q4]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4, o8, p14, q5]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9, p15, q6]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10, p0, q7]
		String[] ConditionColumns2 = {"l","j","c","n","i","m","p","h"};
		String[] ConditionColumnsValues2 = {"l5","j9","c2","n5","i8","m11","p9","h1"};
		DBApp.selectIndex("vq3x5", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11, n5, o14, p9, q4]
		String tableTrace2 = DBApp.getLastTrace("vq3x5");
		Arrays.sort(ConditionColumns2);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace2.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace2.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace2.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace2.contains(Arrays.toString(ConditionColumns2)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace2.contains("Final count: "+1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("l3eg3", cols3);
		String [][] records_l3eg3 = new String[431][cols3.length];
		for(int i=0;i<431;i++)
		{
			records_l3eg3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l3eg3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l3eg3", records_l3eg3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10]
		String[] ConditionColumns3 = {"f","e","i","k"};
		String[] ConditionColumnsValues3 = {"f0","e1","i0","k9"};
		DBApp.selectIndex("l3eg3", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6]
		String tableTrace3 = DBApp.getLastTrace("l3eg3");
		Arrays.sort(ConditionColumns3);
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace3.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace3.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace3.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace3.contains(Arrays.toString(ConditionColumns3)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace3.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(228);
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