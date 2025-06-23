package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTestsMS2
{

	@Test(timeout = 1000000)
	public void test0TableValidationSoundDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"pcn9n","cx2","e5jt","ty812","rxyg"};
		DBApp.createTable("rrkg", cols0);
		for(int i=0;i<91;i++)
		{
			String [] record_rrkg = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rrkg[j] = genRandString();
			}
			DBApp.insert("rrkg", record_rrkg);
		}
		ArrayList<String[]> tr_rrkg =DBApp.validateRecords("rrkg");
		assertTrue("Validating a non-afected table should report zero sized missing records list.", tr_rrkg.size() == 0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableValidationSoundTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"os","vuv","x1v4","v0","q14","rmaf","yxb","zv"};
		DBApp.createTable("ge3", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_ge3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ge3[j] = genRandString();
			}
			DBApp.insert("ge3", record_ge3);
		}
		DBApp.validateRecords("ge3");
		String tableTrace0 = DBApp.getLastTrace("ge3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-afected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableValidationAffectedDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y633", cols0);
		String [][] records_y633 = new String[179][cols0.length];
		for(int i=0;i<179;i++)
		{
			records_y633[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y633[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y633", records_y633[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2]
		int pageCount0 = (int)Math.ceil(179.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(Math.random()>0.75)
			{
				File dir_y633 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y633"+ File.separator+i+".db");
				dir_y633.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("y633");
		assertTrue("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size() == (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableValidationAffectedTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j4", cols0);
		String [][] records_j4 = new String[173][cols0.length];
		for(int i=0;i<173;i++)
		{
			records_j4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j4[i][j] = cols0[j]+((i%(j+1)));
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
		//record168: [a168, b0, c0, d0, e3, f0, g0]
		//record169: [a169, b1, c1, d1, e4, f1, g1]
		//record170: [a170, b0, c2, d2, e0, f2, g2]
		//record171: [a171, b1, c0, d3, e1, f3, g3]
		//record172: [a172, b0, c1, d0, e2, f4, g4]
		int pageCount0 = (int)Math.ceil(173.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(Math.random()>0.75)
			{
				File dir_j4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j4"+ File.separator+i+".db");
				dir_j4.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("j4");
		String tableTrace0 = DBApp.getLastTrace("j4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableRecoverDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("z21", cols0);
		String [][] records_z21 = new String[456][cols0.length];
		for(int i=0;i<456;i++)
		{
			records_z21[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z21[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z21", records_z21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3, o1, p3, q9]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4, o2, p4, q10]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5, o3, p5, q11]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6, q12]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7, o5, p7, q13]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("z21");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(456.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(Math.random()>0.75)
			{
				File dir_z21 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z21"+ File.separator+i+".db");
				dir_z21.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_z21[j]);
				}
			}
		}
		DBApp.recoverRecords("z21", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("z21");
		assertTrue("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size() == selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertTrue("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)).equals(Arrays.toString(selectAfterRecovery0.get(i))));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableRecoverTrceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z0g", cols0);
		String [][] records_z0g = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			records_z0g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z0g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z0g", records_z0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3, f3, g0]
		//record64: [a64, b0, c1, d0, e4, f4, g1]
		//record65: [a65, b1, c2, d1, e0, f5, g2]
		//record66: [a66, b0, c0, d2, e1, f0, g3]
		//record67: [a67, b1, c1, d3, e2, f1, g4]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(Math.random()>0.75)
			{
				File dir_z0g = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z0g"+ File.separator+i+".db");
				dir_z0g.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_z0g[j]);
				}
			}
		}
		DBApp.recoverRecords("z0g", missing0);
		String tableTrace0 = DBApp.getLastTrace("z0g");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionNonIndexedDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gaz", cols0);
		String [][] records_gaz = new String[230][cols0.length];
		for(int i=0;i<230;i++)
		{
			records_gaz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gaz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gaz", records_gaz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9, m4, n1, o0]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5, n2, o1]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3, o2]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4, o3]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("gaz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 46, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		assertTrue("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size() == 46);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9","o8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionNonIndexedTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t43", cols0);
		String [][] records_t43 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			records_t43[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t43[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t43", records_t43[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		//record49: [a49, b1, c1, d1, e4, f1, g0]
		//record50: [a50, b0, c2, d2, e0, f2, g1]
		//record51: [a51, b1, c0, d3, e1, f3, g2]
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d0"};
		DBApp.selectIndex("t43", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		String tableTrace0 = DBApp.getLastTrace("t43");
		assertTrue("Select Index when no columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when no columns are indexed Should Not report indexed columns when non are.", !tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when no columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when no columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when no columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+13));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionSomeIndexedDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fqg", cols0);
		String [][] records_fqg = new String[419][cols0.length];
		for(int i=0;i<419;i++)
		{
			records_fqg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fqg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fqg", records_fqg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		//record416: [a416, b0, c2, d0, e1, f2, g3]
		//record417: [a417, b1, c0, d1, e2, f3, g4]
		//record418: [a418, b0, c1, d2, e3, f4, g5]
		String[] ConditionColumns0 = {"b","f","e"};
		String[] ConditionColumnsValues0 = {"b0","f0","e4"};
		DBApp.createBitMapIndex("fqg","b");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("fqg", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 14, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3]
		//record1: [a54, b0, c0, d2, e4, f0, g5]
		//record2: [a84, b0, c0, d0, e4, f0, g0]
		//record3: [a114, b0, c0, d2, e4, f0, g2]
		//record4: [a144, b0, c0, d0, e4, f0, g4]
		assertTrue("Conditional Index selection when some columns are indexed should return the correct number of records.", tableSelect0.size() == 14);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a84","b0","c0","d0","e4","f0","g0"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a114","b0","c0","d2","e4","f0","g2"})));
		assertTrue("Conditional Index selection when some columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a144","b0","c0","d0","e4","f0","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionSomeIndexedTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("az6r8", cols0);
		String [][] records_az6r8 = new String[429][cols0.length];
		for(int i=0;i<429;i++)
		{
			records_az6r8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_az6r8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("az6r8", records_az6r8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8]
		String[] ConditionColumns0 = {"g","i","b","d","e"};
		String[] ConditionColumnsValues0 = {"g2","i7","b0","d2","e4"};
		DBApp.createBitMapIndex("az6r8","i");
		DBApp.createBitMapIndex("az6r8","b");
		DBApp.createBitMapIndex("az6r8","d");
		DBApp.selectIndex("az6r8", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4]
		String tableTrace0 = DBApp.getLastTrace("az6r8");
		assertTrue("Select Index when some columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when some columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains("[i, b, d]"));
		assertTrue("Select Index when some columns are indexed Should report non indexed columns.", tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when some columns are indexed Should report the correct set of non indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when some columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionAllIndexedDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("yb10", cols0);
		String [][] records_yb10 = new String[334][cols0.length];
		for(int i=0;i<334;i++)
		{
			records_yb10[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yb10[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yb10", records_yb10[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3]
		String[] ConditionColumns0 = {"d","f"};
		String[] ConditionColumnsValues0 = {"d2","f0"};
		DBApp.createBitMapIndex("yb10","d");
		DBApp.createBitMapIndex("yb10","f");
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("yb10", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 28, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record2: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record3: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10]
		assertTrue("Conditional Index selection when all columns are indexed should return the correct number of records.", tableSelect0.size() == 28);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a42","b0","c0","d2","e2","f0","g0","h2","i6","j2","k9"})));
		assertTrue("Conditional Index selection when all columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableSelectionIndexConditionAllIndexedTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yba", cols0);
		String [][] records_yba = new String[452][cols0.length];
		for(int i=0;i<452;i++)
		{
			records_yba[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yba[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yba", records_yba[i]);
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
		String[] ConditionColumns0 = {"e","a","d"};
		String[] ConditionColumnsValues0 = {"e3","a148","d0"};
		DBApp.createBitMapIndex("yba","e");
		DBApp.createBitMapIndex("yba","a");
		DBApp.createBitMapIndex("yba","d");
		DBApp.selectIndex("yba", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a148, b0, c1, d0, e3, f4, g1]
		String tableTrace0 = DBApp.getLastTrace("yba");
		assertTrue("Select Index when all columns are indexed Should appear in the last trace.", tableTrace0.contains("Select index"));
		assertTrue("Select Index when all columns are indexed Should report indexed columns.", tableTrace0.contains("Indexed columns:"));
		assertTrue("Select Index when all columns are indexed Should report the correct set of indexed columns.", tableTrace0.contains(Arrays.toString(ConditionColumns0)));
		assertTrue("Select Index when all columns are indexed Should not report non indexed columns.", !tableTrace0.contains("Non Indexed:"));
		assertTrue("Select Index when all columns are indexed Should report the correct number of selected records.", tableTrace0.contains("Final count: "+1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0CreationIndexFullTraceDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("chj", cols0);
		String tableTrace0 = DBApp.getFullTrace("chj");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("chj","a");
		DBApp.createBitMapIndex("chj","b");
		DBApp.createBitMapIndex("chj","c");
		DBApp.createBitMapIndex("chj","d");
		DBApp.createBitMapIndex("chj","e");
		DBApp.createBitMapIndex("chj","f");
		DBApp.createBitMapIndex("chj","g");
		DBApp.createBitMapIndex("chj","h");
		DBApp.createBitMapIndex("chj","i");
		DBApp.createBitMapIndex("chj","j");
		DBApp.createBitMapIndex("chj","k");
		DBApp.createBitMapIndex("chj","l");
		DBApp.createBitMapIndex("chj","m");
		tableTrace0 = DBApp.getFullTrace("chj");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionPreIndexingDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rv", cols0);
		//To be indexed: [c, e, g], to be queried: [c0, e0, g7].
		String [][] records_rv = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_rv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rv", records_rv[i]);
		}
		DBApp.createBitMapIndex("rv","c");
		DBApp.createBitMapIndex("rv","e");
		DBApp.createBitMapIndex("rv","g");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//last 15 records:
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rv", "c", "c0").equals("1001001001001001001001"));
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rv", "e", "e0").equals("1000010000100001000010"));
		assertTrue("Bitmap index should return a sequence of zeros fo non-present values.", DBApp.getValueBits("rv", "g", "g7").equals("0000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	@Test(timeout = 1000000)
	public void test0TableInsertionPostIndexingDBApp() throws Exception
	{
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u2d", cols0);
		DBApp.createBitMapIndex("u2d","b");
		DBApp.createBitMapIndex("u2d","c");
		DBApp.createBitMapIndex("u2d","d");
		DBApp.createBitMapIndex("u2d","f");
		DBApp.createBitMapIndex("u2d","g");
		String [][] records_u2d = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			records_u2d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u2d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u2d", records_u2d[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//last 15 records:
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//record9: [a9, b1, c0, d1, e4, f3, g2]
		//record10: [a10, b0, c1, d2, e0, f4, g3]
		//record11: [a11, b1, c2, d3, e1, f5, g4]
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2d", "b", "b0").equals("10101010101010101010"));
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2d", "c", "c1").equals("01001001001001001001"));
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2d", "d", "d0").equals("10001000100010001000"));
		assertTrue("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2d", "f", "f0").equals("10000010000010000010"));
		assertTrue("Bitmap index should return a sequence of zeros fo non-present values.", DBApp.getValueBits("u2d", "g", "g7").equals("00000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1111);
		return (int) (random.nextDouble()*max);
	}
	private static String genRandString() 
	{
		char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		char[] digits = "0123456789".toCharArray();
		int nameSize = genRandNum(5);
		if (nameSize < 2)
			nameSize += 2;
		String res = "";
		res += alphabet[(genRandNum(alphabet.length)) - 1];
		for (int i = 1; i < nameSize; i++)
			if (Math.random() >= 0.5)
				res += alphabet[(genRandNum(alphabet.length)) - 1];
			else
				res += digits[(genRandNum(digits.length)) - 1];
		return res;
	}
}