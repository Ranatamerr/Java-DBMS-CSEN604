package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_03
{

	@Test(timeout = 1000000)
	public void TableRecover_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x48", cols0);
		String [][] records_x48 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			records_x48[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x48[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x48", records_x48[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("x48");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x48 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x48"+ File.separator+i+".db");
				dir_x48.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_x48[j]);
				}
			}
		}
		DBApp.recoverRecords("x48", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("x48");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s87", cols1);
		String [][] records_s87 = new String[266][cols1.length];
		for(int i=0;i<266;i++)
		{
			records_s87[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s87[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s87", records_s87[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10]
		//record263: [a263, b1, c2, d3, e3, f5, g4, h7, i2, j3, k10, l11]
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("s87");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(266.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s87 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s87"+ File.separator+i+".db");
				dir_s87.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_s87[j]);
				}
			}
		}
		DBApp.recoverRecords("s87", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("s87");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("w2", cols0);
		String [][] records_w2 = new String[283][cols0.length];
		for(int i=0;i<283;i++)
		{
			records_w2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w2", records_w2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8]
		//record279: [a279, b1, c0, d3, e4, f3, g6, h7, i0, j9]
		//record280: [a280, b0, c1, d0, e0, f4, g0, h0, i1, j0]
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("w2");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(283.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w2"+ File.separator+i+".db");
				dir_w2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_w2[j]);
				}
			}
		}
		DBApp.recoverRecords("w2", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("w2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ge6we", cols1);
		String [][] records_ge6we = new String[427][cols1.length];
		for(int i=0;i<427;i++)
		{
			records_ge6we[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ge6we[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ge6we", records_ge6we[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("ge6we");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(427.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ge6we = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ge6we"+ File.separator+i+".db");
				dir_ge6we.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ge6we[j]);
				}
			}
		}
		DBApp.recoverRecords("ge6we", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("ge6we");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oq0d", cols2);
		String [][] records_oq0d = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_oq0d[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oq0d[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oq0d", records_oq0d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("oq0d");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_oq0d = new File(FileManager.directory.getAbsolutePath()+ File.separator + "oq0d"+ File.separator+i+".db");
				dir_oq0d.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_oq0d[j]);
				}
			}
		}
		DBApp.recoverRecords("oq0d", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("oq0d");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qdn9", cols0);
		String [][] records_qdn9 = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_qdn9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qdn9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qdn9", records_qdn9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record172: [a172, b0, c1, d0, e2, f4, g4]
		//record173: [a173, b1, c2, d1, e3, f5, g5]
		//record174: [a174, b0, c0, d2, e4, f0, g6]
		//record175: [a175, b1, c1, d3, e0, f1, g0]
		//record176: [a176, b0, c2, d0, e1, f2, g1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("qdn9");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(177.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qdn9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qdn9"+ File.separator+i+".db");
				dir_qdn9.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_qdn9[j]);
				}
			}
		}
		DBApp.recoverRecords("qdn9", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("qdn9");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("biwr9", cols1);
		String [][] records_biwr9 = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			records_biwr9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_biwr9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("biwr9", records_biwr9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("biwr9");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_biwr9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "biwr9"+ File.separator+i+".db");
				dir_biwr9.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_biwr9[j]);
				}
			}
		}
		DBApp.recoverRecords("biwr9", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("biwr9");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("lzf", cols0);
		String [][] records_lzf = new String[269][cols0.length];
		for(int i=0;i<269;i++)
		{
			records_lzf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lzf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lzf", records_lzf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9, p8, q9, r12, s17, t4]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10, p9, q10, r13, s18, t5]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11, p10, q11, r14, s0, t6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12, p11, q12, r15, s1, t7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13, p12, q13, r16, s2, t8]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("lzf");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(269.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lzf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lzf"+ File.separator+i+".db");
				dir_lzf.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_lzf[j]);
				}
			}
		}
		DBApp.recoverRecords("lzf", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("lzf");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l7q", cols1);
		String [][] records_l7q = new String[380][cols1.length];
		for(int i=0;i<380;i++)
		{
			records_l7q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l7q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l7q", records_l7q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record375: [a375, b1, c0, d3, e0, f3, g4]
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("l7q");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(380.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l7q = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l7q"+ File.separator+i+".db");
				dir_l7q.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_l7q[j]);
				}
			}
		}
		DBApp.recoverRecords("l7q", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("l7q");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sp7", cols2);
		String [][] records_sp7 = new String[274][cols2.length];
		for(int i=0;i<274;i++)
		{
			records_sp7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sp7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sp7", records_sp7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record269: [a269, b1, c2, d1, e4, f5, g3]
		//record270: [a270, b0, c0, d2, e0, f0, g4]
		//record271: [a271, b1, c1, d3, e1, f1, g5]
		//record272: [a272, b0, c2, d0, e2, f2, g6]
		//record273: [a273, b1, c0, d1, e3, f3, g0]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("sp7");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(274.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_sp7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "sp7"+ File.separator+i+".db");
				dir_sp7.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_sp7[j]);
				}
			}
		}
		DBApp.recoverRecords("sp7", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("sp7");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("h29", cols0);
		String [][] records_h29 = new String[139][cols0.length];
		for(int i=0;i<139;i++)
		{
			records_h29[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h29[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h29", records_h29[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8, o14, p6, q15, r8, s1, t14]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5, n9, o0, p7, q16, r9, s2, t15]
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0, r10, s3, t16]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1, r11, s4, t17]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2, r12, s5, t18]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("h29");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(139.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h29 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h29"+ File.separator+i+".db");
				dir_h29.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_h29[j]);
				}
			}
		}
		DBApp.recoverRecords("h29", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("h29");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("w619h", cols1);
		String [][] records_w619h = new String[442][cols1.length];
		for(int i=0;i<442;i++)
		{
			records_w619h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w619h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w619h", records_w619h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2, p5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4, o3, p6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5, o4, p7]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6, o5, p8]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7, o6, p9]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("w619h");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(442.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w619h = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w619h"+ File.separator+i+".db");
				dir_w619h.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_w619h[j]);
				}
			}
		}
		DBApp.recoverRecords("w619h", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("w619h");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("pq", cols2);
		String [][] records_pq = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			records_pq[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pq[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pq", records_pq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("pq");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pq = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pq"+ File.separator+i+".db");
				dir_pq.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_pq[j]);
				}
			}
		}
		DBApp.recoverRecords("pq", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("pq");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lz5", cols0);
		String [][] records_lz5 = new String[381][cols0.length];
		for(int i=0;i<381;i++)
		{
			records_lz5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lz5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lz5", records_lz5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("lz5");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(381.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lz5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lz5"+ File.separator+i+".db");
				dir_lz5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_lz5[j]);
				}
			}
		}
		DBApp.recoverRecords("lz5", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("lz5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ojm0q", cols1);
		String [][] records_ojm0q = new String[416][cols1.length];
		for(int i=0;i<416;i++)
		{
			records_ojm0q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ojm0q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ojm0q", records_ojm0q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3, m8, n5, o6, p11, q3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9, n6, o7, p12, q4]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10, n7, o8, p13, q5]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8, o9, p14, q6]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12, n9, o10, p15, q7]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("ojm0q");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(416.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ojm0q = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ojm0q"+ File.separator+i+".db");
				dir_ojm0q.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ojm0q[j]);
				}
			}
		}
		DBApp.recoverRecords("ojm0q", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("ojm0q");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("b78", cols0);
		String [][] records_b78 = new String[234][cols0.length];
		for(int i=0;i<234;i++)
		{
			records_b78[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b78[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b78", records_b78[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4, p5, q8, r13, s1, t9]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6, o5, p6, q9, r14, s2, t10]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7, o6, p7, q10, r15, s3, t11]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8, q11, r16, s4, t12]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9, q12, r17, s5, t13]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("b78");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(234.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b78 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b78"+ File.separator+i+".db");
				dir_b78.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_b78[j]);
				}
			}
		}
		DBApp.recoverRecords("b78", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("b78");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("f080", cols1);
		String [][] records_f080 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			records_f080[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f080[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f080", records_f080[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("f080");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f080 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f080"+ File.separator+i+".db");
				dir_f080.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_f080[j]);
				}
			}
		}
		DBApp.recoverRecords("f080", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("f080");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("cljv", cols2);
		String [][] records_cljv = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			records_cljv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cljv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cljv", records_cljv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("cljv");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cljv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cljv"+ File.separator+i+".db");
				dir_cljv.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_cljv[j]);
				}
			}
		}
		DBApp.recoverRecords("cljv", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("cljv");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("wa", cols3);
		String [][] records_wa = new String[441][cols3.length];
		for(int i=0;i<441;i++)
		{
			records_wa[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wa[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wa", records_wa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("wa");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(441.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wa = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wa"+ File.separator+i+".db");
				dir_wa.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_wa[j]);
				}
			}
		}
		DBApp.recoverRecords("wa", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("wa");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("nc667", cols0);
		String [][] records_nc667 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			records_nc667[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nc667[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nc667", records_nc667[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("nc667");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_nc667 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "nc667"+ File.separator+i+".db");
				dir_nc667.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_nc667[j]);
				}
			}
		}
		DBApp.recoverRecords("nc667", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("nc667");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b6i", cols1);
		String [][] records_b6i = new String[184][cols1.length];
		for(int i=0;i<184;i++)
		{
			records_b6i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b6i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b6i", records_b6i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8, j9, k3, l11, m10, n11]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4, i0, j0, k4, l0, m11, n12]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0]
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("b6i");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(184.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b6i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b6i"+ File.separator+i+".db");
				dir_b6i.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_b6i[j]);
				}
			}
		}
		DBApp.recoverRecords("b6i", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("b6i");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("pt2", cols0);
		String [][] records_pt2 = new String[121][cols0.length];
		for(int i=0;i<121;i++)
		{
			records_pt2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pt2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pt2", records_pt2[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("pt2");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(121.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pt2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pt2"+ File.separator+i+".db");
				dir_pt2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_pt2[j]);
				}
			}
		}
		DBApp.recoverRecords("pt2", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("pt2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("jr", cols1);
		String [][] records_jr = new String[121][cols1.length];
		for(int i=0;i<121;i++)
		{
			records_jr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jr", records_jr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6]
		//record119: [a119, b1, c2, d3, e4, f5, g0, h7]
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("jr");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(121.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jr"+ File.separator+i+".db");
				dir_jr.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_jr[j]);
				}
			}
		}
		DBApp.recoverRecords("jr", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("jr");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yj", cols2);
		String [][] records_yj = new String[239][cols2.length];
		for(int i=0;i<239;i++)
		{
			records_yj[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yj[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yj", records_yj[i]);
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
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("yj");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(239.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yj"+ File.separator+i+".db");
				dir_yj.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_yj[j]);
				}
			}
		}
		DBApp.recoverRecords("yj", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("yj");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z28", cols0);
		String [][] records_z28 = new String[215][cols0.length];
		for(int i=0;i<215;i++)
		{
			records_z28[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z28[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z28", records_z28[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record210: [a210, b0, c0, d2, e0, f0, g0]
		//record211: [a211, b1, c1, d3, e1, f1, g1]
		//record212: [a212, b0, c2, d0, e2, f2, g2]
		//record213: [a213, b1, c0, d1, e3, f3, g3]
		//record214: [a214, b0, c1, d2, e4, f4, g4]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("z28");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(215.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z28 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z28"+ File.separator+i+".db");
				dir_z28.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_z28[j]);
				}
			}
		}
		DBApp.recoverRecords("z28", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("z28");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("grnw", cols1);
		String [][] records_grnw = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			records_grnw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_grnw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("grnw", records_grnw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("grnw");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_grnw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "grnw"+ File.separator+i+".db");
				dir_grnw.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_grnw[j]);
				}
			}
		}
		DBApp.recoverRecords("grnw", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("grnw");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x1h", cols0);
		String [][] records_x1h = new String[263][cols0.length];
		for(int i=0;i<263;i++)
		{
			records_x1h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x1h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x1h", records_x1h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9]
		//record262: [a262, b0, c1, d2, e2, f4, g3, h6, i1, j2, k9, l10]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("x1h");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(263.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x1h = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x1h"+ File.separator+i+".db");
				dir_x1h.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_x1h[j]);
				}
			}
		}
		DBApp.recoverRecords("x1h", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("x1h");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("gn7", cols1);
		String [][] records_gn7 = new String[338][cols1.length];
		for(int i=0;i<338;i++)
		{
			records_gn7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gn7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gn7", records_gn7[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("gn7");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(338.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gn7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gn7"+ File.separator+i+".db");
				dir_gn7.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_gn7[j]);
				}
			}
		}
		DBApp.recoverRecords("gn7", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("gn7");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("m4j", cols2);
		String [][] records_m4j = new String[488][cols2.length];
		for(int i=0;i<488;i++)
		{
			records_m4j[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m4j[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m4j", records_m4j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3, m2, n7, o3, p3, q7, r15]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3, n8, o4, p4, q8, r16]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5, m4, n9, o5, p5, q9, r17]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5, n10, o6, p6, q10, r0]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7, p7, q11, r1]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("m4j");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(488.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m4j = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m4j"+ File.separator+i+".db");
				dir_m4j.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_m4j[j]);
				}
			}
		}
		DBApp.recoverRecords("m4j", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("m4j");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("r0", cols0);
		String [][] records_r0 = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			records_r0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r0", records_r0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6, p3, q0, r15, s13]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7, p4, q1, r16, s14]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8, p5, q2, r17, s15]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9, p6, q3, r0, s16]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13, o10, p7, q4, r1, s17]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("r0");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r0 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r0"+ File.separator+i+".db");
				dir_r0.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_r0[j]);
				}
			}
		}
		DBApp.recoverRecords("r0", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("r0");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jxp", cols1);
		String [][] records_jxp = new String[351][cols1.length];
		for(int i=0;i<351;i++)
		{
			records_jxp[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jxp[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jxp", records_jxp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record346: [a346, b0, c1, d2, e1, f4, g3]
		//record347: [a347, b1, c2, d3, e2, f5, g4]
		//record348: [a348, b0, c0, d0, e3, f0, g5]
		//record349: [a349, b1, c1, d1, e4, f1, g6]
		//record350: [a350, b0, c2, d2, e0, f2, g0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("jxp");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(351.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jxp = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jxp"+ File.separator+i+".db");
				dir_jxp.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_jxp[j]);
				}
			}
		}
		DBApp.recoverRecords("jxp", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("jxp");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fh2", cols2);
		String [][] records_fh2 = new String[373][cols2.length];
		for(int i=0;i<373;i++)
		{
			records_fh2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fh2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fh2", records_fh2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record368: [a368, b0, c2, d0, e3, f2, g4]
		//record369: [a369, b1, c0, d1, e4, f3, g5]
		//record370: [a370, b0, c1, d2, e0, f4, g6]
		//record371: [a371, b1, c2, d3, e1, f5, g0]
		//record372: [a372, b0, c0, d0, e2, f0, g1]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("fh2");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(373.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fh2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fh2"+ File.separator+i+".db");
				dir_fh2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_fh2[j]);
				}
			}
		}
		DBApp.recoverRecords("fh2", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("fh2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("su3", cols3);
		String [][] records_su3 = new String[163][cols3.length];
		for(int i=0;i<163;i++)
		{
			records_su3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_su3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("su3", records_su3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("su3");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(163.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_su3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "su3"+ File.separator+i+".db");
				dir_su3.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_su3[j]);
				}
			}
		}
		DBApp.recoverRecords("su3", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("su3");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pcc2f", cols0);
		String [][] records_pcc2f = new String[381][cols0.length];
		for(int i=0;i<381;i++)
		{
			records_pcc2f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pcc2f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pcc2f", records_pcc2f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record376: [a376, b0, c1, d0, e1, f4, g5]
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("pcc2f");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(381.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pcc2f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pcc2f"+ File.separator+i+".db");
				dir_pcc2f.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_pcc2f[j]);
				}
			}
		}
		DBApp.recoverRecords("pcc2f", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("pcc2f");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("x61t8", cols1);
		String [][] records_x61t8 = new String[350][cols1.length];
		for(int i=0;i<350;i++)
		{
			records_x61t8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x61t8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x61t8", records_x61t8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9, q5, r3, s3, t5]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6, r4, s4, t6]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7, r5, s5, t7]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8, r6, s6, t8]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13, o4, p13, q9, r7, s7, t9]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("x61t8");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(350.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x61t8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x61t8"+ File.separator+i+".db");
				dir_x61t8.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_x61t8[j]);
				}
			}
		}
		DBApp.recoverRecords("x61t8", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("x61t8");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i31o6", cols0);
		String [][] records_i31o6 = new String[105][cols0.length];
		for(int i=0;i<105;i++)
		{
			records_i31o6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i31o6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i31o6", records_i31o6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("i31o6");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(105.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i31o6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i31o6"+ File.separator+i+".db");
				dir_i31o6.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_i31o6[j]);
				}
			}
		}
		DBApp.recoverRecords("i31o6", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("i31o6");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z0", cols1);
		String [][] records_z0 = new String[470][cols1.length];
		for(int i=0;i<470;i++)
		{
			records_z0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z0", records_z0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9, m10]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("z0");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(470.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z0 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z0"+ File.separator+i+".db");
				dir_z0.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_z0[j]);
				}
			}
		}
		DBApp.recoverRecords("z0", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("z0");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("izb", cols0);
		String [][] records_izb = new String[483][cols0.length];
		for(int i=0;i<483;i++)
		{
			records_izb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_izb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("izb", records_izb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11, m11]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0]
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9, l2, m1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("izb");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(483.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_izb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "izb"+ File.separator+i+".db");
				dir_izb.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_izb[j]);
				}
			}
		}
		DBApp.recoverRecords("izb", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("izb");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("p1m1k", cols0);
		String [][] records_p1m1k = new String[388][cols0.length];
		for(int i=0;i<388;i++)
		{
			records_p1m1k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p1m1k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p1m1k", records_p1m1k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9, r5]
		//record384: [a384, b0, c0, d0, e4, f0, g6, h0, i6, j4, k10, l0, m7, n6, o9, p0, q10, r6]
		//record385: [a385, b1, c1, d1, e0, f1, g0, h1, i7, j5, k0, l1, m8, n7, o10, p1, q11, r7]
		//record386: [a386, b0, c2, d2, e1, f2, g1, h2, i8, j6, k1, l2, m9, n8, o11, p2, q12, r8]
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3, m10, n9, o12, p3, q13, r9]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("p1m1k");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(388.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p1m1k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p1m1k"+ File.separator+i+".db");
				dir_p1m1k.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_p1m1k[j]);
				}
			}
		}
		DBApp.recoverRecords("p1m1k", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("p1m1k");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yhvd", cols1);
		String [][] records_yhvd = new String[444][cols1.length];
		for(int i=0;i<444;i++)
		{
			records_yhvd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yhvd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yhvd", records_yhvd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("yhvd");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(444.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yhvd = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yhvd"+ File.separator+i+".db");
				dir_yhvd.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_yhvd[j]);
				}
			}
		}
		DBApp.recoverRecords("yhvd", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("yhvd");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b527y", cols2);
		String [][] records_b527y = new String[209][cols2.length];
		for(int i=0;i<209;i++)
		{
			records_b527y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b527y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b527y", records_b527y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6, j4, k6, l0, m9, n8, o9]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1, m10, n9, o10]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10, o11]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11, o12]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12, o13]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("b527y");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(209.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b527y = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b527y"+ File.separator+i+".db");
				dir_b527y.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_b527y[j]);
				}
			}
		}
		DBApp.recoverRecords("b527y", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("b527y");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("dtf", cols3);
		String [][] records_dtf = new String[286][cols3.length];
		for(int i=0;i<286;i++)
		{
			records_dtf[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_dtf[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("dtf", records_dtf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5, m8, n1, o11, p9, q9, r11, s15]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9, n2, o12, p10, q10, r12, s16]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3, o13, p11, q11, r13, s17]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4, o14, p12, q12, r14, s18]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5, o0, p13, q13, r15, s0]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("dtf");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(286.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dtf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dtf"+ File.separator+i+".db");
				dir_dtf.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_dtf[j]);
				}
			}
		}
		DBApp.recoverRecords("dtf", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("dtf");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i99", cols4);
		String [][] records_i99 = new String[121][cols4.length];
		for(int i=0;i<121;i++)
		{
			records_i99[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_i99[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("i99", records_i99[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record116: [a116, b0, c2, d0, e1, f2, g4]
		//record117: [a117, b1, c0, d1, e2, f3, g5]
		//record118: [a118, b0, c1, d2, e3, f4, g6]
		//record119: [a119, b1, c2, d3, e4, f5, g0]
		//record120: [a120, b0, c0, d0, e0, f0, g1]
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("i99");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(121.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i99 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i99"+ File.separator+i+".db");
				dir_i99.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_i99[j]);
				}
			}
		}
		DBApp.recoverRecords("i99", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("i99");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dhz", cols0);
		String [][] records_dhz = new String[361][cols0.length];
		for(int i=0;i<361;i++)
		{
			records_dhz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dhz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dhz", records_dhz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record356: [a356, b0, c2, d0, e1, f2, g6]
		//record357: [a357, b1, c0, d1, e2, f3, g0]
		//record358: [a358, b0, c1, d2, e3, f4, g1]
		//record359: [a359, b1, c2, d3, e4, f5, g2]
		//record360: [a360, b0, c0, d0, e0, f0, g3]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("dhz");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(361.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dhz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dhz"+ File.separator+i+".db");
				dir_dhz.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_dhz[j]);
				}
			}
		}
		DBApp.recoverRecords("dhz", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("dhz");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o4a05", cols1);
		String [][] records_o4a05 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			records_o4a05[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o4a05[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o4a05", records_o4a05[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("o4a05");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o4a05 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o4a05"+ File.separator+i+".db");
				dir_o4a05.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_o4a05[j]);
				}
			}
		}
		DBApp.recoverRecords("o4a05", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("o4a05");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m5i", cols2);
		String [][] records_m5i = new String[279][cols2.length];
		for(int i=0;i<279;i++)
		{
			records_m5i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m5i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m5i", records_m5i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record274: [a274, b0, c1, d2, e4, f4, g1]
		//record275: [a275, b1, c2, d3, e0, f5, g2]
		//record276: [a276, b0, c0, d0, e1, f0, g3]
		//record277: [a277, b1, c1, d1, e2, f1, g4]
		//record278: [a278, b0, c2, d2, e3, f2, g5]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("m5i");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(279.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m5i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m5i"+ File.separator+i+".db");
				dir_m5i.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_m5i[j]);
				}
			}
		}
		DBApp.recoverRecords("m5i", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("m5i");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jk88k", cols3);
		String [][] records_jk88k = new String[410][cols3.length];
		for(int i=0;i<410;i++)
		{
			records_jk88k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jk88k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jk88k", records_jk88k[i]);
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
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("jk88k");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(410.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jk88k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jk88k"+ File.separator+i+".db");
				dir_jk88k.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_jk88k[j]);
				}
			}
		}
		DBApp.recoverRecords("jk88k", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("jk88k");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("caed", cols0);
		String [][] records_caed = new String[460][cols0.length];
		for(int i=0;i<460;i++)
		{
			records_caed[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_caed[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("caed", records_caed[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record455: [a455, b1, c2, d3, e0, f5, g0]
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		//record458: [a458, b0, c2, d2, e3, f2, g3]
		//record459: [a459, b1, c0, d3, e4, f3, g4]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("caed");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(460.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_caed = new File(FileManager.directory.getAbsolutePath()+ File.separator + "caed"+ File.separator+i+".db");
				dir_caed.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_caed[j]);
				}
			}
		}
		DBApp.recoverRecords("caed", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("caed");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("mn1ws", cols1);
		String [][] records_mn1ws = new String[235][cols1.length];
		for(int i=0;i<235;i++)
		{
			records_mn1ws[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mn1ws[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mn1ws", records_mn1ws[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("mn1ws");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(235.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mn1ws = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mn1ws"+ File.separator+i+".db");
				dir_mn1ws.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_mn1ws[j]);
				}
			}
		}
		DBApp.recoverRecords("mn1ws", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("mn1ws");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("d7rt", cols2);
		String [][] records_d7rt = new String[473][cols2.length];
		for(int i=0;i<473;i++)
		{
			records_d7rt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d7rt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d7rt", records_d7rt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0, n6, o3, p4, q9, r0, s12, t8]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4, p5, q10, r1, s13, t9]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5, p6, q11, r2, s14, t10]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6, p7, q12, r3, s15, t11]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7, p8, q13, r4, s16, t12]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("d7rt");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(473.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d7rt = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d7rt"+ File.separator+i+".db");
				dir_d7rt.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_d7rt[j]);
				}
			}
		}
		DBApp.recoverRecords("d7rt", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("d7rt");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rel", cols0);
		String [][] records_rel = new String[355][cols0.length];
		for(int i=0;i<355;i++)
		{
			records_rel[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rel[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rel", records_rel[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("rel");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(355.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rel = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rel"+ File.separator+i+".db");
				dir_rel.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_rel[j]);
				}
			}
		}
		DBApp.recoverRecords("rel", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("rel");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s3d", cols1);
		String [][] records_s3d = new String[107][cols1.length];
		for(int i=0;i<107;i++)
		{
			records_s3d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s3d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s3d", records_s3d[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("s3d");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(107.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s3d = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s3d"+ File.separator+i+".db");
				dir_s3d.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_s3d[j]);
				}
			}
		}
		DBApp.recoverRecords("s3d", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("s3d");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ct2au", cols0);
		String [][] records_ct2au = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			records_ct2au[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ct2au[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ct2au", records_ct2au[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ct2au");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ct2au = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ct2au"+ File.separator+i+".db");
				dir_ct2au.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ct2au[j]);
				}
			}
		}
		DBApp.recoverRecords("ct2au", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ct2au");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jb5", cols1);
		String [][] records_jb5 = new String[480][cols1.length];
		for(int i=0;i<480;i++)
		{
			records_jb5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jb5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jb5", records_jb5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("jb5");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(480.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jb5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jb5"+ File.separator+i+".db");
				dir_jb5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_jb5[j]);
				}
			}
		}
		DBApp.recoverRecords("jb5", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("jb5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("co6l", cols2);
		String [][] records_co6l = new String[493][cols2.length];
		for(int i=0;i<493;i++)
		{
			records_co6l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_co6l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("co6l", records_co6l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record488: [a488, b0, c2, d0, e3, f2, g5]
		//record489: [a489, b1, c0, d1, e4, f3, g6]
		//record490: [a490, b0, c1, d2, e0, f4, g0]
		//record491: [a491, b1, c2, d3, e1, f5, g1]
		//record492: [a492, b0, c0, d0, e2, f0, g2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("co6l");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(493.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_co6l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "co6l"+ File.separator+i+".db");
				dir_co6l.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_co6l[j]);
				}
			}
		}
		DBApp.recoverRecords("co6l", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("co6l");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o31", cols0);
		String [][] records_o31 = new String[266][cols0.length];
		for(int i=0;i<266;i++)
		{
			records_o31[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o31[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o31", records_o31[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("o31");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(266.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o31 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o31"+ File.separator+i+".db");
				dir_o31.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_o31[j]);
				}
			}
		}
		DBApp.recoverRecords("o31", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("o31");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k5", cols0);
		String [][] records_k5 = new String[276][cols0.length];
		for(int i=0;i<276;i++)
		{
			records_k5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k5", records_k5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1, n8]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2, n9]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("k5");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(276.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k5"+ File.separator+i+".db");
				dir_k5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_k5[j]);
				}
			}
		}
		DBApp.recoverRecords("k5", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("k5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("cmi", cols1);
		String [][] records_cmi = new String[410][cols1.length];
		for(int i=0;i<410;i++)
		{
			records_cmi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cmi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cmi", records_cmi[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("cmi");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(410.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cmi = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cmi"+ File.separator+i+".db");
				dir_cmi.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_cmi[j]);
				}
			}
		}
		DBApp.recoverRecords("cmi", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("cmi");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tb9", cols2);
		String [][] records_tb9 = new String[302][cols2.length];
		for(int i=0;i<302;i++)
		{
			records_tb9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tb9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tb9", records_tb9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		//record298: [a298, b0, c1, d2, e3, f4, g4]
		//record299: [a299, b1, c2, d3, e4, f5, g5]
		//record300: [a300, b0, c0, d0, e0, f0, g6]
		//record301: [a301, b1, c1, d1, e1, f1, g0]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("tb9");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(302.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tb9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tb9"+ File.separator+i+".db");
				dir_tb9.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_tb9[j]);
				}
			}
		}
		DBApp.recoverRecords("tb9", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("tb9");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wqr", cols3);
		String [][] records_wqr = new String[130][cols3.length];
		for(int i=0;i<130;i++)
		{
			records_wqr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wqr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wqr", records_wqr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6]
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		//record129: [a129, b1, c0, d1, e4, f3, g3]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("wqr");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(130.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wqr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wqr"+ File.separator+i+".db");
				dir_wqr.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_wqr[j]);
				}
			}
		}
		DBApp.recoverRecords("wqr", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("wqr");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("yq", cols0);
		String [][] records_yq = new String[279][cols0.length];
		for(int i=0;i<279;i++)
		{
			records_yq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yq", records_yq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7, k2, l1, m4]
		//record278: [a278, b0, c2, d2, e3, f2, g5, h6, i8, j8, k3, l2, m5]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("yq");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(279.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yq = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yq"+ File.separator+i+".db");
				dir_yq.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_yq[j]);
				}
			}
		}
		DBApp.recoverRecords("yq", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("yq");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r0j", cols1);
		String [][] records_r0j = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_r0j[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r0j[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r0j", records_r0j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("r0j");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r0j = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r0j"+ File.separator+i+".db");
				dir_r0j.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_r0j[j]);
				}
			}
		}
		DBApp.recoverRecords("r0j", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("r0j");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("b7r", cols2);
		String [][] records_b7r = new String[330][cols2.length];
		for(int i=0;i<330;i++)
		{
			records_b7r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b7r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b7r", records_b7r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8]
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("b7r");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(330.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b7r = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b7r"+ File.separator+i+".db");
				dir_b7r.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_b7r[j]);
				}
			}
		}
		DBApp.recoverRecords("b7r", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("b7r");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("bny", cols3);
		String [][] records_bny = new String[370][cols3.length];
		for(int i=0;i<370;i++)
		{
			records_bny[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bny[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bny", records_bny[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("bny");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(370.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bny = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bny"+ File.separator+i+".db");
				dir_bny.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_bny[j]);
				}
			}
		}
		DBApp.recoverRecords("bny", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("bny");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ev3j2", cols0);
		String [][] records_ev3j2 = new String[185][cols0.length];
		for(int i=0;i<185;i++)
		{
			records_ev3j2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ev3j2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ev3j2", records_ev3j2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record180: [a180, b0, c0, d0, e0, f0, g5]
		//record181: [a181, b1, c1, d1, e1, f1, g6]
		//record182: [a182, b0, c2, d2, e2, f2, g0]
		//record183: [a183, b1, c0, d3, e3, f3, g1]
		//record184: [a184, b0, c1, d0, e4, f4, g2]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ev3j2");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(185.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ev3j2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ev3j2"+ File.separator+i+".db");
				dir_ev3j2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ev3j2[j]);
				}
			}
		}
		DBApp.recoverRecords("ev3j2", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ev3j2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("pygm", cols1);
		String [][] records_pygm = new String[187][cols1.length];
		for(int i=0;i<187;i++)
		{
			records_pygm[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pygm[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pygm", records_pygm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0, o2, p6, q12, r2, s11, t2]
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1, o3, p7, q13, r3, s12, t3]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2, n2, o4, p8, q14, r4, s13, t4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5, p9, q15, r5, s14, t5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6, p10, q16, r6, s15, t6]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("pygm");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(187.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pygm = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pygm"+ File.separator+i+".db");
				dir_pygm.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_pygm[j]);
				}
			}
		}
		DBApp.recoverRecords("pygm", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("pygm");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p17o", cols2);
		String [][] records_p17o = new String[225][cols2.length];
		for(int i=0;i<225;i++)
		{
			records_p17o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p17o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p17o", records_p17o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3]
		//record221: [a221, b1, c2, d1, e1, f5, g4]
		//record222: [a222, b0, c0, d2, e2, f0, g5]
		//record223: [a223, b1, c1, d3, e3, f1, g6]
		//record224: [a224, b0, c2, d0, e4, f2, g0]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("p17o");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(225.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p17o = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p17o"+ File.separator+i+".db");
				dir_p17o.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_p17o[j]);
				}
			}
		}
		DBApp.recoverRecords("p17o", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("p17o");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h9", cols3);
		String [][] records_h9 = new String[36][cols3.length];
		for(int i=0;i<36;i++)
		{
			records_h9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h9", records_h9[i]);
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
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("h9");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h9"+ File.separator+i+".db");
				dir_h9.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_h9[j]);
				}
			}
		}
		DBApp.recoverRecords("h9", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("h9");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jv", cols0);
		String [][] records_jv = new String[142][cols0.length];
		for(int i=0;i<142;i++)
		{
			records_jv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jv", records_jv[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("jv");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(142.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jv"+ File.separator+i+".db");
				dir_jv.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_jv[j]);
				}
			}
		}
		DBApp.recoverRecords("jv", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("jv");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x65", cols1);
		String [][] records_x65 = new String[460][cols1.length];
		for(int i=0;i<460;i++)
		{
			records_x65[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x65[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x65", records_x65[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0, j9, k8, l3]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("x65");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(460.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x65 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x65"+ File.separator+i+".db");
				dir_x65.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_x65[j]);
				}
			}
		}
		DBApp.recoverRecords("x65", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("x65");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d34", cols0);
		String [][] records_d34 = new String[328][cols0.length];
		for(int i=0;i<328;i++)
		{
			records_d34[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d34[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d34", records_d34[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("d34");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(328.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d34 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d34"+ File.separator+i+".db");
				dir_d34.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_d34[j]);
				}
			}
		}
		DBApp.recoverRecords("d34", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("d34");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t4u8", cols1);
		String [][] records_t4u8 = new String[301][cols1.length];
		for(int i=0;i<301;i++)
		{
			records_t4u8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t4u8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t4u8", records_t4u8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		//record298: [a298, b0, c1, d2, e3, f4, g4]
		//record299: [a299, b1, c2, d3, e4, f5, g5]
		//record300: [a300, b0, c0, d0, e0, f0, g6]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("t4u8");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(301.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t4u8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t4u8"+ File.separator+i+".db");
				dir_t4u8.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_t4u8[j]);
				}
			}
		}
		DBApp.recoverRecords("t4u8", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("t4u8");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("pg8", cols2);
		String [][] records_pg8 = new String[430][cols2.length];
		for(int i=0;i<430;i++)
		{
			records_pg8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pg8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pg8", records_pg8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5, m9, n5, o5, p9, q0]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6, m10, n6, o6, p10, q1]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7, m11, n7, o7, p11, q2]
		//record428: [a428, b0, c2, d0, e3, f2, g1, h4, i5, j8, k10, l8, m12, n8, o8, p12, q3]
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9, p13, q4]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("pg8");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(430.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pg8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pg8"+ File.separator+i+".db");
				dir_pg8.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_pg8[j]);
				}
			}
		}
		DBApp.recoverRecords("pg8", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("pg8");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h373", cols3);
		String [][] records_h373 = new String[103][cols3.length];
		for(int i=0;i<103;i++)
		{
			records_h373[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h373[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h373", records_h373[i]);
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
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("h373");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(103.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h373 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h373"+ File.separator+i+".db");
				dir_h373.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_h373[j]);
				}
			}
		}
		DBApp.recoverRecords("h373", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("h373");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ozmzv", cols0);
		String [][] records_ozmzv = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_ozmzv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ozmzv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ozmzv", records_ozmzv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7, m2, n1, o4, p11, q5, r1, s18, t19]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6, l8, m3, n2, o5, p12, q6, r2, s0, t0]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3, o6, p13, q7, r3, s1, t1]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4, o7, p14, q8, r4, s2, t2]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9, r5, s3, t3]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ozmzv");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(384.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ozmzv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ozmzv"+ File.separator+i+".db");
				dir_ozmzv.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ozmzv[j]);
				}
			}
		}
		DBApp.recoverRecords("ozmzv", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ozmzv");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("fmn", cols1);
		String [][] records_fmn = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			records_fmn[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_fmn[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("fmn", records_fmn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("fmn");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fmn = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fmn"+ File.separator+i+".db");
				dir_fmn.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_fmn[j]);
				}
			}
		}
		DBApp.recoverRecords("fmn", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("fmn");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n7r", cols2);
		String [][] records_n7r = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_n7r[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n7r[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n7r", records_n7r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("n7r");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n7r = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n7r"+ File.separator+i+".db");
				dir_n7r.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_n7r[j]);
				}
			}
		}
		DBApp.recoverRecords("n7r", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("n7r");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("f6", cols0);
		String [][] records_f6 = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_f6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f6", records_f6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3]
		//record180: [a180, b0, c0, d0, e0, f0, g5, h4]
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("f6");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(183.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f6"+ File.separator+i+".db");
				dir_f6.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_f6[j]);
				}
			}
		}
		DBApp.recoverRecords("f6", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("f6");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tb20p", cols1);
		String [][] records_tb20p = new String[311][cols1.length];
		for(int i=0;i<311;i++)
		{
			records_tb20p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tb20p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tb20p", records_tb20p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10, l7]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("tb20p");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(311.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tb20p = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tb20p"+ File.separator+i+".db");
				dir_tb20p.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_tb20p[j]);
				}
			}
		}
		DBApp.recoverRecords("tb20p", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("tb20p");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("em51", cols2);
		String [][] records_em51 = new String[435][cols2.length];
		for(int i=0;i<435;i++)
		{
			records_em51[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_em51[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("em51", records_em51[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10, p14]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("em51");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(435.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_em51 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "em51"+ File.separator+i+".db");
				dir_em51.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_em51[j]);
				}
			}
		}
		DBApp.recoverRecords("em51", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("em51");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ch", cols3);
		String [][] records_ch = new String[7][cols3.length];
		for(int i=0;i<7;i++)
		{
			records_ch[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ch[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ch", records_ch[i]);
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
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("ch");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ch = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ch"+ File.separator+i+".db");
				dir_ch.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_ch[j]);
				}
			}
		}
		DBApp.recoverRecords("ch", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("ch");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zb", cols4);
		String [][] records_zb = new String[244][cols4.length];
		for(int i=0;i<244;i++)
		{
			records_zb[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_zb[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("zb", records_zb[i]);
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
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("zb");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(244.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zb"+ File.separator+i+".db");
				dir_zb.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_zb[j]);
				}
			}
		}
		DBApp.recoverRecords("zb", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("zb");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("a3q", cols0);
		String [][] records_a3q = new String[247][cols0.length];
		for(int i=0;i<247;i++)
		{
			records_a3q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a3q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a3q", records_a3q[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("a3q");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(247.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a3q = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a3q"+ File.separator+i+".db");
				dir_a3q.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_a3q[j]);
				}
			}
		}
		DBApp.recoverRecords("a3q", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("a3q");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o7400", cols1);
		String [][] records_o7400 = new String[482][cols1.length];
		for(int i=0;i<482;i++)
		{
			records_o7400[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o7400[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o7400", records_o7400[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		//record479: [a479, b1, c2, d3, e4, f5, g3]
		//record480: [a480, b0, c0, d0, e0, f0, g4]
		//record481: [a481, b1, c1, d1, e1, f1, g5]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("o7400");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(482.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o7400 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o7400"+ File.separator+i+".db");
				dir_o7400.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_o7400[j]);
				}
			}
		}
		DBApp.recoverRecords("o7400", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("o7400");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ad7", cols2);
		String [][] records_ad7 = new String[127][cols2.length];
		for(int i=0;i<127;i++)
		{
			records_ad7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ad7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ad7", records_ad7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10, o2]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("ad7");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(127.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ad7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ad7"+ File.separator+i+".db");
				dir_ad7.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_ad7[j]);
				}
			}
		}
		DBApp.recoverRecords("ad7", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("ad7");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("im862", cols0);
		String [][] records_im862 = new String[285][cols0.length];
		for(int i=0;i<285;i++)
		{
			records_im862[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_im862[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("im862", records_im862[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record280: [a280, b0, c1, d0, e0, f4, g0]
		//record281: [a281, b1, c2, d1, e1, f5, g1]
		//record282: [a282, b0, c0, d2, e2, f0, g2]
		//record283: [a283, b1, c1, d3, e3, f1, g3]
		//record284: [a284, b0, c2, d0, e4, f2, g4]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("im862");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(285.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_im862 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "im862"+ File.separator+i+".db");
				dir_im862.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_im862[j]);
				}
			}
		}
		DBApp.recoverRecords("im862", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("im862");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uao5", cols1);
		String [][] records_uao5 = new String[499][cols1.length];
		for(int i=0;i<499;i++)
		{
			records_uao5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uao5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uao5", records_uao5[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("uao5");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(499.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uao5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uao5"+ File.separator+i+".db");
				dir_uao5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_uao5[j]);
				}
			}
		}
		DBApp.recoverRecords("uao5", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("uao5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("fr38", cols2);
		String [][] records_fr38 = new String[68][cols2.length];
		for(int i=0;i<68;i++)
		{
			records_fr38[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fr38[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fr38", records_fr38[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("fr38");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fr38 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fr38"+ File.separator+i+".db");
				dir_fr38.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_fr38[j]);
				}
			}
		}
		DBApp.recoverRecords("fr38", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("fr38");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("m5d0", cols3);
		String [][] records_m5d0 = new String[493][cols3.length];
		for(int i=0;i<493;i++)
		{
			records_m5d0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m5d0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m5d0", records_m5d0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1, o11]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11, n2, o12]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("m5d0");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(493.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m5d0 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m5d0"+ File.separator+i+".db");
				dir_m5d0.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_m5d0[j]);
				}
			}
		}
		DBApp.recoverRecords("m5d0", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("m5d0");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("uzsn5", cols0);
		String [][] records_uzsn5 = new String[479][cols0.length];
		for(int i=0;i<479;i++)
		{
			records_uzsn5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uzsn5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uzsn5", records_uzsn5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6, n12, o9, p10]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13, o10, p11]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0, o11, p12]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1, o12, p13]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10, n2, o13, p14]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("uzsn5");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(479.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uzsn5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uzsn5"+ File.separator+i+".db");
				dir_uzsn5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_uzsn5[j]);
				}
			}
		}
		DBApp.recoverRecords("uzsn5", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("uzsn5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t01", cols1);
		String [][] records_t01 = new String[488][cols1.length];
		for(int i=0;i<488;i++)
		{
			records_t01[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t01[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t01", records_t01[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record483: [a483, b1, c0, d3, e3, f3, g0]
		//record484: [a484, b0, c1, d0, e4, f4, g1]
		//record485: [a485, b1, c2, d1, e0, f5, g2]
		//record486: [a486, b0, c0, d2, e1, f0, g3]
		//record487: [a487, b1, c1, d3, e2, f1, g4]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("t01");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(488.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t01 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t01"+ File.separator+i+".db");
				dir_t01.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_t01[j]);
				}
			}
		}
		DBApp.recoverRecords("t01", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("t01");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("y3k18", cols2);
		String [][] records_y3k18 = new String[482][cols2.length];
		for(int i=0;i<482;i++)
		{
			records_y3k18[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y3k18[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y3k18", records_y3k18[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10, n2]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11, m11, n3]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12, n4]
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0, n5]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("y3k18");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(482.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y3k18 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y3k18"+ File.separator+i+".db");
				dir_y3k18.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_y3k18[j]);
				}
			}
		}
		DBApp.recoverRecords("y3k18", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("y3k18");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("gmbk", cols0);
		String [][] records_gmbk = new String[349][cols0.length];
		for(int i=0;i<349;i++)
		{
			records_gmbk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gmbk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gmbk", records_gmbk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14, p8, q4]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9, q5]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("gmbk");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(349.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gmbk = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gmbk"+ File.separator+i+".db");
				dir_gmbk.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_gmbk[j]);
				}
			}
		}
		DBApp.recoverRecords("gmbk", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("gmbk");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("s23", cols1);
		String [][] records_s23 = new String[340][cols1.length];
		for(int i=0;i<340;i++)
		{
			records_s23[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s23[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s23", records_s23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13, o5, p15]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6, p0]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7, p1]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("s23");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(340.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s23 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s23"+ File.separator+i+".db");
				dir_s23.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_s23[j]);
				}
			}
		}
		DBApp.recoverRecords("s23", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("s23");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("jd7vz", cols0);
		String [][] records_jd7vz = new String[340][cols0.length];
		for(int i=0;i<340;i++)
		{
			records_jd7vz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jd7vz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jd7vz", records_jd7vz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13, o5, p15, q12, r11]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6, p0, q13, r12]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7, p1, q14, r13]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2, q15, r14]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3, q16, r15]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("jd7vz");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(340.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jd7vz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jd7vz"+ File.separator+i+".db");
				dir_jd7vz.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_jd7vz[j]);
				}
			}
		}
		DBApp.recoverRecords("jd7vz", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("jd7vz");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("n0r", cols1);
		String [][] records_n0r = new String[393][cols1.length];
		for(int i=0;i<393;i++)
		{
			records_n0r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n0r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n0r", records_n0r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11, n10, o13, p4, q14]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12, n11, o14, p5, q15]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12, o0, p6, q16]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13, o1, p7, q0]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2, p8, q1]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("n0r");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(393.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n0r = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n0r"+ File.separator+i+".db");
				dir_n0r.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_n0r[j]);
				}
			}
		}
		DBApp.recoverRecords("n0r", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("n0r");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ik", cols0);
		String [][] records_ik = new String[216][cols0.length];
		for(int i=0;i<216;i++)
		{
			records_ik[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ik[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ik", records_ik[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ik");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(216.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ik = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ik"+ File.separator+i+".db");
				dir_ik.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ik[j]);
				}
			}
		}
		DBApp.recoverRecords("ik", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ik");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("x4", cols0);
		String [][] records_x4 = new String[319][cols0.length];
		for(int i=0;i<319;i++)
		{
			records_x4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x4", records_x4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2, n6, o14, p10]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7, o0, p11]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1, p12]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("x4");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(319.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x4"+ File.separator+i+".db");
				dir_x4.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_x4[j]);
				}
			}
		}
		DBApp.recoverRecords("x4", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("x4");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e37", cols1);
		String [][] records_e37 = new String[149][cols1.length];
		for(int i=0;i<149;i++)
		{
			records_e37[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e37[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e37", records_e37[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3]
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("e37");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(149.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_e37 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "e37"+ File.separator+i+".db");
				dir_e37.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_e37[j]);
				}
			}
		}
		DBApp.recoverRecords("e37", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("e37");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("j6x", cols0);
		String [][] records_j6x = new String[306][cols0.length];
		for(int i=0;i<306;i++)
		{
			records_j6x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j6x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j6x", records_j6x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1, m2, n7, o1, p13]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3, n8, o2, p14]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4, n9, o3, p15]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5, n10, o4, p0]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6, n11, o5, p1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("j6x");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(306.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j6x = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j6x"+ File.separator+i+".db");
				dir_j6x.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_j6x[j]);
				}
			}
		}
		DBApp.recoverRecords("j6x", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("j6x");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k6", cols1);
		String [][] records_k6 = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			records_k6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k6", records_k6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("k6");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k6"+ File.separator+i+".db");
				dir_k6.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_k6[j]);
				}
			}
		}
		DBApp.recoverRecords("k6", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("k6");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("o00", cols2);
		String [][] records_o00 = new String[322][cols2.length];
		for(int i=0;i<322;i++)
		{
			records_o00[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o00[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o00", records_o00[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13, q11, r11, s13]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12, s14]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13, s15]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0, q14, r14, s16]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13, o6, p1, q15, r15, s17]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("o00");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(322.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o00 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o00"+ File.separator+i+".db");
				dir_o00.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_o00[j]);
				}
			}
		}
		DBApp.recoverRecords("o00", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("o00");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l11o5", cols3);
		String [][] records_l11o5 = new String[464][cols3.length];
		for(int i=0;i<464;i++)
		{
			records_l11o5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l11o5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l11o5", records_l11o5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record459: [a459, b1, c0, d3, e4, f3, g4]
		//record460: [a460, b0, c1, d0, e0, f4, g5]
		//record461: [a461, b1, c2, d1, e1, f5, g6]
		//record462: [a462, b0, c0, d2, e2, f0, g0]
		//record463: [a463, b1, c1, d3, e3, f1, g1]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("l11o5");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(464.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l11o5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l11o5"+ File.separator+i+".db");
				dir_l11o5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_l11o5[j]);
				}
			}
		}
		DBApp.recoverRecords("l11o5", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("l11o5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("wp5", cols4);
		String [][] records_wp5 = new String[438][cols4.length];
		for(int i=0;i<438;i++)
		{
			records_wp5[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wp5[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wp5", records_wp5[i]);
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
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("wp5");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(438.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wp5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wp5"+ File.separator+i+".db");
				dir_wp5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_wp5[j]);
				}
			}
		}
		DBApp.recoverRecords("wp5", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("wp5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("j2", cols0);
		String [][] records_j2 = new String[488][cols0.length];
		for(int i=0;i<488;i++)
		{
			records_j2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j2", records_j2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(488.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j2"+ File.separator+i+".db");
				dir_j2.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_j2[j]);
				}
			}
		}
		DBApp.recoverRecords("j2", missing0);
		String tableTrace0 = DBApp.getLastTrace("j2");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zck6", cols1);
		String [][] records_zck6 = new String[402][cols1.length];
		for(int i=0;i<402;i++)
		{
			records_zck6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zck6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zck6", records_zck6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		//record400: [a400, b0, c1, d0, e0, f4, g1]
		//record401: [a401, b1, c2, d1, e1, f5, g2]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(402.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zck6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zck6"+ File.separator+i+".db");
				dir_zck6.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_zck6[j]);
				}
			}
		}
		DBApp.recoverRecords("zck6", missing1);
		String tableTrace1 = DBApp.getLastTrace("zck6");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("z67", cols2);
		String [][] records_z67 = new String[159][cols2.length];
		for(int i=0;i<159;i++)
		{
			records_z67[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z67[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z67", records_z67[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10, m11, n0, o4, p10, q1, r10, s2, t14]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12, n1, o5, p11, q2, r11, s3, t15]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6, p12, q3, r12, s4, t16]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7, p13, q4, r13, s5, t17]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4, o8, p14, q5, r14, s6, t18]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(159.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z67 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z67"+ File.separator+i+".db");
				dir_z67.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_z67[j]);
				}
			}
		}
		DBApp.recoverRecords("z67", missing2);
		String tableTrace2 = DBApp.getLastTrace("z67");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("yvk", cols3);
		String [][] records_yvk = new String[157][cols3.length];
		for(int i=0;i<157;i++)
		{
			records_yvk[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_yvk[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("yvk", records_yvk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9, n12, o2, p8, q16]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9, m10, n13, o3, p9, q0]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10, m11, n0, o4, p10, q1]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12, n1, o5, p11, q2]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6, p12, q3]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(157.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yvk = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yvk"+ File.separator+i+".db");
				dir_yvk.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_yvk[j]);
				}
			}
		}
		DBApp.recoverRecords("yvk", missing3);
		String tableTrace3 = DBApp.getLastTrace("yvk");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v8h", cols0);
		String [][] records_v8h = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_v8h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v8h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v8h", records_v8h[i]);
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
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v8h = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v8h"+ File.separator+i+".db");
				dir_v8h.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_v8h[j]);
				}
			}
		}
		DBApp.recoverRecords("v8h", missing0);
		String tableTrace0 = DBApp.getLastTrace("v8h");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zn8u", cols1);
		String [][] records_zn8u = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_zn8u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zn8u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zn8u", records_zn8u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zn8u = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zn8u"+ File.separator+i+".db");
				dir_zn8u.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_zn8u[j]);
				}
			}
		}
		DBApp.recoverRecords("zn8u", missing1);
		String tableTrace1 = DBApp.getLastTrace("zn8u");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ebczs", cols2);
		String [][] records_ebczs = new String[464][cols2.length];
		for(int i=0;i<464;i++)
		{
			records_ebczs[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ebczs[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ebczs", records_ebczs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4]
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(464.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ebczs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ebczs"+ File.separator+i+".db");
				dir_ebczs.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_ebczs[j]);
				}
			}
		}
		DBApp.recoverRecords("ebczs", missing2);
		String tableTrace2 = DBApp.getLastTrace("ebczs");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("urgg", cols3);
		String [][] records_urgg = new String[7][cols3.length];
		for(int i=0;i<7;i++)
		{
			records_urgg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_urgg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("urgg", records_urgg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_urgg = new File(FileManager.directory.getAbsolutePath()+ File.separator + "urgg"+ File.separator+i+".db");
				dir_urgg.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_urgg[j]);
				}
			}
		}
		DBApp.recoverRecords("urgg", missing3);
		String tableTrace3 = DBApp.getLastTrace("urgg");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("c5iq1", cols0);
		String [][] records_c5iq1 = new String[125][cols0.length];
		for(int i=0;i<125;i++)
		{
			records_c5iq1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c5iq1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c5iq1", records_c5iq1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record120: [a120, b0, c0, d0, e0, f0, g1, h0, i3, j0, k10, l0, m3, n8, o0]
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1, m4, n9, o1]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10, o2]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(125.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_c5iq1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "c5iq1"+ File.separator+i+".db");
				dir_c5iq1.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_c5iq1[j]);
				}
			}
		}
		DBApp.recoverRecords("c5iq1", missing0);
		String tableTrace0 = DBApp.getLastTrace("c5iq1");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("do6", cols1);
		String [][] records_do6 = new String[215][cols1.length];
		for(int i=0;i<215;i++)
		{
			records_do6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_do6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("do6", records_do6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0, o0, p2, q6, r12]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7, m3, n1, o1, p3, q7, r13]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3, l8, m4, n2, o2, p4, q8, r14]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5, n3, o3, p5, q9, r15]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4, o4, p6, q10, r16]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(215.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_do6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "do6"+ File.separator+i+".db");
				dir_do6.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_do6[j]);
				}
			}
		}
		DBApp.recoverRecords("do6", missing1);
		String tableTrace1 = DBApp.getLastTrace("do6");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("bp9", cols2);
		String [][] records_bp9 = new String[397][cols2.length];
		for(int i=0;i<397;i++)
		{
			records_bp9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bp9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bp9", records_bp9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1, o3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2, o4]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10, l11, m5, n3, o5]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0, l0, m6, n4, o6]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(397.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bp9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bp9"+ File.separator+i+".db");
				dir_bp9.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_bp9[j]);
				}
			}
		}
		DBApp.recoverRecords("bp9", missing2);
		String tableTrace2 = DBApp.getLastTrace("bp9");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("f2p0l", cols3);
		String [][] records_f2p0l = new String[234][cols3.length];
		for(int i=0;i<234;i++)
		{
			records_f2p0l[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f2p0l[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f2p0l", records_f2p0l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6, o5]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7, o6]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(234.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f2p0l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f2p0l"+ File.separator+i+".db");
				dir_f2p0l.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_f2p0l[j]);
				}
			}
		}
		DBApp.recoverRecords("f2p0l", missing3);
		String tableTrace3 = DBApp.getLastTrace("f2p0l");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("la6wz", cols0);
		String [][] records_la6wz = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_la6wz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_la6wz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("la6wz", records_la6wz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_la6wz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "la6wz"+ File.separator+i+".db");
				dir_la6wz.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_la6wz[j]);
				}
			}
		}
		DBApp.recoverRecords("la6wz", missing0);
		String tableTrace0 = DBApp.getLastTrace("la6wz");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("r21ki", cols0);
		String [][] records_r21ki = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			records_r21ki[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r21ki[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r21ki", records_r21ki[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r21ki = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r21ki"+ File.separator+i+".db");
				dir_r21ki.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_r21ki[j]);
				}
			}
		}
		DBApp.recoverRecords("r21ki", missing0);
		String tableTrace0 = DBApp.getLastTrace("r21ki");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u4u", cols1);
		String [][] records_u4u = new String[233][cols1.length];
		for(int i=0;i<233;i++)
		{
			records_u4u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u4u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u4u", records_u4u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(233.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u4u = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u4u"+ File.separator+i+".db");
				dir_u4u.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_u4u[j]);
				}
			}
		}
		DBApp.recoverRecords("u4u", missing1);
		String tableTrace1 = DBApp.getLastTrace("u4u");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("y76", cols2);
		String [][] records_y76 = new String[404][cols2.length];
		for(int i=0;i<404;i++)
		{
			records_y76[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y76[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y76", records_y76[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3, l3, m9, n7, o9, p15, q8, r3, s0]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8, o10, p0, q9, r4, s1]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9, o11, p1, q10, r5, s2]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12, n10, o12, p2, q11, r6, s3]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13, p3, q12, r7, s4]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(404.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y76 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y76"+ File.separator+i+".db");
				dir_y76.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_y76[j]);
				}
			}
		}
		DBApp.recoverRecords("y76", missing2);
		String tableTrace2 = DBApp.getLastTrace("y76");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("f1", cols3);
		String [][] records_f1 = new String[420][cols3.length];
		for(int i=0;i<420;i++)
		{
			records_f1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f1", records_f1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8, k0, l10, m2]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9, k1, l11, m3]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(420.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f1"+ File.separator+i+".db");
				dir_f1.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_f1[j]);
				}
			}
		}
		DBApp.recoverRecords("f1", missing3);
		String tableTrace3 = DBApp.getLastTrace("f1");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wp00c", cols4);
		String [][] records_wp00c = new String[123][cols4.length];
		for(int i=0;i<123;i++)
		{
			records_wp00c[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wp00c[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wp00c", records_wp00c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record118: [a118, b0, c1, d2, e3, f4, g6]
		//record119: [a119, b1, c2, d3, e4, f5, g0]
		//record120: [a120, b0, c0, d0, e0, f0, g1]
		//record121: [a121, b1, c1, d1, e1, f1, g2]
		//record122: [a122, b0, c2, d2, e2, f2, g3]
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages4 = new ArrayList<Integer>();
		int pageCount4 = (int)Math.ceil(123.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wp00c = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wp00c"+ File.separator+i+".db");
				dir_wp00c.delete();
				deletedPages4.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_wp00c[j]);
				}
			}
		}
		DBApp.recoverRecords("wp00c", missing4);
		String tableTrace4 = DBApp.getLastTrace("wp00c");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace4.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace4.contains(missing4.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace4.contains("in pages: "+deletedPages4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y3hn", cols0);
		String [][] records_y3hn = new String[453][cols0.length];
		for(int i=0;i<453;i++)
		{
			records_y3hn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y3hn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y3hn", records_y3hn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(453.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y3hn = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y3hn"+ File.separator+i+".db");
				dir_y3hn.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_y3hn[j]);
				}
			}
		}
		DBApp.recoverRecords("y3hn", missing0);
		String tableTrace0 = DBApp.getLastTrace("y3hn");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qc7", cols0);
		String [][] records_qc7 = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			records_qc7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qc7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qc7", records_qc7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qc7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qc7"+ File.separator+i+".db");
				dir_qc7.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_qc7[j]);
				}
			}
		}
		DBApp.recoverRecords("qc7", missing0);
		String tableTrace0 = DBApp.getLastTrace("qc7");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("yjd", cols1);
		String [][] records_yjd = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			records_yjd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yjd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yjd", records_yjd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1, r14, s10, t6]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11, t7]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4, o13, p8, q3, r16, s12, t8]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11, n5, o14, p9, q4, r17, s13, t9]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6, m12, n6, o0, p10, q5, r0, s14, t10]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yjd = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yjd"+ File.separator+i+".db");
				dir_yjd.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_yjd[j]);
				}
			}
		}
		DBApp.recoverRecords("yjd", missing1);
		String tableTrace1 = DBApp.getLastTrace("yjd");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("lixk", cols2);
		String [][] records_lixk = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			records_lixk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lixk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lixk", records_lixk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8, p3, q15]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0, o9, p4, q16]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10, p5, q0]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lixk = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lixk"+ File.separator+i+".db");
				dir_lixk.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_lixk[j]);
				}
			}
		}
		DBApp.recoverRecords("lixk", missing2);
		String tableTrace2 = DBApp.getLastTrace("lixk");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("du936", cols3);
		String [][] records_du936 = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			records_du936[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_du936[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("du936", records_du936[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_du936 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "du936"+ File.separator+i+".db");
				dir_du936.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_du936[j]);
				}
			}
		}
		DBApp.recoverRecords("du936", missing3);
		String tableTrace3 = DBApp.getLastTrace("du936");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("i20i", cols0);
		String [][] records_i20i = new String[459][cols0.length];
		for(int i=0;i<459;i++)
		{
			records_i20i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i20i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i20i", records_i20i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7, o5, p7]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1, n8, o6, p8]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1, m2, n9, o7, p9]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2, m3, n10, o8, p10]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(459.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i20i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i20i"+ File.separator+i+".db");
				dir_i20i.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_i20i[j]);
				}
			}
		}
		DBApp.recoverRecords("i20i", missing0);
		String tableTrace0 = DBApp.getLastTrace("i20i");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ew", cols1);
		String [][] records_ew = new String[420][cols1.length];
		for(int i=0;i<420;i++)
		{
			records_ew[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ew[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ew", records_ew[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8]
		//record419: [a419, b1, c2, d3, e4, f5, g6, h3, i5, j9]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(420.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ew = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ew"+ File.separator+i+".db");
				dir_ew.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ew[j]);
				}
			}
		}
		DBApp.recoverRecords("ew", missing1);
		String tableTrace1 = DBApp.getLastTrace("ew");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("vigm", cols2);
		String [][] records_vigm = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			records_vigm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vigm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vigm", records_vigm[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(66.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vigm = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vigm"+ File.separator+i+".db");
				dir_vigm.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_vigm[j]);
				}
			}
		}
		DBApp.recoverRecords("vigm", missing2);
		String tableTrace2 = DBApp.getLastTrace("vigm");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ydy", cols0);
		String [][] records_ydy = new String[225][cols0.length];
		for(int i=0;i<225;i++)
		{
			records_ydy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ydy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ydy", records_ydy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(225.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ydy = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ydy"+ File.separator+i+".db");
				dir_ydy.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ydy[j]);
				}
			}
		}
		DBApp.recoverRecords("ydy", missing0);
		String tableTrace0 = DBApp.getLastTrace("ydy");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f1u", cols1);
		String [][] records_f1u = new String[414][cols1.length];
		for(int i=0;i<414;i++)
		{
			records_f1u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f1u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f1u", records_f1u[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record409: [a409, b1, c1, d1, e4, f1, g3]
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(414.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f1u = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f1u"+ File.separator+i+".db");
				dir_f1u.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_f1u[j]);
				}
			}
		}
		DBApp.recoverRecords("f1u", missing1);
		String tableTrace1 = DBApp.getLastTrace("f1u");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("cny5", cols2);
		String [][] records_cny5 = new String[127][cols2.length];
		for(int i=0;i<127;i++)
		{
			records_cny5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cny5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cny5", records_cny5[i]);
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
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(127.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cny5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cny5"+ File.separator+i+".db");
				dir_cny5.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_cny5[j]);
				}
			}
		}
		DBApp.recoverRecords("cny5", missing2);
		String tableTrace2 = DBApp.getLastTrace("cny5");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ey3", cols0);
		String [][] records_ey3 = new String[225][cols0.length];
		for(int i=0;i<225;i++)
		{
			records_ey3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ey3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ey3", records_ey3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10, o10, p12, q16, r4, s11]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5, m0, n11, o11, p13, q0, r5, s12]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6, m1, n12, o12, p14, q1, r6, s13]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7, m2, n13, o13, p15, q2, r7, s14]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8, m3, n0, o14, p0, q3, r8, s15]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(225.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ey3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ey3"+ File.separator+i+".db");
				dir_ey3.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ey3[j]);
				}
			}
		}
		DBApp.recoverRecords("ey3", missing0);
		String tableTrace0 = DBApp.getLastTrace("ey3");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ia", cols1);
		String [][] records_ia = new String[405][cols1.length];
		for(int i=0;i<405;i++)
		{
			records_ia[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ia[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ia", records_ia[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4, l4, m10, n8, o10]
		//record401: [a401, b1, c2, d1, e1, f5, g2, h1, i5, j1, k5, l5, m11, n9, o11]
		//record402: [a402, b0, c0, d2, e2, f0, g3, h2, i6, j2, k6, l6, m12, n10, o12]
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12, o14]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(405.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ia = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ia"+ File.separator+i+".db");
				dir_ia.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ia[j]);
				}
			}
		}
		DBApp.recoverRecords("ia", missing1);
		String tableTrace1 = DBApp.getLastTrace("ia");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o955v", cols2);
		String [][] records_o955v = new String[496][cols2.length];
		for(int i=0;i<496;i++)
		{
			records_o955v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o955v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o955v", records_o955v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record491: [a491, b1, c2, d3, e1, f5, g1]
		//record492: [a492, b0, c0, d0, e2, f0, g2]
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(496.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o955v = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o955v"+ File.separator+i+".db");
				dir_o955v.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_o955v[j]);
				}
			}
		}
		DBApp.recoverRecords("o955v", missing2);
		String tableTrace2 = DBApp.getLastTrace("o955v");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hn", cols3);
		String [][] records_hn = new String[154][cols3.length];
		for(int i=0;i<154;i++)
		{
			records_hn[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hn[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hn", records_hn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record149: [a149, b1, c2, d1, e4, f5, g2]
		//record150: [a150, b0, c0, d2, e0, f0, g3]
		//record151: [a151, b1, c1, d3, e1, f1, g4]
		//record152: [a152, b0, c2, d0, e2, f2, g5]
		//record153: [a153, b1, c0, d1, e3, f3, g6]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(154.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hn = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hn"+ File.separator+i+".db");
				dir_hn.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_hn[j]);
				}
			}
		}
		DBApp.recoverRecords("hn", missing3);
		String tableTrace3 = DBApp.getLastTrace("hn");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("z12", cols0);
		String [][] records_z12 = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			records_z12[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z12[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z12", records_z12[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4, o14]
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3, m10, n5, o0]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11, n6, o1]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7, o2]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z12 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z12"+ File.separator+i+".db");
				dir_z12.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_z12[j]);
				}
			}
		}
		DBApp.recoverRecords("z12", missing0);
		String tableTrace0 = DBApp.getLastTrace("z12");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r7b", cols0);
		String [][] records_r7b = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			records_r7b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r7b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r7b", records_r7b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8]
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r7b = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r7b"+ File.separator+i+".db");
				dir_r7b.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_r7b[j]);
				}
			}
		}
		DBApp.recoverRecords("r7b", missing0);
		String tableTrace0 = DBApp.getLastTrace("r7b");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("mn077", cols0);
		String [][] records_mn077 = new String[214][cols0.length];
		for(int i=0;i<214;i++)
		{
			records_mn077[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mn077[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mn077", records_mn077[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4, i5, j2, k3]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(214.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mn077 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mn077"+ File.separator+i+".db");
				dir_mn077.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_mn077[j]);
				}
			}
		}
		DBApp.recoverRecords("mn077", missing0);
		String tableTrace0 = DBApp.getLastTrace("mn077");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("b44s2", cols1);
		String [][] records_b44s2 = new String[243][cols1.length];
		for(int i=0;i<243;i++)
		{
			records_b44s2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b44s2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b44s2", records_b44s2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13, p14, q0, r4]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14, p15, q1, r5]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6, n2, o0, p0, q2, r6]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7, n3, o1, p1, q3, r7]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8, n4, o2, p2, q4, r8]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(243.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b44s2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b44s2"+ File.separator+i+".db");
				dir_b44s2.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_b44s2[j]);
				}
			}
		}
		DBApp.recoverRecords("b44s2", missing1);
		String tableTrace1 = DBApp.getLastTrace("b44s2");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("tl2", cols2);
		String [][] records_tl2 = new String[240][cols2.length];
		for(int i=0;i<240;i++)
		{
			records_tl2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tl2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tl2", records_tl2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(240.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tl2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tl2"+ File.separator+i+".db");
				dir_tl2.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_tl2[j]);
				}
			}
		}
		DBApp.recoverRecords("tl2", missing2);
		String tableTrace2 = DBApp.getLastTrace("tl2");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p3k", cols3);
		String [][] records_p3k = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			records_p3k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p3k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p3k", records_p3k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p3k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p3k"+ File.separator+i+".db");
				dir_p3k.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_p3k[j]);
				}
			}
		}
		DBApp.recoverRecords("p3k", missing3);
		String tableTrace3 = DBApp.getLastTrace("p3k");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m8ay", cols0);
		String [][] records_m8ay = new String[334][cols0.length];
		for(int i=0;i<334;i++)
		{
			records_m8ay[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m8ay[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m8ay", records_m8ay[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4, n7]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(334.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m8ay = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m8ay"+ File.separator+i+".db");
				dir_m8ay.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_m8ay[j]);
				}
			}
		}
		DBApp.recoverRecords("m8ay", missing0);
		String tableTrace0 = DBApp.getLastTrace("m8ay");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sc", cols1);
		String [][] records_sc = new String[259][cols1.length];
		for(int i=0;i<259;i++)
		{
			records_sc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sc", records_sc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(259.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_sc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "sc"+ File.separator+i+".db");
				dir_sc.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_sc[j]);
				}
			}
		}
		DBApp.recoverRecords("sc", missing1);
		String tableTrace1 = DBApp.getLastTrace("sc");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z0o", cols2);
		String [][] records_z0o = new String[444][cols2.length];
		for(int i=0;i<444;i++)
		{
			records_z0o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z0o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z0o", records_z0o[i]);
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
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(444.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z0o = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z0o"+ File.separator+i+".db");
				dir_z0o.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_z0o[j]);
				}
			}
		}
		DBApp.recoverRecords("z0o", missing2);
		String tableTrace2 = DBApp.getLastTrace("z0o");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uok", cols3);
		String [][] records_uok = new String[173][cols3.length];
		for(int i=0;i<173;i++)
		{
			records_uok[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_uok[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("uok", records_uok[i]);
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
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(173.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uok = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uok"+ File.separator+i+".db");
				dir_uok.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_uok[j]);
				}
			}
		}
		DBApp.recoverRecords("uok", missing3);
		String tableTrace3 = DBApp.getLastTrace("uok");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("bj8", cols4);
		String [][] records_bj8 = new String[362][cols4.length];
		for(int i=0;i<362;i++)
		{
			records_bj8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bj8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bj8", records_bj8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9, n10]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11]
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages4 = new ArrayList<Integer>();
		int pageCount4 = (int)Math.ceil(362.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bj8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bj8"+ File.separator+i+".db");
				dir_bj8.delete();
				deletedPages4.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_bj8[j]);
				}
			}
		}
		DBApp.recoverRecords("bj8", missing4);
		String tableTrace4 = DBApp.getLastTrace("bj8");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace4.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace4.contains(missing4.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace4.contains("in pages: "+deletedPages4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m21", cols0);
		String [][] records_m21 = new String[326][cols0.length];
		for(int i=0;i<326;i++)
		{
			records_m21[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m21[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m21", records_m21[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(326.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m21 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m21"+ File.separator+i+".db");
				dir_m21.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_m21[j]);
				}
			}
		}
		DBApp.recoverRecords("m21", missing0);
		String tableTrace0 = DBApp.getLastTrace("m21");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("zqq", cols1);
		String [][] records_zqq = new String[219][cols1.length];
		for(int i=0;i<219;i++)
		{
			records_zqq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zqq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zqq", records_zqq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4, o4, p6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5, p7]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6, p8]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7, p9]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8, p10]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(219.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zqq = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zqq"+ File.separator+i+".db");
				dir_zqq.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_zqq[j]);
				}
			}
		}
		DBApp.recoverRecords("zqq", missing1);
		String tableTrace1 = DBApp.getLastTrace("zqq");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yjte", cols0);
		String [][] records_yjte = new String[443][cols0.length];
		for(int i=0;i<443;i++)
		{
			records_yjte[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yjte[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yjte", records_yjte[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record438: [a438, b0, c0, d2, e3, f0, g4]
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		//record441: [a441, b1, c0, d1, e1, f3, g0]
		//record442: [a442, b0, c1, d2, e2, f4, g1]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(443.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yjte = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yjte"+ File.separator+i+".db");
				dir_yjte.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_yjte[j]);
				}
			}
		}
		DBApp.recoverRecords("yjte", missing0);
		String tableTrace0 = DBApp.getLastTrace("yjte");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("hq05", cols0);
		String [][] records_hq05 = new String[366][cols0.length];
		for(int i=0;i<366;i++)
		{
			records_hq05[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hq05[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hq05", records_hq05[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11, o1, p9, q4, r1, s0]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11, n12, o2, p10, q5, r2, s1]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3, m12, n13, o3, p11, q6, r3, s2]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4, p12, q7, r4, s3]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5, m1, n1, o5, p13, q8, r5, s4]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(366.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hq05 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hq05"+ File.separator+i+".db");
				dir_hq05.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_hq05[j]);
				}
			}
		}
		DBApp.recoverRecords("hq05", missing0);
		String tableTrace0 = DBApp.getLastTrace("hq05");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mkx", cols0);
		String [][] records_mkx = new String[142][cols0.length];
		for(int i=0;i<142;i++)
		{
			records_mkx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mkx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mkx", records_mkx[i]);
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
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(142.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mkx = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mkx"+ File.separator+i+".db");
				dir_mkx.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_mkx[j]);
				}
			}
		}
		DBApp.recoverRecords("mkx", missing0);
		String tableTrace0 = DBApp.getLastTrace("mkx");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k26", cols1);
		String [][] records_k26 = new String[457][cols1.length];
		for(int i=0;i<457;i++)
		{
			records_k26[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k26[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k26", records_k26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(457.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k26 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k26"+ File.separator+i+".db");
				dir_k26.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_k26[j]);
				}
			}
		}
		DBApp.recoverRecords("k26", missing1);
		String tableTrace1 = DBApp.getLastTrace("k26");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("ty1", cols2);
		String [][] records_ty1 = new String[165][cols2.length];
		for(int i=0;i<165;i++)
		{
			records_ty1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ty1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ty1", records_ty1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10, p0]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11, p1]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8, o12, p2]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13, p3]
		//record164: [a164, b0, c2, d0, e4, f2, g3, h4, i2, j4, k10, l8, m8, n10, o14, p4]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(165.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ty1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ty1"+ File.separator+i+".db");
				dir_ty1.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_ty1[j]);
				}
			}
		}
		DBApp.recoverRecords("ty1", missing2);
		String tableTrace2 = DBApp.getLastTrace("ty1");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("v5s", cols3);
		String [][] records_v5s = new String[366][cols3.length];
		for(int i=0;i<366;i++)
		{
			records_v5s[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v5s[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v5s", records_v5s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11, o1, p9, q4, r1]
		//record362: [a362, b0, c2, d2, e2, f2, g5, h2, i2, j2, k10, l2, m11, n12, o2, p10, q5, r2]
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3, m12, n13, o3, p11, q6, r3]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4, p12, q7, r4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5, m1, n1, o5, p13, q8, r5]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(366.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v5s = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v5s"+ File.separator+i+".db");
				dir_v5s.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_v5s[j]);
				}
			}
		}
		DBApp.recoverRecords("v5s", missing3);
		String tableTrace3 = DBApp.getLastTrace("v5s");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("o1y", cols4);
		String [][] records_o1y = new String[80][cols4.length];
		for(int i=0;i<80;i++)
		{
			records_o1y[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o1y[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o1y", records_o1y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record75: [a75, b1, c0, d3, e0, f3, g5, h3, i3, j5, k9, l3, m10, n5, o0, p11]
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11, n6, o1, p12]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7, o2, p13]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3, p14]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15]
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages4 = new ArrayList<Integer>();
		int pageCount4 = (int)Math.ceil(80.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o1y = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o1y"+ File.separator+i+".db");
				dir_o1y.delete();
				deletedPages4.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_o1y[j]);
				}
			}
		}
		DBApp.recoverRecords("o1y", missing4);
		String tableTrace4 = DBApp.getLastTrace("o1y");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace4.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace4.contains(missing4.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace4.contains("in pages: "+deletedPages4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l4k5", cols0);
		String [][] records_l4k5 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			records_l4k5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l4k5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l4k5", records_l4k5[i]);
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
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l4k5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l4k5"+ File.separator+i+".db");
				dir_l4k5.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_l4k5[j]);
				}
			}
		}
		DBApp.recoverRecords("l4k5", missing0);
		String tableTrace0 = DBApp.getLastTrace("l4k5");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kd64", cols1);
		String [][] records_kd64 = new String[176][cols1.length];
		for(int i=0;i<176;i++)
		{
			records_kd64[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kd64[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kd64", records_kd64[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1]
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(176.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_kd64 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "kd64"+ File.separator+i+".db");
				dir_kd64.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_kd64[j]);
				}
			}
		}
		DBApp.recoverRecords("kd64", missing1);
		String tableTrace1 = DBApp.getLastTrace("kd64");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o12p", cols2);
		String [][] records_o12p = new String[118][cols2.length];
		for(int i=0;i<118;i++)
		{
			records_o12p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o12p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o12p", records_o12p[i]);
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
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(118.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o12p = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o12p"+ File.separator+i+".db");
				dir_o12p.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_o12p[j]);
				}
			}
		}
		DBApp.recoverRecords("o12p", missing2);
		String tableTrace2 = DBApp.getLastTrace("o12p");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p2", cols0);
		String [][] records_p2 = new String[333][cols0.length];
		for(int i=0;i<333;i++)
		{
			records_p2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p2", records_p2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		//record330: [a330, b0, c0, d2, e0, f0, g1]
		//record331: [a331, b1, c1, d3, e1, f1, g2]
		//record332: [a332, b0, c2, d0, e2, f2, g3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(333.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p2"+ File.separator+i+".db");
				dir_p2.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_p2[j]);
				}
			}
		}
		DBApp.recoverRecords("p2", missing0);
		String tableTrace0 = DBApp.getLastTrace("p2");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j4z", cols1);
		String [][] records_j4z = new String[315][cols1.length];
		for(int i=0;i<315;i++)
		{
			records_j4z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j4z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j4z", records_j4z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record310: [a310, b0, c1, d2, e0, f4, g2]
		//record311: [a311, b1, c2, d3, e1, f5, g3]
		//record312: [a312, b0, c0, d0, e2, f0, g4]
		//record313: [a313, b1, c1, d1, e3, f1, g5]
		//record314: [a314, b0, c2, d2, e4, f2, g6]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(315.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j4z = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j4z"+ File.separator+i+".db");
				dir_j4z.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_j4z[j]);
				}
			}
		}
		DBApp.recoverRecords("j4z", missing1);
		String tableTrace1 = DBApp.getLastTrace("j4z");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j77", cols2);
		String [][] records_j77 = new String[401][cols2.length];
		for(int i=0;i<401;i++)
		{
			records_j77[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j77[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j77", records_j77[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record396: [a396, b0, c0, d0, e1, f0, g4]
		//record397: [a397, b1, c1, d1, e2, f1, g5]
		//record398: [a398, b0, c2, d2, e3, f2, g6]
		//record399: [a399, b1, c0, d3, e4, f3, g0]
		//record400: [a400, b0, c1, d0, e0, f4, g1]
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(401.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j77 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j77"+ File.separator+i+".db");
				dir_j77.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_j77[j]);
				}
			}
		}
		DBApp.recoverRecords("j77", missing2);
		String tableTrace2 = DBApp.getLastTrace("j77");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q2", cols3);
		String [][] records_q2 = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			records_q2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q2", records_q2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record66: [a66, b0, c0, d2, e1, f0, g3]
		//record67: [a67, b1, c1, d3, e2, f1, g4]
		//record68: [a68, b0, c2, d0, e3, f2, g5]
		//record69: [a69, b1, c0, d1, e4, f3, g6]
		//record70: [a70, b0, c1, d2, e0, f4, g0]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q2"+ File.separator+i+".db");
				dir_q2.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_q2[j]);
				}
			}
		}
		DBApp.recoverRecords("q2", missing3);
		String tableTrace3 = DBApp.getLastTrace("q2");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jo6", cols0);
		String [][] records_jo6 = new String[334][cols0.length];
		for(int i=0;i<334;i++)
		{
			records_jo6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jo6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jo6", records_jo6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(334.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jo6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jo6"+ File.separator+i+".db");
				dir_jo6.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_jo6[j]);
				}
			}
		}
		DBApp.recoverRecords("jo6", missing0);
		String tableTrace0 = DBApp.getLastTrace("jo6");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecoverTrce_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xg", cols0);
		String [][] records_xg = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			records_xg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xg", records_xg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages0 = new ArrayList<Integer>();
		int pageCount0 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xg = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xg"+ File.separator+i+".db");
				dir_xg.delete();
				deletedPages0.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_xg[j]);
				}
			}
		}
		DBApp.recoverRecords("xg", missing0);
		String tableTrace0 = DBApp.getLastTrace("xg");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace0.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace0.contains(missing0.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace0.contains("in pages: "+deletedPages0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("e321k", cols1);
		String [][] records_e321k = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_e321k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e321k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e321k", records_e321k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0, r10, s3, t16]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1, r11, s4, t17]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2, r12, s5, t18]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3, r13, s6, t19]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0, o5, p12, q4, r14, s7, t0]
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages1 = new ArrayList<Integer>();
		int pageCount1 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_e321k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "e321k"+ File.separator+i+".db");
				dir_e321k.delete();
				deletedPages1.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_e321k[j]);
				}
			}
		}
		DBApp.recoverRecords("e321k", missing1);
		String tableTrace1 = DBApp.getLastTrace("e321k");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace1.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace1.contains(missing1.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace1.contains("in pages: "+deletedPages1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kba8n", cols2);
		String [][] records_kba8n = new String[115][cols2.length];
		for(int i=0;i<115;i++)
		{
			records_kba8n[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kba8n[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kba8n", records_kba8n[i]);
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
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages2 = new ArrayList<Integer>();
		int pageCount2 = (int)Math.ceil(115.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_kba8n = new File(FileManager.directory.getAbsolutePath()+ File.separator + "kba8n"+ File.separator+i+".db");
				dir_kba8n.delete();
				deletedPages2.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_kba8n[j]);
				}
			}
		}
		DBApp.recoverRecords("kba8n", missing2);
		String tableTrace2 = DBApp.getLastTrace("kba8n");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace2.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace2.contains(missing2.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace2.contains("in pages: "+deletedPages2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v4", cols3);
		String [][] records_v4 = new String[336][cols3.length];
		for(int i=0;i<336;i++)
		{
			records_v4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v4", records_v4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5]
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages3 = new ArrayList<Integer>();
		int pageCount3 = (int)Math.ceil(336.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v4"+ File.separator+i+".db");
				dir_v4.delete();
				deletedPages3.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_v4[j]);
				}
			}
		}
		DBApp.recoverRecords("v4", missing3);
		String tableTrace3 = DBApp.getLastTrace("v4");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace3.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace3.contains(missing3.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace3.contains("in pages: "+deletedPages3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ns352", cols4);
		String [][] records_ns352 = new String[25][cols4.length];
		for(int i=0;i<25;i++)
		{
			records_ns352[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ns352[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ns352", records_ns352[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		ArrayList<Integer> deletedPages4 = new ArrayList<Integer>();
		int pageCount4 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ns352 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ns352"+ File.separator+i+".db");
				dir_ns352.delete();
				deletedPages4.add(i);
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_ns352[j]);
				}
			}
		}
		DBApp.recoverRecords("ns352", missing4);
		String tableTrace4 = DBApp.getLastTrace("ns352");
		assertTrue("Recovering missing records from a table should appear in the last trace.", tableTrace4.contains("Recovering"));
		assertTrue("Recovering missing records from a table should report the correct number of recovered records.", tableTrace4.contains(missing4.size() + " records"));
		assertTrue("Recovering missing records from a table should report the correct pages list in which recovery took effect.", tableTrace4.contains("in pages: "+deletedPages4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("cp8", cols0);
		String tableTrace0 = DBApp.getFullTrace("cp8");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("cp8","a");
		DBApp.createBitMapIndex("cp8","b");
		DBApp.createBitMapIndex("cp8","c");
		DBApp.createBitMapIndex("cp8","d");
		DBApp.createBitMapIndex("cp8","e");
		DBApp.createBitMapIndex("cp8","f");
		DBApp.createBitMapIndex("cp8","g");
		DBApp.createBitMapIndex("cp8","h");
		DBApp.createBitMapIndex("cp8","i");
		DBApp.createBitMapIndex("cp8","j");
		DBApp.createBitMapIndex("cp8","k");
		DBApp.createBitMapIndex("cp8","l");
		DBApp.createBitMapIndex("cp8","m");
		DBApp.createBitMapIndex("cp8","n");
		DBApp.createBitMapIndex("cp8","o");
		DBApp.createBitMapIndex("cp8","p");
		DBApp.createBitMapIndex("cp8","q");
		tableTrace0 = DBApp.getFullTrace("cp8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f672", cols0);
		String tableTrace0 = DBApp.getFullTrace("f672");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("f672","a");
		DBApp.createBitMapIndex("f672","b");
		DBApp.createBitMapIndex("f672","c");
		DBApp.createBitMapIndex("f672","d");
		DBApp.createBitMapIndex("f672","e");
		DBApp.createBitMapIndex("f672","f");
		DBApp.createBitMapIndex("f672","g");
		DBApp.createBitMapIndex("f672","h");
		DBApp.createBitMapIndex("f672","i");
		DBApp.createBitMapIndex("f672","j");
		DBApp.createBitMapIndex("f672","k");
		tableTrace0 = DBApp.getFullTrace("f672");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k38", cols1);
		String tableTrace1 = DBApp.getFullTrace("k38");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("k38","a");
		DBApp.createBitMapIndex("k38","b");
		DBApp.createBitMapIndex("k38","c");
		DBApp.createBitMapIndex("k38","d");
		DBApp.createBitMapIndex("k38","e");
		DBApp.createBitMapIndex("k38","f");
		DBApp.createBitMapIndex("k38","g");
		DBApp.createBitMapIndex("k38","h");
		DBApp.createBitMapIndex("k38","i");
		tableTrace1 = DBApp.getFullTrace("k38");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ray20", cols2);
		String tableTrace2 = DBApp.getFullTrace("ray20");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ray20","a");
		DBApp.createBitMapIndex("ray20","b");
		DBApp.createBitMapIndex("ray20","c");
		DBApp.createBitMapIndex("ray20","d");
		DBApp.createBitMapIndex("ray20","e");
		DBApp.createBitMapIndex("ray20","f");
		DBApp.createBitMapIndex("ray20","g");
		tableTrace2 = DBApp.getFullTrace("ray20");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xmk", cols0);
		String tableTrace0 = DBApp.getFullTrace("xmk");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("xmk","a");
		DBApp.createBitMapIndex("xmk","b");
		DBApp.createBitMapIndex("xmk","c");
		DBApp.createBitMapIndex("xmk","d");
		DBApp.createBitMapIndex("xmk","e");
		DBApp.createBitMapIndex("xmk","f");
		DBApp.createBitMapIndex("xmk","g");
		DBApp.createBitMapIndex("xmk","h");
		DBApp.createBitMapIndex("xmk","i");
		tableTrace0 = DBApp.getFullTrace("xmk");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k74", cols1);
		String tableTrace1 = DBApp.getFullTrace("k74");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("k74","a");
		DBApp.createBitMapIndex("k74","b");
		DBApp.createBitMapIndex("k74","c");
		DBApp.createBitMapIndex("k74","d");
		DBApp.createBitMapIndex("k74","e");
		DBApp.createBitMapIndex("k74","f");
		DBApp.createBitMapIndex("k74","g");
		tableTrace1 = DBApp.getFullTrace("k74");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s0g3", cols2);
		String tableTrace2 = DBApp.getFullTrace("s0g3");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("s0g3","a");
		DBApp.createBitMapIndex("s0g3","b");
		DBApp.createBitMapIndex("s0g3","c");
		DBApp.createBitMapIndex("s0g3","d");
		DBApp.createBitMapIndex("s0g3","e");
		DBApp.createBitMapIndex("s0g3","f");
		DBApp.createBitMapIndex("s0g3","g");
		DBApp.createBitMapIndex("s0g3","h");
		tableTrace2 = DBApp.getFullTrace("s0g3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("srq", cols3);
		String tableTrace3 = DBApp.getFullTrace("srq");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("srq","a");
		DBApp.createBitMapIndex("srq","b");
		DBApp.createBitMapIndex("srq","c");
		DBApp.createBitMapIndex("srq","d");
		DBApp.createBitMapIndex("srq","e");
		DBApp.createBitMapIndex("srq","f");
		DBApp.createBitMapIndex("srq","g");
		tableTrace3 = DBApp.getFullTrace("srq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h4633", cols4);
		String tableTrace4 = DBApp.getFullTrace("h4633");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("h4633","a");
		DBApp.createBitMapIndex("h4633","b");
		DBApp.createBitMapIndex("h4633","c");
		DBApp.createBitMapIndex("h4633","d");
		DBApp.createBitMapIndex("h4633","e");
		DBApp.createBitMapIndex("h4633","f");
		DBApp.createBitMapIndex("h4633","g");
		DBApp.createBitMapIndex("h4633","h");
		DBApp.createBitMapIndex("h4633","i");
		DBApp.createBitMapIndex("h4633","j");
		DBApp.createBitMapIndex("h4633","k");
		DBApp.createBitMapIndex("h4633","l");
		DBApp.createBitMapIndex("h4633","m");
		DBApp.createBitMapIndex("h4633","n");
		tableTrace4 = DBApp.getFullTrace("h4633");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m9", cols0);
		String tableTrace0 = DBApp.getFullTrace("m9");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("m9","a");
		DBApp.createBitMapIndex("m9","b");
		DBApp.createBitMapIndex("m9","c");
		DBApp.createBitMapIndex("m9","d");
		DBApp.createBitMapIndex("m9","e");
		DBApp.createBitMapIndex("m9","f");
		DBApp.createBitMapIndex("m9","g");
		tableTrace0 = DBApp.getFullTrace("m9");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y1n", cols1);
		String tableTrace1 = DBApp.getFullTrace("y1n");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("y1n","a");
		DBApp.createBitMapIndex("y1n","b");
		DBApp.createBitMapIndex("y1n","c");
		DBApp.createBitMapIndex("y1n","d");
		DBApp.createBitMapIndex("y1n","e");
		DBApp.createBitMapIndex("y1n","f");
		DBApp.createBitMapIndex("y1n","g");
		tableTrace1 = DBApp.getFullTrace("y1n");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g7h", cols0);
		String tableTrace0 = DBApp.getFullTrace("g7h");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("g7h","a");
		DBApp.createBitMapIndex("g7h","b");
		DBApp.createBitMapIndex("g7h","c");
		DBApp.createBitMapIndex("g7h","d");
		DBApp.createBitMapIndex("g7h","e");
		DBApp.createBitMapIndex("g7h","f");
		DBApp.createBitMapIndex("g7h","g");
		tableTrace0 = DBApp.getFullTrace("g7h");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m39", cols1);
		String tableTrace1 = DBApp.getFullTrace("m39");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("m39","a");
		DBApp.createBitMapIndex("m39","b");
		DBApp.createBitMapIndex("m39","c");
		DBApp.createBitMapIndex("m39","d");
		DBApp.createBitMapIndex("m39","e");
		DBApp.createBitMapIndex("m39","f");
		DBApp.createBitMapIndex("m39","g");
		tableTrace1 = DBApp.getFullTrace("m39");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("q83s4", cols2);
		String tableTrace2 = DBApp.getFullTrace("q83s4");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("q83s4","a");
		DBApp.createBitMapIndex("q83s4","b");
		DBApp.createBitMapIndex("q83s4","c");
		DBApp.createBitMapIndex("q83s4","d");
		DBApp.createBitMapIndex("q83s4","e");
		DBApp.createBitMapIndex("q83s4","f");
		DBApp.createBitMapIndex("q83s4","g");
		DBApp.createBitMapIndex("q83s4","h");
		tableTrace2 = DBApp.getFullTrace("q83s4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kx6", cols0);
		String tableTrace0 = DBApp.getFullTrace("kx6");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("kx6","a");
		DBApp.createBitMapIndex("kx6","b");
		DBApp.createBitMapIndex("kx6","c");
		DBApp.createBitMapIndex("kx6","d");
		DBApp.createBitMapIndex("kx6","e");
		DBApp.createBitMapIndex("kx6","f");
		DBApp.createBitMapIndex("kx6","g");
		DBApp.createBitMapIndex("kx6","h");
		DBApp.createBitMapIndex("kx6","i");
		DBApp.createBitMapIndex("kx6","j");
		tableTrace0 = DBApp.getFullTrace("kx6");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("k1f", cols1);
		String tableTrace1 = DBApp.getFullTrace("k1f");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("k1f","a");
		DBApp.createBitMapIndex("k1f","b");
		DBApp.createBitMapIndex("k1f","c");
		DBApp.createBitMapIndex("k1f","d");
		DBApp.createBitMapIndex("k1f","e");
		DBApp.createBitMapIndex("k1f","f");
		DBApp.createBitMapIndex("k1f","g");
		DBApp.createBitMapIndex("k1f","h");
		DBApp.createBitMapIndex("k1f","i");
		DBApp.createBitMapIndex("k1f","j");
		DBApp.createBitMapIndex("k1f","k");
		DBApp.createBitMapIndex("k1f","l");
		DBApp.createBitMapIndex("k1f","m");
		DBApp.createBitMapIndex("k1f","n");
		DBApp.createBitMapIndex("k1f","o");
		DBApp.createBitMapIndex("k1f","p");
		DBApp.createBitMapIndex("k1f","q");
		DBApp.createBitMapIndex("k1f","r");
		tableTrace1 = DBApp.getFullTrace("k1f");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("x80", cols2);
		String tableTrace2 = DBApp.getFullTrace("x80");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("x80","a");
		DBApp.createBitMapIndex("x80","b");
		DBApp.createBitMapIndex("x80","c");
		DBApp.createBitMapIndex("x80","d");
		DBApp.createBitMapIndex("x80","e");
		DBApp.createBitMapIndex("x80","f");
		DBApp.createBitMapIndex("x80","g");
		DBApp.createBitMapIndex("x80","h");
		DBApp.createBitMapIndex("x80","i");
		DBApp.createBitMapIndex("x80","j");
		DBApp.createBitMapIndex("x80","k");
		DBApp.createBitMapIndex("x80","l");
		DBApp.createBitMapIndex("x80","m");
		DBApp.createBitMapIndex("x80","n");
		DBApp.createBitMapIndex("x80","o");
		DBApp.createBitMapIndex("x80","p");
		tableTrace2 = DBApp.getFullTrace("x80");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("c0", cols3);
		String tableTrace3 = DBApp.getFullTrace("c0");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("c0","a");
		DBApp.createBitMapIndex("c0","b");
		DBApp.createBitMapIndex("c0","c");
		DBApp.createBitMapIndex("c0","d");
		DBApp.createBitMapIndex("c0","e");
		DBApp.createBitMapIndex("c0","f");
		DBApp.createBitMapIndex("c0","g");
		DBApp.createBitMapIndex("c0","h");
		DBApp.createBitMapIndex("c0","i");
		DBApp.createBitMapIndex("c0","j");
		DBApp.createBitMapIndex("c0","k");
		DBApp.createBitMapIndex("c0","l");
		tableTrace3 = DBApp.getFullTrace("c0");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("udd", cols0);
		String tableTrace0 = DBApp.getFullTrace("udd");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("udd","a");
		DBApp.createBitMapIndex("udd","b");
		DBApp.createBitMapIndex("udd","c");
		DBApp.createBitMapIndex("udd","d");
		DBApp.createBitMapIndex("udd","e");
		DBApp.createBitMapIndex("udd","f");
		DBApp.createBitMapIndex("udd","g");
		tableTrace0 = DBApp.getFullTrace("udd");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fw", cols0);
		String tableTrace0 = DBApp.getFullTrace("fw");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("fw","a");
		DBApp.createBitMapIndex("fw","b");
		DBApp.createBitMapIndex("fw","c");
		DBApp.createBitMapIndex("fw","d");
		DBApp.createBitMapIndex("fw","e");
		DBApp.createBitMapIndex("fw","f");
		DBApp.createBitMapIndex("fw","g");
		tableTrace0 = DBApp.getFullTrace("fw");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d54u", cols1);
		String tableTrace1 = DBApp.getFullTrace("d54u");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("d54u","a");
		DBApp.createBitMapIndex("d54u","b");
		DBApp.createBitMapIndex("d54u","c");
		DBApp.createBitMapIndex("d54u","d");
		DBApp.createBitMapIndex("d54u","e");
		DBApp.createBitMapIndex("d54u","f");
		DBApp.createBitMapIndex("d54u","g");
		tableTrace1 = DBApp.getFullTrace("d54u");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n4ls4", cols2);
		String tableTrace2 = DBApp.getFullTrace("n4ls4");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("n4ls4","a");
		DBApp.createBitMapIndex("n4ls4","b");
		DBApp.createBitMapIndex("n4ls4","c");
		DBApp.createBitMapIndex("n4ls4","d");
		DBApp.createBitMapIndex("n4ls4","e");
		DBApp.createBitMapIndex("n4ls4","f");
		DBApp.createBitMapIndex("n4ls4","g");
		DBApp.createBitMapIndex("n4ls4","h");
		DBApp.createBitMapIndex("n4ls4","i");
		DBApp.createBitMapIndex("n4ls4","j");
		DBApp.createBitMapIndex("n4ls4","k");
		DBApp.createBitMapIndex("n4ls4","l");
		DBApp.createBitMapIndex("n4ls4","m");
		tableTrace2 = DBApp.getFullTrace("n4ls4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o2", cols0);
		String tableTrace0 = DBApp.getFullTrace("o2");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("o2","a");
		DBApp.createBitMapIndex("o2","b");
		DBApp.createBitMapIndex("o2","c");
		DBApp.createBitMapIndex("o2","d");
		DBApp.createBitMapIndex("o2","e");
		DBApp.createBitMapIndex("o2","f");
		DBApp.createBitMapIndex("o2","g");
		DBApp.createBitMapIndex("o2","h");
		DBApp.createBitMapIndex("o2","i");
		DBApp.createBitMapIndex("o2","j");
		DBApp.createBitMapIndex("o2","k");
		tableTrace0 = DBApp.getFullTrace("o2");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("zd4", cols1);
		String tableTrace1 = DBApp.getFullTrace("zd4");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("zd4","a");
		DBApp.createBitMapIndex("zd4","b");
		DBApp.createBitMapIndex("zd4","c");
		DBApp.createBitMapIndex("zd4","d");
		DBApp.createBitMapIndex("zd4","e");
		DBApp.createBitMapIndex("zd4","f");
		DBApp.createBitMapIndex("zd4","g");
		DBApp.createBitMapIndex("zd4","h");
		DBApp.createBitMapIndex("zd4","i");
		DBApp.createBitMapIndex("zd4","j");
		DBApp.createBitMapIndex("zd4","k");
		tableTrace1 = DBApp.getFullTrace("zd4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("iau1t", cols0);
		String tableTrace0 = DBApp.getFullTrace("iau1t");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("iau1t","a");
		DBApp.createBitMapIndex("iau1t","b");
		DBApp.createBitMapIndex("iau1t","c");
		DBApp.createBitMapIndex("iau1t","d");
		DBApp.createBitMapIndex("iau1t","e");
		DBApp.createBitMapIndex("iau1t","f");
		DBApp.createBitMapIndex("iau1t","g");
		DBApp.createBitMapIndex("iau1t","h");
		DBApp.createBitMapIndex("iau1t","i");
		DBApp.createBitMapIndex("iau1t","j");
		DBApp.createBitMapIndex("iau1t","k");
		DBApp.createBitMapIndex("iau1t","l");
		DBApp.createBitMapIndex("iau1t","m");
		DBApp.createBitMapIndex("iau1t","n");
		DBApp.createBitMapIndex("iau1t","o");
		tableTrace0 = DBApp.getFullTrace("iau1t");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i40", cols1);
		String tableTrace1 = DBApp.getFullTrace("i40");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("i40","a");
		DBApp.createBitMapIndex("i40","b");
		DBApp.createBitMapIndex("i40","c");
		DBApp.createBitMapIndex("i40","d");
		DBApp.createBitMapIndex("i40","e");
		DBApp.createBitMapIndex("i40","f");
		DBApp.createBitMapIndex("i40","g");
		DBApp.createBitMapIndex("i40","h");
		DBApp.createBitMapIndex("i40","i");
		DBApp.createBitMapIndex("i40","j");
		DBApp.createBitMapIndex("i40","k");
		DBApp.createBitMapIndex("i40","l");
		tableTrace1 = DBApp.getFullTrace("i40");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p20", cols2);
		String tableTrace2 = DBApp.getFullTrace("p20");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("p20","a");
		DBApp.createBitMapIndex("p20","b");
		DBApp.createBitMapIndex("p20","c");
		DBApp.createBitMapIndex("p20","d");
		DBApp.createBitMapIndex("p20","e");
		DBApp.createBitMapIndex("p20","f");
		DBApp.createBitMapIndex("p20","g");
		DBApp.createBitMapIndex("p20","h");
		DBApp.createBitMapIndex("p20","i");
		DBApp.createBitMapIndex("p20","j");
		tableTrace2 = DBApp.getFullTrace("p20");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("iat", cols0);
		String tableTrace0 = DBApp.getFullTrace("iat");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("iat","a");
		DBApp.createBitMapIndex("iat","b");
		DBApp.createBitMapIndex("iat","c");
		DBApp.createBitMapIndex("iat","d");
		DBApp.createBitMapIndex("iat","e");
		DBApp.createBitMapIndex("iat","f");
		DBApp.createBitMapIndex("iat","g");
		tableTrace0 = DBApp.getFullTrace("iat");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("s2ve8", cols0);
		String tableTrace0 = DBApp.getFullTrace("s2ve8");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("s2ve8","a");
		DBApp.createBitMapIndex("s2ve8","b");
		DBApp.createBitMapIndex("s2ve8","c");
		DBApp.createBitMapIndex("s2ve8","d");
		DBApp.createBitMapIndex("s2ve8","e");
		DBApp.createBitMapIndex("s2ve8","f");
		DBApp.createBitMapIndex("s2ve8","g");
		DBApp.createBitMapIndex("s2ve8","h");
		DBApp.createBitMapIndex("s2ve8","i");
		DBApp.createBitMapIndex("s2ve8","j");
		DBApp.createBitMapIndex("s2ve8","k");
		DBApp.createBitMapIndex("s2ve8","l");
		DBApp.createBitMapIndex("s2ve8","m");
		DBApp.createBitMapIndex("s2ve8","n");
		DBApp.createBitMapIndex("s2ve8","o");
		DBApp.createBitMapIndex("s2ve8","p");
		DBApp.createBitMapIndex("s2ve8","q");
		tableTrace0 = DBApp.getFullTrace("s2ve8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u20", cols1);
		String tableTrace1 = DBApp.getFullTrace("u20");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("u20","a");
		DBApp.createBitMapIndex("u20","b");
		DBApp.createBitMapIndex("u20","c");
		DBApp.createBitMapIndex("u20","d");
		DBApp.createBitMapIndex("u20","e");
		DBApp.createBitMapIndex("u20","f");
		DBApp.createBitMapIndex("u20","g");
		tableTrace1 = DBApp.getFullTrace("u20");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a9mg", cols2);
		String tableTrace2 = DBApp.getFullTrace("a9mg");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("a9mg","a");
		DBApp.createBitMapIndex("a9mg","b");
		DBApp.createBitMapIndex("a9mg","c");
		DBApp.createBitMapIndex("a9mg","d");
		DBApp.createBitMapIndex("a9mg","e");
		DBApp.createBitMapIndex("a9mg","f");
		DBApp.createBitMapIndex("a9mg","g");
		tableTrace2 = DBApp.getFullTrace("a9mg");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ibrd7", cols0);
		String tableTrace0 = DBApp.getFullTrace("ibrd7");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ibrd7","a");
		DBApp.createBitMapIndex("ibrd7","b");
		DBApp.createBitMapIndex("ibrd7","c");
		DBApp.createBitMapIndex("ibrd7","d");
		DBApp.createBitMapIndex("ibrd7","e");
		DBApp.createBitMapIndex("ibrd7","f");
		DBApp.createBitMapIndex("ibrd7","g");
		tableTrace0 = DBApp.getFullTrace("ibrd7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("zt", cols1);
		String tableTrace1 = DBApp.getFullTrace("zt");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("zt","a");
		DBApp.createBitMapIndex("zt","b");
		DBApp.createBitMapIndex("zt","c");
		DBApp.createBitMapIndex("zt","d");
		DBApp.createBitMapIndex("zt","e");
		DBApp.createBitMapIndex("zt","f");
		DBApp.createBitMapIndex("zt","g");
		DBApp.createBitMapIndex("zt","h");
		DBApp.createBitMapIndex("zt","i");
		DBApp.createBitMapIndex("zt","j");
		DBApp.createBitMapIndex("zt","k");
		DBApp.createBitMapIndex("zt","l");
		DBApp.createBitMapIndex("zt","m");
		DBApp.createBitMapIndex("zt","n");
		DBApp.createBitMapIndex("zt","o");
		DBApp.createBitMapIndex("zt","p");
		DBApp.createBitMapIndex("zt","q");
		tableTrace1 = DBApp.getFullTrace("zt");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("sbs30", cols2);
		String tableTrace2 = DBApp.getFullTrace("sbs30");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("sbs30","a");
		DBApp.createBitMapIndex("sbs30","b");
		DBApp.createBitMapIndex("sbs30","c");
		DBApp.createBitMapIndex("sbs30","d");
		DBApp.createBitMapIndex("sbs30","e");
		DBApp.createBitMapIndex("sbs30","f");
		DBApp.createBitMapIndex("sbs30","g");
		DBApp.createBitMapIndex("sbs30","h");
		DBApp.createBitMapIndex("sbs30","i");
		DBApp.createBitMapIndex("sbs30","j");
		tableTrace2 = DBApp.getFullTrace("sbs30");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g1", cols3);
		String tableTrace3 = DBApp.getFullTrace("g1");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("g1","a");
		DBApp.createBitMapIndex("g1","b");
		DBApp.createBitMapIndex("g1","c");
		DBApp.createBitMapIndex("g1","d");
		DBApp.createBitMapIndex("g1","e");
		DBApp.createBitMapIndex("g1","f");
		DBApp.createBitMapIndex("g1","g");
		DBApp.createBitMapIndex("g1","h");
		DBApp.createBitMapIndex("g1","i");
		DBApp.createBitMapIndex("g1","j");
		tableTrace3 = DBApp.getFullTrace("g1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p8u", cols4);
		String tableTrace4 = DBApp.getFullTrace("p8u");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("p8u","a");
		DBApp.createBitMapIndex("p8u","b");
		DBApp.createBitMapIndex("p8u","c");
		DBApp.createBitMapIndex("p8u","d");
		DBApp.createBitMapIndex("p8u","e");
		DBApp.createBitMapIndex("p8u","f");
		DBApp.createBitMapIndex("p8u","g");
		DBApp.createBitMapIndex("p8u","h");
		DBApp.createBitMapIndex("p8u","i");
		DBApp.createBitMapIndex("p8u","j");
		DBApp.createBitMapIndex("p8u","k");
		tableTrace4 = DBApp.getFullTrace("p8u");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("uz1", cols0);
		String tableTrace0 = DBApp.getFullTrace("uz1");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("uz1","a");
		DBApp.createBitMapIndex("uz1","b");
		DBApp.createBitMapIndex("uz1","c");
		DBApp.createBitMapIndex("uz1","d");
		DBApp.createBitMapIndex("uz1","e");
		DBApp.createBitMapIndex("uz1","f");
		DBApp.createBitMapIndex("uz1","g");
		DBApp.createBitMapIndex("uz1","h");
		DBApp.createBitMapIndex("uz1","i");
		DBApp.createBitMapIndex("uz1","j");
		DBApp.createBitMapIndex("uz1","k");
		DBApp.createBitMapIndex("uz1","l");
		DBApp.createBitMapIndex("uz1","m");
		DBApp.createBitMapIndex("uz1","n");
		DBApp.createBitMapIndex("uz1","o");
		DBApp.createBitMapIndex("uz1","p");
		DBApp.createBitMapIndex("uz1","q");
		DBApp.createBitMapIndex("uz1","r");
		DBApp.createBitMapIndex("uz1","s");
		DBApp.createBitMapIndex("uz1","t");
		tableTrace0 = DBApp.getFullTrace("uz1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wjc", cols1);
		String tableTrace1 = DBApp.getFullTrace("wjc");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("wjc","a");
		DBApp.createBitMapIndex("wjc","b");
		DBApp.createBitMapIndex("wjc","c");
		DBApp.createBitMapIndex("wjc","d");
		DBApp.createBitMapIndex("wjc","e");
		DBApp.createBitMapIndex("wjc","f");
		DBApp.createBitMapIndex("wjc","g");
		DBApp.createBitMapIndex("wjc","h");
		tableTrace1 = DBApp.getFullTrace("wjc");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("r3", cols2);
		String tableTrace2 = DBApp.getFullTrace("r3");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("r3","a");
		DBApp.createBitMapIndex("r3","b");
		DBApp.createBitMapIndex("r3","c");
		DBApp.createBitMapIndex("r3","d");
		DBApp.createBitMapIndex("r3","e");
		DBApp.createBitMapIndex("r3","f");
		DBApp.createBitMapIndex("r3","g");
		DBApp.createBitMapIndex("r3","h");
		DBApp.createBitMapIndex("r3","i");
		DBApp.createBitMapIndex("r3","j");
		DBApp.createBitMapIndex("r3","k");
		DBApp.createBitMapIndex("r3","l");
		DBApp.createBitMapIndex("r3","m");
		DBApp.createBitMapIndex("r3","n");
		tableTrace2 = DBApp.getFullTrace("r3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("g9m8", cols0);
		String tableTrace0 = DBApp.getFullTrace("g9m8");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("g9m8","a");
		DBApp.createBitMapIndex("g9m8","b");
		DBApp.createBitMapIndex("g9m8","c");
		DBApp.createBitMapIndex("g9m8","d");
		DBApp.createBitMapIndex("g9m8","e");
		DBApp.createBitMapIndex("g9m8","f");
		DBApp.createBitMapIndex("g9m8","g");
		DBApp.createBitMapIndex("g9m8","h");
		DBApp.createBitMapIndex("g9m8","i");
		DBApp.createBitMapIndex("g9m8","j");
		DBApp.createBitMapIndex("g9m8","k");
		DBApp.createBitMapIndex("g9m8","l");
		DBApp.createBitMapIndex("g9m8","m");
		tableTrace0 = DBApp.getFullTrace("g9m8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ts4n", cols1);
		String tableTrace1 = DBApp.getFullTrace("ts4n");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("ts4n","a");
		DBApp.createBitMapIndex("ts4n","b");
		DBApp.createBitMapIndex("ts4n","c");
		DBApp.createBitMapIndex("ts4n","d");
		DBApp.createBitMapIndex("ts4n","e");
		DBApp.createBitMapIndex("ts4n","f");
		DBApp.createBitMapIndex("ts4n","g");
		tableTrace1 = DBApp.getFullTrace("ts4n");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ap", cols2);
		String tableTrace2 = DBApp.getFullTrace("ap");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ap","a");
		DBApp.createBitMapIndex("ap","b");
		DBApp.createBitMapIndex("ap","c");
		DBApp.createBitMapIndex("ap","d");
		DBApp.createBitMapIndex("ap","e");
		DBApp.createBitMapIndex("ap","f");
		DBApp.createBitMapIndex("ap","g");
		DBApp.createBitMapIndex("ap","h");
		tableTrace2 = DBApp.getFullTrace("ap");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("cdc", cols3);
		String tableTrace3 = DBApp.getFullTrace("cdc");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("cdc","a");
		DBApp.createBitMapIndex("cdc","b");
		DBApp.createBitMapIndex("cdc","c");
		DBApp.createBitMapIndex("cdc","d");
		DBApp.createBitMapIndex("cdc","e");
		DBApp.createBitMapIndex("cdc","f");
		DBApp.createBitMapIndex("cdc","g");
		DBApp.createBitMapIndex("cdc","h");
		DBApp.createBitMapIndex("cdc","i");
		DBApp.createBitMapIndex("cdc","j");
		DBApp.createBitMapIndex("cdc","k");
		DBApp.createBitMapIndex("cdc","l");
		DBApp.createBitMapIndex("cdc","m");
		DBApp.createBitMapIndex("cdc","n");
		DBApp.createBitMapIndex("cdc","o");
		DBApp.createBitMapIndex("cdc","p");
		DBApp.createBitMapIndex("cdc","q");
		DBApp.createBitMapIndex("cdc","r");
		DBApp.createBitMapIndex("cdc","s");
		tableTrace3 = DBApp.getFullTrace("cdc");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("oi", cols4);
		String tableTrace4 = DBApp.getFullTrace("oi");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("oi","a");
		DBApp.createBitMapIndex("oi","b");
		DBApp.createBitMapIndex("oi","c");
		DBApp.createBitMapIndex("oi","d");
		DBApp.createBitMapIndex("oi","e");
		DBApp.createBitMapIndex("oi","f");
		DBApp.createBitMapIndex("oi","g");
		DBApp.createBitMapIndex("oi","h");
		tableTrace4 = DBApp.getFullTrace("oi");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xc", cols0);
		String tableTrace0 = DBApp.getFullTrace("xc");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("xc","a");
		DBApp.createBitMapIndex("xc","b");
		DBApp.createBitMapIndex("xc","c");
		DBApp.createBitMapIndex("xc","d");
		DBApp.createBitMapIndex("xc","e");
		DBApp.createBitMapIndex("xc","f");
		DBApp.createBitMapIndex("xc","g");
		DBApp.createBitMapIndex("xc","h");
		DBApp.createBitMapIndex("xc","i");
		DBApp.createBitMapIndex("xc","j");
		DBApp.createBitMapIndex("xc","k");
		DBApp.createBitMapIndex("xc","l");
		DBApp.createBitMapIndex("xc","m");
		DBApp.createBitMapIndex("xc","n");
		tableTrace0 = DBApp.getFullTrace("xc");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hf5g", cols1);
		String tableTrace1 = DBApp.getFullTrace("hf5g");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("hf5g","a");
		DBApp.createBitMapIndex("hf5g","b");
		DBApp.createBitMapIndex("hf5g","c");
		DBApp.createBitMapIndex("hf5g","d");
		DBApp.createBitMapIndex("hf5g","e");
		DBApp.createBitMapIndex("hf5g","f");
		DBApp.createBitMapIndex("hf5g","g");
		tableTrace1 = DBApp.getFullTrace("hf5g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m2", cols2);
		String tableTrace2 = DBApp.getFullTrace("m2");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("m2","a");
		DBApp.createBitMapIndex("m2","b");
		DBApp.createBitMapIndex("m2","c");
		DBApp.createBitMapIndex("m2","d");
		DBApp.createBitMapIndex("m2","e");
		DBApp.createBitMapIndex("m2","f");
		DBApp.createBitMapIndex("m2","g");
		DBApp.createBitMapIndex("m2","h");
		DBApp.createBitMapIndex("m2","i");
		DBApp.createBitMapIndex("m2","j");
		DBApp.createBitMapIndex("m2","k");
		DBApp.createBitMapIndex("m2","l");
		DBApp.createBitMapIndex("m2","m");
		tableTrace2 = DBApp.getFullTrace("m2");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sk", cols0);
		String tableTrace0 = DBApp.getFullTrace("sk");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("sk","a");
		DBApp.createBitMapIndex("sk","b");
		DBApp.createBitMapIndex("sk","c");
		DBApp.createBitMapIndex("sk","d");
		DBApp.createBitMapIndex("sk","e");
		DBApp.createBitMapIndex("sk","f");
		DBApp.createBitMapIndex("sk","g");
		DBApp.createBitMapIndex("sk","h");
		DBApp.createBitMapIndex("sk","i");
		DBApp.createBitMapIndex("sk","j");
		DBApp.createBitMapIndex("sk","k");
		DBApp.createBitMapIndex("sk","l");
		DBApp.createBitMapIndex("sk","m");
		DBApp.createBitMapIndex("sk","n");
		tableTrace0 = DBApp.getFullTrace("sk");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("px3", cols0);
		String tableTrace0 = DBApp.getFullTrace("px3");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("px3","a");
		DBApp.createBitMapIndex("px3","b");
		DBApp.createBitMapIndex("px3","c");
		DBApp.createBitMapIndex("px3","d");
		DBApp.createBitMapIndex("px3","e");
		DBApp.createBitMapIndex("px3","f");
		DBApp.createBitMapIndex("px3","g");
		DBApp.createBitMapIndex("px3","h");
		DBApp.createBitMapIndex("px3","i");
		DBApp.createBitMapIndex("px3","j");
		DBApp.createBitMapIndex("px3","k");
		DBApp.createBitMapIndex("px3","l");
		DBApp.createBitMapIndex("px3","m");
		DBApp.createBitMapIndex("px3","n");
		DBApp.createBitMapIndex("px3","o");
		tableTrace0 = DBApp.getFullTrace("px3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y93", cols1);
		String tableTrace1 = DBApp.getFullTrace("y93");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("y93","a");
		DBApp.createBitMapIndex("y93","b");
		DBApp.createBitMapIndex("y93","c");
		DBApp.createBitMapIndex("y93","d");
		DBApp.createBitMapIndex("y93","e");
		DBApp.createBitMapIndex("y93","f");
		DBApp.createBitMapIndex("y93","g");
		tableTrace1 = DBApp.getFullTrace("y93");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i7g4", cols2);
		String tableTrace2 = DBApp.getFullTrace("i7g4");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("i7g4","a");
		DBApp.createBitMapIndex("i7g4","b");
		DBApp.createBitMapIndex("i7g4","c");
		DBApp.createBitMapIndex("i7g4","d");
		DBApp.createBitMapIndex("i7g4","e");
		DBApp.createBitMapIndex("i7g4","f");
		DBApp.createBitMapIndex("i7g4","g");
		DBApp.createBitMapIndex("i7g4","h");
		DBApp.createBitMapIndex("i7g4","i");
		DBApp.createBitMapIndex("i7g4","j");
		DBApp.createBitMapIndex("i7g4","k");
		DBApp.createBitMapIndex("i7g4","l");
		DBApp.createBitMapIndex("i7g4","m");
		DBApp.createBitMapIndex("i7g4","n");
		DBApp.createBitMapIndex("i7g4","o");
		tableTrace2 = DBApp.getFullTrace("i7g4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z9hc", cols3);
		String tableTrace3 = DBApp.getFullTrace("z9hc");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("z9hc","a");
		DBApp.createBitMapIndex("z9hc","b");
		DBApp.createBitMapIndex("z9hc","c");
		DBApp.createBitMapIndex("z9hc","d");
		DBApp.createBitMapIndex("z9hc","e");
		DBApp.createBitMapIndex("z9hc","f");
		DBApp.createBitMapIndex("z9hc","g");
		DBApp.createBitMapIndex("z9hc","h");
		DBApp.createBitMapIndex("z9hc","i");
		DBApp.createBitMapIndex("z9hc","j");
		DBApp.createBitMapIndex("z9hc","k");
		DBApp.createBitMapIndex("z9hc","l");
		tableTrace3 = DBApp.getFullTrace("z9hc");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("j42", cols0);
		String tableTrace0 = DBApp.getFullTrace("j42");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("j42","a");
		DBApp.createBitMapIndex("j42","b");
		DBApp.createBitMapIndex("j42","c");
		DBApp.createBitMapIndex("j42","d");
		DBApp.createBitMapIndex("j42","e");
		DBApp.createBitMapIndex("j42","f");
		DBApp.createBitMapIndex("j42","g");
		DBApp.createBitMapIndex("j42","h");
		DBApp.createBitMapIndex("j42","i");
		DBApp.createBitMapIndex("j42","j");
		DBApp.createBitMapIndex("j42","k");
		DBApp.createBitMapIndex("j42","l");
		DBApp.createBitMapIndex("j42","m");
		DBApp.createBitMapIndex("j42","n");
		DBApp.createBitMapIndex("j42","o");
		DBApp.createBitMapIndex("j42","p");
		tableTrace0 = DBApp.getFullTrace("j42");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mz", cols1);
		String tableTrace1 = DBApp.getFullTrace("mz");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("mz","a");
		DBApp.createBitMapIndex("mz","b");
		DBApp.createBitMapIndex("mz","c");
		DBApp.createBitMapIndex("mz","d");
		DBApp.createBitMapIndex("mz","e");
		DBApp.createBitMapIndex("mz","f");
		DBApp.createBitMapIndex("mz","g");
		DBApp.createBitMapIndex("mz","h");
		DBApp.createBitMapIndex("mz","i");
		DBApp.createBitMapIndex("mz","j");
		DBApp.createBitMapIndex("mz","k");
		DBApp.createBitMapIndex("mz","l");
		DBApp.createBitMapIndex("mz","m");
		DBApp.createBitMapIndex("mz","n");
		DBApp.createBitMapIndex("mz","o");
		tableTrace1 = DBApp.getFullTrace("mz");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ef7o", cols2);
		String tableTrace2 = DBApp.getFullTrace("ef7o");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ef7o","a");
		DBApp.createBitMapIndex("ef7o","b");
		DBApp.createBitMapIndex("ef7o","c");
		DBApp.createBitMapIndex("ef7o","d");
		DBApp.createBitMapIndex("ef7o","e");
		DBApp.createBitMapIndex("ef7o","f");
		DBApp.createBitMapIndex("ef7o","g");
		tableTrace2 = DBApp.getFullTrace("ef7o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("fb", cols3);
		String tableTrace3 = DBApp.getFullTrace("fb");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("fb","a");
		DBApp.createBitMapIndex("fb","b");
		DBApp.createBitMapIndex("fb","c");
		DBApp.createBitMapIndex("fb","d");
		DBApp.createBitMapIndex("fb","e");
		DBApp.createBitMapIndex("fb","f");
		DBApp.createBitMapIndex("fb","g");
		DBApp.createBitMapIndex("fb","h");
		DBApp.createBitMapIndex("fb","i");
		DBApp.createBitMapIndex("fb","j");
		tableTrace3 = DBApp.getFullTrace("fb");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("du3l", cols0);
		String tableTrace0 = DBApp.getFullTrace("du3l");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("du3l","a");
		DBApp.createBitMapIndex("du3l","b");
		DBApp.createBitMapIndex("du3l","c");
		DBApp.createBitMapIndex("du3l","d");
		DBApp.createBitMapIndex("du3l","e");
		DBApp.createBitMapIndex("du3l","f");
		DBApp.createBitMapIndex("du3l","g");
		tableTrace0 = DBApp.getFullTrace("du3l");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tgjz", cols1);
		String tableTrace1 = DBApp.getFullTrace("tgjz");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("tgjz","a");
		DBApp.createBitMapIndex("tgjz","b");
		DBApp.createBitMapIndex("tgjz","c");
		DBApp.createBitMapIndex("tgjz","d");
		DBApp.createBitMapIndex("tgjz","e");
		DBApp.createBitMapIndex("tgjz","f");
		DBApp.createBitMapIndex("tgjz","g");
		tableTrace1 = DBApp.getFullTrace("tgjz");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("cz3", cols0);
		String tableTrace0 = DBApp.getFullTrace("cz3");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("cz3","a");
		DBApp.createBitMapIndex("cz3","b");
		DBApp.createBitMapIndex("cz3","c");
		DBApp.createBitMapIndex("cz3","d");
		DBApp.createBitMapIndex("cz3","e");
		DBApp.createBitMapIndex("cz3","f");
		DBApp.createBitMapIndex("cz3","g");
		DBApp.createBitMapIndex("cz3","h");
		DBApp.createBitMapIndex("cz3","i");
		DBApp.createBitMapIndex("cz3","j");
		DBApp.createBitMapIndex("cz3","k");
		DBApp.createBitMapIndex("cz3","l");
		DBApp.createBitMapIndex("cz3","m");
		DBApp.createBitMapIndex("cz3","n");
		DBApp.createBitMapIndex("cz3","o");
		DBApp.createBitMapIndex("cz3","p");
		DBApp.createBitMapIndex("cz3","q");
		tableTrace0 = DBApp.getFullTrace("cz3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("x75q", cols1);
		String tableTrace1 = DBApp.getFullTrace("x75q");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("x75q","a");
		DBApp.createBitMapIndex("x75q","b");
		DBApp.createBitMapIndex("x75q","c");
		DBApp.createBitMapIndex("x75q","d");
		DBApp.createBitMapIndex("x75q","e");
		DBApp.createBitMapIndex("x75q","f");
		DBApp.createBitMapIndex("x75q","g");
		DBApp.createBitMapIndex("x75q","h");
		DBApp.createBitMapIndex("x75q","i");
		DBApp.createBitMapIndex("x75q","j");
		DBApp.createBitMapIndex("x75q","k");
		DBApp.createBitMapIndex("x75q","l");
		DBApp.createBitMapIndex("x75q","m");
		DBApp.createBitMapIndex("x75q","n");
		DBApp.createBitMapIndex("x75q","o");
		DBApp.createBitMapIndex("x75q","p");
		DBApp.createBitMapIndex("x75q","q");
		tableTrace1 = DBApp.getFullTrace("x75q");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("t2o", cols2);
		String tableTrace2 = DBApp.getFullTrace("t2o");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("t2o","a");
		DBApp.createBitMapIndex("t2o","b");
		DBApp.createBitMapIndex("t2o","c");
		DBApp.createBitMapIndex("t2o","d");
		DBApp.createBitMapIndex("t2o","e");
		DBApp.createBitMapIndex("t2o","f");
		DBApp.createBitMapIndex("t2o","g");
		DBApp.createBitMapIndex("t2o","h");
		DBApp.createBitMapIndex("t2o","i");
		DBApp.createBitMapIndex("t2o","j");
		DBApp.createBitMapIndex("t2o","k");
		DBApp.createBitMapIndex("t2o","l");
		DBApp.createBitMapIndex("t2o","m");
		DBApp.createBitMapIndex("t2o","n");
		DBApp.createBitMapIndex("t2o","o");
		DBApp.createBitMapIndex("t2o","p");
		DBApp.createBitMapIndex("t2o","q");
		DBApp.createBitMapIndex("t2o","r");
		DBApp.createBitMapIndex("t2o","s");
		tableTrace2 = DBApp.getFullTrace("t2o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("a3i", cols3);
		String tableTrace3 = DBApp.getFullTrace("a3i");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("a3i","a");
		DBApp.createBitMapIndex("a3i","b");
		DBApp.createBitMapIndex("a3i","c");
		DBApp.createBitMapIndex("a3i","d");
		DBApp.createBitMapIndex("a3i","e");
		DBApp.createBitMapIndex("a3i","f");
		DBApp.createBitMapIndex("a3i","g");
		DBApp.createBitMapIndex("a3i","h");
		DBApp.createBitMapIndex("a3i","i");
		DBApp.createBitMapIndex("a3i","j");
		DBApp.createBitMapIndex("a3i","k");
		DBApp.createBitMapIndex("a3i","l");
		DBApp.createBitMapIndex("a3i","m");
		DBApp.createBitMapIndex("a3i","n");
		DBApp.createBitMapIndex("a3i","o");
		DBApp.createBitMapIndex("a3i","p");
		DBApp.createBitMapIndex("a3i","q");
		DBApp.createBitMapIndex("a3i","r");
		DBApp.createBitMapIndex("a3i","s");
		DBApp.createBitMapIndex("a3i","t");
		tableTrace3 = DBApp.getFullTrace("a3i");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fl99", cols0);
		String tableTrace0 = DBApp.getFullTrace("fl99");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("fl99","a");
		DBApp.createBitMapIndex("fl99","b");
		DBApp.createBitMapIndex("fl99","c");
		DBApp.createBitMapIndex("fl99","d");
		DBApp.createBitMapIndex("fl99","e");
		DBApp.createBitMapIndex("fl99","f");
		DBApp.createBitMapIndex("fl99","g");
		tableTrace0 = DBApp.getFullTrace("fl99");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("v0", cols0);
		String tableTrace0 = DBApp.getFullTrace("v0");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("v0","a");
		DBApp.createBitMapIndex("v0","b");
		DBApp.createBitMapIndex("v0","c");
		DBApp.createBitMapIndex("v0","d");
		DBApp.createBitMapIndex("v0","e");
		DBApp.createBitMapIndex("v0","f");
		DBApp.createBitMapIndex("v0","g");
		DBApp.createBitMapIndex("v0","h");
		DBApp.createBitMapIndex("v0","i");
		DBApp.createBitMapIndex("v0","j");
		DBApp.createBitMapIndex("v0","k");
		DBApp.createBitMapIndex("v0","l");
		DBApp.createBitMapIndex("v0","m");
		DBApp.createBitMapIndex("v0","n");
		DBApp.createBitMapIndex("v0","o");
		DBApp.createBitMapIndex("v0","p");
		DBApp.createBitMapIndex("v0","q");
		DBApp.createBitMapIndex("v0","r");
		DBApp.createBitMapIndex("v0","s");
		tableTrace0 = DBApp.getFullTrace("v0");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("evj", cols1);
		String tableTrace1 = DBApp.getFullTrace("evj");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("evj","a");
		DBApp.createBitMapIndex("evj","b");
		DBApp.createBitMapIndex("evj","c");
		DBApp.createBitMapIndex("evj","d");
		DBApp.createBitMapIndex("evj","e");
		DBApp.createBitMapIndex("evj","f");
		DBApp.createBitMapIndex("evj","g");
		DBApp.createBitMapIndex("evj","h");
		DBApp.createBitMapIndex("evj","i");
		DBApp.createBitMapIndex("evj","j");
		DBApp.createBitMapIndex("evj","k");
		DBApp.createBitMapIndex("evj","l");
		DBApp.createBitMapIndex("evj","m");
		tableTrace1 = DBApp.getFullTrace("evj");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eg5", cols2);
		String tableTrace2 = DBApp.getFullTrace("eg5");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("eg5","a");
		DBApp.createBitMapIndex("eg5","b");
		DBApp.createBitMapIndex("eg5","c");
		DBApp.createBitMapIndex("eg5","d");
		DBApp.createBitMapIndex("eg5","e");
		DBApp.createBitMapIndex("eg5","f");
		DBApp.createBitMapIndex("eg5","g");
		tableTrace2 = DBApp.getFullTrace("eg5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k67", cols3);
		String tableTrace3 = DBApp.getFullTrace("k67");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("k67","a");
		DBApp.createBitMapIndex("k67","b");
		DBApp.createBitMapIndex("k67","c");
		DBApp.createBitMapIndex("k67","d");
		DBApp.createBitMapIndex("k67","e");
		DBApp.createBitMapIndex("k67","f");
		DBApp.createBitMapIndex("k67","g");
		tableTrace3 = DBApp.getFullTrace("k67");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("g4ip", cols0);
		String tableTrace0 = DBApp.getFullTrace("g4ip");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("g4ip","a");
		DBApp.createBitMapIndex("g4ip","b");
		DBApp.createBitMapIndex("g4ip","c");
		DBApp.createBitMapIndex("g4ip","d");
		DBApp.createBitMapIndex("g4ip","e");
		DBApp.createBitMapIndex("g4ip","f");
		DBApp.createBitMapIndex("g4ip","g");
		DBApp.createBitMapIndex("g4ip","h");
		DBApp.createBitMapIndex("g4ip","i");
		DBApp.createBitMapIndex("g4ip","j");
		DBApp.createBitMapIndex("g4ip","k");
		DBApp.createBitMapIndex("g4ip","l");
		DBApp.createBitMapIndex("g4ip","m");
		DBApp.createBitMapIndex("g4ip","n");
		DBApp.createBitMapIndex("g4ip","o");
		DBApp.createBitMapIndex("g4ip","p");
		DBApp.createBitMapIndex("g4ip","q");
		DBApp.createBitMapIndex("g4ip","r");
		tableTrace0 = DBApp.getFullTrace("g4ip");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mg3", cols0);
		String tableTrace0 = DBApp.getFullTrace("mg3");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("mg3","a");
		DBApp.createBitMapIndex("mg3","b");
		DBApp.createBitMapIndex("mg3","c");
		DBApp.createBitMapIndex("mg3","d");
		DBApp.createBitMapIndex("mg3","e");
		DBApp.createBitMapIndex("mg3","f");
		DBApp.createBitMapIndex("mg3","g");
		DBApp.createBitMapIndex("mg3","h");
		tableTrace0 = DBApp.getFullTrace("mg3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s47", cols1);
		String tableTrace1 = DBApp.getFullTrace("s47");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("s47","a");
		DBApp.createBitMapIndex("s47","b");
		DBApp.createBitMapIndex("s47","c");
		DBApp.createBitMapIndex("s47","d");
		DBApp.createBitMapIndex("s47","e");
		DBApp.createBitMapIndex("s47","f");
		DBApp.createBitMapIndex("s47","g");
		tableTrace1 = DBApp.getFullTrace("s47");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c75", cols2);
		String tableTrace2 = DBApp.getFullTrace("c75");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("c75","a");
		DBApp.createBitMapIndex("c75","b");
		DBApp.createBitMapIndex("c75","c");
		DBApp.createBitMapIndex("c75","d");
		DBApp.createBitMapIndex("c75","e");
		DBApp.createBitMapIndex("c75","f");
		DBApp.createBitMapIndex("c75","g");
		tableTrace2 = DBApp.getFullTrace("c75");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k35", cols0);
		String tableTrace0 = DBApp.getFullTrace("k35");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("k35","a");
		DBApp.createBitMapIndex("k35","b");
		DBApp.createBitMapIndex("k35","c");
		DBApp.createBitMapIndex("k35","d");
		DBApp.createBitMapIndex("k35","e");
		DBApp.createBitMapIndex("k35","f");
		DBApp.createBitMapIndex("k35","g");
		tableTrace0 = DBApp.getFullTrace("k35");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mhq", cols1);
		String tableTrace1 = DBApp.getFullTrace("mhq");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("mhq","a");
		DBApp.createBitMapIndex("mhq","b");
		DBApp.createBitMapIndex("mhq","c");
		DBApp.createBitMapIndex("mhq","d");
		DBApp.createBitMapIndex("mhq","e");
		DBApp.createBitMapIndex("mhq","f");
		DBApp.createBitMapIndex("mhq","g");
		tableTrace1 = DBApp.getFullTrace("mhq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("an", cols2);
		String tableTrace2 = DBApp.getFullTrace("an");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("an","a");
		DBApp.createBitMapIndex("an","b");
		DBApp.createBitMapIndex("an","c");
		DBApp.createBitMapIndex("an","d");
		DBApp.createBitMapIndex("an","e");
		DBApp.createBitMapIndex("an","f");
		DBApp.createBitMapIndex("an","g");
		tableTrace2 = DBApp.getFullTrace("an");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("hd9u", cols3);
		String tableTrace3 = DBApp.getFullTrace("hd9u");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("hd9u","a");
		DBApp.createBitMapIndex("hd9u","b");
		DBApp.createBitMapIndex("hd9u","c");
		DBApp.createBitMapIndex("hd9u","d");
		DBApp.createBitMapIndex("hd9u","e");
		DBApp.createBitMapIndex("hd9u","f");
		DBApp.createBitMapIndex("hd9u","g");
		DBApp.createBitMapIndex("hd9u","h");
		DBApp.createBitMapIndex("hd9u","i");
		DBApp.createBitMapIndex("hd9u","j");
		DBApp.createBitMapIndex("hd9u","k");
		DBApp.createBitMapIndex("hd9u","l");
		DBApp.createBitMapIndex("hd9u","m");
		DBApp.createBitMapIndex("hd9u","n");
		DBApp.createBitMapIndex("hd9u","o");
		DBApp.createBitMapIndex("hd9u","p");
		DBApp.createBitMapIndex("hd9u","q");
		DBApp.createBitMapIndex("hd9u","r");
		DBApp.createBitMapIndex("hd9u","s");
		DBApp.createBitMapIndex("hd9u","t");
		tableTrace3 = DBApp.getFullTrace("hd9u");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xtqch", cols4);
		String tableTrace4 = DBApp.getFullTrace("xtqch");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("xtqch","a");
		DBApp.createBitMapIndex("xtqch","b");
		DBApp.createBitMapIndex("xtqch","c");
		DBApp.createBitMapIndex("xtqch","d");
		DBApp.createBitMapIndex("xtqch","e");
		DBApp.createBitMapIndex("xtqch","f");
		DBApp.createBitMapIndex("xtqch","g");
		tableTrace4 = DBApp.getFullTrace("xtqch");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b9307", cols0);
		String tableTrace0 = DBApp.getFullTrace("b9307");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("b9307","a");
		DBApp.createBitMapIndex("b9307","b");
		DBApp.createBitMapIndex("b9307","c");
		DBApp.createBitMapIndex("b9307","d");
		DBApp.createBitMapIndex("b9307","e");
		DBApp.createBitMapIndex("b9307","f");
		DBApp.createBitMapIndex("b9307","g");
		tableTrace0 = DBApp.getFullTrace("b9307");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gt", cols1);
		String tableTrace1 = DBApp.getFullTrace("gt");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("gt","a");
		DBApp.createBitMapIndex("gt","b");
		DBApp.createBitMapIndex("gt","c");
		DBApp.createBitMapIndex("gt","d");
		DBApp.createBitMapIndex("gt","e");
		DBApp.createBitMapIndex("gt","f");
		DBApp.createBitMapIndex("gt","g");
		DBApp.createBitMapIndex("gt","h");
		tableTrace1 = DBApp.getFullTrace("gt");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ya389", cols2);
		String tableTrace2 = DBApp.getFullTrace("ya389");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ya389","a");
		DBApp.createBitMapIndex("ya389","b");
		DBApp.createBitMapIndex("ya389","c");
		DBApp.createBitMapIndex("ya389","d");
		DBApp.createBitMapIndex("ya389","e");
		DBApp.createBitMapIndex("ya389","f");
		DBApp.createBitMapIndex("ya389","g");
		DBApp.createBitMapIndex("ya389","h");
		DBApp.createBitMapIndex("ya389","i");
		DBApp.createBitMapIndex("ya389","j");
		DBApp.createBitMapIndex("ya389","k");
		DBApp.createBitMapIndex("ya389","l");
		DBApp.createBitMapIndex("ya389","m");
		DBApp.createBitMapIndex("ya389","n");
		DBApp.createBitMapIndex("ya389","o");
		DBApp.createBitMapIndex("ya389","p");
		DBApp.createBitMapIndex("ya389","q");
		DBApp.createBitMapIndex("ya389","r");
		tableTrace2 = DBApp.getFullTrace("ya389");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o9268", cols0);
		String tableTrace0 = DBApp.getFullTrace("o9268");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("o9268","a");
		DBApp.createBitMapIndex("o9268","b");
		DBApp.createBitMapIndex("o9268","c");
		DBApp.createBitMapIndex("o9268","d");
		DBApp.createBitMapIndex("o9268","e");
		DBApp.createBitMapIndex("o9268","f");
		DBApp.createBitMapIndex("o9268","g");
		tableTrace0 = DBApp.getFullTrace("o9268");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wocm8", cols0);
		String tableTrace0 = DBApp.getFullTrace("wocm8");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("wocm8","a");
		DBApp.createBitMapIndex("wocm8","b");
		DBApp.createBitMapIndex("wocm8","c");
		DBApp.createBitMapIndex("wocm8","d");
		DBApp.createBitMapIndex("wocm8","e");
		DBApp.createBitMapIndex("wocm8","f");
		DBApp.createBitMapIndex("wocm8","g");
		tableTrace0 = DBApp.getFullTrace("wocm8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z3w", cols1);
		String tableTrace1 = DBApp.getFullTrace("z3w");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("z3w","a");
		DBApp.createBitMapIndex("z3w","b");
		DBApp.createBitMapIndex("z3w","c");
		DBApp.createBitMapIndex("z3w","d");
		DBApp.createBitMapIndex("z3w","e");
		DBApp.createBitMapIndex("z3w","f");
		DBApp.createBitMapIndex("z3w","g");
		tableTrace1 = DBApp.getFullTrace("z3w");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("gh98", cols2);
		String tableTrace2 = DBApp.getFullTrace("gh98");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("gh98","a");
		DBApp.createBitMapIndex("gh98","b");
		DBApp.createBitMapIndex("gh98","c");
		DBApp.createBitMapIndex("gh98","d");
		DBApp.createBitMapIndex("gh98","e");
		DBApp.createBitMapIndex("gh98","f");
		DBApp.createBitMapIndex("gh98","g");
		DBApp.createBitMapIndex("gh98","h");
		DBApp.createBitMapIndex("gh98","i");
		DBApp.createBitMapIndex("gh98","j");
		DBApp.createBitMapIndex("gh98","k");
		DBApp.createBitMapIndex("gh98","l");
		DBApp.createBitMapIndex("gh98","m");
		DBApp.createBitMapIndex("gh98","n");
		DBApp.createBitMapIndex("gh98","o");
		DBApp.createBitMapIndex("gh98","p");
		DBApp.createBitMapIndex("gh98","q");
		DBApp.createBitMapIndex("gh98","r");
		DBApp.createBitMapIndex("gh98","s");
		DBApp.createBitMapIndex("gh98","t");
		tableTrace2 = DBApp.getFullTrace("gh98");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("d2t8", cols3);
		String tableTrace3 = DBApp.getFullTrace("d2t8");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("d2t8","a");
		DBApp.createBitMapIndex("d2t8","b");
		DBApp.createBitMapIndex("d2t8","c");
		DBApp.createBitMapIndex("d2t8","d");
		DBApp.createBitMapIndex("d2t8","e");
		DBApp.createBitMapIndex("d2t8","f");
		DBApp.createBitMapIndex("d2t8","g");
		DBApp.createBitMapIndex("d2t8","h");
		DBApp.createBitMapIndex("d2t8","i");
		DBApp.createBitMapIndex("d2t8","j");
		DBApp.createBitMapIndex("d2t8","k");
		DBApp.createBitMapIndex("d2t8","l");
		DBApp.createBitMapIndex("d2t8","m");
		DBApp.createBitMapIndex("d2t8","n");
		DBApp.createBitMapIndex("d2t8","o");
		DBApp.createBitMapIndex("d2t8","p");
		DBApp.createBitMapIndex("d2t8","q");
		DBApp.createBitMapIndex("d2t8","r");
		DBApp.createBitMapIndex("d2t8","s");
		tableTrace3 = DBApp.getFullTrace("d2t8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ug7", cols4);
		String tableTrace4 = DBApp.getFullTrace("ug7");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("ug7","a");
		DBApp.createBitMapIndex("ug7","b");
		DBApp.createBitMapIndex("ug7","c");
		DBApp.createBitMapIndex("ug7","d");
		DBApp.createBitMapIndex("ug7","e");
		DBApp.createBitMapIndex("ug7","f");
		DBApp.createBitMapIndex("ug7","g");
		tableTrace4 = DBApp.getFullTrace("ug7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v2v4", cols0);
		String tableTrace0 = DBApp.getFullTrace("v2v4");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("v2v4","a");
		DBApp.createBitMapIndex("v2v4","b");
		DBApp.createBitMapIndex("v2v4","c");
		DBApp.createBitMapIndex("v2v4","d");
		DBApp.createBitMapIndex("v2v4","e");
		DBApp.createBitMapIndex("v2v4","f");
		DBApp.createBitMapIndex("v2v4","g");
		tableTrace0 = DBApp.getFullTrace("v2v4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("t6", cols1);
		String tableTrace1 = DBApp.getFullTrace("t6");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("t6","a");
		DBApp.createBitMapIndex("t6","b");
		DBApp.createBitMapIndex("t6","c");
		DBApp.createBitMapIndex("t6","d");
		DBApp.createBitMapIndex("t6","e");
		DBApp.createBitMapIndex("t6","f");
		DBApp.createBitMapIndex("t6","g");
		DBApp.createBitMapIndex("t6","h");
		tableTrace1 = DBApp.getFullTrace("t6");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("zqvrx", cols2);
		String tableTrace2 = DBApp.getFullTrace("zqvrx");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("zqvrx","a");
		DBApp.createBitMapIndex("zqvrx","b");
		DBApp.createBitMapIndex("zqvrx","c");
		DBApp.createBitMapIndex("zqvrx","d");
		DBApp.createBitMapIndex("zqvrx","e");
		DBApp.createBitMapIndex("zqvrx","f");
		DBApp.createBitMapIndex("zqvrx","g");
		DBApp.createBitMapIndex("zqvrx","h");
		DBApp.createBitMapIndex("zqvrx","i");
		DBApp.createBitMapIndex("zqvrx","j");
		DBApp.createBitMapIndex("zqvrx","k");
		DBApp.createBitMapIndex("zqvrx","l");
		DBApp.createBitMapIndex("zqvrx","m");
		DBApp.createBitMapIndex("zqvrx","n");
		DBApp.createBitMapIndex("zqvrx","o");
		tableTrace2 = DBApp.getFullTrace("zqvrx");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("eaa", cols3);
		String tableTrace3 = DBApp.getFullTrace("eaa");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("eaa","a");
		DBApp.createBitMapIndex("eaa","b");
		DBApp.createBitMapIndex("eaa","c");
		DBApp.createBitMapIndex("eaa","d");
		DBApp.createBitMapIndex("eaa","e");
		DBApp.createBitMapIndex("eaa","f");
		DBApp.createBitMapIndex("eaa","g");
		DBApp.createBitMapIndex("eaa","h");
		DBApp.createBitMapIndex("eaa","i");
		DBApp.createBitMapIndex("eaa","j");
		DBApp.createBitMapIndex("eaa","k");
		tableTrace3 = DBApp.getFullTrace("eaa");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vt", cols4);
		String tableTrace4 = DBApp.getFullTrace("vt");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("vt","a");
		DBApp.createBitMapIndex("vt","b");
		DBApp.createBitMapIndex("vt","c");
		DBApp.createBitMapIndex("vt","d");
		DBApp.createBitMapIndex("vt","e");
		DBApp.createBitMapIndex("vt","f");
		DBApp.createBitMapIndex("vt","g");
		DBApp.createBitMapIndex("vt","h");
		DBApp.createBitMapIndex("vt","i");
		DBApp.createBitMapIndex("vt","j");
		DBApp.createBitMapIndex("vt","k");
		DBApp.createBitMapIndex("vt","l");
		DBApp.createBitMapIndex("vt","m");
		DBApp.createBitMapIndex("vt","n");
		DBApp.createBitMapIndex("vt","o");
		DBApp.createBitMapIndex("vt","p");
		DBApp.createBitMapIndex("vt","q");
		DBApp.createBitMapIndex("vt","r");
		DBApp.createBitMapIndex("vt","s");
		DBApp.createBitMapIndex("vt","t");
		tableTrace4 = DBApp.getFullTrace("vt");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y3o", cols0);
		String tableTrace0 = DBApp.getFullTrace("y3o");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("y3o","a");
		DBApp.createBitMapIndex("y3o","b");
		DBApp.createBitMapIndex("y3o","c");
		DBApp.createBitMapIndex("y3o","d");
		DBApp.createBitMapIndex("y3o","e");
		DBApp.createBitMapIndex("y3o","f");
		DBApp.createBitMapIndex("y3o","g");
		DBApp.createBitMapIndex("y3o","h");
		DBApp.createBitMapIndex("y3o","i");
		DBApp.createBitMapIndex("y3o","j");
		tableTrace0 = DBApp.getFullTrace("y3o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("yf80x", cols1);
		String tableTrace1 = DBApp.getFullTrace("yf80x");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("yf80x","a");
		DBApp.createBitMapIndex("yf80x","b");
		DBApp.createBitMapIndex("yf80x","c");
		DBApp.createBitMapIndex("yf80x","d");
		DBApp.createBitMapIndex("yf80x","e");
		DBApp.createBitMapIndex("yf80x","f");
		DBApp.createBitMapIndex("yf80x","g");
		DBApp.createBitMapIndex("yf80x","h");
		DBApp.createBitMapIndex("yf80x","i");
		DBApp.createBitMapIndex("yf80x","j");
		DBApp.createBitMapIndex("yf80x","k");
		DBApp.createBitMapIndex("yf80x","l");
		DBApp.createBitMapIndex("yf80x","m");
		DBApp.createBitMapIndex("yf80x","n");
		DBApp.createBitMapIndex("yf80x","o");
		DBApp.createBitMapIndex("yf80x","p");
		DBApp.createBitMapIndex("yf80x","q");
		DBApp.createBitMapIndex("yf80x","r");
		DBApp.createBitMapIndex("yf80x","s");
		tableTrace1 = DBApp.getFullTrace("yf80x");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r91", cols0);
		String tableTrace0 = DBApp.getFullTrace("r91");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("r91","a");
		DBApp.createBitMapIndex("r91","b");
		DBApp.createBitMapIndex("r91","c");
		DBApp.createBitMapIndex("r91","d");
		DBApp.createBitMapIndex("r91","e");
		DBApp.createBitMapIndex("r91","f");
		DBApp.createBitMapIndex("r91","g");
		tableTrace0 = DBApp.getFullTrace("r91");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("o7", cols1);
		String tableTrace1 = DBApp.getFullTrace("o7");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("o7","a");
		DBApp.createBitMapIndex("o7","b");
		DBApp.createBitMapIndex("o7","c");
		DBApp.createBitMapIndex("o7","d");
		DBApp.createBitMapIndex("o7","e");
		DBApp.createBitMapIndex("o7","f");
		DBApp.createBitMapIndex("o7","g");
		DBApp.createBitMapIndex("o7","h");
		DBApp.createBitMapIndex("o7","i");
		DBApp.createBitMapIndex("o7","j");
		DBApp.createBitMapIndex("o7","k");
		DBApp.createBitMapIndex("o7","l");
		DBApp.createBitMapIndex("o7","m");
		DBApp.createBitMapIndex("o7","n");
		DBApp.createBitMapIndex("o7","o");
		DBApp.createBitMapIndex("o7","p");
		tableTrace1 = DBApp.getFullTrace("o7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("df4", cols2);
		String tableTrace2 = DBApp.getFullTrace("df4");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("df4","a");
		DBApp.createBitMapIndex("df4","b");
		DBApp.createBitMapIndex("df4","c");
		DBApp.createBitMapIndex("df4","d");
		DBApp.createBitMapIndex("df4","e");
		DBApp.createBitMapIndex("df4","f");
		DBApp.createBitMapIndex("df4","g");
		tableTrace2 = DBApp.getFullTrace("df4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("p2", cols3);
		String tableTrace3 = DBApp.getFullTrace("p2");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("p2","a");
		DBApp.createBitMapIndex("p2","b");
		DBApp.createBitMapIndex("p2","c");
		DBApp.createBitMapIndex("p2","d");
		DBApp.createBitMapIndex("p2","e");
		DBApp.createBitMapIndex("p2","f");
		DBApp.createBitMapIndex("p2","g");
		DBApp.createBitMapIndex("p2","h");
		DBApp.createBitMapIndex("p2","i");
		DBApp.createBitMapIndex("p2","j");
		DBApp.createBitMapIndex("p2","k");
		DBApp.createBitMapIndex("p2","l");
		DBApp.createBitMapIndex("p2","m");
		DBApp.createBitMapIndex("p2","n");
		DBApp.createBitMapIndex("p2","o");
		DBApp.createBitMapIndex("p2","p");
		DBApp.createBitMapIndex("p2","q");
		tableTrace3 = DBApp.getFullTrace("p2");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hi", cols4);
		String tableTrace4 = DBApp.getFullTrace("hi");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("hi","a");
		DBApp.createBitMapIndex("hi","b");
		DBApp.createBitMapIndex("hi","c");
		DBApp.createBitMapIndex("hi","d");
		DBApp.createBitMapIndex("hi","e");
		DBApp.createBitMapIndex("hi","f");
		DBApp.createBitMapIndex("hi","g");
		tableTrace4 = DBApp.getFullTrace("hi");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("t9c5d", cols0);
		String tableTrace0 = DBApp.getFullTrace("t9c5d");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("t9c5d","a");
		DBApp.createBitMapIndex("t9c5d","b");
		DBApp.createBitMapIndex("t9c5d","c");
		DBApp.createBitMapIndex("t9c5d","d");
		DBApp.createBitMapIndex("t9c5d","e");
		DBApp.createBitMapIndex("t9c5d","f");
		DBApp.createBitMapIndex("t9c5d","g");
		DBApp.createBitMapIndex("t9c5d","h");
		DBApp.createBitMapIndex("t9c5d","i");
		DBApp.createBitMapIndex("t9c5d","j");
		tableTrace0 = DBApp.getFullTrace("t9c5d");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v40", cols1);
		String tableTrace1 = DBApp.getFullTrace("v40");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("v40","a");
		DBApp.createBitMapIndex("v40","b");
		DBApp.createBitMapIndex("v40","c");
		DBApp.createBitMapIndex("v40","d");
		DBApp.createBitMapIndex("v40","e");
		DBApp.createBitMapIndex("v40","f");
		DBApp.createBitMapIndex("v40","g");
		DBApp.createBitMapIndex("v40","h");
		DBApp.createBitMapIndex("v40","i");
		DBApp.createBitMapIndex("v40","j");
		DBApp.createBitMapIndex("v40","k");
		DBApp.createBitMapIndex("v40","l");
		DBApp.createBitMapIndex("v40","m");
		DBApp.createBitMapIndex("v40","n");
		DBApp.createBitMapIndex("v40","o");
		tableTrace1 = DBApp.getFullTrace("v40");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x2g", cols2);
		String tableTrace2 = DBApp.getFullTrace("x2g");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("x2g","a");
		DBApp.createBitMapIndex("x2g","b");
		DBApp.createBitMapIndex("x2g","c");
		DBApp.createBitMapIndex("x2g","d");
		DBApp.createBitMapIndex("x2g","e");
		DBApp.createBitMapIndex("x2g","f");
		DBApp.createBitMapIndex("x2g","g");
		tableTrace2 = DBApp.getFullTrace("x2g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pw7", cols3);
		String tableTrace3 = DBApp.getFullTrace("pw7");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("pw7","a");
		DBApp.createBitMapIndex("pw7","b");
		DBApp.createBitMapIndex("pw7","c");
		DBApp.createBitMapIndex("pw7","d");
		DBApp.createBitMapIndex("pw7","e");
		DBApp.createBitMapIndex("pw7","f");
		DBApp.createBitMapIndex("pw7","g");
		tableTrace3 = DBApp.getFullTrace("pw7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jzd", cols4);
		String tableTrace4 = DBApp.getFullTrace("jzd");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("jzd","a");
		DBApp.createBitMapIndex("jzd","b");
		DBApp.createBitMapIndex("jzd","c");
		DBApp.createBitMapIndex("jzd","d");
		DBApp.createBitMapIndex("jzd","e");
		DBApp.createBitMapIndex("jzd","f");
		DBApp.createBitMapIndex("jzd","g");
		tableTrace4 = DBApp.getFullTrace("jzd");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ar4", cols0);
		String tableTrace0 = DBApp.getFullTrace("ar4");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ar4","a");
		DBApp.createBitMapIndex("ar4","b");
		DBApp.createBitMapIndex("ar4","c");
		DBApp.createBitMapIndex("ar4","d");
		DBApp.createBitMapIndex("ar4","e");
		DBApp.createBitMapIndex("ar4","f");
		DBApp.createBitMapIndex("ar4","g");
		tableTrace0 = DBApp.getFullTrace("ar4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t5cq", cols1);
		String tableTrace1 = DBApp.getFullTrace("t5cq");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("t5cq","a");
		DBApp.createBitMapIndex("t5cq","b");
		DBApp.createBitMapIndex("t5cq","c");
		DBApp.createBitMapIndex("t5cq","d");
		DBApp.createBitMapIndex("t5cq","e");
		DBApp.createBitMapIndex("t5cq","f");
		DBApp.createBitMapIndex("t5cq","g");
		tableTrace1 = DBApp.getFullTrace("t5cq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("lah", cols2);
		String tableTrace2 = DBApp.getFullTrace("lah");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("lah","a");
		DBApp.createBitMapIndex("lah","b");
		DBApp.createBitMapIndex("lah","c");
		DBApp.createBitMapIndex("lah","d");
		DBApp.createBitMapIndex("lah","e");
		DBApp.createBitMapIndex("lah","f");
		DBApp.createBitMapIndex("lah","g");
		DBApp.createBitMapIndex("lah","h");
		DBApp.createBitMapIndex("lah","i");
		tableTrace2 = DBApp.getFullTrace("lah");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f5", cols3);
		String tableTrace3 = DBApp.getFullTrace("f5");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("f5","a");
		DBApp.createBitMapIndex("f5","b");
		DBApp.createBitMapIndex("f5","c");
		DBApp.createBitMapIndex("f5","d");
		DBApp.createBitMapIndex("f5","e");
		DBApp.createBitMapIndex("f5","f");
		DBApp.createBitMapIndex("f5","g");
		tableTrace3 = DBApp.getFullTrace("f5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a95", cols4);
		String tableTrace4 = DBApp.getFullTrace("a95");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("a95","a");
		DBApp.createBitMapIndex("a95","b");
		DBApp.createBitMapIndex("a95","c");
		DBApp.createBitMapIndex("a95","d");
		DBApp.createBitMapIndex("a95","e");
		DBApp.createBitMapIndex("a95","f");
		DBApp.createBitMapIndex("a95","g");
		tableTrace4 = DBApp.getFullTrace("a95");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pszj", cols0);
		String tableTrace0 = DBApp.getFullTrace("pszj");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("pszj","a");
		DBApp.createBitMapIndex("pszj","b");
		DBApp.createBitMapIndex("pszj","c");
		DBApp.createBitMapIndex("pszj","d");
		DBApp.createBitMapIndex("pszj","e");
		DBApp.createBitMapIndex("pszj","f");
		DBApp.createBitMapIndex("pszj","g");
		DBApp.createBitMapIndex("pszj","h");
		tableTrace0 = DBApp.getFullTrace("pszj");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("a9b", cols1);
		String tableTrace1 = DBApp.getFullTrace("a9b");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("a9b","a");
		DBApp.createBitMapIndex("a9b","b");
		DBApp.createBitMapIndex("a9b","c");
		DBApp.createBitMapIndex("a9b","d");
		DBApp.createBitMapIndex("a9b","e");
		DBApp.createBitMapIndex("a9b","f");
		DBApp.createBitMapIndex("a9b","g");
		DBApp.createBitMapIndex("a9b","h");
		DBApp.createBitMapIndex("a9b","i");
		DBApp.createBitMapIndex("a9b","j");
		DBApp.createBitMapIndex("a9b","k");
		DBApp.createBitMapIndex("a9b","l");
		DBApp.createBitMapIndex("a9b","m");
		DBApp.createBitMapIndex("a9b","n");
		DBApp.createBitMapIndex("a9b","o");
		DBApp.createBitMapIndex("a9b","p");
		DBApp.createBitMapIndex("a9b","q");
		DBApp.createBitMapIndex("a9b","r");
		tableTrace1 = DBApp.getFullTrace("a9b");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("v6e", cols2);
		String tableTrace2 = DBApp.getFullTrace("v6e");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("v6e","a");
		DBApp.createBitMapIndex("v6e","b");
		DBApp.createBitMapIndex("v6e","c");
		DBApp.createBitMapIndex("v6e","d");
		DBApp.createBitMapIndex("v6e","e");
		DBApp.createBitMapIndex("v6e","f");
		DBApp.createBitMapIndex("v6e","g");
		DBApp.createBitMapIndex("v6e","h");
		DBApp.createBitMapIndex("v6e","i");
		DBApp.createBitMapIndex("v6e","j");
		DBApp.createBitMapIndex("v6e","k");
		DBApp.createBitMapIndex("v6e","l");
		DBApp.createBitMapIndex("v6e","m");
		DBApp.createBitMapIndex("v6e","n");
		DBApp.createBitMapIndex("v6e","o");
		DBApp.createBitMapIndex("v6e","p");
		DBApp.createBitMapIndex("v6e","q");
		DBApp.createBitMapIndex("v6e","r");
		DBApp.createBitMapIndex("v6e","s");
		DBApp.createBitMapIndex("v6e","t");
		tableTrace2 = DBApp.getFullTrace("v6e");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(823);
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