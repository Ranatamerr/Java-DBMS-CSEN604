package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_02
{

	@Test(timeout = 1000000)
	public void TableValidationAffected_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u26", cols0);
		String [][] records_u26 = new String[456][cols0.length];
		for(int i=0;i<456;i++)
		{
			records_u26[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u26[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u26", records_u26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		//record453: [a453, b1, c0, d1, e3, f3, g5]
		//record454: [a454, b0, c1, d2, e4, f4, g6]
		//record455: [a455, b1, c2, d3, e0, f5, g0]
		int pageCount0 = (int)Math.ceil(456.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u26 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u26"+ File.separator+i+".db");
				dir_u26.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("u26");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lvs", cols1);
		String [][] records_lvs = new String[160][cols1.length];
		for(int i=0;i<160;i++)
		{
			records_lvs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lvs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lvs", records_lvs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3]
		int pageCount1 = (int)Math.ceil(160.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lvs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lvs"+ File.separator+i+".db");
				dir_lvs.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("lvs");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("cv25", cols2);
		String [][] records_cv25 = new String[131][cols2.length];
		for(int i=0;i<131;i++)
		{
			records_cv25[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cv25[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cv25", records_cv25[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6, p14, q7, r0, s12]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7, p15, q8, r1, s13]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2, o8, p0, q9, r2, s14]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1, q10, r3, s15]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0, n4, o10, p2, q11, r4, s16]
		int pageCount2 = (int)Math.ceil(131.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cv25 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cv25"+ File.separator+i+".db");
				dir_cv25.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("cv25");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ze9pv", cols3);
		String [][] records_ze9pv = new String[493][cols3.length];
		for(int i=0;i<493;i++)
		{
			records_ze9pv[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ze9pv[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ze9pv", records_ze9pv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8]
		int pageCount3 = (int)Math.ceil(493.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ze9pv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ze9pv"+ File.separator+i+".db");
				dir_ze9pv.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("ze9pv");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uc", cols0);
		String [][] records_uc = new String[341][cols0.length];
		for(int i=0;i<341;i++)
		{
			records_uc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uc", records_uc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record336: [a336, b0, c0, d0, e1, f0, g0]
		//record337: [a337, b1, c1, d1, e2, f1, g1]
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		int pageCount0 = (int)Math.ceil(341.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uc"+ File.separator+i+".db");
				dir_uc.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("uc");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g4gtv", cols1);
		String [][] records_g4gtv = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_g4gtv[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g4gtv[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g4gtv", records_g4gtv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g4gtv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g4gtv"+ File.separator+i+".db");
				dir_g4gtv.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("g4gtv");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("g4", cols2);
		String [][] records_g4 = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			records_g4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g4", records_g4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9]
		int pageCount2 = (int)Math.ceil(66.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g4"+ File.separator+i+".db");
				dir_g4.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("g4");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("y51", cols3);
		String [][] records_y51 = new String[199][cols3.length];
		for(int i=0;i<199;i++)
		{
			records_y51[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y51[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y51", records_y51[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0]
		int pageCount3 = (int)Math.ceil(199.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y51 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y51"+ File.separator+i+".db");
				dir_y51.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("y51");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("bf", cols4);
		String [][] records_bf = new String[486][cols4.length];
		for(int i=0;i<486;i++)
		{
			records_bf[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bf[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bf", records_bf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record481: [a481, b1, c1, d1, e1, f1, g5, h1, i4, j1, k8, l1, m0, n5, o1, p1, q5, r13]
		//record482: [a482, b0, c2, d2, e2, f2, g6, h2, i5, j2, k9, l2, m1, n6, o2, p2, q6, r14]
		//record483: [a483, b1, c0, d3, e3, f3, g0, h3, i6, j3, k10, l3, m2, n7, o3, p3, q7, r15]
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3, n8, o4, p4, q8, r16]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5, m4, n9, o5, p5, q9, r17]
		int pageCount4 = (int)Math.ceil(486.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bf"+ File.separator+i+".db");
				dir_bf.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("bf");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vk8", cols0);
		String [][] records_vk8 = new String[318][cols0.length];
		for(int i=0;i<318;i++)
		{
			records_vk8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vk8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vk8", records_vk8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7]
		int pageCount0 = (int)Math.ceil(318.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vk8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vk8"+ File.separator+i+".db");
				dir_vk8.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("vk8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jcs", cols1);
		String [][] records_jcs = new String[352][cols1.length];
		for(int i=0;i<352;i++)
		{
			records_jcs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jcs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jcs", records_jcs[i]);
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
		int pageCount1 = (int)Math.ceil(352.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jcs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jcs"+ File.separator+i+".db");
				dir_jcs.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("jcs");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q9i", cols2);
		String [][] records_q9i = new String[383][cols2.length];
		for(int i=0;i<383;i++)
		{
			records_q9i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q9i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q9i", records_q9i[i]);
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
		int pageCount2 = (int)Math.ceil(383.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q9i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q9i"+ File.separator+i+".db");
				dir_q9i.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("q9i");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w1v", cols0);
		String [][] records_w1v = new String[168][cols0.length];
		for(int i=0;i<168;i++)
		{
			records_w1v[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w1v[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w1v", records_w1v[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record163: [a163, b1, c1, d3, e3, f1, g2]
		//record164: [a164, b0, c2, d0, e4, f2, g3]
		//record165: [a165, b1, c0, d1, e0, f3, g4]
		//record166: [a166, b0, c1, d2, e1, f4, g5]
		//record167: [a167, b1, c2, d3, e2, f5, g6]
		int pageCount0 = (int)Math.ceil(168.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w1v = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w1v"+ File.separator+i+".db");
				dir_w1v.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("w1v");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ama", cols1);
		String [][] records_ama = new String[424][cols1.length];
		for(int i=0;i<424;i++)
		{
			records_ama[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ama[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ama", records_ama[i]);
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
		int pageCount1 = (int)Math.ceil(424.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ama = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ama"+ File.separator+i+".db");
				dir_ama.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("ama");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f7ok", cols2);
		String [][] records_f7ok = new String[443][cols2.length];
		for(int i=0;i<443;i++)
		{
			records_f7ok[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f7ok[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f7ok", records_f7ok[i]);
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
		int pageCount2 = (int)Math.ceil(443.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f7ok = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f7ok"+ File.separator+i+".db");
				dir_f7ok.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("f7ok");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("m6f04", cols3);
		String [][] records_m6f04 = new String[248][cols3.length];
		for(int i=0;i<248;i++)
		{
			records_m6f04[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m6f04[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m6f04", records_m6f04[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9, n5, o3, p3, q5]
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2, l4, m10, n6, o4, p4, q6]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11, n7, o5, p5, q7]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8, o6, p6, q8]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0, n9, o7, p7, q9]
		int pageCount3 = (int)Math.ceil(248.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m6f04 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m6f04"+ File.separator+i+".db");
				dir_m6f04.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("m6f04");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("g3", cols0);
		String [][] records_g3 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			records_g3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g3", records_g3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g3"+ File.separator+i+".db");
				dir_g3.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("g3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("wz6t", cols0);
		String [][] records_wz6t = new String[104][cols0.length];
		for(int i=0;i<104;i++)
		{
			records_wz6t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wz6t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wz6t", records_wz6t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15, r10]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3, o11, p5, q16, r11]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4, o12, p6, q0, r12]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5, o13, p7, q1, r13]
		int pageCount0 = (int)Math.ceil(104.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wz6t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wz6t"+ File.separator+i+".db");
				dir_wz6t.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("wz6t");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("szc80", cols1);
		String [][] records_szc80 = new String[122][cols1.length];
		for(int i=0;i<122;i++)
		{
			records_szc80[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_szc80[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("szc80", records_szc80[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record117: [a117, b1, c0, d1, e2, f3, g5]
		//record118: [a118, b0, c1, d2, e3, f4, g6]
		//record119: [a119, b1, c2, d3, e4, f5, g0]
		//record120: [a120, b0, c0, d0, e0, f0, g1]
		//record121: [a121, b1, c1, d1, e1, f1, g2]
		int pageCount1 = (int)Math.ceil(122.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_szc80 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "szc80"+ File.separator+i+".db");
				dir_szc80.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("szc80");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ud6s", cols0);
		String [][] records_ud6s = new String[324][cols0.length];
		for(int i=0;i<324;i++)
		{
			records_ud6s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ud6s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ud6s", records_ud6s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13, s15, t19]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0, q14, r14, s16, t0]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13, o6, p1, q15, r15, s17, t1]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0, o7, p2, q16, r16, s18, t2]
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1, o8, p3, q0, r17, s0, t3]
		int pageCount0 = (int)Math.ceil(324.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ud6s = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ud6s"+ File.separator+i+".db");
				dir_ud6s.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("ud6s");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a1hw4", cols1);
		String [][] records_a1hw4 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			records_a1hw4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a1hw4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a1hw4", records_a1hw4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		//record89: [a89, b1, c2, d1, e4, f5, g5]
		int pageCount1 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a1hw4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a1hw4"+ File.separator+i+".db");
				dir_a1hw4.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("a1hw4");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s28", cols2);
		String [][] records_s28 = new String[382][cols2.length];
		for(int i=0;i<382;i++)
		{
			records_s28[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s28[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s28", records_s28[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record377: [a377, b1, c2, d1, e2, f5, g6]
		//record378: [a378, b0, c0, d2, e3, f0, g0]
		//record379: [a379, b1, c1, d3, e4, f1, g1]
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		int pageCount2 = (int)Math.ceil(382.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s28 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s28"+ File.separator+i+".db");
				dir_s28.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("s28");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("axp", cols3);
		String [][] records_axp = new String[255][cols3.length];
		for(int i=0;i<255;i++)
		{
			records_axp[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_axp[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("axp", records_axp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12, o10]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0, o12]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6, n1, o13]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14]
		int pageCount3 = (int)Math.ceil(255.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_axp = new File(FileManager.directory.getAbsolutePath()+ File.separator + "axp"+ File.separator+i+".db");
				dir_axp.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("axp");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("at1", cols0);
		String [][] records_at1 = new String[415][cols0.length];
		for(int i=0;i<415;i++)
		{
			records_at1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_at1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("at1", records_at1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4]
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		int pageCount0 = (int)Math.ceil(415.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_at1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "at1"+ File.separator+i+".db");
				dir_at1.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("at1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("pfo", cols1);
		String [][] records_pfo = new String[158][cols1.length];
		for(int i=0;i<158;i++)
		{
			records_pfo[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pfo[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pfo", records_pfo[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10, l9, m10, n13, o3, p9, q0, r9, s1, t13]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0, l10, m11, n0, o4, p10, q1, r10, s2, t14]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1, l11, m12, n1, o5, p11, q2, r11, s3, t15]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6, p12, q3, r12, s4, t16]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7, p13, q4, r13, s5, t17]
		int pageCount1 = (int)Math.ceil(158.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pfo = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pfo"+ File.separator+i+".db");
				dir_pfo.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("pfo");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zp9", cols0);
		String [][] records_zp9 = new String[467][cols0.length];
		for(int i=0;i<467;i++)
		{
			records_zp9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zp9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zp9", records_zp9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record462: [a462, b0, c0, d2, e2, f0, g0]
		//record463: [a463, b1, c1, d3, e3, f1, g1]
		//record464: [a464, b0, c2, d0, e4, f2, g2]
		//record465: [a465, b1, c0, d1, e0, f3, g3]
		//record466: [a466, b0, c1, d2, e1, f4, g4]
		int pageCount0 = (int)Math.ceil(467.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zp9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zp9"+ File.separator+i+".db");
				dir_zp9.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("zp9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("jw6", cols1);
		String [][] records_jw6 = new String[113][cols1.length];
		for(int i=0;i<113;i++)
		{
			records_jw6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jw6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jw6", records_jw6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4]
		int pageCount1 = (int)Math.ceil(113.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jw6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jw6"+ File.separator+i+".db");
				dir_jw6.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("jw6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k11", cols2);
		String [][] records_k11 = new String[347][cols2.length];
		for(int i=0;i<347;i++)
		{
			records_k11[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k11[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k11", records_k11[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record342: [a342, b0, c0, d2, e2, f0, g6]
		//record343: [a343, b1, c1, d3, e3, f1, g0]
		//record344: [a344, b0, c2, d0, e4, f2, g1]
		//record345: [a345, b1, c0, d1, e0, f3, g2]
		//record346: [a346, b0, c1, d2, e1, f4, g3]
		int pageCount2 = (int)Math.ceil(347.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k11 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k11"+ File.separator+i+".db");
				dir_k11.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("k11");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l08", cols0);
		String [][] records_l08 = new String[259][cols0.length];
		for(int i=0;i<259;i++)
		{
			records_l08[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l08[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l08", records_l08[i]);
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
		int pageCount0 = (int)Math.ceil(259.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l08 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l08"+ File.separator+i+".db");
				dir_l08.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("l08");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("a52", cols1);
		String [][] records_a52 = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_a52[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a52[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a52", records_a52[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		int pageCount1 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a52 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a52"+ File.separator+i+".db");
				dir_a52.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("a52");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q3m54", cols2);
		String [][] records_q3m54 = new String[326][cols2.length];
		for(int i=0;i<326;i++)
		{
			records_q3m54[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q3m54[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q3m54", records_q3m54[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record321: [a321, b1, c0, d1, e1, f3, g6]
		//record322: [a322, b0, c1, d2, e2, f4, g0]
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		//record324: [a324, b0, c0, d0, e4, f0, g2]
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		int pageCount2 = (int)Math.ceil(326.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q3m54 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q3m54"+ File.separator+i+".db");
				dir_q3m54.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("q3m54");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y36", cols3);
		String [][] records_y36 = new String[185][cols3.length];
		for(int i=0;i<185;i++)
		{
			records_y36[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y36[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y36", records_y36[i]);
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
		int pageCount3 = (int)Math.ceil(185.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y36 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y36"+ File.separator+i+".db");
				dir_y36.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("y36");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("tcb", cols0);
		String [][] records_tcb = new String[335][cols0.length];
		for(int i=0;i<335;i++)
		{
			records_tcb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tcb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tcb", records_tcb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8, o0]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9, o1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10, o2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12, o4]
		int pageCount0 = (int)Math.ceil(335.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tcb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tcb"+ File.separator+i+".db");
				dir_tcb.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("tcb");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y5", cols0);
		String [][] records_y5 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			records_y5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y5", records_y5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7]
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y5"+ File.separator+i+".db");
				dir_y5.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("y5");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xa1j1", cols0);
		String [][] records_xa1j1 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			records_xa1j1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xa1j1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xa1j1", records_xa1j1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0]
		int pageCount0 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xa1j1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xa1j1"+ File.separator+i+".db");
				dir_xa1j1.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("xa1j1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dxt", cols1);
		String [][] records_dxt = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			records_dxt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dxt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dxt", records_dxt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record68: [a68, b0, c2, d0, e3, f2, g5]
		//record69: [a69, b1, c0, d1, e4, f3, g6]
		//record70: [a70, b0, c1, d2, e0, f4, g0]
		//record71: [a71, b1, c2, d3, e1, f5, g1]
		//record72: [a72, b0, c0, d0, e2, f0, g2]
		int pageCount1 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dxt = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dxt"+ File.separator+i+".db");
				dir_dxt.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("dxt");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("b74", cols2);
		String [][] records_b74 = new String[130][cols2.length];
		for(int i=0;i<130;i++)
		{
			records_b74[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b74[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b74", records_b74[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5, p13, q6, r17]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6, p14, q7, r0]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7, m10, n1, o7, p15, q8, r1]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8, m11, n2, o8, p0, q9, r2]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1, q10, r3]
		int pageCount2 = (int)Math.ceil(130.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b74 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b74"+ File.separator+i+".db");
				dir_b74.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("b74");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g3", cols0);
		String [][] records_g3 = new String[439][cols0.length];
		for(int i=0;i<439;i++)
		{
			records_g3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g3", records_g3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2]
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6]
		int pageCount0 = (int)Math.ceil(439.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g3"+ File.separator+i+".db");
				dir_g3.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("g3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j1b", cols1);
		String [][] records_j1b = new String[329][cols1.length];
		for(int i=0;i<329;i++)
		{
			records_j1b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j1b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j1b", records_j1b[i]);
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
		int pageCount1 = (int)Math.ceil(329.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j1b = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j1b"+ File.separator+i+".db");
				dir_j1b.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("j1b");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("is", cols2);
		String [][] records_is = new String[489][cols2.length];
		for(int i=0;i<489;i++)
		{
			records_is[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_is[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("is", records_is[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record484: [a484, b0, c1, d0, e4, f4, g1, h4, i7, j4, k0, l4, m3, n8, o4, p4, q8]
		//record485: [a485, b1, c2, d1, e0, f5, g2, h5, i8, j5, k1, l5, m4, n9, o5, p5, q9]
		//record486: [a486, b0, c0, d2, e1, f0, g3, h6, i0, j6, k2, l6, m5, n10, o6, p6, q10]
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7, p7, q11]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8, p8, q12]
		int pageCount2 = (int)Math.ceil(489.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_is = new File(FileManager.directory.getAbsolutePath()+ File.separator + "is"+ File.separator+i+".db");
				dir_is.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("is");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("hsf7", cols3);
		String [][] records_hsf7 = new String[195][cols3.length];
		for(int i=0;i<195;i++)
		{
			records_hsf7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hsf7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hsf7", records_hsf7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5]
		int pageCount3 = (int)Math.ceil(195.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hsf7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hsf7"+ File.separator+i+".db");
				dir_hsf7.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("hsf7");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u4", cols4);
		String [][] records_u4 = new String[393][cols4.length];
		for(int i=0;i<393;i++)
		{
			records_u4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u4", records_u4[i]);
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
		int pageCount4 = (int)Math.ceil(393.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u4"+ File.separator+i+".db");
				dir_u4.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("u4");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h31", cols0);
		String [][] records_h31 = new String[342][cols0.length];
		for(int i=0;i<342;i++)
		{
			records_h31[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h31[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h31", records_h31[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7, p1]
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3, n5, o11, p5]
		int pageCount0 = (int)Math.ceil(342.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h31 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h31"+ File.separator+i+".db");
				dir_h31.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("h31");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tb", cols1);
		String [][] records_tb = new String[203][cols1.length];
		for(int i=0;i<203;i++)
		{
			records_tb[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tb[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tb", records_tb[i]);
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
		int pageCount1 = (int)Math.ceil(203.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tb"+ File.separator+i+".db");
				dir_tb.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("tb");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ckc", cols2);
		String [][] records_ckc = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			records_ckc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ckc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ckc", records_ckc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13]
		int pageCount2 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ckc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ckc"+ File.separator+i+".db");
				dir_ckc.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("ckc");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("q2", cols3);
		String [][] records_q2 = new String[428][cols3.length];
		for(int i=0;i<428;i++)
		{
			records_q2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q2", records_q2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9]
		int pageCount3 = (int)Math.ceil(428.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q2"+ File.separator+i+".db");
				dir_q2.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("q2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("o5y", cols0);
		String [][] records_o5y = new String[362][cols0.length];
		for(int i=0;i<362;i++)
		{
			records_o5y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o5y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o5y", records_o5y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6, n7, o12, p5, q0, r15, s15, t17]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7, n8, o13, p6, q1, r16, s16, t18]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8, n9, o14, p7, q2, r17, s17, t19]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0, m9, n10, o0, p8, q3, r0, s18, t0]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1, m10, n11, o1, p9, q4, r1, s0, t1]
		int pageCount0 = (int)Math.ceil(362.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o5y = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o5y"+ File.separator+i+".db");
				dir_o5y.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("o5y");
		String tableTrace0 = DBApp.getLastTrace("o5y");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("jgs", cols1);
		String [][] records_jgs = new String[211][cols1.length];
		for(int i=0;i<211;i++)
		{
			records_jgs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jgs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jgs", records_jgs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1]
		int pageCount1 = (int)Math.ceil(211.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jgs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jgs"+ File.separator+i+".db");
				dir_jgs.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("jgs");
		String tableTrace1 = DBApp.getLastTrace("jgs");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n4", cols2);
		String [][] records_n4 = new String[140][cols2.length];
		for(int i=0;i<140;i++)
		{
			records_n4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n4", records_n4[i]);
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
		int pageCount2 = (int)Math.ceil(140.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n4"+ File.separator+i+".db");
				dir_n4.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("n4");
		String tableTrace2 = DBApp.getLastTrace("n4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("yhij", cols3);
		String [][] records_yhij = new String[230][cols3.length];
		for(int i=0;i<230;i++)
		{
			records_yhij[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_yhij[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("yhij", records_yhij[i]);
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
		int pageCount3 = (int)Math.ceil(230.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yhij = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yhij"+ File.separator+i+".db");
				dir_yhij.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("yhij");
		String tableTrace3 = DBApp.getLastTrace("yhij");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("gw", cols4);
		String [][] records_gw = new String[394][cols4.length];
		for(int i=0;i<394;i++)
		{
			records_gw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_gw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("gw", records_gw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12, n11, o14, p5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12, o0, p6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13, o1, p7]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2, p8]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1, o3, p9]
		int pageCount4 = (int)Math.ceil(394.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gw"+ File.separator+i+".db");
				dir_gw.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("gw");
		String tableTrace4 = DBApp.getLastTrace("gw");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lz6", cols0);
		String [][] records_lz6 = new String[489][cols0.length];
		for(int i=0;i<489;i++)
		{
			records_lz6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lz6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lz6", records_lz6[i]);
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
		int pageCount0 = (int)Math.ceil(489.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lz6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lz6"+ File.separator+i+".db");
				dir_lz6.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("lz6");
		String tableTrace0 = DBApp.getLastTrace("lz6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tmby", cols0);
		String [][] records_tmby = new String[414][cols0.length];
		for(int i=0;i<414;i++)
		{
			records_tmby[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tmby[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tmby", records_tmby[i]);
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
		int pageCount0 = (int)Math.ceil(414.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tmby = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tmby"+ File.separator+i+".db");
				dir_tmby.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("tmby");
		String tableTrace0 = DBApp.getLastTrace("tmby");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("o34", cols0);
		String [][] records_o34 = new String[101][cols0.length];
		for(int i=0;i<101;i++)
		{
			records_o34[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o34[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o34", records_o34[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6, p0, q11, r6, s1, t16]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7, p1, q12, r7, s2, t17]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8, p2, q13, r8, s3, t18]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14, r9, s4, t19]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2, o10, p4, q15, r10, s5, t0]
		int pageCount0 = (int)Math.ceil(101.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o34 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o34"+ File.separator+i+".db");
				dir_o34.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("o34");
		String tableTrace0 = DBApp.getLastTrace("o34");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("ky82e", cols1);
		String [][] records_ky82e = new String[349][cols1.length];
		for(int i=0;i<349;i++)
		{
			records_ky82e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ky82e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ky82e", records_ky82e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record344: [a344, b0, c2, d0, e4, f2, g1, h0, i2, j4, k3, l8, m6, n8, o14, p8]
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12]
		int pageCount1 = (int)Math.ceil(349.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ky82e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ky82e"+ File.separator+i+".db");
				dir_ky82e.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("ky82e");
		String tableTrace1 = DBApp.getLastTrace("ky82e");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("rdtty", cols2);
		String [][] records_rdtty = new String[159][cols2.length];
		for(int i=0;i<159;i++)
		{
			records_rdtty[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rdtty[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rdtty", records_rdtty[i]);
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
		int pageCount2 = (int)Math.ceil(159.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rdtty = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rdtty"+ File.separator+i+".db");
				dir_rdtty.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("rdtty");
		String tableTrace2 = DBApp.getLastTrace("rdtty");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("g3gc", cols0);
		String [][] records_g3gc = new String[237][cols0.length];
		for(int i=0;i<237;i++)
		{
			records_g3gc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g3gc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g3gc", records_g3gc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8, q11, r16]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9, q12, r17]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10, o9, p10, q13, r0]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10, p11, q14, r1]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11, p12, q15, r2]
		int pageCount0 = (int)Math.ceil(237.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g3gc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g3gc"+ File.separator+i+".db");
				dir_g3gc.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("g3gc");
		String tableTrace0 = DBApp.getLastTrace("g3gc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o23", cols0);
		String [][] records_o23 = new String[242][cols0.length];
		for(int i=0;i<242;i++)
		{
			records_o23[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o23[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o23", records_o23[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1]
		int pageCount0 = (int)Math.ceil(242.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o23 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o23"+ File.separator+i+".db");
				dir_o23.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("o23");
		String tableTrace0 = DBApp.getLastTrace("o23");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rm55", cols1);
		String [][] records_rm55 = new String[223][cols1.length];
		for(int i=0;i<223;i++)
		{
			records_rm55[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rm55[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rm55", records_rm55[i]);
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
		int pageCount1 = (int)Math.ceil(223.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rm55 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rm55"+ File.separator+i+".db");
				dir_rm55.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("rm55");
		String tableTrace1 = DBApp.getLastTrace("rm55");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("u91", cols2);
		String [][] records_u91 = new String[305][cols2.length];
		for(int i=0;i<305;i++)
		{
			records_u91[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u91[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u91", records_u91[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0, m1]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1, m2]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2, m3]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5]
		int pageCount2 = (int)Math.ceil(305.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u91 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u91"+ File.separator+i+".db");
				dir_u91.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("u91");
		String tableTrace2 = DBApp.getLastTrace("u91");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y32", cols3);
		String [][] records_y32 = new String[343][cols3.length];
		for(int i=0;i<343;i++)
		{
			records_y32[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y32[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y32", records_y32[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record338: [a338, b0, c2, d2, e3, f2, g2]
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		//record342: [a342, b0, c0, d2, e2, f0, g6]
		int pageCount3 = (int)Math.ceil(343.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y32 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y32"+ File.separator+i+".db");
				dir_y32.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("y32");
		String tableTrace3 = DBApp.getLastTrace("y32");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oz", cols4);
		String [][] records_oz = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			records_oz[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_oz[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("oz", records_oz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		int pageCount4 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_oz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "oz"+ File.separator+i+".db");
				dir_oz.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("oz");
		String tableTrace4 = DBApp.getLastTrace("oz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i18rj", cols0);
		String [][] records_i18rj = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_i18rj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i18rj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i18rj", records_i18rj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		int pageCount0 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i18rj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i18rj"+ File.separator+i+".db");
				dir_i18rj.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("i18rj");
		String tableTrace0 = DBApp.getLastTrace("i18rj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("l60", cols1);
		String [][] records_l60 = new String[320][cols1.length];
		for(int i=0;i<320;i++)
		{
			records_l60[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l60[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l60", records_l60[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7, o0, p11, q9, r9]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8, o1, p12, q10, r10]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13, q11, r11]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13]
		int pageCount1 = (int)Math.ceil(320.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l60 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l60"+ File.separator+i+".db");
				dir_l60.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("l60");
		String tableTrace1 = DBApp.getLastTrace("l60");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("en3", cols0);
		String [][] records_en3 = new String[408][cols0.length];
		for(int i=0;i<408;i++)
		{
			records_en3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_en3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("en3", records_en3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record403: [a403, b1, c1, d3, e3, f1, g4, h3, i7, j3, k7, l7, m0, n11, o13, p3, q12]
		//record404: [a404, b0, c2, d0, e4, f2, g5, h4, i8, j4, k8, l8, m1, n12, o14, p4, q13]
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9, l9, m2, n13, o0, p5, q14]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10, l10, m3, n0, o1, p6, q15]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0, l11, m4, n1, o2, p7, q16]
		int pageCount0 = (int)Math.ceil(408.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_en3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "en3"+ File.separator+i+".db");
				dir_en3.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("en3");
		String tableTrace0 = DBApp.getLastTrace("en3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("g1j", cols0);
		String [][] records_g1j = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			records_g1j[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g1j[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g1j", records_g1j[i]);
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
		int pageCount0 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g1j = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g1j"+ File.separator+i+".db");
				dir_g1j.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("g1j");
		String tableTrace0 = DBApp.getLastTrace("g1j");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("geq33", cols1);
		String [][] records_geq33 = new String[420][cols1.length];
		for(int i=0;i<420;i++)
		{
			records_geq33[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_geq33[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("geq33", records_geq33[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		//record416: [a416, b0, c2, d0, e1, f2, g3]
		//record417: [a417, b1, c0, d1, e2, f3, g4]
		//record418: [a418, b0, c1, d2, e3, f4, g5]
		//record419: [a419, b1, c2, d3, e4, f5, g6]
		int pageCount1 = (int)Math.ceil(420.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_geq33 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "geq33"+ File.separator+i+".db");
				dir_geq33.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("geq33");
		String tableTrace1 = DBApp.getLastTrace("geq33");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q8", cols2);
		String [][] records_q8 = new String[109][cols2.length];
		for(int i=0;i<109;i++)
		{
			records_q8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q8", records_q8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2, g6]
		//record105: [a105, b1, c0, d1, e0, f3, g0]
		//record106: [a106, b0, c1, d2, e1, f4, g1]
		//record107: [a107, b1, c2, d3, e2, f5, g2]
		//record108: [a108, b0, c0, d0, e3, f0, g3]
		int pageCount2 = (int)Math.ceil(109.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q8"+ File.separator+i+".db");
				dir_q8.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("q8");
		String tableTrace2 = DBApp.getLastTrace("q8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qi", cols0);
		String [][] records_qi = new String[337][cols0.length];
		for(int i=0;i<337;i++)
		{
			records_qi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qi", records_qi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6]
		int pageCount0 = (int)Math.ceil(337.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qi = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qi"+ File.separator+i+".db");
				dir_qi.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("qi");
		String tableTrace0 = DBApp.getLastTrace("qi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n5l", cols0);
		String [][] records_n5l = new String[199][cols0.length];
		for(int i=0;i<199;i++)
		{
			records_n5l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n5l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n5l", records_n5l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record194: [a194, b0, c2, d2, e4, f2, g5]
		//record195: [a195, b1, c0, d3, e0, f3, g6]
		//record196: [a196, b0, c1, d0, e1, f4, g0]
		//record197: [a197, b1, c2, d1, e2, f5, g1]
		//record198: [a198, b0, c0, d2, e3, f0, g2]
		int pageCount0 = (int)Math.ceil(199.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n5l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n5l"+ File.separator+i+".db");
				dir_n5l.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("n5l");
		String tableTrace0 = DBApp.getLastTrace("n5l");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j4jcc", cols0);
		String [][] records_j4jcc = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			records_j4jcc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j4jcc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j4jcc", records_j4jcc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4]
		//record54: [a54, b0, c0, d2, e4, f0, g5]
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		//record56: [a56, b0, c2, d0, e1, f2, g0]
		//record57: [a57, b1, c0, d1, e2, f3, g1]
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j4jcc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j4jcc"+ File.separator+i+".db");
				dir_j4jcc.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("j4jcc");
		String tableTrace0 = DBApp.getLastTrace("j4jcc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u22", cols1);
		String [][] records_u22 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			records_u22[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u22[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u22", records_u22[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		int pageCount1 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u22 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u22"+ File.separator+i+".db");
				dir_u22.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("u22");
		String tableTrace1 = DBApp.getLastTrace("u22");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("psqby", cols0);
		String [][] records_psqby = new String[245][cols0.length];
		for(int i=0;i<245;i++)
		{
			records_psqby[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_psqby[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("psqby", records_psqby[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record240: [a240, b0, c0, d0, e0, f0, g2]
		//record241: [a241, b1, c1, d1, e1, f1, g3]
		//record242: [a242, b0, c2, d2, e2, f2, g4]
		//record243: [a243, b1, c0, d3, e3, f3, g5]
		//record244: [a244, b0, c1, d0, e4, f4, g6]
		int pageCount0 = (int)Math.ceil(245.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_psqby = new File(FileManager.directory.getAbsolutePath()+ File.separator + "psqby"+ File.separator+i+".db");
				dir_psqby.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("psqby");
		String tableTrace0 = DBApp.getLastTrace("psqby");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("el4q3", cols0);
		String [][] records_el4q3 = new String[171][cols0.length];
		for(int i=0;i<171;i++)
		{
			records_el4q3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_el4q3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("el4q3", records_el4q3[i]);
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
		int pageCount0 = (int)Math.ceil(171.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_el4q3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "el4q3"+ File.separator+i+".db");
				dir_el4q3.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("el4q3");
		String tableTrace0 = DBApp.getLastTrace("el4q3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e1693", cols0);
		String [][] records_e1693 = new String[160][cols0.length];
		for(int i=0;i<160;i++)
		{
			records_e1693[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e1693[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e1693", records_e1693[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6]
		int pageCount0 = (int)Math.ceil(160.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_e1693 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "e1693"+ File.separator+i+".db");
				dir_e1693.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("e1693");
		String tableTrace0 = DBApp.getLastTrace("e1693");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("xj96y", cols1);
		String [][] records_xj96y = new String[344][cols1.length];
		for(int i=0;i<344;i++)
		{
			records_xj96y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xj96y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xj96y", records_xj96y[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3, n5, o11, p5]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4, n6, o12, p6]
		//record343: [a343, b1, c1, d3, e3, f1, g0, h7, i1, j3, k2, l7, m5, n7, o13, p7]
		int pageCount1 = (int)Math.ceil(344.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xj96y = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xj96y"+ File.separator+i+".db");
				dir_xj96y.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("xj96y");
		String tableTrace1 = DBApp.getLastTrace("xj96y");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("k6", cols0);
		String [][] records_k6 = new String[478][cols0.length];
		for(int i=0;i<478;i++)
		{
			records_k6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k6", records_k6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8, p9, q14, r5]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6, n12, o9, p10, q15, r6]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13, o10, p11, q16, r7]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0, o11, p12, q0, r8]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1, o12, p13, q1, r9]
		int pageCount0 = (int)Math.ceil(478.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k6"+ File.separator+i+".db");
				dir_k6.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("k6");
		String tableTrace0 = DBApp.getLastTrace("k6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y5c", cols1);
		String [][] records_y5c = new String[109][cols1.length];
		for(int i=0;i<109;i++)
		{
			records_y5c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y5c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y5c", records_y5c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4]
		int pageCount1 = (int)Math.ceil(109.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y5c = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y5c"+ File.separator+i+".db");
				dir_y5c.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("y5c");
		String tableTrace1 = DBApp.getLastTrace("y5c");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uc", cols2);
		String [][] records_uc = new String[415][cols2.length];
		for(int i=0;i<415;i++)
		{
			records_uc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_uc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("uc", records_uc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3, l2]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4, l3]
		//record412: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4]
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6]
		int pageCount2 = (int)Math.ceil(415.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uc"+ File.separator+i+".db");
				dir_uc.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("uc");
		String tableTrace2 = DBApp.getLastTrace("uc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i04", cols3);
		String [][] records_i04 = new String[397][cols3.length];
		for(int i=0;i<397;i++)
		{
			records_i04[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_i04[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("i04", records_i04[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6]
		int pageCount3 = (int)Math.ceil(397.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i04 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i04"+ File.separator+i+".db");
				dir_i04.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("i04");
		String tableTrace3 = DBApp.getLastTrace("i04");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mwz", cols4);
		String [][] records_mwz = new String[476][cols4.length];
		for(int i=0;i<476;i++)
		{
			records_mwz[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mwz[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mwz", records_mwz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record471: [a471, b1, c0, d3, e1, f3, g2]
		//record472: [a472, b0, c1, d0, e2, f4, g3]
		//record473: [a473, b1, c2, d1, e3, f5, g4]
		//record474: [a474, b0, c0, d2, e4, f0, g5]
		//record475: [a475, b1, c1, d3, e0, f1, g6]
		int pageCount4 = (int)Math.ceil(476.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mwz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mwz"+ File.separator+i+".db");
				dir_mwz.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("mwz");
		String tableTrace4 = DBApp.getLastTrace("mwz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ol", cols0);
		String [][] records_ol = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_ol[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ol[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ol", records_ol[i]);
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
		int pageCount0 = (int)Math.ceil(177.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ol = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ol"+ File.separator+i+".db");
				dir_ol.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("ol");
		String tableTrace0 = DBApp.getLastTrace("ol");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ii0", cols1);
		String [][] records_ii0 = new String[497][cols1.length];
		for(int i=0;i<497;i++)
		{
			records_ii0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ii0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ii0", records_ii0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record492: [a492, b0, c0, d0, e2, f0, g2]
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		int pageCount1 = (int)Math.ceil(497.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ii0 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ii0"+ File.separator+i+".db");
				dir_ii0.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("ii0");
		String tableTrace1 = DBApp.getLastTrace("ii0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("g3", cols2);
		String [][] records_g3 = new String[360][cols2.length];
		for(int i=0;i<360;i++)
		{
			records_g3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g3", records_g3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record355: [a355, b1, c1, d3, e0, f1, g5, h3, i4, j5, k3, l7, m4]
		//record356: [a356, b0, c2, d0, e1, f2, g6, h4, i5, j6, k4, l8, m5]
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9, m6]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10, m7]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11, m8]
		int pageCount2 = (int)Math.ceil(360.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g3"+ File.separator+i+".db");
				dir_g3.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("g3");
		String tableTrace2 = DBApp.getLastTrace("g3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i33", cols0);
		String [][] records_i33 = new String[170][cols0.length];
		for(int i=0;i<170;i++)
		{
			records_i33[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i33[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i33", records_i33[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record165: [a165, b1, c0, d1, e0, f3, g4]
		//record166: [a166, b0, c1, d2, e1, f4, g5]
		//record167: [a167, b1, c2, d3, e2, f5, g6]
		//record168: [a168, b0, c0, d0, e3, f0, g0]
		//record169: [a169, b1, c1, d1, e4, f1, g1]
		int pageCount0 = (int)Math.ceil(170.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i33 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i33"+ File.separator+i+".db");
				dir_i33.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("i33");
		String tableTrace0 = DBApp.getLastTrace("i33");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("a1", cols1);
		String [][] records_a1 = new String[239][cols1.length];
		for(int i=0;i<239;i++)
		{
			records_a1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a1", records_a1[i]);
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
		int pageCount1 = (int)Math.ceil(239.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a1"+ File.separator+i+".db");
				dir_a1.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("a1");
		String tableTrace1 = DBApp.getLastTrace("a1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("xzi", cols2);
		String [][] records_xzi = new String[298][cols2.length];
		for(int i=0;i<298;i++)
		{
			records_xzi[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xzi[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xzi", records_xzi[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7, n13, o8, p5]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8, n0, o9, p6]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7, m9, n1, o10, p7]
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8, m10, n2, o11, p8]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9, m11, n3, o12, p9]
		int pageCount2 = (int)Math.ceil(298.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xzi = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xzi"+ File.separator+i+".db");
				dir_xzi.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("xzi");
		String tableTrace2 = DBApp.getLastTrace("xzi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fxs", cols3);
		String [][] records_fxs = new String[416][cols3.length];
		for(int i=0;i<416;i++)
		{
			records_fxs[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_fxs[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("fxs", records_fxs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record411: [a411, b1, c0, d3, e1, f3, g5]
		//record412: [a412, b0, c1, d0, e2, f4, g6]
		//record413: [a413, b1, c2, d1, e3, f5, g0]
		//record414: [a414, b0, c0, d2, e4, f0, g1]
		//record415: [a415, b1, c1, d3, e0, f1, g2]
		int pageCount3 = (int)Math.ceil(416.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fxs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fxs"+ File.separator+i+".db");
				dir_fxs.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("fxs");
		String tableTrace3 = DBApp.getLastTrace("fxs");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s88", cols0);
		String [][] records_s88 = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			records_s88[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s88[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s88", records_s88[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		int pageCount0 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s88 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s88"+ File.separator+i+".db");
				dir_s88.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("s88");
		String tableTrace0 = DBApp.getLastTrace("s88");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("pr", cols0);
		String [][] records_pr = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_pr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pr", records_pr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		int pageCount0 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pr"+ File.separator+i+".db");
				dir_pr.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("pr");
		String tableTrace0 = DBApp.getLastTrace("pr");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("n5618", cols1);
		String [][] records_n5618 = new String[237][cols1.length];
		for(int i=0;i<237;i++)
		{
			records_n5618[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n5618[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n5618", records_n5618[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8, q11, r16, s4, t12]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9, q12, r17, s5, t13]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10, o9, p10, q13, r0, s6, t14]
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10, p11, q14, r1, s7, t15]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11, p12, q15, r2, s8, t16]
		int pageCount1 = (int)Math.ceil(237.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n5618 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n5618"+ File.separator+i+".db");
				dir_n5618.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("n5618");
		String tableTrace1 = DBApp.getLastTrace("n5618");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fs147", cols2);
		String [][] records_fs147 = new String[111][cols2.length];
		for(int i=0;i<111;i++)
		{
			records_fs147[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fs147[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fs147", records_fs147[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record106: [a106, b0, c1, d2, e1, f4, g1]
		//record107: [a107, b1, c2, d3, e2, f5, g2]
		//record108: [a108, b0, c0, d0, e3, f0, g3]
		//record109: [a109, b1, c1, d1, e4, f1, g4]
		//record110: [a110, b0, c2, d2, e0, f2, g5]
		int pageCount2 = (int)Math.ceil(111.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fs147 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fs147"+ File.separator+i+".db");
				dir_fs147.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("fs147");
		String tableTrace2 = DBApp.getLastTrace("fs147");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("da", cols3);
		String [][] records_da = new String[72][cols3.length];
		for(int i=0;i<72;i++)
		{
			records_da[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_da[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("da", records_da[i]);
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
		int pageCount3 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_da = new File(FileManager.directory.getAbsolutePath()+ File.separator + "da"+ File.separator+i+".db");
				dir_da.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("da");
		String tableTrace3 = DBApp.getLastTrace("da");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l766", cols0);
		String [][] records_l766 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			records_l766[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l766[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l766", records_l766[i]);
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
		int pageCount0 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l766 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l766"+ File.separator+i+".db");
				dir_l766.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("l766");
		String tableTrace0 = DBApp.getLastTrace("l766");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y5k", cols0);
		String [][] records_y5k = new String[144][cols0.length];
		for(int i=0;i<144;i++)
		{
			records_y5k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y5k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y5k", records_y5k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record139: [a139, b1, c1, d3, e4, f1, g6]
		//record140: [a140, b0, c2, d0, e0, f2, g0]
		//record141: [a141, b1, c0, d1, e1, f3, g1]
		//record142: [a142, b0, c1, d2, e2, f4, g2]
		//record143: [a143, b1, c2, d3, e3, f5, g3]
		int pageCount0 = (int)Math.ceil(144.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y5k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y5k"+ File.separator+i+".db");
				dir_y5k.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("y5k");
		String tableTrace0 = DBApp.getLastTrace("y5k");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jw4w", cols1);
		String [][] records_jw4w = new String[306][cols1.length];
		for(int i=0;i<306;i++)
		{
			records_jw4w[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_jw4w[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("jw4w", records_jw4w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record301: [a301, b1, c1, d1, e1, f1, g0]
		//record302: [a302, b0, c2, d2, e2, f2, g1]
		//record303: [a303, b1, c0, d3, e3, f3, g2]
		//record304: [a304, b0, c1, d0, e4, f4, g3]
		//record305: [a305, b1, c2, d1, e0, f5, g4]
		int pageCount1 = (int)Math.ceil(306.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jw4w = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jw4w"+ File.separator+i+".db");
				dir_jw4w.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("jw4w");
		String tableTrace1 = DBApp.getLastTrace("jw4w");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("iqkv", cols2);
		String [][] records_iqkv = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			records_iqkv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_iqkv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("iqkv", records_iqkv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10, p5, q0, r13, s9]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1, r14, s10]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10, l3, m9, n3, o12, p7, q2, r15, s11]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0, i7, j8, k0, l4, m10, n4, o13, p8, q3, r16, s12]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1, i8, j9, k1, l5, m11, n5, o14, p9, q4, r17, s13]
		int pageCount2 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_iqkv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "iqkv"+ File.separator+i+".db");
				dir_iqkv.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("iqkv");
		String tableTrace2 = DBApp.getLastTrace("iqkv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ehb", cols3);
		String [][] records_ehb = new String[233][cols3.length];
		for(int i=0;i<233;i++)
		{
			records_ehb[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ehb[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ehb", records_ehb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record228: [a228, b0, c0, d0, e3, f0, g4]
		//record229: [a229, b1, c1, d1, e4, f1, g5]
		//record230: [a230, b0, c2, d2, e0, f2, g6]
		//record231: [a231, b1, c0, d3, e1, f3, g0]
		//record232: [a232, b0, c1, d0, e2, f4, g1]
		int pageCount3 = (int)Math.ceil(233.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ehb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ehb"+ File.separator+i+".db");
				dir_ehb.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("ehb");
		String tableTrace3 = DBApp.getLastTrace("ehb");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qvv", cols4);
		String [][] records_qvv = new String[9][cols4.length];
		for(int i=0;i<9;i++)
		{
			records_qvv[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_qvv[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("qvv", records_qvv[i]);
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
		int pageCount4 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qvv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qvv"+ File.separator+i+".db");
				dir_qvv.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("qvv");
		String tableTrace4 = DBApp.getLastTrace("qvv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("f81", cols0);
		String [][] records_f81 = new String[369][cols0.length];
		for(int i=0;i<369;i++)
		{
			records_f81[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f81[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f81", records_f81[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8]
		int pageCount0 = (int)Math.ceil(369.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f81 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f81"+ File.separator+i+".db");
				dir_f81.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("f81");
		String tableTrace0 = DBApp.getLastTrace("f81");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xpz", cols1);
		String [][] records_xpz = new String[332][cols1.length];
		for(int i=0;i<332;i++)
		{
			records_xpz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xpz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xpz", records_xpz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0]
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3]
		int pageCount1 = (int)Math.ceil(332.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xpz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xpz"+ File.separator+i+".db");
				dir_xpz.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("xpz");
		String tableTrace1 = DBApp.getLastTrace("xpz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g4q9", cols2);
		String [][] records_g4q9 = new String[461][cols2.length];
		for(int i=0;i<461;i++)
		{
			records_g4q9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g4q9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g4q9", records_g4q9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		//record458: [a458, b0, c2, d2, e3, f2, g3]
		//record459: [a459, b1, c0, d3, e4, f3, g4]
		//record460: [a460, b0, c1, d0, e0, f4, g5]
		int pageCount2 = (int)Math.ceil(461.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g4q9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g4q9"+ File.separator+i+".db");
				dir_g4q9.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("g4q9");
		String tableTrace2 = DBApp.getLastTrace("g4q9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("uy2", cols0);
		String [][] records_uy2 = new String[163][cols0.length];
		for(int i=0;i<163;i++)
		{
			records_uy2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uy2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uy2", records_uy2[i]);
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
		int pageCount0 = (int)Math.ceil(163.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uy2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uy2"+ File.separator+i+".db");
				dir_uy2.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("uy2");
		String tableTrace0 = DBApp.getLastTrace("uy2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vd", cols1);
		String [][] records_vd = new String[495][cols1.length];
		for(int i=0;i<495;i++)
		{
			records_vd[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vd[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vd", records_vd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11]
		//record493: [a493, b1, c1, d1, e3, f1, g3, h5, i7, j3, k9, l1, m12]
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0]
		int pageCount1 = (int)Math.ceil(495.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vd = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vd"+ File.separator+i+".db");
				dir_vd.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("vd");
		String tableTrace1 = DBApp.getLastTrace("vd");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l5v61", cols2);
		String [][] records_l5v61 = new String[182][cols2.length];
		for(int i=0;i<182;i++)
		{
			records_l5v61[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l5v61[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l5v61", records_l5v61[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record177: [a177, b1, c0, d1, e2, f3, g2]
		//record178: [a178, b0, c1, d2, e3, f4, g3]
		//record179: [a179, b1, c2, d3, e4, f5, g4]
		//record180: [a180, b0, c0, d0, e0, f0, g5]
		//record181: [a181, b1, c1, d1, e1, f1, g6]
		int pageCount2 = (int)Math.ceil(182.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l5v61 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l5v61"+ File.separator+i+".db");
				dir_l5v61.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("l5v61");
		String tableTrace2 = DBApp.getLastTrace("l5v61");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("je1", cols3);
		String [][] records_je1 = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			records_je1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_je1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("je1", records_je1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		int pageCount3 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_je1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "je1"+ File.separator+i+".db");
				dir_je1.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("je1");
		String tableTrace3 = DBApp.getLastTrace("je1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("xw", cols4);
		String [][] records_xw = new String[253][cols4.length];
		for(int i=0;i<253;i++)
		{
			records_xw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_xw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("xw", records_xw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2, n11]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3, n12]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0]
		int pageCount4 = (int)Math.ceil(253.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xw"+ File.separator+i+".db");
				dir_xw.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("xw");
		String tableTrace4 = DBApp.getLastTrace("xw");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ew91", cols0);
		String [][] records_ew91 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_ew91[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ew91[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ew91", records_ew91[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ew91 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ew91"+ File.separator+i+".db");
				dir_ew91.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("ew91");
		String tableTrace0 = DBApp.getLastTrace("ew91");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("drk7", cols0);
		String [][] records_drk7 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_drk7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_drk7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("drk7", records_drk7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
		int pageCount0 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_drk7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "drk7"+ File.separator+i+".db");
				dir_drk7.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("drk7");
		String tableTrace0 = DBApp.getLastTrace("drk7");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y3zx", cols1);
		String [][] records_y3zx = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			records_y3zx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y3zx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y3zx", records_y3zx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y3zx = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y3zx"+ File.separator+i+".db");
				dir_y3zx.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("y3zx");
		String tableTrace1 = DBApp.getLastTrace("y3zx");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q92", cols0);
		String [][] records_q92 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			records_q92[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q92[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q92", records_q92[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record70: [a70, b0, c1, d2, e0, f4, g0]
		//record71: [a71, b1, c2, d3, e1, f5, g1]
		//record72: [a72, b0, c0, d0, e2, f0, g2]
		//record73: [a73, b1, c1, d1, e3, f1, g3]
		//record74: [a74, b0, c2, d2, e4, f2, g4]
		int pageCount0 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q92 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q92"+ File.separator+i+".db");
				dir_q92.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("q92");
		String tableTrace0 = DBApp.getLastTrace("q92");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w4f", cols1);
		String [][] records_w4f = new String[435][cols1.length];
		for(int i=0;i<435;i++)
		{
			records_w4f[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w4f[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w4f", records_w4f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14]
		int pageCount1 = (int)Math.ceil(435.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w4f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w4f"+ File.separator+i+".db");
				dir_w4f.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("w4f");
		String tableTrace1 = DBApp.getLastTrace("w4f");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s33", cols2);
		String [][] records_s33 = new String[237][cols2.length];
		for(int i=0;i<237;i++)
		{
			records_s33[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s33[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s33", records_s33[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record232: [a232, b0, c1, d0, e2, f4, g1]
		//record233: [a233, b1, c2, d1, e3, f5, g2]
		//record234: [a234, b0, c0, d2, e4, f0, g3]
		//record235: [a235, b1, c1, d3, e0, f1, g4]
		//record236: [a236, b0, c2, d0, e1, f2, g5]
		int pageCount2 = (int)Math.ceil(237.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s33 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s33"+ File.separator+i+".db");
				dir_s33.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("s33");
		String tableTrace2 = DBApp.getLastTrace("s33");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lmuy4", cols3);
		String [][] records_lmuy4 = new String[152][cols3.length];
		for(int i=0;i<152;i++)
		{
			records_lmuy4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lmuy4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lmuy4", records_lmuy4[i]);
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
		int pageCount3 = (int)Math.ceil(152.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lmuy4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lmuy4"+ File.separator+i+".db");
				dir_lmuy4.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("lmuy4");
		String tableTrace3 = DBApp.getLastTrace("lmuy4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nw931", cols4);
		String [][] records_nw931 = new String[124][cols4.length];
		for(int i=0;i<124;i++)
		{
			records_nw931[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_nw931[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("nw931", records_nw931[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record119: [a119, b1, c2, d3, e4, f5, g0]
		//record120: [a120, b0, c0, d0, e0, f0, g1]
		//record121: [a121, b1, c1, d1, e1, f1, g2]
		//record122: [a122, b0, c2, d2, e2, f2, g3]
		//record123: [a123, b1, c0, d3, e3, f3, g4]
		int pageCount4 = (int)Math.ceil(124.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_nw931 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "nw931"+ File.separator+i+".db");
				dir_nw931.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("nw931");
		String tableTrace4 = DBApp.getLastTrace("nw931");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y27", cols0);
		String [][] records_y27 = new String[383][cols0.length];
		for(int i=0;i<383;i++)
		{
			records_y27[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y27[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y27", records_y27[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record378: [a378, b0, c0, d2, e3, f0, g0, h2, i0]
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4]
		int pageCount0 = (int)Math.ceil(383.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y27 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y27"+ File.separator+i+".db");
				dir_y27.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("y27");
		String tableTrace0 = DBApp.getLastTrace("y27");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k2t", cols1);
		String [][] records_k2t = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			records_k2t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k2t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k2t", records_k2t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6]
		//record84: [a84, b0, c0, d0, e4, f0, g0]
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		int pageCount1 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k2t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k2t"+ File.separator+i+".db");
				dir_k2t.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("k2t");
		String tableTrace1 = DBApp.getLastTrace("k2t");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ycsz", cols2);
		String [][] records_ycsz = new String[351][cols2.length];
		for(int i=0;i<351;i++)
		{
			records_ycsz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ycsz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ycsz", records_ycsz[i]);
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
		int pageCount2 = (int)Math.ceil(351.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ycsz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ycsz"+ File.separator+i+".db");
				dir_ycsz.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("ycsz");
		String tableTrace2 = DBApp.getLastTrace("ycsz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d30", cols3);
		String [][] records_d30 = new String[493][cols3.length];
		for(int i=0;i<493;i++)
		{
			records_d30[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_d30[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("d30", records_d30[i]);
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
		int pageCount3 = (int)Math.ceil(493.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d30 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d30"+ File.separator+i+".db");
				dir_d30.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("d30");
		String tableTrace3 = DBApp.getLastTrace("d30");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z5", cols0);
		String [][] records_z5 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			records_z5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z5", records_z5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6]
		int pageCount0 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z5"+ File.separator+i+".db");
				dir_z5.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("z5");
		String tableTrace0 = DBApp.getLastTrace("z5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("az8", cols1);
		String [][] records_az8 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			records_az8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_az8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("az8", records_az8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0]
		int pageCount1 = (int)Math.ceil(49.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_az8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "az8"+ File.separator+i+".db");
				dir_az8.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("az8");
		String tableTrace1 = DBApp.getLastTrace("az8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("lf4h8", cols2);
		String [][] records_lf4h8 = new String[226][cols2.length];
		for(int i=0;i<226;i++)
		{
			records_lf4h8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lf4h8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lf4h8", records_lf4h8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1, k1, l5]
		//record222: [a222, b0, c0, d2, e2, f0, g5, h6, i6, j2, k2, l6]
		//record223: [a223, b1, c1, d3, e3, f1, g6, h7, i7, j3, k3, l7]
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9]
		int pageCount2 = (int)Math.ceil(226.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lf4h8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lf4h8"+ File.separator+i+".db");
				dir_lf4h8.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("lf4h8");
		String tableTrace2 = DBApp.getLastTrace("lf4h8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qxj", cols3);
		String [][] records_qxj = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			records_qxj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qxj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qxj", records_qxj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1, f3, g2]
		//record52: [a52, b0, c1, d0, e2, f4, g3]
		//record53: [a53, b1, c2, d1, e3, f5, g4]
		//record54: [a54, b0, c0, d2, e4, f0, g5]
		//record55: [a55, b1, c1, d3, e0, f1, g6]
		int pageCount3 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qxj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qxj"+ File.separator+i+".db");
				dir_qxj.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("qxj");
		String tableTrace3 = DBApp.getLastTrace("qxj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q1z7", cols4);
		String [][] records_q1z7 = new String[25][cols4.length];
		for(int i=0;i<25;i++)
		{
			records_q1z7[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q1z7[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q1z7", records_q1z7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		int pageCount4 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q1z7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q1z7"+ File.separator+i+".db");
				dir_q1z7.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("q1z7");
		String tableTrace4 = DBApp.getLastTrace("q1z7");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("g59", cols0);
		String [][] records_g59 = new String[115][cols0.length];
		for(int i=0;i<115;i++)
		{
			records_g59[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g59[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g59", records_g59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4]
		int pageCount0 = (int)Math.ceil(115.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g59 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g59"+ File.separator+i+".db");
				dir_g59.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("g59");
		String tableTrace0 = DBApp.getLastTrace("g59");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ew78", cols0);
		String [][] records_ew78 = new String[220][cols0.length];
		for(int i=0;i<220;i++)
		{
			records_ew78[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ew78[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ew78", records_ew78[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5, o5, p7, q11]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6, p8, q12]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7, p9, q13]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8, p10, q14]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9, o9, p11, q15]
		int pageCount0 = (int)Math.ceil(220.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ew78 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ew78"+ File.separator+i+".db");
				dir_ew78.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("ew78");
		String tableTrace0 = DBApp.getLastTrace("ew78");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vh", cols1);
		String [][] records_vh = new String[287][cols1.length];
		for(int i=0;i<287;i++)
		{
			records_vh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vh", records_vh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12]
		//record286: [a286, b0, c1, d2, e1, f4, g6, h6, i7, j6, k0, l10, m0]
		int pageCount1 = (int)Math.ceil(287.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vh = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vh"+ File.separator+i+".db");
				dir_vh.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("vh");
		String tableTrace1 = DBApp.getLastTrace("vh");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l870", cols0);
		String [][] records_l870 = new String[308][cols0.length];
		for(int i=0;i<308;i++)
		{
			records_l870[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l870[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l870", records_l870[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3, m4]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4, m5]
		//record305: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5, m6]
		//record306: [a306, b0, c0, d2, e1, f0, g5, h2, i0, j6, k9, l6, m7]
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10, l7, m8]
		int pageCount0 = (int)Math.ceil(308.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l870 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l870"+ File.separator+i+".db");
				dir_l870.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("l870");
		String tableTrace0 = DBApp.getLastTrace("l870");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x7ys", cols1);
		String [][] records_x7ys = new String[474][cols1.length];
		for(int i=0;i<474;i++)
		{
			records_x7ys[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x7ys[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x7ys", records_x7ys[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7, l1, m1, n7, o4]
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2, j0, k8, l2, m2, n8, o5]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3, j1, k9, l3, m3, n9, o6]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10, o7]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8]
		int pageCount1 = (int)Math.ceil(474.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x7ys = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x7ys"+ File.separator+i+".db");
				dir_x7ys.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("x7ys");
		String tableTrace1 = DBApp.getLastTrace("x7ys");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xljq2", cols0);
		String [][] records_xljq2 = new String[126][cols0.length];
		for(int i=0;i<126;i++)
		{
			records_xljq2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xljq2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xljq2", records_xljq2[i]);
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
		int pageCount0 = (int)Math.ceil(126.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xljq2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xljq2"+ File.separator+i+".db");
				dir_xljq2.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("xljq2");
		String tableTrace0 = DBApp.getLastTrace("xljq2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n26", cols1);
		String [][] records_n26 = new String[181][cols1.length];
		for(int i=0;i<181;i++)
		{
			records_n26[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n26[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n26", records_n26[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record176: [a176, b0, c2, d0, e1, f2, g1]
		//record177: [a177, b1, c0, d1, e2, f3, g2]
		//record178: [a178, b0, c1, d2, e3, f4, g3]
		//record179: [a179, b1, c2, d3, e4, f5, g4]
		//record180: [a180, b0, c0, d0, e0, f0, g5]
		int pageCount1 = (int)Math.ceil(181.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n26 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n26"+ File.separator+i+".db");
				dir_n26.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("n26");
		String tableTrace1 = DBApp.getLastTrace("n26");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ob4", cols2);
		String [][] records_ob4 = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_ob4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ob4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ob4", records_ob4[i]);
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
		int pageCount2 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ob4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ob4"+ File.separator+i+".db");
				dir_ob4.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("ob4");
		String tableTrace2 = DBApp.getLastTrace("ob4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("vppry", cols0);
		String [][] records_vppry = new String[262][cols0.length];
		for(int i=0;i<262;i++)
		{
			records_vppry[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vppry[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vppry", records_vppry[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1, q2, r5]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2, q3, r6]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7, o4, p3, q4, r7]
		//record260: [a260, b0, c2, d0, e0, f2, g1, h4, i8, j0, k7, l8, m0, n8, o5, p4, q5, r8]
		//record261: [a261, b1, c0, d1, e1, f3, g2, h5, i0, j1, k8, l9, m1, n9, o6, p5, q6, r9]
		int pageCount0 = (int)Math.ceil(262.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vppry = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vppry"+ File.separator+i+".db");
				dir_vppry.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("vppry");
		String tableTrace0 = DBApp.getLastTrace("vppry");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("d51", cols1);
		String [][] records_d51 = new String[211][cols1.length];
		for(int i=0;i<211;i++)
		{
			records_d51[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d51[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d51", records_d51[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10, o11, p14, q2, r8]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11, o12, p15, q3, r9]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12, o13, p0, q4, r10]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13, o14, p1, q5, r11]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0, o0, p2, q6, r12]
		int pageCount1 = (int)Math.ceil(211.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d51 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d51"+ File.separator+i+".db");
				dir_d51.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("d51");
		String tableTrace1 = DBApp.getLastTrace("d51");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("at", cols2);
		String [][] records_at = new String[67][cols2.length];
		for(int i=0;i<67;i++)
		{
			records_at[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_at[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("at", records_at[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10]
		int pageCount2 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_at = new File(FileManager.directory.getAbsolutePath()+ File.separator + "at"+ File.separator+i+".db");
				dir_at.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("at");
		String tableTrace2 = DBApp.getLastTrace("at");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("unpf9", cols0);
		String [][] records_unpf9 = new String[349][cols0.length];
		for(int i=0;i<349;i++)
		{
			records_unpf9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_unpf9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("unpf9", records_unpf9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record344: [a344, b0, c2, d0, e4, f2, g1]
		//record345: [a345, b1, c0, d1, e0, f3, g2]
		//record346: [a346, b0, c1, d2, e1, f4, g3]
		//record347: [a347, b1, c2, d3, e2, f5, g4]
		//record348: [a348, b0, c0, d0, e3, f0, g5]
		int pageCount0 = (int)Math.ceil(349.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_unpf9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "unpf9"+ File.separator+i+".db");
				dir_unpf9.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("unpf9");
		String tableTrace0 = DBApp.getLastTrace("unpf9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("z1f", cols1);
		String [][] records_z1f = new String[273][cols1.length];
		for(int i=0;i<273;i++)
		{
			records_z1f[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z1f[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z1f", records_z1f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13, p12, q13, r16]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3, o14, p13, q14, r17]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4, o0, p14, q15, r0]
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7, l7, m11, n5, o1, p15, q16, r1]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8, l8, m12, n6, o2, p0, q0, r2]
		int pageCount1 = (int)Math.ceil(273.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z1f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z1f"+ File.separator+i+".db");
				dir_z1f.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("z1f");
		String tableTrace1 = DBApp.getLastTrace("z1f");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q19vw", cols2);
		String [][] records_q19vw = new String[229][cols2.length];
		for(int i=0;i<229;i++)
		{
			records_q19vw[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q19vw[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q19vw", records_q19vw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record224: [a224, b0, c2, d0, e4, f2, g0, h0, i8, j4, k4, l8]
		//record225: [a225, b1, c0, d1, e0, f3, g1, h1, i0, j5, k5, l9]
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0]
		int pageCount2 = (int)Math.ceil(229.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q19vw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q19vw"+ File.separator+i+".db");
				dir_q19vw.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("q19vw");
		String tableTrace2 = DBApp.getLastTrace("q19vw");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p96jl", cols0);
		String [][] records_p96jl = new String[282][cols0.length];
		for(int i=0;i<282;i++)
		{
			records_p96jl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p96jl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p96jl", records_p96jl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record277: [a277, b1, c1, d1, e2, f1, g4]
		//record278: [a278, b0, c2, d2, e3, f2, g5]
		//record279: [a279, b1, c0, d3, e4, f3, g6]
		//record280: [a280, b0, c1, d0, e0, f4, g0]
		//record281: [a281, b1, c2, d1, e1, f5, g1]
		int pageCount0 = (int)Math.ceil(282.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p96jl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p96jl"+ File.separator+i+".db");
				dir_p96jl.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("p96jl");
		String tableTrace0 = DBApp.getLastTrace("p96jl");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("adf", cols1);
		String [][] records_adf = new String[444][cols1.length];
		for(int i=0;i<444;i++)
		{
			records_adf[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_adf[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("adf", records_adf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1]
		int pageCount1 = (int)Math.ceil(444.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_adf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "adf"+ File.separator+i+".db");
				dir_adf.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("adf");
		String tableTrace1 = DBApp.getLastTrace("adf");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("exow", cols2);
		String [][] records_exow = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			records_exow[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_exow[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("exow", records_exow[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		int pageCount2 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_exow = new File(FileManager.directory.getAbsolutePath()+ File.separator + "exow"+ File.separator+i+".db");
				dir_exow.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("exow");
		String tableTrace2 = DBApp.getLastTrace("exow");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("m6i", cols3);
		String [][] records_m6i = new String[240][cols3.length];
		for(int i=0;i<240;i++)
		{
			records_m6i[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m6i[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m6i", records_m6i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7]
		int pageCount3 = (int)Math.ceil(240.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m6i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m6i"+ File.separator+i+".db");
				dir_m6i.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("m6i");
		String tableTrace3 = DBApp.getLastTrace("m6i");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p8", cols4);
		String [][] records_p8 = new String[348][cols4.length];
		for(int i=0;i<348;i++)
		{
			records_p8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_p8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("p8", records_p8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record343: [a343, b1, c1, d3, e3, f1, g0]
		//record344: [a344, b0, c2, d0, e4, f2, g1]
		//record345: [a345, b1, c0, d1, e0, f3, g2]
		//record346: [a346, b0, c1, d2, e1, f4, g3]
		//record347: [a347, b1, c2, d3, e2, f5, g4]
		int pageCount4 = (int)Math.ceil(348.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p8"+ File.separator+i+".db");
				dir_p8.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("p8");
		String tableTrace4 = DBApp.getLastTrace("p8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mv53", cols0);
		String [][] records_mv53 = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			records_mv53[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mv53[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mv53", records_mv53[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record85: [a85, b1, c1, d1, e0, f1, g1]
		//record86: [a86, b0, c2, d2, e1, f2, g2]
		//record87: [a87, b1, c0, d3, e2, f3, g3]
		//record88: [a88, b0, c1, d0, e3, f4, g4]
		//record89: [a89, b1, c2, d1, e4, f5, g5]
		int pageCount0 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mv53 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mv53"+ File.separator+i+".db");
				dir_mv53.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("mv53");
		String tableTrace0 = DBApp.getLastTrace("mv53");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p37uy", cols1);
		String [][] records_p37uy = new String[158][cols1.length];
		for(int i=0;i<158;i++)
		{
			records_p37uy[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p37uy[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p37uy", records_p37uy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3, k10]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5, k1]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3]
		int pageCount1 = (int)Math.ceil(158.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p37uy = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p37uy"+ File.separator+i+".db");
				dir_p37uy.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("p37uy");
		String tableTrace1 = DBApp.getLastTrace("p37uy");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jr", cols2);
		String [][] records_jr = new String[492][cols2.length];
		for(int i=0;i<492;i++)
		{
			records_jr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jr", records_jr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10]
		int pageCount2 = (int)Math.ceil(492.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jr"+ File.separator+i+".db");
				dir_jr.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("jr");
		String tableTrace2 = DBApp.getLastTrace("jr");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fli", cols3);
		String [][] records_fli = new String[370][cols3.length];
		for(int i=0;i<370;i++)
		{
			records_fli[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_fli[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("fli", records_fli[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0]
		int pageCount3 = (int)Math.ceil(370.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fli = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fli"+ File.separator+i+".db");
				dir_fli.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("fli");
		String tableTrace3 = DBApp.getLastTrace("fli");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("sr9t", cols0);
		String [][] records_sr9t = new String[127][cols0.length];
		for(int i=0;i<127;i++)
		{
			records_sr9t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sr9t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sr9t", records_sr9t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10, o2, p10, q3, r14, s8]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11, o3, p11, q4, r15, s9]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12, o4, p12, q5, r16, s10]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13, o5, p13, q6, r17, s11]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6, m9, n0, o6, p14, q7, r0, s12]
		int pageCount0 = (int)Math.ceil(127.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_sr9t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "sr9t"+ File.separator+i+".db");
				dir_sr9t.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("sr9t");
		String tableTrace0 = DBApp.getLastTrace("sr9t");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("lnm08", cols1);
		String [][] records_lnm08 = new String[126][cols1.length];
		for(int i=0;i<126;i++)
		{
			records_lnm08[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lnm08[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lnm08", records_lnm08[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record121: [a121, b1, c1, d1, e1, f1, g2, h1, i4, j1, k0, l1, m4, n9]
		//record122: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10]
		//record123: [a123, b1, c0, d3, e3, f3, g4, h3, i6, j3, k2, l3, m6, n11]
		//record124: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4, k3, l4, m7, n12]
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5, m8, n13]
		int pageCount1 = (int)Math.ceil(126.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lnm08 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lnm08"+ File.separator+i+".db");
				dir_lnm08.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("lnm08");
		String tableTrace1 = DBApp.getLastTrace("lnm08");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n5s3w", cols2);
		String [][] records_n5s3w = new String[459][cols2.length];
		for(int i=0;i<459;i++)
		{
			records_n5s3w[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n5s3w[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n5s3w", records_n5s3w[i]);
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
		int pageCount2 = (int)Math.ceil(459.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n5s3w = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n5s3w"+ File.separator+i+".db");
				dir_n5s3w.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("n5s3w");
		String tableTrace2 = DBApp.getLastTrace("n5s3w");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l3w", cols3);
		String [][] records_l3w = new String[454][cols3.length];
		for(int i=0;i<454;i++)
		{
			records_l3w[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l3w[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l3w", records_l3w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3]
		int pageCount3 = (int)Math.ceil(454.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l3w = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l3w"+ File.separator+i+".db");
				dir_l3w.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("l3w");
		String tableTrace3 = DBApp.getLastTrace("l3w");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c9", cols0);
		String [][] records_c9 = new String[461][cols0.length];
		for(int i=0;i<461;i++)
		{
			records_c9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c9", records_c9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record456: [a456, b0, c0, d0, e1, f0, g1]
		//record457: [a457, b1, c1, d1, e2, f1, g2]
		//record458: [a458, b0, c2, d2, e3, f2, g3]
		//record459: [a459, b1, c0, d3, e4, f3, g4]
		//record460: [a460, b0, c1, d0, e0, f4, g5]
		int pageCount0 = (int)Math.ceil(461.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_c9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "c9"+ File.separator+i+".db");
				dir_c9.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("c9");
		String tableTrace0 = DBApp.getLastTrace("c9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("znp", cols0);
		String [][] records_znp = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_znp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_znp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("znp", records_znp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		int pageCount0 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_znp = new File(FileManager.directory.getAbsolutePath()+ File.separator + "znp"+ File.separator+i+".db");
				dir_znp.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("znp");
		String tableTrace0 = DBApp.getLastTrace("znp");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l31k", cols0);
		String [][] records_l31k = new String[265][cols0.length];
		for(int i=0;i<265;i++)
		{
			records_l31k[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l31k[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l31k", records_l31k[i]);
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
		int pageCount0 = (int)Math.ceil(265.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l31k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l31k"+ File.separator+i+".db");
				dir_l31k.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("l31k");
		String tableTrace0 = DBApp.getLastTrace("l31k");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q77", cols1);
		String [][] records_q77 = new String[195][cols1.length];
		for(int i=0;i<195;i++)
		{
			records_q77[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q77[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q77", records_q77[i]);
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
		int pageCount1 = (int)Math.ceil(195.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q77 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q77"+ File.separator+i+".db");
				dir_q77.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("q77");
		String tableTrace1 = DBApp.getLastTrace("q77");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("go9", cols2);
		String [][] records_go9 = new String[278][cols2.length];
		for(int i=0;i<278;i++)
		{
			records_go9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_go9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("go9", records_go9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9, l9, m0, n7, o3, p1, q1, r3]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10, l10, m1, n8, o4, p2, q2, r4]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0, l11, m2, n9, o5, p3, q3, r5]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6, k1, l0, m3, n10, o6, p4, q4, r6]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7, k2, l1, m4, n11, o7, p5, q5, r7]
		int pageCount2 = (int)Math.ceil(278.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_go9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "go9"+ File.separator+i+".db");
				dir_go9.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("go9");
		String tableTrace2 = DBApp.getLastTrace("go9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("uc1", cols0);
		String [][] records_uc1 = new String[219][cols0.length];
		for(int i=0;i<219;i++)
		{
			records_uc1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uc1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uc1", records_uc1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2]
		int pageCount0 = (int)Math.ceil(219.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uc1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uc1"+ File.separator+i+".db");
				dir_uc1.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("uc1");
		String tableTrace0 = DBApp.getLastTrace("uc1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q9", cols1);
		String [][] records_q9 = new String[104][cols1.length];
		for(int i=0;i<104;i++)
		{
			records_q9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q9", records_q9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record99: [a99, b1, c0, d3, e4, f3, g1]
		//record100: [a100, b0, c1, d0, e0, f4, g2]
		//record101: [a101, b1, c2, d1, e1, f5, g3]
		//record102: [a102, b0, c0, d2, e2, f0, g4]
		//record103: [a103, b1, c1, d3, e3, f1, g5]
		int pageCount1 = (int)Math.ceil(104.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q9"+ File.separator+i+".db");
				dir_q9.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("q9");
		String tableTrace1 = DBApp.getLastTrace("q9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wq5", cols2);
		String [][] records_wq5 = new String[312][cols2.length];
		for(int i=0;i<312;i++)
		{
			records_wq5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wq5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wq5", records_wq5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record307: [a307, b1, c1, d3, e2, f1, g6, h3, i1, j7, k10, l7, m8]
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8, m9]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12]
		int pageCount2 = (int)Math.ceil(312.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wq5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wq5"+ File.separator+i+".db");
				dir_wq5.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("wq5");
		String tableTrace2 = DBApp.getLastTrace("wq5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q1ka", cols3);
		String [][] records_q1ka = new String[359][cols3.length];
		for(int i=0;i<359;i++)
		{
			records_q1ka[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q1ka[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q1ka", records_q1ka[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record354: [a354, b0, c0, d2, e4, f0, g4]
		//record355: [a355, b1, c1, d3, e0, f1, g5]
		//record356: [a356, b0, c2, d0, e1, f2, g6]
		//record357: [a357, b1, c0, d1, e2, f3, g0]
		//record358: [a358, b0, c1, d2, e3, f4, g1]
		int pageCount3 = (int)Math.ceil(359.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q1ka = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q1ka"+ File.separator+i+".db");
				dir_q1ka.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("q1ka");
		String tableTrace3 = DBApp.getLastTrace("q1ka");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bv5", cols4);
		String [][] records_bv5 = new String[481][cols4.length];
		for(int i=0;i<481;i++)
		{
			records_bv5[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bv5[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bv5", records_bv5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record476: [a476, b0, c2, d0, e1, f2, g0]
		//record477: [a477, b1, c0, d1, e2, f3, g1]
		//record478: [a478, b0, c1, d2, e3, f4, g2]
		//record479: [a479, b1, c2, d3, e4, f5, g3]
		//record480: [a480, b0, c0, d0, e0, f0, g4]
		int pageCount4 = (int)Math.ceil(481.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bv5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bv5"+ File.separator+i+".db");
				dir_bv5.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("bv5");
		String tableTrace4 = DBApp.getLastTrace("bv5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("n6pp", cols0);
		String [][] records_n6pp = new String[260][cols0.length];
		for(int i=0;i<260;i++)
		{
			records_n6pp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n6pp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n6pp", records_n6pp[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0, p15, q0, r3, s8, t15]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1, p0, q1, r4, s9, t16]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1, q2, r5, s10, t17]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2, q3, r6, s11, t18]
		//record259: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6, l7, m12, n7, o4, p3, q4, r7, s12, t19]
		int pageCount0 = (int)Math.ceil(260.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n6pp = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n6pp"+ File.separator+i+".db");
				dir_n6pp.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("n6pp");
		String tableTrace0 = DBApp.getLastTrace("n6pp");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("b5h", cols1);
		String [][] records_b5h = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_b5h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b5h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b5h", records_b5h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		int pageCount1 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b5h = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b5h"+ File.separator+i+".db");
				dir_b5h.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("b5h");
		String tableTrace1 = DBApp.getLastTrace("b5h");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v29", cols2);
		String [][] records_v29 = new String[387][cols2.length];
		for(int i=0;i<387;i++)
		{
			records_v29[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v29[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v29", records_v29[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		//record384: [a384, b0, c0, d0, e4, f0, g6]
		//record385: [a385, b1, c1, d1, e0, f1, g0]
		//record386: [a386, b0, c2, d2, e1, f2, g1]
		int pageCount2 = (int)Math.ceil(387.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v29 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v29"+ File.separator+i+".db");
				dir_v29.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("v29");
		String tableTrace2 = DBApp.getLastTrace("v29");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("r8", cols3);
		String [][] records_r8 = new String[119][cols3.length];
		for(int i=0;i<119;i++)
		{
			records_r8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r8", records_r8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10, n2, o9, p2, q12]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11, n3, o10, p3, q13]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12, n4, o11, p4, q14]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7, k7, l9, m0, n5, o12, p5, q15]
		//record118: [a118, b0, c1, d2, e3, f4, g6, h6, i1, j8, k8, l10, m1, n6, o13, p6, q16]
		int pageCount3 = (int)Math.ceil(119.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r8"+ File.separator+i+".db");
				dir_r8.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("r8");
		String tableTrace3 = DBApp.getLastTrace("r8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("psc", cols0);
		String [][] records_psc = new String[244][cols0.length];
		for(int i=0;i<244;i++)
		{
			records_psc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_psc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("psc", records_psc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3]
		int pageCount0 = (int)Math.ceil(244.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_psc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "psc"+ File.separator+i+".db");
				dir_psc.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("psc");
		String tableTrace0 = DBApp.getLastTrace("psc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ql", cols1);
		String [][] records_ql = new String[163][cols1.length];
		for(int i=0;i<163;i++)
		{
			records_ql[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ql[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ql", records_ql[i]);
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
		int pageCount1 = (int)Math.ceil(163.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ql = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ql"+ File.separator+i+".db");
				dir_ql.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("ql");
		String tableTrace1 = DBApp.getLastTrace("ql");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("dg", cols2);
		String [][] records_dg = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			records_dg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dg", records_dg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11, s10]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13, s12]
		int pageCount2 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dg = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dg"+ File.separator+i+".db");
				dir_dg.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("dg");
		String tableTrace2 = DBApp.getLastTrace("dg");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("k9l18", cols3);
		String [][] records_k9l18 = new String[286][cols3.length];
		for(int i=0;i<286;i++)
		{
			records_k9l18[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k9l18[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k9l18", records_k9l18[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record281: [a281, b1, c2, d1, e1, f5, g1, h1, i2, j1, k6, l5, m8, n1, o11, p9]
		//record282: [a282, b0, c0, d2, e2, f0, g2, h2, i3, j2, k7, l6, m9, n2, o12, p10]
		//record283: [a283, b1, c1, d3, e3, f1, g3, h3, i4, j3, k8, l7, m10, n3, o13, p11]
		//record284: [a284, b0, c2, d0, e4, f2, g4, h4, i5, j4, k9, l8, m11, n4, o14, p12]
		//record285: [a285, b1, c0, d1, e0, f3, g5, h5, i6, j5, k10, l9, m12, n5, o0, p13]
		int pageCount3 = (int)Math.ceil(286.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k9l18 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k9l18"+ File.separator+i+".db");
				dir_k9l18.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("k9l18");
		String tableTrace3 = DBApp.getLastTrace("k9l18");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mbi", cols0);
		String [][] records_mbi = new String[266][cols0.length];
		for(int i=0;i<266;i++)
		{
			records_mbi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mbi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mbi", records_mbi[i]);
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
		int pageCount0 = (int)Math.ceil(266.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mbi = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mbi"+ File.separator+i+".db");
				dir_mbi.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("mbi");
		String tableTrace0 = DBApp.getLastTrace("mbi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u0933", cols0);
		String [][] records_u0933 = new String[189][cols0.length];
		for(int i=0;i<189;i++)
		{
			records_u0933[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u0933[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u0933", records_u0933[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8]
		int pageCount0 = (int)Math.ceil(189.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u0933 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u0933"+ File.separator+i+".db");
				dir_u0933.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("u0933");
		String tableTrace0 = DBApp.getLastTrace("u0933");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rx", cols1);
		String [][] records_rx = new String[107][cols1.length];
		for(int i=0;i<107;i++)
		{
			records_rx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rx", records_rx[i]);
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
		int pageCount1 = (int)Math.ceil(107.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rx = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rx"+ File.separator+i+".db");
				dir_rx.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("rx");
		String tableTrace1 = DBApp.getLastTrace("rx");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w53t", cols0);
		String [][] records_w53t = new String[498][cols0.length];
		for(int i=0;i<498;i++)
		{
			records_w53t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w53t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w53t", records_w53t[i]);
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
		int pageCount0 = (int)Math.ceil(498.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w53t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w53t"+ File.separator+i+".db");
				dir_w53t.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("w53t");
		String tableTrace0 = DBApp.getLastTrace("w53t");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w5f3", cols1);
		String [][] records_w5f3 = new String[225][cols1.length];
		for(int i=0;i<225;i++)
		{
			records_w5f3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w5f3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w5f3", records_w5f3[i]);
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
		int pageCount1 = (int)Math.ceil(225.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w5f3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w5f3"+ File.separator+i+".db");
				dir_w5f3.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("w5f3");
		String tableTrace1 = DBApp.getLastTrace("w5f3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m81", cols2);
		String [][] records_m81 = new String[251][cols2.length];
		for(int i=0;i<251;i++)
		{
			records_m81[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m81[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m81", records_m81[i]);
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
		int pageCount2 = (int)Math.ceil(251.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m81 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m81"+ File.separator+i+".db");
				dir_m81.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("m81");
		String tableTrace2 = DBApp.getLastTrace("m81");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s69", cols3);
		String [][] records_s69 = new String[119][cols3.length];
		for(int i=0;i<119;i++)
		{
			records_s69[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s69[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s69", records_s69[i]);
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
		int pageCount3 = (int)Math.ceil(119.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s69 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s69"+ File.separator+i+".db");
				dir_s69.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("s69");
		String tableTrace3 = DBApp.getLastTrace("s69");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dt740", cols4);
		String [][] records_dt740 = new String[190][cols4.length];
		for(int i=0;i<190;i++)
		{
			records_dt740[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dt740[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dt740", records_dt740[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5, o7]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6, n6, o8]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7, o9]
		int pageCount4 = (int)Math.ceil(190.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dt740 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dt740"+ File.separator+i+".db");
				dir_dt740.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("dt740");
		String tableTrace4 = DBApp.getLastTrace("dt740");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w8l", cols0);
		String [][] records_w8l = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_w8l[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w8l[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w8l", records_w8l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w8l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w8l"+ File.separator+i+".db");
				dir_w8l.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("w8l");
		String tableTrace0 = DBApp.getLastTrace("w8l");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("gn2g", cols1);
		String [][] records_gn2g = new String[186][cols1.length];
		for(int i=0;i<186;i++)
		{
			records_gn2g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gn2g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gn2g", records_gn2g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record181: [a181, b1, c1, d1, e1, f1, g6, h5, i1, j1, k5, l1, m12, n13, o1, p5, q11, r1, s10]
		//record182: [a182, b0, c2, d2, e2, f2, g0, h6, i2, j2, k6, l2, m0, n0, o2, p6, q12, r2, s11]
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1, o3, p7, q13, r3, s12]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2, n2, o4, p8, q14, r4, s13]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5, p9, q15, r5, s14]
		int pageCount1 = (int)Math.ceil(186.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gn2g = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gn2g"+ File.separator+i+".db");
				dir_gn2g.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("gn2g");
		String tableTrace1 = DBApp.getLastTrace("gn2g");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("es", cols2);
		String [][] records_es = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			records_es[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_es[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("es", records_es[i]);
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
		int pageCount2 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_es = new File(FileManager.directory.getAbsolutePath()+ File.separator + "es"+ File.separator+i+".db");
				dir_es.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("es");
		String tableTrace2 = DBApp.getLastTrace("es");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y0d88", cols3);
		String [][] records_y0d88 = new String[441][cols3.length];
		for(int i=0;i<441;i++)
		{
			records_y0d88[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y0d88[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y0d88", records_y0d88[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		//record437: [a437, b1, c2, d1, e2, f5, g3]
		//record438: [a438, b0, c0, d2, e3, f0, g4]
		//record439: [a439, b1, c1, d3, e4, f1, g5]
		//record440: [a440, b0, c2, d0, e0, f2, g6]
		int pageCount3 = (int)Math.ceil(441.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y0d88 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y0d88"+ File.separator+i+".db");
				dir_y0d88.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("y0d88");
		String tableTrace3 = DBApp.getLastTrace("y0d88");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ih2", cols0);
		String [][] records_ih2 = new String[316][cols0.length];
		for(int i=0;i<316;i++)
		{
			records_ih2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ih2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ih2", records_ih2[i]);
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
		int pageCount0 = (int)Math.ceil(316.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ih2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ih2"+ File.separator+i+".db");
				dir_ih2.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("ih2");
		String tableTrace0 = DBApp.getLastTrace("ih2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j5m", cols0);
		String [][] records_j5m = new String[158][cols0.length];
		for(int i=0;i<158;i++)
		{
			records_j5m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j5m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j5m", records_j5m[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0, j3]
		//record154: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4]
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3, i2, j5]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7]
		int pageCount0 = (int)Math.ceil(158.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j5m = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j5m"+ File.separator+i+".db");
				dir_j5m.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("j5m");
		String tableTrace0 = DBApp.getLastTrace("j5m");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gio6c", cols1);
		String [][] records_gio6c = new String[160][cols1.length];
		for(int i=0;i<160;i++)
		{
			records_gio6c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gio6c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gio6c", records_gio6c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record155: [a155, b1, c2, d3, e0, f5, g1, h3]
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7]
		int pageCount1 = (int)Math.ceil(160.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gio6c = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gio6c"+ File.separator+i+".db");
				dir_gio6c.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("gio6c");
		String tableTrace1 = DBApp.getLastTrace("gio6c");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("gkg2", cols0);
		String [][] records_gkg2 = new String[499][cols0.length];
		for(int i=0;i<499;i++)
		{
			records_gkg2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gkg2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gkg2", records_gkg2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record494: [a494, b0, c2, d2, e4, f2, g4, h6, i8, j4, k10, l2, m0, n4, o14, p14]
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1, n5, o0, p15]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2, n6, o1, p0]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3, n7, o2, p1]
		//record498: [a498, b0, c0, d2, e3, f0, g1, h2, i3, j8, k3, l6, m4, n8, o3, p2]
		int pageCount0 = (int)Math.ceil(499.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gkg2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gkg2"+ File.separator+i+".db");
				dir_gkg2.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("gkg2");
		String tableTrace0 = DBApp.getLastTrace("gkg2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("tq96r", cols1);
		String [][] records_tq96r = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			records_tq96r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tq96r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tq96r", records_tq96r[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5, p15, q10]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6, p0, q11]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1, i7, j7, k9, l1, m6, n13, o7, p1, q12]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0, o8, p2, q13]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14]
		int pageCount1 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tq96r = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tq96r"+ File.separator+i+".db");
				dir_tq96r.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("tq96r");
		String tableTrace1 = DBApp.getLastTrace("tq96r");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eb", cols2);
		String [][] records_eb = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			records_eb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eb", records_eb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record95: [a95, b1, c2, d3, e0, f5, g4]
		//record96: [a96, b0, c0, d0, e1, f0, g5]
		//record97: [a97, b1, c1, d1, e2, f1, g6]
		//record98: [a98, b0, c2, d2, e3, f2, g0]
		//record99: [a99, b1, c0, d3, e4, f3, g1]
		int pageCount2 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_eb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "eb"+ File.separator+i+".db");
				dir_eb.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("eb");
		String tableTrace2 = DBApp.getLastTrace("eb");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("b0h", cols0);
		String [][] records_b0h = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_b0h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b0h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b0h", records_b0h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1, t19]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2, t0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3, t1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4, t2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7, s5, t3]
		int pageCount0 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b0h = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b0h"+ File.separator+i+".db");
				dir_b0h.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("b0h");
		String tableTrace0 = DBApp.getLastTrace("b0h");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("zqw7q", cols1);
		String [][] records_zqw7q = new String[188][cols1.length];
		for(int i=0;i<188;i++)
		{
			records_zqw7q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zqw7q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zqw7q", records_zqw7q[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record183: [a183, b1, c0, d3, e3, f3, g1, h7, i3, j3, k7, l3, m1, n1, o3, p7, q13, r3, s12]
		//record184: [a184, b0, c1, d0, e4, f4, g2, h0, i4, j4, k8, l4, m2, n2, o4, p8, q14, r4, s13]
		//record185: [a185, b1, c2, d1, e0, f5, g3, h1, i5, j5, k9, l5, m3, n3, o5, p9, q15, r5, s14]
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4, o6, p10, q16, r6, s15]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5, o7, p11, q0, r7, s16]
		int pageCount1 = (int)Math.ceil(188.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zqw7q = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zqw7q"+ File.separator+i+".db");
				dir_zqw7q.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("zqw7q");
		String tableTrace1 = DBApp.getLastTrace("zqw7q");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d20", cols2);
		String [][] records_d20 = new String[407][cols2.length];
		for(int i=0;i<407;i++)
		{
			records_d20[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d20[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d20", records_d20[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record402: [a402, b0, c0, d2, e2, f0, g3]
		//record403: [a403, b1, c1, d3, e3, f1, g4]
		//record404: [a404, b0, c2, d0, e4, f2, g5]
		//record405: [a405, b1, c0, d1, e0, f3, g6]
		//record406: [a406, b0, c1, d2, e1, f4, g0]
		int pageCount2 = (int)Math.ceil(407.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d20 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d20"+ File.separator+i+".db");
				dir_d20.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("d20");
		String tableTrace2 = DBApp.getLastTrace("d20");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("xf", cols3);
		String [][] records_xf = new String[196][cols3.length];
		for(int i=0;i<196;i++)
		{
			records_xf[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xf[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xf", records_xf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4, l11, m9]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0]
		int pageCount3 = (int)Math.ceil(196.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xf"+ File.separator+i+".db");
				dir_xf.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("xf");
		String tableTrace3 = DBApp.getLastTrace("xf");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ph", cols4);
		String [][] records_ph = new String[203][cols4.length];
		for(int i=0;i<203;i++)
		{
			records_ph[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ph[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ph", records_ph[i]);
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
		int pageCount4 = (int)Math.ceil(203.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ph = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ph"+ File.separator+i+".db");
				dir_ph.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("ph");
		String tableTrace4 = DBApp.getLastTrace("ph");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vi", cols0);
		String [][] records_vi = new String[417][cols0.length];
		for(int i=0;i<417;i++)
		{
			records_vi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vi", records_vi[i]);
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
		int pageCount0 = (int)Math.ceil(417.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vi = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vi"+ File.separator+i+".db");
				dir_vi.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("vi");
		String tableTrace0 = DBApp.getLastTrace("vi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tgl", cols1);
		String [][] records_tgl = new String[107][cols1.length];
		for(int i=0;i<107;i++)
		{
			records_tgl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_tgl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("tgl", records_tgl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4]
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12, n5]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0, n6]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9, m1, n7]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10, m2, n8]
		int pageCount1 = (int)Math.ceil(107.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_tgl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "tgl"+ File.separator+i+".db");
				dir_tgl.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("tgl");
		String tableTrace1 = DBApp.getLastTrace("tgl");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b59", cols2);
		String [][] records_b59 = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			records_b59[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b59[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b59", records_b59[i]);
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
		int pageCount2 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b59 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b59"+ File.separator+i+".db");
				dir_b59.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("b59");
		String tableTrace2 = DBApp.getLastTrace("b59");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("biome", cols3);
		String [][] records_biome = new String[7][cols3.length];
		for(int i=0;i<7;i++)
		{
			records_biome[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_biome[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("biome", records_biome[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		int pageCount3 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_biome = new File(FileManager.directory.getAbsolutePath()+ File.separator + "biome"+ File.separator+i+".db");
				dir_biome.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("biome");
		String tableTrace3 = DBApp.getLastTrace("biome");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("o5gv", cols4);
		String [][] records_o5gv = new String[252][cols4.length];
		for(int i=0;i<252;i++)
		{
			records_o5gv[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o5gv[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o5gv", records_o5gv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6, j9, k7, l9, m2]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8, l10, m3]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4]
		int pageCount4 = (int)Math.ceil(252.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o5gv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o5gv"+ File.separator+i+".db");
				dir_o5gv.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("o5gv");
		String tableTrace4 = DBApp.getLastTrace("o5gv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uuv", cols0);
		String [][] records_uuv = new String[392][cols0.length];
		for(int i=0;i<392;i++)
		{
			records_uuv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uuv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uuv", records_uuv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record387: [a387, b1, c0, d3, e2, f3, g2, h3, i0, j7, k2, l3]
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7]
		int pageCount0 = (int)Math.ceil(392.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_uuv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "uuv"+ File.separator+i+".db");
				dir_uuv.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("uuv");
		String tableTrace0 = DBApp.getLastTrace("uuv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f4ud2", cols1);
		String [][] records_f4ud2 = new String[168][cols1.length];
		for(int i=0;i<168;i++)
		{
			records_f4ud2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f4ud2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f4ud2", records_f4ud2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record163: [a163, b1, c1, d3, e3, f1, g2]
		//record164: [a164, b0, c2, d0, e4, f2, g3]
		//record165: [a165, b1, c0, d1, e0, f3, g4]
		//record166: [a166, b0, c1, d2, e1, f4, g5]
		//record167: [a167, b1, c2, d3, e2, f5, g6]
		int pageCount1 = (int)Math.ceil(168.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_f4ud2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "f4ud2"+ File.separator+i+".db");
				dir_f4ud2.delete();
				randDel1++;
			}
		}
		DBApp.validateRecords("f4ud2");
		String tableTrace1 = DBApp.getLastTrace("f4ud2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace1.contains((randDel1*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l26e", cols2);
		String [][] records_l26e = new String[484][cols2.length];
		for(int i=0;i<484;i++)
		{
			records_l26e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l26e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l26e", records_l26e[i]);
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
		int pageCount2 = (int)Math.ceil(484.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l26e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l26e"+ File.separator+i+".db");
				dir_l26e.delete();
				randDel2++;
			}
		}
		DBApp.validateRecords("l26e");
		String tableTrace2 = DBApp.getLastTrace("l26e");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace2.contains((randDel2*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("npn8", cols3);
		String [][] records_npn8 = new String[86][cols3.length];
		for(int i=0;i<86;i++)
		{
			records_npn8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_npn8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("npn8", records_npn8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1]
		int pageCount3 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_npn8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "npn8"+ File.separator+i+".db");
				dir_npn8.delete();
				randDel3++;
			}
		}
		DBApp.validateRecords("npn8");
		String tableTrace3 = DBApp.getLastTrace("npn8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace3.contains((randDel3*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("rav3p", cols4);
		String [][] records_rav3p = new String[103][cols4.length];
		for(int i=0;i<103;i++)
		{
			records_rav3p[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_rav3p[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("rav3p", records_rav3p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2, i8, j8, k10, l2, m7, n0]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4, i1, j0, k1, l4, m9, n2]
		//record101: [a101, b1, c2, d1, e1, f5, g3, h5, i2, j1, k2, l5, m10, n3]
		//record102: [a102, b0, c0, d2, e2, f0, g4, h6, i3, j2, k3, l6, m11, n4]
		int pageCount4 = (int)Math.ceil(103.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rav3p = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rav3p"+ File.separator+i+".db");
				dir_rav3p.delete();
				randDel4++;
			}
		}
		DBApp.validateRecords("rav3p");
		String tableTrace4 = DBApp.getLastTrace("rav3p");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace4.contains((randDel4*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffectedTrace_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o93", cols0);
		String [][] records_o93 = new String[195][cols0.length];
		for(int i=0;i<195;i++)
		{
			records_o93[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o93[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o93", records_o93[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8]
		//record191: [a191, b1, c2, d3, e1, f5, g2, h7, i2, j1, k4, l11, m9, n9]
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10, n10]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12]
		int pageCount0 = (int)Math.ceil(195.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o93 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o93"+ File.separator+i+".db");
				dir_o93.delete();
				randDel0++;
			}
		}
		DBApp.validateRecords("o93");
		String tableTrace0 = DBApp.getLastTrace("o93");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a Validating a table with some pages deleted should report the number of missing records.", tableTrace0.contains((randDel0*DBApp.dataPageSize)+" records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("j83", cols0);
		String [][] records_j83 = new String[299][cols0.length];
		for(int i=0;i<299;i++)
		{
			records_j83[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j83[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j83", records_j83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8, n0]
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7, m9, n1]
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8, m10, n2]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9, m11, n3]
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1, l10, m12, n4]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("j83");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(299.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j83 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j83"+ File.separator+i+".db");
				dir_j83.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_j83[j]);
				}
			}
		}
		DBApp.recoverRecords("j83", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("j83");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("isd2l", cols1);
		String [][] records_isd2l = new String[418][cols1.length];
		for(int i=0;i<418;i++)
		{
			records_isd2l[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_isd2l[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("isd2l", records_isd2l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record413: [a413, b1, c2, d1, e3, f5, g0, h5, i8, j3, k6, l5, m10, n7, o8, p13, q5]
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4, k7, l6, m11, n8, o9, p14, q6]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5, k8, l7, m12, n9, o10, p15, q7]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6, k9, l8, m0, n10, o11, p0, q8]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7, k10, l9, m1, n11, o12, p1, q9]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("isd2l");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(418.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_isd2l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "isd2l"+ File.separator+i+".db");
				dir_isd2l.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_isd2l[j]);
				}
			}
		}
		DBApp.recoverRecords("isd2l", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("isd2l");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j7t", cols2);
		String [][] records_j7t = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_j7t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j7t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j7t", records_j7t[i]);
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
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("j7t");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j7t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j7t"+ File.separator+i+".db");
				dir_j7t.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_j7t[j]);
				}
			}
		}
		DBApp.recoverRecords("j7t", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("j7t");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ay09", cols0);
		String [][] records_ay09 = new String[204][cols0.length];
		for(int i=0;i<204;i++)
		{
			records_ay09[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ay09[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ay09", records_ay09[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ay09");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(204.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ay09 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ay09"+ File.separator+i+".db");
				dir_ay09.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ay09[j]);
				}
			}
		}
		DBApp.recoverRecords("ay09", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ay09");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("n2z", cols1);
		String [][] records_n2z = new String[492][cols1.length];
		for(int i=0;i<492;i++)
		{
			records_n2z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n2z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n2z", records_n2z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record487: [a487, b1, c1, d3, e2, f1, g4, h7, i1, j7, k3, l7, m6, n11, o7, p7, q11, r1]
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8, p8, q12, r2]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9, p9, q13, r3]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10, p10, q14, r4]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1, o11, p11, q15, r5]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("n2z");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(492.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n2z = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n2z"+ File.separator+i+".db");
				dir_n2z.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_n2z[j]);
				}
			}
		}
		DBApp.recoverRecords("n2z", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("n2z");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("byrjy", cols0);
		String [][] records_byrjy = new String[318][cols0.length];
		for(int i=0;i<318;i++)
		{
			records_byrjy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_byrjy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("byrjy", records_byrjy[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("byrjy");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(318.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_byrjy = new File(FileManager.directory.getAbsolutePath()+ File.separator + "byrjy"+ File.separator+i+".db");
				dir_byrjy.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_byrjy[j]);
				}
			}
		}
		DBApp.recoverRecords("byrjy", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("byrjy");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ue3", cols1);
		String [][] records_ue3 = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			records_ue3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ue3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ue3", records_ue3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("ue3");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ue3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ue3"+ File.separator+i+".db");
				dir_ue3.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ue3[j]);
				}
			}
		}
		DBApp.recoverRecords("ue3", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("ue3");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("hsuy2", cols2);
		String [][] records_hsuy2 = new String[461][cols2.length];
		for(int i=0;i<461;i++)
		{
			records_hsuy2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hsuy2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hsuy2", records_hsuy2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("hsuy2");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(461.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hsuy2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hsuy2"+ File.separator+i+".db");
				dir_hsuy2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_hsuy2[j]);
				}
			}
		}
		DBApp.recoverRecords("hsuy2", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("hsuy2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j01tq", cols0);
		String [][] records_j01tq = new String[303][cols0.length];
		for(int i=0;i<303;i++)
		{
			records_j01tq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j01tq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j01tq", records_j01tq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record298: [a298, b0, c1, d2, e3, f4, g4]
		//record299: [a299, b1, c2, d3, e4, f5, g5]
		//record300: [a300, b0, c0, d0, e0, f0, g6]
		//record301: [a301, b1, c1, d1, e1, f1, g0]
		//record302: [a302, b0, c2, d2, e2, f2, g1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("j01tq");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(303.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j01tq = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j01tq"+ File.separator+i+".db");
				dir_j01tq.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_j01tq[j]);
				}
			}
		}
		DBApp.recoverRecords("j01tq", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("j01tq");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("v44n", cols1);
		String [][] records_v44n = new String[244][cols1.length];
		for(int i=0;i<244;i++)
		{
			records_v44n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v44n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v44n", records_v44n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14, p15]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6, n2, o0, p0]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7, n3, o1, p1]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8, n4, o2, p2]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9, n5, o3, p3]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("v44n");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(244.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v44n = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v44n"+ File.separator+i+".db");
				dir_v44n.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_v44n[j]);
				}
			}
		}
		DBApp.recoverRecords("v44n", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("v44n");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("zs", cols2);
		String [][] records_zs = new String[144][cols2.length];
		for(int i=0;i<144;i++)
		{
			records_zs[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zs[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zs", records_zs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0]
		//record141: [a141, b1, c0, d1, e1, f3, g1, h5, i6, j1, k9, l9, m11, n1]
		//record142: [a142, b0, c1, d2, e2, f4, g2, h6, i7, j2, k10, l10, m12, n2]
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("zs");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(144.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zs = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zs"+ File.separator+i+".db");
				dir_zs.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_zs[j]);
				}
			}
		}
		DBApp.recoverRecords("zs", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("zs");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("wh", cols0);
		String [][] records_wh = new String[452][cols0.length];
		for(int i=0;i<452;i++)
		{
			records_wh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wh", records_wh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13, o12, p15, q5, r15, s10]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0, o13, p0, q6, r16, s11]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1, o14, p1, q7, r17, s12]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2, o0, p2, q8, r0, s13]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3, o1, p3, q9, r1, s14]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("wh");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(452.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wh = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wh"+ File.separator+i+".db");
				dir_wh.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_wh[j]);
				}
			}
		}
		DBApp.recoverRecords("wh", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("wh");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vq43", cols1);
		String [][] records_vq43 = new String[425][cols1.length];
		for(int i=0;i<425;i++)
		{
			records_vq43[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vq43[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vq43", records_vq43[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("vq43");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(425.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vq43 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vq43"+ File.separator+i+".db");
				dir_vq43.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_vq43[j]);
				}
			}
		}
		DBApp.recoverRecords("vq43", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("vq43");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l9xpu", cols2);
		String [][] records_l9xpu = new String[161][cols2.length];
		for(int i=0;i<161;i++)
		{
			records_l9xpu[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l9xpu[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l9xpu", records_l9xpu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record156: [a156, b0, c0, d0, e1, f0, g2, h4, i3, j6, k2, l0, m0, n2, o6]
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4, o8]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("l9xpu");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(161.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l9xpu = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l9xpu"+ File.separator+i+".db");
				dir_l9xpu.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_l9xpu[j]);
				}
			}
		}
		DBApp.recoverRecords("l9xpu", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("l9xpu");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("xtw", cols3);
		String [][] records_xtw = new String[368][cols3.length];
		for(int i=0;i<368;i++)
		{
			records_xtw[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xtw[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xtw", records_xtw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record363: [a363, b1, c0, d3, e3, f3, g6, h3, i3, j3, k0, l3, m12, n13, o3, p11, q6, r3, s2, t3]
		//record364: [a364, b0, c1, d0, e4, f4, g0, h4, i4, j4, k1, l4, m0, n0, o4, p12, q7, r4, s3, t4]
		//record365: [a365, b1, c2, d1, e0, f5, g1, h5, i5, j5, k2, l5, m1, n1, o5, p13, q8, r5, s4, t5]
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6, m2, n2, o6, p14, q9, r6, s5, t6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7, m3, n3, o7, p15, q10, r7, s6, t7]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("xtw");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(368.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xtw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xtw"+ File.separator+i+".db");
				dir_xtw.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_xtw[j]);
				}
			}
		}
		DBApp.recoverRecords("xtw", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("xtw");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dug", cols0);
		String [][] records_dug = new String[114][cols0.length];
		for(int i=0;i<114;i++)
		{
			records_dug[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dug[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dug", records_dug[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record109: [a109, b1, c1, d1, e4, f1, g4]
		//record110: [a110, b0, c2, d2, e0, f2, g5]
		//record111: [a111, b1, c0, d3, e1, f3, g6]
		//record112: [a112, b0, c1, d0, e2, f4, g0]
		//record113: [a113, b1, c2, d1, e3, f5, g1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("dug");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(114.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_dug = new File(FileManager.directory.getAbsolutePath()+ File.separator + "dug"+ File.separator+i+".db");
				dir_dug.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_dug[j]);
				}
			}
		}
		DBApp.recoverRecords("dug", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("dug");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("zoi4e", cols0);
		String [][] records_zoi4e = new String[162][cols0.length];
		for(int i=0;i<162;i++)
		{
			records_zoi4e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zoi4e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zoi4e", records_zoi4e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record157: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7, p13, q4]
		//record158: [a158, b0, c2, d2, e3, f2, g4, h6, i5, j8, k4, l2, m2, n4, o8, p14, q5]
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9, p15, q6]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10, p0, q7]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11, p1, q8]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("zoi4e");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(162.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zoi4e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zoi4e"+ File.separator+i+".db");
				dir_zoi4e.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_zoi4e[j]);
				}
			}
		}
		DBApp.recoverRecords("zoi4e", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("zoi4e");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("g6353", cols0);
		String [][] records_g6353 = new String[360][cols0.length];
		for(int i=0;i<360;i++)
		{
			records_g6353[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g6353[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g6353", records_g6353[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("g6353");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(360.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g6353 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g6353"+ File.separator+i+".db");
				dir_g6353.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_g6353[j]);
				}
			}
		}
		DBApp.recoverRecords("g6353", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("g6353");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cc", cols1);
		String [][] records_cc = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			records_cc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cc", records_cc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("cc");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cc"+ File.separator+i+".db");
				dir_cc.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_cc[j]);
				}
			}
		}
		DBApp.recoverRecords("cc", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("cc");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("zfk", cols2);
		String [][] records_zfk = new String[336][cols2.length];
		for(int i=0;i<336;i++)
		{
			records_zfk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zfk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zfk", records_zfk[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("zfk");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(336.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zfk = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zfk"+ File.separator+i+".db");
				dir_zfk.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_zfk[j]);
				}
			}
		}
		DBApp.recoverRecords("zfk", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("zfk");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("pve", cols3);
		String [][] records_pve = new String[350][cols3.length];
		for(int i=0;i<350;i++)
		{
			records_pve[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_pve[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("pve", records_pve[i]);
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
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("pve");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(350.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pve = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pve"+ File.separator+i+".db");
				dir_pve.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_pve[j]);
				}
			}
		}
		DBApp.recoverRecords("pve", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("pve");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("if", cols4);
		String [][] records_if = new String[276][cols4.length];
		for(int i=0;i<276;i++)
		{
			records_if[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_if[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("if", records_if[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record271: [a271, b1, c1, d3, e1, f1, g5, h7, i1, j1, k7]
		//record272: [a272, b0, c2, d0, e2, f2, g6, h0, i2, j2, k8]
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3, k9]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4, k10]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5, k0]
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("if");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(276.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_if = new File(FileManager.directory.getAbsolutePath()+ File.separator + "if"+ File.separator+i+".db");
				dir_if.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_if[j]);
				}
			}
		}
		DBApp.recoverRecords("if", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("if");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y40", cols0);
		String [][] records_y40 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_y40[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y40[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y40", records_y40[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("y40");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(8.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y40 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y40"+ File.separator+i+".db");
				dir_y40.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_y40[j]);
				}
			}
		}
		DBApp.recoverRecords("y40", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("y40");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pc90", cols1);
		String [][] records_pc90 = new String[201][cols1.length];
		for(int i=0;i<201;i++)
		{
			records_pc90[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pc90[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pc90", records_pc90[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("pc90");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(201.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pc90 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pc90"+ File.separator+i+".db");
				dir_pc90.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_pc90[j]);
				}
			}
		}
		DBApp.recoverRecords("pc90", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("pc90");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("wrpf5", cols2);
		String [][] records_wrpf5 = new String[271][cols2.length];
		for(int i=0;i<271;i++)
		{
			records_wrpf5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wrpf5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wrpf5", records_wrpf5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11, p10, q11]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12, p11, q12]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13, p12, q13]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9, k5, l5, m9, n3, o14, p13, q14]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0, k6, l6, m10, n4, o0, p14, q15]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("wrpf5");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(271.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wrpf5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wrpf5"+ File.separator+i+".db");
				dir_wrpf5.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_wrpf5[j]);
				}
			}
		}
		DBApp.recoverRecords("wrpf5", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("wrpf5");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("sp4", cols0);
		String [][] records_sp4 = new String[466][cols0.length];
		for(int i=0;i<466;i++)
		{
			records_sp4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sp4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sp4", records_sp4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5]
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("sp4");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(466.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_sp4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "sp4"+ File.separator+i+".db");
				dir_sp4.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_sp4[j]);
				}
			}
		}
		DBApp.recoverRecords("sp4", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("sp4");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ex", cols1);
		String [][] records_ex = new String[309][cols1.length];
		for(int i=0;i<309;i++)
		{
			records_ex[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ex[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ex", records_ex[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record304: [a304, b0, c1, d0, e4, f4, g3]
		//record305: [a305, b1, c2, d1, e0, f5, g4]
		//record306: [a306, b0, c0, d2, e1, f0, g5]
		//record307: [a307, b1, c1, d3, e2, f1, g6]
		//record308: [a308, b0, c2, d0, e3, f2, g0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("ex");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(309.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ex = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ex"+ File.separator+i+".db");
				dir_ex.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_ex[j]);
				}
			}
		}
		DBApp.recoverRecords("ex", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("ex");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vw7", cols2);
		String [][] records_vw7 = new String[323][cols2.length];
		for(int i=0;i<323;i++)
		{
			records_vw7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vw7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vw7", records_vw7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12, s14, t18]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13, s15, t19]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0, q14, r14, s16, t0]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13, o6, p1, q15, r15, s17, t1]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2, k3, l10, m10, n0, o7, p2, q16, r16, s18, t2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("vw7");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(323.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_vw7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "vw7"+ File.separator+i+".db");
				dir_vw7.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_vw7[j]);
				}
			}
		}
		DBApp.recoverRecords("vw7", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("vw7");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("cu61a", cols3);
		String [][] records_cu61a = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			records_cu61a[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_cu61a[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("cu61a", records_cu61a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4, p1, q15, r13]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("cu61a");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cu61a = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cu61a"+ File.separator+i+".db");
				dir_cu61a.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_cu61a[j]);
				}
			}
		}
		DBApp.recoverRecords("cu61a", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("cu61a");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ni1q", cols0);
		String [][] records_ni1q = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			records_ni1q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ni1q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ni1q", records_ni1q[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("ni1q");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ni1q = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ni1q"+ File.separator+i+".db");
				dir_ni1q.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_ni1q[j]);
				}
			}
		}
		DBApp.recoverRecords("ni1q", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("ni1q");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h87", cols1);
		String [][] records_h87 = new String[218][cols1.length];
		for(int i=0;i<218;i++)
		{
			records_h87[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h87[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h87", records_h87[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5, i6, j3, k4, l9, m5, n3]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6, i7, j4, k5, l10, m6, n4]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7, i8, j5, k6, l11, m7, n5]
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("h87");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(218.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h87 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h87"+ File.separator+i+".db");
				dir_h87.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_h87[j]);
				}
			}
		}
		DBApp.recoverRecords("h87", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("h87");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("yv", cols2);
		String [][] records_yv = new String[338][cols2.length];
		for(int i=0;i<338;i++)
		{
			records_yv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yv", records_yv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12, o4]
		//record335: [a335, b1, c2, d3, e0, f5, g6, h7, i2, j5, k5, l11, m10, n13, o5]
		//record336: [a336, b0, c0, d0, e1, f0, g0, h0, i3, j6, k6, l0, m11, n0, o6]
		//record337: [a337, b1, c1, d1, e2, f1, g1, h1, i4, j7, k7, l1, m12, n1, o7]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("yv");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(338.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yv = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yv"+ File.separator+i+".db");
				dir_yv.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_yv[j]);
				}
			}
		}
		DBApp.recoverRecords("yv", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("yv");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("yrs2", cols3);
		String [][] records_yrs2 = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			records_yrs2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_yrs2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("yrs2", records_yrs2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("yrs2");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yrs2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yrs2"+ File.separator+i+".db");
				dir_yrs2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_yrs2[j]);
				}
			}
		}
		DBApp.recoverRecords("yrs2", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("yrs2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("y4", cols4);
		String [][] records_y4 = new String[447][cols4.length];
		for(int i=0;i<447;i++)
		{
			records_y4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_y4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("y4", records_y4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0, n8, o7, p10, q0, r10, s5, t2]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1, n9, o8, p11, q1, r11, s6, t3]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4, l0, m2, n10, o9, p12, q2, r12, s7, t4]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5, l1, m3, n11, o10, p13, q3, r13, s8, t5]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2, m4, n12, o11, p14, q4, r14, s9, t6]
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("y4");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(447.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y4"+ File.separator+i+".db");
				dir_y4.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_y4[j]);
				}
			}
		}
		DBApp.recoverRecords("y4", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("y4");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u76", cols0);
		String [][] records_u76 = new String[370][cols0.length];
		for(int i=0;i<370;i++)
		{
			records_u76[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u76[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u76", records_u76[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record365: [a365, b1, c2, d1, e0, f5, g1]
		//record366: [a366, b0, c0, d2, e1, f0, g2]
		//record367: [a367, b1, c1, d3, e2, f1, g3]
		//record368: [a368, b0, c2, d0, e3, f2, g4]
		//record369: [a369, b1, c0, d1, e4, f3, g5]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("u76");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(370.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u76 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u76"+ File.separator+i+".db");
				dir_u76.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_u76[j]);
				}
			}
		}
		DBApp.recoverRecords("u76", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("u76");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h2", cols1);
		String [][] records_h2 = new String[481][cols1.length];
		for(int i=0;i<481;i++)
		{
			records_h2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h2", records_h2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0, o11, p12]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1, o12, p13]
		//record478: [a478, b0, c1, d2, e3, f4, g2, h6, i1, j8, k5, l10, m10, n2, o13, p14]
		//record479: [a479, b1, c2, d3, e4, f5, g3, h7, i2, j9, k6, l11, m11, n3, o14, p15]
		//record480: [a480, b0, c0, d0, e0, f0, g4, h0, i3, j0, k7, l0, m12, n4, o0, p0]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("h2");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(481.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h2"+ File.separator+i+".db");
				dir_h2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_h2[j]);
				}
			}
		}
		DBApp.recoverRecords("h2", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("h2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z28m", cols2);
		String [][] records_z28m = new String[41][cols2.length];
		for(int i=0;i<41;i++)
		{
			records_z28m[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z28m[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z28m", records_z28m[i]);
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
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("z28m");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z28m = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z28m"+ File.separator+i+".db");
				dir_z28m.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_z28m[j]);
				}
			}
		}
		DBApp.recoverRecords("z28m", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("z28m");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lng", cols3);
		String [][] records_lng = new String[130][cols3.length];
		for(int i=0;i<130;i++)
		{
			records_lng[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lng[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lng", records_lng[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("lng");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(130.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lng = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lng"+ File.separator+i+".db");
				dir_lng.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_lng[j]);
				}
			}
		}
		DBApp.recoverRecords("lng", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("lng");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("t3sc6", cols4);
		String [][] records_t3sc6 = new String[491][cols4.length];
		for(int i=0;i<491;i++)
		{
			records_t3sc6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_t3sc6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("t3sc6", records_t3sc6[i]);
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
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("t3sc6");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(491.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t3sc6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t3sc6"+ File.separator+i+".db");
				dir_t3sc6.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_t3sc6[j]);
				}
			}
		}
		DBApp.recoverRecords("t3sc6", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("t3sc6");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mc", cols0);
		String [][] records_mc = new String[178][cols0.length];
		for(int i=0;i<178;i++)
		{
			records_mc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mc[i][j] = cols0[j]+((i%(j+1)));
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
		//record173: [a173, b1, c2, d1, e3, f5, g5]
		//record174: [a174, b0, c0, d2, e4, f0, g6]
		//record175: [a175, b1, c1, d3, e0, f1, g0]
		//record176: [a176, b0, c2, d0, e1, f2, g1]
		//record177: [a177, b1, c0, d1, e2, f3, g2]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("mc");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(178.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mc = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mc"+ File.separator+i+".db");
				dir_mc.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_mc[j]);
				}
			}
		}
		DBApp.recoverRecords("mc", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("mc");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("o9c", cols1);
		String [][] records_o9c = new String[434][cols1.length];
		for(int i=0;i<434;i++)
		{
			records_o9c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o9c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o9c", records_o9c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record429: [a429, b1, c0, d1, e4, f3, g2, h5, i6, j9, k0, l9, m0, n9, o9, p13, q4, r15, s11, t9]
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10, p14, q5, r16, s12, t10]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15, q6, r17, s13, t11]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7, r0, s14, t12]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8, r1, s15, t13]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("o9c");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(434.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o9c = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o9c"+ File.separator+i+".db");
				dir_o9c.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_o9c[j]);
				}
			}
		}
		DBApp.recoverRecords("o9c", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("o9c");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("d9", cols2);
		String [][] records_d9 = new String[259][cols2.length];
		for(int i=0;i<259;i++)
		{
			records_d9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d9", records_d9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14, p14]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0, p15]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4, o1, p0]
		//record257: [a257, b1, c2, d1, e2, f5, g5, h1, i5, j7, k4, l5, m10, n5, o2, p1]
		//record258: [a258, b0, c0, d2, e3, f0, g6, h2, i6, j8, k5, l6, m11, n6, o3, p2]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("d9");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(259.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d9"+ File.separator+i+".db");
				dir_d9.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_d9[j]);
				}
			}
		}
		DBApp.recoverRecords("d9", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("d9");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zl5t2", cols3);
		String [][] records_zl5t2 = new String[362][cols3.length];
		for(int i=0;i<362;i++)
		{
			records_zl5t2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zl5t2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zl5t2", records_zl5t2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record357: [a357, b1, c0, d1, e2, f3, g0, h5, i6, j7, k5, l9]
		//record358: [a358, b0, c1, d2, e3, f4, g1, h6, i7, j8, k6, l10]
		//record359: [a359, b1, c2, d3, e4, f5, g2, h7, i8, j9, k7, l11]
		//record360: [a360, b0, c0, d0, e0, f0, g3, h0, i0, j0, k8, l0]
		//record361: [a361, b1, c1, d1, e1, f1, g4, h1, i1, j1, k9, l1]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("zl5t2");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(362.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zl5t2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zl5t2"+ File.separator+i+".db");
				dir_zl5t2.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_zl5t2[j]);
				}
			}
		}
		DBApp.recoverRecords("zl5t2", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("zl5t2");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i8b", cols0);
		String [][] records_i8b = new String[271][cols0.length];
		for(int i=0;i<271;i++)
		{
			records_i8b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i8b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i8b", records_i8b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8]
		//record269: [a269, b1, c2, d1, e4, f5, g3, h5, i8, j9]
		//record270: [a270, b0, c0, d2, e0, f0, g4, h6, i0, j0]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("i8b");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(271.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i8b = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i8b"+ File.separator+i+".db");
				dir_i8b.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_i8b[j]);
				}
			}
		}
		DBApp.recoverRecords("i8b", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("i8b");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hj0o", cols1);
		String [][] records_hj0o = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			records_hj0o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hj0o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hj0o", records_hj0o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8]
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7, i6, j7, k10]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("hj0o");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hj0o = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hj0o"+ File.separator+i+".db");
				dir_hj0o.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_hj0o[j]);
				}
			}
		}
		DBApp.recoverRecords("hj0o", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("hj0o");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("d0w", cols2);
		String [][] records_d0w = new String[462][cols2.length];
		for(int i=0;i<462;i++)
		{
			records_d0w[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d0w[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d0w", records_d0w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1, m2, n9, o7, p9, q15, r7, s1, t17]
		//record458: [a458, b0, c2, d2, e3, f2, g3, h2, i8, j8, k7, l2, m3, n10, o8, p10, q16, r8, s2, t18]
		//record459: [a459, b1, c0, d3, e4, f3, g4, h3, i0, j9, k8, l3, m4, n11, o9, p11, q0, r9, s3, t19]
		//record460: [a460, b0, c1, d0, e0, f4, g5, h4, i1, j0, k9, l4, m5, n12, o10, p12, q1, r10, s4, t0]
		//record461: [a461, b1, c2, d1, e1, f5, g6, h5, i2, j1, k10, l5, m6, n13, o11, p13, q2, r11, s5, t1]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("d0w");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(462.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d0w = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d0w"+ File.separator+i+".db");
				dir_d0w.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_d0w[j]);
				}
			}
		}
		DBApp.recoverRecords("d0w", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("d0w");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("met28", cols3);
		String [][] records_met28 = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			records_met28[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_met28[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("met28", records_met28[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("met28");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_met28 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "met28"+ File.separator+i+".db");
				dir_met28.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_met28[j]);
				}
			}
		}
		DBApp.recoverRecords("met28", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("met28");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xn63m", cols0);
		String [][] records_xn63m = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_xn63m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xn63m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xn63m", records_xn63m[i]);
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
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("xn63m");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xn63m = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xn63m"+ File.separator+i+".db");
				dir_xn63m.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_xn63m[j]);
				}
			}
		}
		DBApp.recoverRecords("xn63m", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("xn63m");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s41", cols1);
		String [][] records_s41 = new String[27][cols1.length];
		for(int i=0;i<27;i++)
		{
			records_s41[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s41[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s41", records_s41[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("s41");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s41 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s41"+ File.separator+i+".db");
				dir_s41.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_s41[j]);
				}
			}
		}
		DBApp.recoverRecords("s41", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("s41");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u8cr", cols2);
		String [][] records_u8cr = new String[43][cols2.length];
		for(int i=0;i<43;i++)
		{
			records_u8cr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u8cr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u8cr", records_u8cr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("u8cr");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u8cr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u8cr"+ File.separator+i+".db");
				dir_u8cr.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_u8cr[j]);
				}
			}
		}
		DBApp.recoverRecords("u8cr", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("u8cr");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m00", cols0);
		String [][] records_m00 = new String[174][cols0.length];
		for(int i=0;i<174;i++)
		{
			records_m00[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m00[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m00", records_m00[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record169: [a169, b1, c1, d1, e4, f1, g1]
		//record170: [a170, b0, c2, d2, e0, f2, g2]
		//record171: [a171, b1, c0, d3, e1, f3, g3]
		//record172: [a172, b0, c1, d0, e2, f4, g4]
		//record173: [a173, b1, c2, d1, e3, f5, g5]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("m00");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(174.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m00 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m00"+ File.separator+i+".db");
				dir_m00.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_m00[j]);
				}
			}
		}
		DBApp.recoverRecords("m00", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("m00");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("x15ug", cols1);
		String [][] records_x15ug = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			records_x15ug[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x15ug[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x15ug", records_x15ug[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2, r16]
		//record71: [a71, b1, c2, d3, e1, f5, g1, h7, i8, j1, k5, l11, m6, n1, o11, p7, q3, r17]
		//record72: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7, n2, o12, p8, q4, r0]
		//record73: [a73, b1, c1, d1, e3, f1, g3, h1, i1, j3, k7, l1, m8, n3, o13, p9, q5, r1]
		//record74: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4, o14, p10, q6, r2]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("x15ug");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x15ug = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x15ug"+ File.separator+i+".db");
				dir_x15ug.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_x15ug[j]);
				}
			}
		}
		DBApp.recoverRecords("x15ug", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("x15ug");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y9l33", cols2);
		String [][] records_y9l33 = new String[231][cols2.length];
		for(int i=0;i<231;i++)
		{
			records_y9l33[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y9l33[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y9l33", records_y9l33[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("y9l33");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(231.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y9l33 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y9l33"+ File.separator+i+".db");
				dir_y9l33.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_y9l33[j]);
				}
			}
		}
		DBApp.recoverRecords("y9l33", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("y9l33");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("yt7", cols3);
		String [][] records_yt7 = new String[384][cols3.length];
		for(int i=0;i<384;i++)
		{
			records_yt7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_yt7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("yt7", records_yt7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5, l7, m2, n1, o4, p11, q5, r1, s18]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6, l8, m3, n2, o5, p12, q6, r2, s0]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7, l9, m4, n3, o6, p13, q7, r3, s1]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8, l10, m5, n4, o7, p14, q8, r4, s2]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9, l11, m6, n5, o8, p15, q9, r5, s3]
		ArrayList<String[]> selectBeforeRecovery3 = DBApp.select("yt7");
		ArrayList<String[]> missing3 = new ArrayList<String[]>();
		int pageCount3 = (int)Math.ceil(384.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yt7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yt7"+ File.separator+i+".db");
				dir_yt7.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing3.add(records_yt7[j]);
				}
			}
		}
		DBApp.recoverRecords("yt7", missing3);
		ArrayList<String[]> selectAfterRecovery3 = DBApp.select("yt7");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery3.size(), selectAfterRecovery3.size());
		for(int i = 0; i < selectBeforeRecovery3.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery3.get(i)), Arrays.toString(selectAfterRecovery3.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g4", cols4);
		String [][] records_g4 = new String[288][cols4.length];
		for(int i=0;i<288;i++)
		{
			records_g4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g4", records_g4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record283: [a283, b1, c1, d3, e3, f1, g3]
		//record284: [a284, b0, c2, d0, e4, f2, g4]
		//record285: [a285, b1, c0, d1, e0, f3, g5]
		//record286: [a286, b0, c1, d2, e1, f4, g6]
		//record287: [a287, b1, c2, d3, e2, f5, g0]
		ArrayList<String[]> selectBeforeRecovery4 = DBApp.select("g4");
		ArrayList<String[]> missing4 = new ArrayList<String[]>();
		int pageCount4 = (int)Math.ceil(288.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g4"+ File.separator+i+".db");
				dir_g4.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing4.add(records_g4[j]);
				}
			}
		}
		DBApp.recoverRecords("g4", missing4);
		ArrayList<String[]> selectAfterRecovery4 = DBApp.select("g4");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery4.size(), selectAfterRecovery4.size());
		for(int i = 0; i < selectBeforeRecovery4.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery4.get(i)), Arrays.toString(selectAfterRecovery4.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d44", cols0);
		String [][] records_d44 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			records_d44[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d44[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d44", records_d44[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("d44");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d44 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d44"+ File.separator+i+".db");
				dir_d44.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_d44[j]);
				}
			}
		}
		DBApp.recoverRecords("d44", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("d44");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("q55o", cols0);
		String [][] records_q55o = new String[322][cols0.length];
		for(int i=0;i<322;i++)
		{
			records_q55o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q55o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q55o", records_q55o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5, n9, o2, p13, q11, r11, s13, t17]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6, n10, o3, p14, q12, r12, s14, t18]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9, k0, l7, m7, n11, o4, p15, q13, r13, s15, t19]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0, k1, l8, m8, n12, o5, p0, q14, r14, s16, t0]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1, k2, l9, m9, n13, o6, p1, q15, r15, s17, t1]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("q55o");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(322.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q55o = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q55o"+ File.separator+i+".db");
				dir_q55o.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_q55o[j]);
				}
			}
		}
		DBApp.recoverRecords("q55o", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("q55o");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e0i", cols1);
		String [][] records_e0i = new String[127][cols1.length];
		for(int i=0;i<127;i++)
		{
			records_e0i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e0i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e0i", records_e0i[i]);
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
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("e0i");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(127.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_e0i = new File(FileManager.directory.getAbsolutePath()+ File.separator + "e0i"+ File.separator+i+".db");
				dir_e0i.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_e0i[j]);
				}
			}
		}
		DBApp.recoverRecords("e0i", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("e0i");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableRecover_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("u22e", cols0);
		String [][] records_u22e = new String[412][cols0.length];
		for(int i=0;i<412;i++)
		{
			records_u22e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u22e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u22e", records_u22e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2]
		//record410: [a410, b0, c2, d2, e0, f2, g4, h2, i5, j0, k3]
		//record411: [a411, b1, c0, d3, e1, f3, g5, h3, i6, j1, k4]
		ArrayList<String[]> selectBeforeRecovery0 = DBApp.select("u22e");
		ArrayList<String[]> missing0 = new ArrayList<String[]>();
		int pageCount0 = (int)Math.ceil(412.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u22e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u22e"+ File.separator+i+".db");
				dir_u22e.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing0.add(records_u22e[j]);
				}
			}
		}
		DBApp.recoverRecords("u22e", missing0);
		ArrayList<String[]> selectAfterRecovery0 = DBApp.select("u22e");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery0.size(), selectAfterRecovery0.size());
		for(int i = 0; i < selectBeforeRecovery0.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery0.get(i)), Arrays.toString(selectAfterRecovery0.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i119", cols1);
		String [][] records_i119 = new String[318][cols1.length];
		for(int i=0;i<318;i++)
		{
			records_i119[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i119[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i119", records_i119[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9]
		ArrayList<String[]> selectBeforeRecovery1 = DBApp.select("i119");
		ArrayList<String[]> missing1 = new ArrayList<String[]>();
		int pageCount1 = (int)Math.ceil(318.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i119 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i119"+ File.separator+i+".db");
				dir_i119.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing1.add(records_i119[j]);
				}
			}
		}
		DBApp.recoverRecords("i119", missing1);
		ArrayList<String[]> selectAfterRecovery1 = DBApp.select("i119");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery1.size(), selectAfterRecovery1.size());
		for(int i = 0; i < selectBeforeRecovery1.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery1.get(i)), Arrays.toString(selectAfterRecovery1.get(i)));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("re1", cols2);
		String [][] records_re1 = new String[278][cols2.length];
		for(int i=0;i<278;i++)
		{
			records_re1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_re1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("re1", records_re1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record273: [a273, b1, c0, d1, e3, f3, g0, h1, i3, j3]
		//record274: [a274, b0, c1, d2, e4, f4, g1, h2, i4, j4]
		//record275: [a275, b1, c2, d3, e0, f5, g2, h3, i5, j5]
		//record276: [a276, b0, c0, d0, e1, f0, g3, h4, i6, j6]
		//record277: [a277, b1, c1, d1, e2, f1, g4, h5, i7, j7]
		ArrayList<String[]> selectBeforeRecovery2 = DBApp.select("re1");
		ArrayList<String[]> missing2 = new ArrayList<String[]>();
		int pageCount2 = (int)Math.ceil(278.0/DBApp.dataPageSize);
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_re1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "re1"+ File.separator+i+".db");
				dir_re1.delete();
				for(int j=i*DBApp.dataPageSize; j < i * DBApp.dataPageSize + DBApp.dataPageSize; j++)
				{
					missing2.add(records_re1[j]);
				}
			}
		}
		DBApp.recoverRecords("re1", missing2);
		ArrayList<String[]> selectAfterRecovery2 = DBApp.select("re1");
		assertEquals("The sizes of select lists before and after recovery should be of equal sizes.", selectBeforeRecovery2.size(), selectAfterRecovery2.size());
		for(int i = 0; i < selectBeforeRecovery2.size(); i++)
		{
			assertEquals("The recovery process should result in returning records to their original location.", Arrays.toString(selectBeforeRecovery2.get(i)), Arrays.toString(selectAfterRecovery2.get(i)));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1301);
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