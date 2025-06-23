package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_01
{

	
	@Test(timeout = 1000000)
	public void TableValidationSound_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"ykh","e7810","d0","fz2","lcv","ea","afo"};
		DBApp.createTable("yunp4", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_yunp4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yunp4[j] = genRandString();
			}
			DBApp.insert("yunp4", record_yunp4);
		}
		ArrayList<String[]> tr_yunp4 =DBApp.validateRecords("yunp4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_yunp4.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"b5g","h6","kir9","bav9","m84","x62","rf"};
		DBApp.createTable("h32l", cols1);
		for(int i=0;i<74;i++)
		{
			String [] record_h32l = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h32l[j] = genRandString();
			}
			DBApp.insert("h32l", record_h32l);
		}
		ArrayList<String[]> tr_h32l =DBApp.validateRecords("h32l");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_h32l.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"sn6","w92o","z3","f2nx","xfu","p2","ad","gash"};
		DBApp.createTable("z88o7", cols2);
		for(int i=0;i<12;i++)
		{
			String [] record_z88o7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_z88o7[j] = genRandString();
			}
			DBApp.insert("z88o7", record_z88o7);
		}
		ArrayList<String[]> tr_z88o7 =DBApp.validateRecords("z88o7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_z88o7.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"xh","vv4l"};
		DBApp.createTable("xlt", cols3);
		for(int i=0;i<93;i++)
		{
			String [] record_xlt = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_xlt[j] = genRandString();
			}
			DBApp.insert("xlt", record_xlt);
		}
		ArrayList<String[]> tr_xlt =DBApp.validateRecords("xlt");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_xlt.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"qax","se80","uq39","g5","m2uda"};
		DBApp.createTable("m47", cols0);
		for(int i=0;i<85;i++)
		{
			String [] record_m47 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m47[j] = genRandString();
			}
			DBApp.insert("m47", record_m47);
		}
		ArrayList<String[]> tr_m47 =DBApp.validateRecords("m47");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_m47.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"p305","i84","cu7dy","z8i","sqmd8","v4i","f2yi2","p90p","l9i","ofq2d"};
		DBApp.createTable("x65", cols0);
		for(int i=0;i<66;i++)
		{
			String [] record_x65 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x65[j] = genRandString();
			}
			DBApp.insert("x65", record_x65);
		}
		ArrayList<String[]> tr_x65 =DBApp.validateRecords("x65");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x65.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"wl4"};
		DBApp.createTable("fk896", cols1);
		for(int i=0;i<38;i++)
		{
			String [] record_fk896 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fk896[j] = genRandString();
			}
			DBApp.insert("fk896", record_fk896);
		}
		ArrayList<String[]> tr_fk896 =DBApp.validateRecords("fk896");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_fk896.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"ijy0","mk0","luaph"};
		DBApp.createTable("cr1o", cols2);
		for(int i=0;i<5;i++)
		{
			String [] record_cr1o = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_cr1o[j] = genRandString();
			}
			DBApp.insert("cr1o", record_cr1o);
		}
		ArrayList<String[]> tr_cr1o =DBApp.validateRecords("cr1o");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_cr1o.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"yvmb7","wu4z8","l9b","xgh","yu81","o3bl7","nat","fzu9","jfd","whbd8"};
		DBApp.createTable("o898", cols0);
		for(int i=0;i<4;i++)
		{
			String [] record_o898 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_o898[j] = genRandString();
			}
			DBApp.insert("o898", record_o898);
		}
		ArrayList<String[]> tr_o898 =DBApp.validateRecords("o898");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_o898.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"g2tf","zwc","x13","a4q15"};
		DBApp.createTable("z8t", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_z8t = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z8t[j] = genRandString();
			}
			DBApp.insert("z8t", record_z8t);
		}
		ArrayList<String[]> tr_z8t =DBApp.validateRecords("z8t");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_z8t.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"ik","e7","zg5"};
		DBApp.createTable("yqq", cols1);
		for(int i=0;i<71;i++)
		{
			String [] record_yqq = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_yqq[j] = genRandString();
			}
			DBApp.insert("yqq", record_yqq);
		}
		ArrayList<String[]> tr_yqq =DBApp.validateRecords("yqq");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_yqq.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"sn","l7","x9","pc","jm","af0"};
		DBApp.createTable("e3", cols2);
		for(int i=0;i<4;i++)
		{
			String [] record_e3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e3[j] = genRandString();
			}
			DBApp.insert("e3", record_e3);
		}
		ArrayList<String[]> tr_e3 =DBApp.validateRecords("e3");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e3.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"z249","xwu","x649","fr"};
		DBApp.createTable("wrpd", cols3);
		for(int i=0;i<27;i++)
		{
			String [] record_wrpd = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_wrpd[j] = genRandString();
			}
			DBApp.insert("wrpd", record_wrpd);
		}
		ArrayList<String[]> tr_wrpd =DBApp.validateRecords("wrpd");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_wrpd.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"so559","o15","b2","tnx"};
		DBApp.createTable("k5o", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_k5o = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k5o[j] = genRandString();
			}
			DBApp.insert("k5o", record_k5o);
		}
		ArrayList<String[]> tr_k5o =DBApp.validateRecords("k5o");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k5o.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"t86","nuu9","r5t4","y6","ry6"};
		DBApp.createTable("u55", cols1);
		for(int i=0;i<89;i++)
		{
			String [] record_u55 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_u55[j] = genRandString();
			}
			DBApp.insert("u55", record_u55);
		}
		ArrayList<String[]> tr_u55 =DBApp.validateRecords("u55");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_u55.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"k29","gn3","he"};
		DBApp.createTable("qw7", cols0);
		for(int i=0;i<30;i++)
		{
			String [] record_qw7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qw7[j] = genRandString();
			}
			DBApp.insert("qw7", record_qw7);
		}
		ArrayList<String[]> tr_qw7 =DBApp.validateRecords("qw7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_qw7.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"qg"};
		DBApp.createTable("gd6q", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_gd6q = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_gd6q[j] = genRandString();
			}
			DBApp.insert("gd6q", record_gd6q);
		}
		ArrayList<String[]> tr_gd6q =DBApp.validateRecords("gd6q");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gd6q.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"a0j","v8r","m0cs1","ty39","g60","iw"};
		DBApp.createTable("u21", cols2);
		for(int i=0;i<80;i++)
		{
			String [] record_u21 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_u21[j] = genRandString();
			}
			DBApp.insert("u21", record_u21);
		}
		ArrayList<String[]> tr_u21 =DBApp.validateRecords("u21");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_u21.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"su","s8r","e83","z01vo","lwi","h3cdl","r2iy","e23v","cd95l","yw"};
		DBApp.createTable("n29", cols0);
		for(int i=0;i<79;i++)
		{
			String [] record_n29 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n29[j] = genRandString();
			}
			DBApp.insert("n29", record_n29);
		}
		ArrayList<String[]> tr_n29 =DBApp.validateRecords("n29");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_n29.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"d4d","ql","o2d0m","m09"};
		DBApp.createTable("k7o40", cols1);
		for(int i=0;i<47;i++)
		{
			String [] record_k7o40 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_k7o40[j] = genRandString();
			}
			DBApp.insert("k7o40", record_k7o40);
		}
		ArrayList<String[]> tr_k7o40 =DBApp.validateRecords("k7o40");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k7o40.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"bnp","r04","c61j","nfo","vjdz"};
		DBApp.createTable("e71", cols2);
		for(int i=0;i<91;i++)
		{
			String [] record_e71 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e71[j] = genRandString();
			}
			DBApp.insert("e71", record_e71);
		}
		ArrayList<String[]> tr_e71 =DBApp.validateRecords("e71");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e71.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"up","x9y33","zmw22","tw6j4","g8"};
		DBApp.createTable("p4", cols3);
		for(int i=0;i<65;i++)
		{
			String [] record_p4 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_p4[j] = genRandString();
			}
			DBApp.insert("p4", record_p4);
		}
		ArrayList<String[]> tr_p4 =DBApp.validateRecords("p4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_p4.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"guu5f"};
		DBApp.createTable("dd", cols4);
		for(int i=0;i<72;i++)
		{
			String [] record_dd = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_dd[j] = genRandString();
			}
			DBApp.insert("dd", record_dd);
		}
		ArrayList<String[]> tr_dd =DBApp.validateRecords("dd");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_dd.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"eg87p","a9"};
		DBApp.createTable("jfx", cols0);
		for(int i=0;i<65;i++)
		{
			String [] record_jfx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jfx[j] = genRandString();
			}
			DBApp.insert("jfx", record_jfx);
		}
		ArrayList<String[]> tr_jfx =DBApp.validateRecords("jfx");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_jfx.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"i00","viel3","c1c","hwe9v","d31mx","v5269","q4","o993","v2t","o7t95"};
		DBApp.createTable("o17", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_o17 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_o17[j] = genRandString();
			}
			DBApp.insert("o17", record_o17);
		}
		ArrayList<String[]> tr_o17 =DBApp.validateRecords("o17");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_o17.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"gz8","z9f","tw77","h9"};
		DBApp.createTable("uz", cols1);
		for(int i=0;i<10;i++)
		{
			String [] record_uz = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uz[j] = genRandString();
			}
			DBApp.insert("uz", record_uz);
		}
		ArrayList<String[]> tr_uz =DBApp.validateRecords("uz");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_uz.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"ke","d64","q7a","x49","nv1","fcs"};
		DBApp.createTable("c8", cols2);
		for(int i=0;i<41;i++)
		{
			String [] record_c8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_c8[j] = genRandString();
			}
			DBApp.insert("c8", record_c8);
		}
		ArrayList<String[]> tr_c8 =DBApp.validateRecords("c8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"k2w","hzkgb","s53a9","o3qp6","t241","je9j","h4","qz4","tf398"};
		DBApp.createTable("zor7", cols3);
		for(int i=0;i<90;i++)
		{
			String [] record_zor7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_zor7[j] = genRandString();
			}
			DBApp.insert("zor7", record_zor7);
		}
		ArrayList<String[]> tr_zor7 =DBApp.validateRecords("zor7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_zor7.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"qc","zoh3b","cc","rs4","o342i","pg42","w8","j3cjt","x56","sjb8"};
		DBApp.createTable("wdm0", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_wdm0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_wdm0[j] = genRandString();
			}
			DBApp.insert("wdm0", record_wdm0);
		}
		ArrayList<String[]> tr_wdm0 =DBApp.validateRecords("wdm0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_wdm0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"z12","t827"};
		DBApp.createTable("mn", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_mn = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mn[j] = genRandString();
			}
			DBApp.insert("mn", record_mn);
		}
		ArrayList<String[]> tr_mn =DBApp.validateRecords("mn");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_mn.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a0","fs","qtn","g57","i7d1","vhaz","ap","qe0","elu7","w21n"};
		DBApp.createTable("e8k", cols0);
		for(int i=0;i<59;i++)
		{
			String [] record_e8k = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_e8k[j] = genRandString();
			}
			DBApp.insert("e8k", record_e8k);
		}
		ArrayList<String[]> tr_e8k =DBApp.validateRecords("e8k");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e8k.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"i9","rew","me6cx","z4w"};
		DBApp.createTable("e0", cols1);
		for(int i=0;i<93;i++)
		{
			String [] record_e0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e0[j] = genRandString();
			}
			DBApp.insert("e0", record_e0);
		}
		ArrayList<String[]> tr_e0 =DBApp.validateRecords("e0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"l2qwt","wr","t7u"};
		DBApp.createTable("v79", cols2);
		for(int i=0;i<3;i++)
		{
			String [] record_v79 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_v79[j] = genRandString();
			}
			DBApp.insert("v79", record_v79);
		}
		ArrayList<String[]> tr_v79 =DBApp.validateRecords("v79");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_v79.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"s2","mq8c","u5","tk8ep","wxi0b","h3s","g405"};
		DBApp.createTable("k0q", cols3);
		for(int i=0;i<40;i++)
		{
			String [] record_k0q = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_k0q[j] = genRandString();
			}
			DBApp.insert("k0q", record_k0q);
		}
		ArrayList<String[]> tr_k0q =DBApp.validateRecords("k0q");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k0q.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"v7"};
		DBApp.createTable("ye", cols4);
		for(int i=0;i<80;i++)
		{
			String [] record_ye = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_ye[j] = genRandString();
			}
			DBApp.insert("ye", record_ye);
		}
		ArrayList<String[]> tr_ye =DBApp.validateRecords("ye");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ye.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"i7"};
		DBApp.createTable("f7bo", cols0);
		for(int i=0;i<82;i++)
		{
			String [] record_f7bo = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f7bo[j] = genRandString();
			}
			DBApp.insert("f7bo", record_f7bo);
		}
		ArrayList<String[]> tr_f7bo =DBApp.validateRecords("f7bo");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_f7bo.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"pte","p6","i4f","qmm","p2","tj"};
		DBApp.createTable("nj9", cols1);
		for(int i=0;i<45;i++)
		{
			String [] record_nj9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_nj9[j] = genRandString();
			}
			DBApp.insert("nj9", record_nj9);
		}
		ArrayList<String[]> tr_nj9 =DBApp.validateRecords("nj9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_nj9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"d3e","h6"};
		DBApp.createTable("cb", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_cb = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_cb[j] = genRandString();
			}
			DBApp.insert("cb", record_cb);
		}
		ArrayList<String[]> tr_cb =DBApp.validateRecords("cb");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_cb.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"cq43u","u80","bsa","f3rl","p16"};
		DBApp.createTable("r00", cols3);
		for(int i=0;i<24;i++)
		{
			String [] record_r00 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_r00[j] = genRandString();
			}
			DBApp.insert("r00", record_r00);
		}
		ArrayList<String[]> tr_r00 =DBApp.validateRecords("r00");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_r00.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"yi2","v4","k9p","no"};
		DBApp.createTable("c10", cols0);
		for(int i=0;i<68;i++)
		{
			String [] record_c10 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c10[j] = genRandString();
			}
			DBApp.insert("c10", record_c10);
		}
		ArrayList<String[]> tr_c10 =DBApp.validateRecords("c10");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c10.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"lz7","fujbt","p8w"};
		DBApp.createTable("kt3", cols1);
		for(int i=0;i<40;i++)
		{
			String [] record_kt3 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_kt3[j] = genRandString();
			}
			DBApp.insert("kt3", record_kt3);
		}
		ArrayList<String[]> tr_kt3 =DBApp.validateRecords("kt3");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_kt3.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"m5i82"};
		DBApp.createTable("zgv", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_zgv = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_zgv[j] = genRandString();
			}
			DBApp.insert("zgv", record_zgv);
		}
		ArrayList<String[]> tr_zgv =DBApp.validateRecords("zgv");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_zgv.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"bz","pk88","yxs","jqf","d2","d247g"};
		DBApp.createTable("ge", cols3);
		for(int i=0;i<66;i++)
		{
			String [] record_ge = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ge[j] = genRandString();
			}
			DBApp.insert("ge", record_ge);
		}
		ArrayList<String[]> tr_ge =DBApp.validateRecords("ge");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ge.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"kd3","jv595","wgu","i3","yv12","rgt0s"};
		DBApp.createTable("pcq", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_pcq = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pcq[j] = genRandString();
			}
			DBApp.insert("pcq", record_pcq);
		}
		ArrayList<String[]> tr_pcq =DBApp.validateRecords("pcq");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_pcq.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"z4a","qj2","vyf","lzq6"};
		DBApp.createTable("hmh", cols0);
		for(int i=0;i<90;i++)
		{
			String [] record_hmh = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_hmh[j] = genRandString();
			}
			DBApp.insert("hmh", record_hmh);
		}
		ArrayList<String[]> tr_hmh =DBApp.validateRecords("hmh");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_hmh.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"gx4","exn","t710","uo7","vg","wpf1f","tci","s1cgj","u31w8","h5"};
		DBApp.createTable("i8", cols1);
		for(int i=0;i<58;i++)
		{
			String [] record_i8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i8[j] = genRandString();
			}
			DBApp.insert("i8", record_i8);
		}
		ArrayList<String[]> tr_i8 =DBApp.validateRecords("i8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"ca7","bq0w","a62n5","p426","b4gz1","dz0","wd6","q195"};
		DBApp.createTable("f1g14", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_f1g14 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_f1g14[j] = genRandString();
			}
			DBApp.insert("f1g14", record_f1g14);
		}
		ArrayList<String[]> tr_f1g14 =DBApp.validateRecords("f1g14");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_f1g14.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"wq","wpo5","amw","s2w73","v6s","l065","gqo","k7","cns2l"};
		DBApp.createTable("qfwn5", cols3);
		for(int i=0;i<22;i++)
		{
			String [] record_qfwn5 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_qfwn5[j] = genRandString();
			}
			DBApp.insert("qfwn5", record_qfwn5);
		}
		ArrayList<String[]> tr_qfwn5 =DBApp.validateRecords("qfwn5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_qfwn5.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"z07","xs1ky"};
		DBApp.createTable("b3g", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_b3g = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_b3g[j] = genRandString();
			}
			DBApp.insert("b3g", record_b3g);
		}
		ArrayList<String[]> tr_b3g =DBApp.validateRecords("b3g");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_b3g.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"k0","r0l","n8m","y83"};
		DBApp.createTable("e61", cols0);
		for(int i=0;i<10;i++)
		{
			String [] record_e61 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_e61[j] = genRandString();
			}
			DBApp.insert("e61", record_e61);
		}
		ArrayList<String[]> tr_e61 =DBApp.validateRecords("e61");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e61.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"m2u","z1g44","i3n","azc5g"};
		DBApp.createTable("bg", cols1);
		for(int i=0;i<62;i++)
		{
			String [] record_bg = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_bg[j] = genRandString();
			}
			DBApp.insert("bg", record_bg);
		}
		ArrayList<String[]> tr_bg =DBApp.validateRecords("bg");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_bg.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"cmg","uh","x4","c76","w5","hkv","a4c","w33","l32","j7"};
		DBApp.createTable("m0j9h", cols2);
		for(int i=0;i<8;i++)
		{
			String [] record_m0j9h = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m0j9h[j] = genRandString();
			}
			DBApp.insert("m0j9h", record_m0j9h);
		}
		ArrayList<String[]> tr_m0j9h =DBApp.validateRecords("m0j9h");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_m0j9h.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"w16u","makg","s6q","t89","os64","hb9mf","je"};
		DBApp.createTable("pb", cols3);
		for(int i=0;i<53;i++)
		{
			String [] record_pb = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_pb[j] = genRandString();
			}
			DBApp.insert("pb", record_pb);
		}
		ArrayList<String[]> tr_pb =DBApp.validateRecords("pb");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_pb.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"b8d","l8i","le4","r801","mz","h8h","p4","y74bn","kr"};
		DBApp.createTable("bwe", cols0);
		for(int i=0;i<78;i++)
		{
			String [] record_bwe = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_bwe[j] = genRandString();
			}
			DBApp.insert("bwe", record_bwe);
		}
		ArrayList<String[]> tr_bwe =DBApp.validateRecords("bwe");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_bwe.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"t62","t24","ftv","czu","mhe1","b6"};
		DBApp.createTable("gp", cols1);
		for(int i=0;i<1;i++)
		{
			String [] record_gp = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_gp[j] = genRandString();
			}
			DBApp.insert("gp", record_gp);
		}
		ArrayList<String[]> tr_gp =DBApp.validateRecords("gp");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gp.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"b1","t3r","f6356","sf45q","i32k","v65"};
		DBApp.createTable("w9", cols2);
		for(int i=0;i<29;i++)
		{
			String [] record_w9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_w9[j] = genRandString();
			}
			DBApp.insert("w9", record_w9);
		}
		ArrayList<String[]> tr_w9 =DBApp.validateRecords("w9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_w9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"qf1"};
		DBApp.createTable("ye51r", cols3);
		for(int i=0;i<16;i++)
		{
			String [] record_ye51r = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ye51r[j] = genRandString();
			}
			DBApp.insert("ye51r", record_ye51r);
		}
		ArrayList<String[]> tr_ye51r =DBApp.validateRecords("ye51r");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ye51r.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"y8","b6","qo9","khh7s","c0s","ks6","kbpp","s35","p2o"};
		DBApp.createTable("e5", cols4);
		for(int i=0;i<36;i++)
		{
			String [] record_e5 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_e5[j] = genRandString();
			}
			DBApp.insert("e5", record_e5);
		}
		ArrayList<String[]> tr_e5 =DBApp.validateRecords("e5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e5.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"n6","y9uj","q34","kh","fol4v","rd","ws"};
		DBApp.createTable("dy2", cols0);
		for(int i=0;i<41;i++)
		{
			String [] record_dy2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dy2[j] = genRandString();
			}
			DBApp.insert("dy2", record_dy2);
		}
		ArrayList<String[]> tr_dy2 =DBApp.validateRecords("dy2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_dy2.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"u94"};
		DBApp.createTable("w9np", cols1);
		for(int i=0;i<32;i++)
		{
			String [] record_w9np = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_w9np[j] = genRandString();
			}
			DBApp.insert("w9np", record_w9np);
		}
		ArrayList<String[]> tr_w9np =DBApp.validateRecords("w9np");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_w9np.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"eau19","xd4c","a59b","rk03","y5t","rh7s","knw7b","vf4","uil"};
		DBApp.createTable("j9u", cols2);
		for(int i=0;i<10;i++)
		{
			String [] record_j9u = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_j9u[j] = genRandString();
			}
			DBApp.insert("j9u", record_j9u);
		}
		ArrayList<String[]> tr_j9u =DBApp.validateRecords("j9u");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_j9u.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"mow","mmc6q","dp5w","z6186","h8l","a65","b54","p0qn7","il","a912"};
		DBApp.createTable("p7", cols3);
		for(int i=0;i<100;i++)
		{
			String [] record_p7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_p7[j] = genRandString();
			}
			DBApp.insert("p7", record_p7);
		}
		ArrayList<String[]> tr_p7 =DBApp.validateRecords("p7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_p7.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"rp5","iv3q7","oo","j9","qy0","dp"};
		DBApp.createTable("k5", cols0);
		for(int i=0;i<80;i++)
		{
			String [] record_k5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k5[j] = genRandString();
			}
			DBApp.insert("k5", record_k5);
		}
		ArrayList<String[]> tr_k5 =DBApp.validateRecords("k5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k5.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"k2g0","rfak","b8","jox93","ry5","hj0","b1u2e"};
		DBApp.createTable("lu8", cols1);
		for(int i=0;i<18;i++)
		{
			String [] record_lu8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_lu8[j] = genRandString();
			}
			DBApp.insert("lu8", record_lu8);
		}
		ArrayList<String[]> tr_lu8 =DBApp.validateRecords("lu8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_lu8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"nw7","jz","lu9o","ssa","m0e","ak9","d39","maf","el","soj"};
		DBApp.createTable("b2z", cols2);
		for(int i=0;i<25;i++)
		{
			String [] record_b2z = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_b2z[j] = genRandString();
			}
			DBApp.insert("b2z", record_b2z);
		}
		ArrayList<String[]> tr_b2z =DBApp.validateRecords("b2z");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_b2z.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"x3","wg9uw","h9342","h7","ji","wwx","qnp","e7m"};
		DBApp.createTable("xko5", cols0);
		for(int i=0;i<94;i++)
		{
			String [] record_xko5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xko5[j] = genRandString();
			}
			DBApp.insert("xko5", record_xko5);
		}
		ArrayList<String[]> tr_xko5 =DBApp.validateRecords("xko5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_xko5.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"c20z","e3a6t","k23o"};
		DBApp.createTable("h1", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_h1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h1[j] = genRandString();
			}
			DBApp.insert("h1", record_h1);
		}
		ArrayList<String[]> tr_h1 =DBApp.validateRecords("h1");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_h1.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"u13","wx94y","lw9p2","dxk","s3c9"};
		DBApp.createTable("wx8", cols2);
		for(int i=0;i<86;i++)
		{
			String [] record_wx8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_wx8[j] = genRandString();
			}
			DBApp.insert("wx8", record_wx8);
		}
		ArrayList<String[]> tr_wx8 =DBApp.validateRecords("wx8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_wx8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"oj","x9","s4k","rbo","x88cl","guf"};
		DBApp.createTable("nx0", cols3);
		for(int i=0;i<81;i++)
		{
			String [] record_nx0 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_nx0[j] = genRandString();
			}
			DBApp.insert("nx0", record_nx0);
		}
		ArrayList<String[]> tr_nx0 =DBApp.validateRecords("nx0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_nx0.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"jr"};
		DBApp.createTable("qy9", cols0);
		for(int i=0;i<88;i++)
		{
			String [] record_qy9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qy9[j] = genRandString();
			}
			DBApp.insert("qy9", record_qy9);
		}
		ArrayList<String[]> tr_qy9 =DBApp.validateRecords("qy9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_qy9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"r87g","p6","p9","z1oa9","m3b","n1","g84"};
		DBApp.createTable("nzum1", cols1);
		for(int i=0;i<41;i++)
		{
			String [] record_nzum1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_nzum1[j] = genRandString();
			}
			DBApp.insert("nzum1", record_nzum1);
		}
		ArrayList<String[]> tr_nzum1 =DBApp.validateRecords("nzum1");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_nzum1.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"p2","z1r","yz","om","tg8u8","d9je2","k7"};
		DBApp.createTable("uvim", cols0);
		for(int i=0;i<73;i++)
		{
			String [] record_uvim = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_uvim[j] = genRandString();
			}
			DBApp.insert("uvim", record_uvim);
		}
		ArrayList<String[]> tr_uvim =DBApp.validateRecords("uvim");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_uvim.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"f3k7","vv4"};
		DBApp.createTable("c0", cols1);
		for(int i=0;i<73;i++)
		{
			String [] record_c0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c0[j] = genRandString();
			}
			DBApp.insert("c0", record_c0);
		}
		ArrayList<String[]> tr_c0 =DBApp.validateRecords("c0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"s9x6l","vxt"};
		DBApp.createTable("t4", cols2);
		for(int i=0;i<12;i++)
		{
			String [] record_t4 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t4[j] = genRandString();
			}
			DBApp.insert("t4", record_t4);
		}
		ArrayList<String[]> tr_t4 =DBApp.validateRecords("t4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t4.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"fty","byb7","i3","c3","a2g","r9","bwl","q82","s8"};
		DBApp.createTable("c42", cols0);
		for(int i=0;i<91;i++)
		{
			String [] record_c42 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c42[j] = genRandString();
			}
			DBApp.insert("c42", record_c42);
		}
		ArrayList<String[]> tr_c42 =DBApp.validateRecords("c42");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c42.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"agu","lf70p","r5m","sq4","xxve","g8p","omi","huzk9"};
		DBApp.createTable("j15", cols1);
		for(int i=0;i<54;i++)
		{
			String [] record_j15 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_j15[j] = genRandString();
			}
			DBApp.insert("j15", record_j15);
		}
		ArrayList<String[]> tr_j15 =DBApp.validateRecords("j15");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_j15.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"i1","ds17","n1","ub93k","l5sy","s5","s22vb","m5n"};
		DBApp.createTable("l91lp", cols2);
		for(int i=0;i<38;i++)
		{
			String [] record_l91lp = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_l91lp[j] = genRandString();
			}
			DBApp.insert("l91lp", record_l91lp);
		}
		ArrayList<String[]> tr_l91lp =DBApp.validateRecords("l91lp");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l91lp.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"uq","i64","bk7","aj"};
		DBApp.createTable("g8e64", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_g8e64 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g8e64[j] = genRandString();
			}
			DBApp.insert("g8e64", record_g8e64);
		}
		ArrayList<String[]> tr_g8e64 =DBApp.validateRecords("g8e64");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_g8e64.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"n7d9u","sk33","zx","oud","c9","g2no6","un6","s1","qp381","cg9"};
		DBApp.createTable("m20", cols1);
		for(int i=0;i<91;i++)
		{
			String [] record_m20 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m20[j] = genRandString();
			}
			DBApp.insert("m20", record_m20);
		}
		ArrayList<String[]> tr_m20 =DBApp.validateRecords("m20");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_m20.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"tg7","ibb"};
		DBApp.createTable("zs", cols0);
		for(int i=0;i<28;i++)
		{
			String [] record_zs = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_zs[j] = genRandString();
			}
			DBApp.insert("zs", record_zs);
		}
		ArrayList<String[]> tr_zs =DBApp.validateRecords("zs");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_zs.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"c26m","o7k","dpjh"};
		DBApp.createTable("d375r", cols1);
		for(int i=0;i<91;i++)
		{
			String [] record_d375r = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_d375r[j] = genRandString();
			}
			DBApp.insert("d375r", record_d375r);
		}
		ArrayList<String[]> tr_d375r =DBApp.validateRecords("d375r");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_d375r.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"vv19u","hq1","d1cfv"};
		DBApp.createTable("hh", cols2);
		for(int i=0;i<91;i++)
		{
			String [] record_hh = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_hh[j] = genRandString();
			}
			DBApp.insert("hh", record_hh);
		}
		ArrayList<String[]> tr_hh =DBApp.validateRecords("hh");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_hh.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"bfs0","cmm","oc1","f7"};
		DBApp.createTable("py15", cols3);
		for(int i=0;i<68;i++)
		{
			String [] record_py15 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_py15[j] = genRandString();
			}
			DBApp.insert("py15", record_py15);
		}
		ArrayList<String[]> tr_py15 =DBApp.validateRecords("py15");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_py15.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"u5g","lu9bx","k43","jf4fg","b45","oev","t11hl","bgt","f26"};
		DBApp.createTable("nh4", cols0);
		for(int i=0;i<77;i++)
		{
			String [] record_nh4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nh4[j] = genRandString();
			}
			DBApp.insert("nh4", record_nh4);
		}
		ArrayList<String[]> tr_nh4 =DBApp.validateRecords("nh4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_nh4.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"v77h","rk8","mln","ih8ce","rzx","o4","h01t5","i7","b1t"};
		DBApp.createTable("mq", cols1);
		for(int i=0;i<79;i++)
		{
			String [] record_mq = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mq[j] = genRandString();
			}
			DBApp.insert("mq", record_mq);
		}
		ArrayList<String[]> tr_mq =DBApp.validateRecords("mq");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_mq.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"au82b","vn475","n343","y21","aw","d79w"};
		DBApp.createTable("o2b8", cols2);
		for(int i=0;i<79;i++)
		{
			String [] record_o2b8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_o2b8[j] = genRandString();
			}
			DBApp.insert("o2b8", record_o2b8);
		}
		ArrayList<String[]> tr_o2b8 =DBApp.validateRecords("o2b8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_o2b8.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"hcl","p4dl4","ns6f0","p88"};
		DBApp.createTable("pq", cols0);
		for(int i=0;i<35;i++)
		{
			String [] record_pq = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pq[j] = genRandString();
			}
			DBApp.insert("pq", record_pq);
		}
		ArrayList<String[]> tr_pq =DBApp.validateRecords("pq");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_pq.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"wge7s","r3i","cet6f","z7"};
		DBApp.createTable("js", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_js = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_js[j] = genRandString();
			}
			DBApp.insert("js", record_js);
		}
		ArrayList<String[]> tr_js =DBApp.validateRecords("js");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_js.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"w65","a607","kaxz5","r7s","mv43","b4n","k8j1","k7","ofwy4","q83"};
		DBApp.createTable("y92ke", cols1);
		for(int i=0;i<17;i++)
		{
			String [] record_y92ke = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y92ke[j] = genRandString();
			}
			DBApp.insert("y92ke", record_y92ke);
		}
		ArrayList<String[]> tr_y92ke =DBApp.validateRecords("y92ke");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_y92ke.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"ja","r01z9"};
		DBApp.createTable("c3vp", cols0);
		for(int i=0;i<65;i++)
		{
			String [] record_c3vp = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c3vp[j] = genRandString();
			}
			DBApp.insert("c3vp", record_c3vp);
		}
		ArrayList<String[]> tr_c3vp =DBApp.validateRecords("c3vp");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c3vp.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"t5x","gjaxf"};
		DBApp.createTable("l8es", cols1);
		for(int i=0;i<20;i++)
		{
			String [] record_l8es = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_l8es[j] = genRandString();
			}
			DBApp.insert("l8es", record_l8es);
		}
		ArrayList<String[]> tr_l8es =DBApp.validateRecords("l8es");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l8es.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"k5q","t1n","rs9t","jqjp2","fj9","s13fx","he19v","ou","zaii"};
		DBApp.createTable("ec05", cols2);
		for(int i=0;i<97;i++)
		{
			String [] record_ec05 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ec05[j] = genRandString();
			}
			DBApp.insert("ec05", record_ec05);
		}
		ArrayList<String[]> tr_ec05 =DBApp.validateRecords("ec05");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ec05.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"k1mk","odl9","w49","cf94","zs7k"};
		DBApp.createTable("l7o", cols3);
		for(int i=0;i<52;i++)
		{
			String [] record_l7o = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_l7o[j] = genRandString();
			}
			DBApp.insert("l7o", record_l7o);
		}
		ArrayList<String[]> tr_l7o =DBApp.validateRecords("l7o");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l7o.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"sn","s76k1","x3h","u72","a94ih","fj36b","hh1c5","v793","ctym"};
		DBApp.createTable("i0", cols0);
		for(int i=0;i<100;i++)
		{
			String [] record_i0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i0[j] = genRandString();
			}
			DBApp.insert("i0", record_i0);
		}
		ArrayList<String[]> tr_i0 =DBApp.validateRecords("i0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"mko"};
		DBApp.createTable("hihk", cols1);
		for(int i=0;i<27;i++)
		{
			String [] record_hihk = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hihk[j] = genRandString();
			}
			DBApp.insert("hihk", record_hihk);
		}
		ArrayList<String[]> tr_hihk =DBApp.validateRecords("hihk");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_hihk.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"tg3u7","e4405","i215","se02","xgf"};
		DBApp.createTable("ft3u", cols2);
		for(int i=0;i<52;i++)
		{
			String [] record_ft3u = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ft3u[j] = genRandString();
			}
			DBApp.insert("ft3u", record_ft3u);
		}
		ArrayList<String[]> tr_ft3u =DBApp.validateRecords("ft3u");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ft3u.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"v5pw","nwo","cx2"};
		DBApp.createTable("f6", cols0);
		for(int i=0;i<72;i++)
		{
			String [] record_f6 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f6[j] = genRandString();
			}
			DBApp.insert("f6", record_f6);
		}
		ArrayList<String[]> tr_f6 =DBApp.validateRecords("f6");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_f6.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"g1ns1","v459","k43","fd","p58","an9j","cx","cb0w1","m39w9","zuo"};
		DBApp.createTable("t1", cols0);
		for(int i=0;i<36;i++)
		{
			String [] record_t1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_t1[j] = genRandString();
			}
			DBApp.insert("t1", record_t1);
		}
		ArrayList<String[]> tr_t1 =DBApp.validateRecords("t1");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t1.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"f64td","f70","s3t","q4","lr5","pec","r8c1","yki","ja2","bq3"};
		DBApp.createTable("ah6", cols1);
		for(int i=0;i<81;i++)
		{
			String [] record_ah6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ah6[j] = genRandString();
			}
			DBApp.insert("ah6", record_ah6);
		}
		ArrayList<String[]> tr_ah6 =DBApp.validateRecords("ah6");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ah6.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"hy7","co9","g852","s98","sq0vu","w2f03"};
		DBApp.createTable("bpo", cols2);
		for(int i=0;i<70;i++)
		{
			String [] record_bpo = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_bpo[j] = genRandString();
			}
			DBApp.insert("bpo", record_bpo);
		}
		ArrayList<String[]> tr_bpo =DBApp.validateRecords("bpo");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_bpo.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"d10q9","m839","mii0","iw8","c08","y4o"};
		DBApp.createTable("dv345", cols0);
		for(int i=0;i<61;i++)
		{
			String [] record_dv345 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dv345[j] = genRandString();
			}
			DBApp.insert("dv345", record_dv345);
		}
		ArrayList<String[]> tr_dv345 =DBApp.validateRecords("dv345");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_dv345.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"v9ugr","w59v0","l5oq","wn9","ps7o","xx"};
		DBApp.createTable("x90", cols1);
		for(int i=0;i<9;i++)
		{
			String [] record_x90 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x90[j] = genRandString();
			}
			DBApp.insert("x90", record_x90);
		}
		ArrayList<String[]> tr_x90 =DBApp.validateRecords("x90");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x90.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"t3","m7","b1w2","nt1k"};
		DBApp.createTable("l8e", cols2);
		for(int i=0;i<57;i++)
		{
			String [] record_l8e = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_l8e[j] = genRandString();
			}
			DBApp.insert("l8e", record_l8e);
		}
		ArrayList<String[]> tr_l8e =DBApp.validateRecords("l8e");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l8e.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"eil7","y60"};
		DBApp.createTable("ka2", cols0);
		for(int i=0;i<100;i++)
		{
			String [] record_ka2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ka2[j] = genRandString();
			}
			DBApp.insert("ka2", record_ka2);
		}
		ArrayList<String[]> tr_ka2 =DBApp.validateRecords("ka2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ka2.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"rz7n","n6","crc","wq","g3p","ihm","ky21"};
		DBApp.createTable("i1q", cols0);
		for(int i=0;i<80;i++)
		{
			String [] record_i1q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i1q[j] = genRandString();
			}
			DBApp.insert("i1q", record_i1q);
		}
		ArrayList<String[]> tr_i1q =DBApp.validateRecords("i1q");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i1q.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"zom9","ci7b"};
		DBApp.createTable("k9", cols0);
		for(int i=0;i<20;i++)
		{
			String [] record_k9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k9[j] = genRandString();
			}
			DBApp.insert("k9", record_k9);
		}
		ArrayList<String[]> tr_k9 =DBApp.validateRecords("k9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"wjc64","g855","wn3","mw2","zof2","oter","u2","g7x9u","o7"};
		DBApp.createTable("i9", cols1);
		for(int i=0;i<48;i++)
		{
			String [] record_i9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i9[j] = genRandString();
			}
			DBApp.insert("i9", record_i9);
		}
		ArrayList<String[]> tr_i9 =DBApp.validateRecords("i9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"qxk","c3f","mb1"};
		DBApp.createTable("t9", cols2);
		for(int i=0;i<19;i++)
		{
			String [] record_t9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t9[j] = genRandString();
			}
			DBApp.insert("t9", record_t9);
		}
		ArrayList<String[]> tr_t9 =DBApp.validateRecords("t9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"br9","a367","kr","hi"};
		DBApp.createTable("t18", cols3);
		for(int i=0;i<50;i++)
		{
			String [] record_t18 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_t18[j] = genRandString();
			}
			DBApp.insert("t18", record_t18);
		}
		ArrayList<String[]> tr_t18 =DBApp.validateRecords("t18");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t18.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"jh64","nh4","fc","yd1mx","yh91g","je6","i1","f88q9"};
		DBApp.createTable("rra", cols4);
		for(int i=0;i<53;i++)
		{
			String [] record_rra = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_rra[j] = genRandString();
			}
			DBApp.insert("rra", record_rra);
		}
		ArrayList<String[]> tr_rra =DBApp.validateRecords("rra");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_rra.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"bxve"};
		DBApp.createTable("t2", cols0);
		for(int i=0;i<22;i++)
		{
			String [] record_t2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_t2[j] = genRandString();
			}
			DBApp.insert("t2", record_t2);
		}
		ArrayList<String[]> tr_t2 =DBApp.validateRecords("t2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t2.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"v6","timh","kv75","ch6"};
		DBApp.createTable("i2", cols0);
		for(int i=0;i<83;i++)
		{
			String [] record_i2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i2[j] = genRandString();
			}
			DBApp.insert("i2", record_i2);
		}
		ArrayList<String[]> tr_i2 =DBApp.validateRecords("i2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i2.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"a86q","wpl23","c0s","rs9","gv8","t0k","cp","d1","vu8"};
		DBApp.createTable("g5h", cols1);
		for(int i=0;i<96;i++)
		{
			String [] record_g5h = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_g5h[j] = genRandString();
			}
			DBApp.insert("g5h", record_g5h);
		}
		ArrayList<String[]> tr_g5h =DBApp.validateRecords("g5h");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_g5h.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"q1i","n1ywv","alv","d746","k16"};
		DBApp.createTable("ke9", cols2);
		for(int i=0;i<23;i++)
		{
			String [] record_ke9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ke9[j] = genRandString();
			}
			DBApp.insert("ke9", record_ke9);
		}
		ArrayList<String[]> tr_ke9 =DBApp.validateRecords("ke9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ke9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"hzt","d2u","dyaa","e3g","r068b","j13","r7b","d529","zp1","bu02"};
		DBApp.createTable("koh75", cols3);
		for(int i=0;i<90;i++)
		{
			String [] record_koh75 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_koh75[j] = genRandString();
			}
			DBApp.insert("koh75", record_koh75);
		}
		ArrayList<String[]> tr_koh75 =DBApp.validateRecords("koh75");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_koh75.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"gc","uy7lm","s3","kvf","oh91","en","s7q","mn"};
		DBApp.createTable("l25v", cols0);
		for(int i=0;i<86;i++)
		{
			String [] record_l25v = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l25v[j] = genRandString();
			}
			DBApp.insert("l25v", record_l25v);
		}
		ArrayList<String[]> tr_l25v =DBApp.validateRecords("l25v");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l25v.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"d4","ddt","ppo1v","v2axc","f0a","bm3","x1z","tnl","m6y4"};
		DBApp.createTable("ysf3c", cols1);
		for(int i=0;i<50;i++)
		{
			String [] record_ysf3c = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ysf3c[j] = genRandString();
			}
			DBApp.insert("ysf3c", record_ysf3c);
		}
		ArrayList<String[]> tr_ysf3c =DBApp.validateRecords("ysf3c");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ysf3c.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"s6gbh","i9","t2lag","qxl16","a2j","icq","hi","ombw","a7","r4"};
		DBApp.createTable("lg5", cols0);
		for(int i=0;i<15;i++)
		{
			String [] record_lg5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lg5[j] = genRandString();
			}
			DBApp.insert("lg5", record_lg5);
		}
		ArrayList<String[]> tr_lg5 =DBApp.validateRecords("lg5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_lg5.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"q8h","os","i8s","f0018","tgspc","au669"};
		DBApp.createTable("fy", cols1);
		for(int i=0;i<81;i++)
		{
			String [] record_fy = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fy[j] = genRandString();
			}
			DBApp.insert("fy", record_fy);
		}
		ArrayList<String[]> tr_fy =DBApp.validateRecords("fy");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_fy.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"t90","ot","ef9","pn54h","tscot","u08e","a84"};
		DBApp.createTable("j2n", cols0);
		for(int i=0;i<81;i++)
		{
			String [] record_j2n = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j2n[j] = genRandString();
			}
			DBApp.insert("j2n", record_j2n);
		}
		ArrayList<String[]> tr_j2n =DBApp.validateRecords("j2n");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_j2n.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"bvq","a029v","a0e","uih84","rzm","o5b","z4n","qg"};
		DBApp.createTable("qx7hx", cols1);
		for(int i=0;i<58;i++)
		{
			String [] record_qx7hx = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_qx7hx[j] = genRandString();
			}
			DBApp.insert("qx7hx", record_qx7hx);
		}
		ArrayList<String[]> tr_qx7hx =DBApp.validateRecords("qx7hx");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_qx7hx.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"d6","lw3d8","r73","qv1","hf6q","oez90","rr","b189","k6j01","g5c9i"};
		DBApp.createTable("gp6", cols2);
		for(int i=0;i<28;i++)
		{
			String [] record_gp6 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_gp6[j] = genRandString();
			}
			DBApp.insert("gp6", record_gp6);
		}
		ArrayList<String[]> tr_gp6 =DBApp.validateRecords("gp6");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gp6.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"xf"};
		DBApp.createTable("s4bg9", cols0);
		for(int i=0;i<12;i++)
		{
			String [] record_s4bg9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_s4bg9[j] = genRandString();
			}
			DBApp.insert("s4bg9", record_s4bg9);
		}
		ArrayList<String[]> tr_s4bg9 =DBApp.validateRecords("s4bg9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_s4bg9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"dp","gww6","f4j"};
		DBApp.createTable("r4gy", cols1);
		for(int i=0;i<37;i++)
		{
			String [] record_r4gy = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_r4gy[j] = genRandString();
			}
			DBApp.insert("r4gy", record_r4gy);
		}
		ArrayList<String[]> tr_r4gy =DBApp.validateRecords("r4gy");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_r4gy.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"nby1","fw"};
		DBApp.createTable("t2962", cols2);
		for(int i=0;i<47;i++)
		{
			String [] record_t2962 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t2962[j] = genRandString();
			}
			DBApp.insert("t2962", record_t2962);
		}
		ArrayList<String[]> tr_t2962 =DBApp.validateRecords("t2962");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t2962.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"b5j"};
		DBApp.createTable("ln404", cols3);
		for(int i=0;i<45;i++)
		{
			String [] record_ln404 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ln404[j] = genRandString();
			}
			DBApp.insert("ln404", record_ln404);
		}
		ArrayList<String[]> tr_ln404 =DBApp.validateRecords("ln404");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ln404.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"x1k","jy9h9","y33","s56"};
		DBApp.createTable("xpd5r", cols4);
		for(int i=0;i<70;i++)
		{
			String [] record_xpd5r = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_xpd5r[j] = genRandString();
			}
			DBApp.insert("xpd5r", record_xpd5r);
		}
		ArrayList<String[]> tr_xpd5r =DBApp.validateRecords("xpd5r");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_xpd5r.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"ss","wjt43","s0","z50","w37","x6a","gc7","zs8","kld"};
		DBApp.createTable("w6i3f", cols0);
		for(int i=0;i<7;i++)
		{
			String [] record_w6i3f = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_w6i3f[j] = genRandString();
			}
			DBApp.insert("w6i3f", record_w6i3f);
		}
		ArrayList<String[]> tr_w6i3f =DBApp.validateRecords("w6i3f");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_w6i3f.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"gadk0","f0b","hy60a","ioc80","o65"};
		DBApp.createTable("v4682", cols1);
		for(int i=0;i<18;i++)
		{
			String [] record_v4682 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_v4682[j] = genRandString();
			}
			DBApp.insert("v4682", record_v4682);
		}
		ArrayList<String[]> tr_v4682 =DBApp.validateRecords("v4682");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_v4682.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"o404","d43"};
		DBApp.createTable("fqaw", cols2);
		for(int i=0;i<25;i++)
		{
			String [] record_fqaw = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fqaw[j] = genRandString();
			}
			DBApp.insert("fqaw", record_fqaw);
		}
		ArrayList<String[]> tr_fqaw =DBApp.validateRecords("fqaw");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_fqaw.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"tl9","cwf","w5aic","do5v","rq3zz","i20ou","brk"};
		DBApp.createTable("l0t9", cols3);
		for(int i=0;i<42;i++)
		{
			String [] record_l0t9 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_l0t9[j] = genRandString();
			}
			DBApp.insert("l0t9", record_l0t9);
		}
		ArrayList<String[]> tr_l0t9 =DBApp.validateRecords("l0t9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l0t9.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"ca1","r89","jki07","y9","yo6"};
		DBApp.createTable("x4u", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_x4u = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x4u[j] = genRandString();
			}
			DBApp.insert("x4u", record_x4u);
		}
		ArrayList<String[]> tr_x4u =DBApp.validateRecords("x4u");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x4u.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"w4","fg","u84h","u5sg","q5q"};
		DBApp.createTable("gn98", cols1);
		for(int i=0;i<42;i++)
		{
			String [] record_gn98 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_gn98[j] = genRandString();
			}
			DBApp.insert("gn98", record_gn98);
		}
		ArrayList<String[]> tr_gn98 =DBApp.validateRecords("gn98");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gn98.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"vq8l3","p5s","aev9","gih","m19p","b5kw","v4jl","m2"};
		DBApp.createTable("s20", cols2);
		for(int i=0;i<28;i++)
		{
			String [] record_s20 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_s20[j] = genRandString();
			}
			DBApp.insert("s20", record_s20);
		}
		ArrayList<String[]> tr_s20 =DBApp.validateRecords("s20");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_s20.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"uy6","l7","q2k93","bi9h","ix3","i81w","fm"};
		DBApp.createTable("i3z", cols0);
		for(int i=0;i<33;i++)
		{
			String [] record_i3z = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i3z[j] = genRandString();
			}
			DBApp.insert("i3z", record_i3z);
		}
		ArrayList<String[]> tr_i3z =DBApp.validateRecords("i3z");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i3z.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"as4","vl"};
		DBApp.createTable("al4x", cols1);
		for(int i=0;i<52;i++)
		{
			String [] record_al4x = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_al4x[j] = genRandString();
			}
			DBApp.insert("al4x", record_al4x);
		}
		ArrayList<String[]> tr_al4x =DBApp.validateRecords("al4x");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_al4x.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"wm118"};
		DBApp.createTable("i46", cols2);
		for(int i=0;i<70;i++)
		{
			String [] record_i46 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i46[j] = genRandString();
			}
			DBApp.insert("i46", record_i46);
		}
		ArrayList<String[]> tr_i46 =DBApp.validateRecords("i46");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_i46.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"rc06z","ht1a","r28","pz","wrv","o2h","o75","o18","x84y","ad"};
		DBApp.createTable("znh", cols3);
		for(int i=0;i<4;i++)
		{
			String [] record_znh = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_znh[j] = genRandString();
			}
			DBApp.insert("znh", record_znh);
		}
		ArrayList<String[]> tr_znh =DBApp.validateRecords("znh");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_znh.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"k7","u5","eqnd"};
		DBApp.createTable("v3", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_v3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v3[j] = genRandString();
			}
			DBApp.insert("v3", record_v3);
		}
		ArrayList<String[]> tr_v3 =DBApp.validateRecords("v3");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_v3.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"ih","bx","qfj8t","m3rz","nz4","vnz6","cyquo","x4j"};
		DBApp.createTable("x53", cols1);
		for(int i=0;i<33;i++)
		{
			String [] record_x53 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x53[j] = genRandString();
			}
			DBApp.insert("x53", record_x53);
		}
		ArrayList<String[]> tr_x53 =DBApp.validateRecords("x53");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x53.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"u0l3","jr0","cm","mt","e8","tu8","g1","df679","q8t0f"};
		DBApp.createTable("twz", cols2);
		for(int i=0;i<48;i++)
		{
			String [] record_twz = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_twz[j] = genRandString();
			}
			DBApp.insert("twz", record_twz);
		}
		ArrayList<String[]> tr_twz =DBApp.validateRecords("twz");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_twz.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"r20","z35bz"};
		DBApp.createTable("a5344", cols0);
		for(int i=0;i<2;i++)
		{
			String [] record_a5344 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a5344[j] = genRandString();
			}
			DBApp.insert("a5344", record_a5344);
		}
		ArrayList<String[]> tr_a5344 =DBApp.validateRecords("a5344");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_a5344.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"aw","qc4","dziu8","qi5"};
		DBApp.createTable("llt9", cols1);
		for(int i=0;i<96;i++)
		{
			String [] record_llt9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_llt9[j] = genRandString();
			}
			DBApp.insert("llt9", record_llt9);
		}
		ArrayList<String[]> tr_llt9 =DBApp.validateRecords("llt9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_llt9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"g552","vw0","ryhvt","mh7","h559j","ww","lqb","z3m","ni"};
		DBApp.createTable("b0l", cols2);
		for(int i=0;i<55;i++)
		{
			String [] record_b0l = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_b0l[j] = genRandString();
			}
			DBApp.insert("b0l", record_b0l);
		}
		ArrayList<String[]> tr_b0l =DBApp.validateRecords("b0l");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_b0l.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"m2ob","i8","coq","wr70j","d8"};
		DBApp.createTable("jw9", cols0);
		for(int i=0;i<27;i++)
		{
			String [] record_jw9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jw9[j] = genRandString();
			}
			DBApp.insert("jw9", record_jw9);
		}
		ArrayList<String[]> tr_jw9 =DBApp.validateRecords("jw9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_jw9.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"ur72","edf18","rl7","or0","xy","nm0","xyc29"};
		DBApp.createTable("x6q", cols0);
		for(int i=0;i<8;i++)
		{
			String [] record_x6q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x6q[j] = genRandString();
			}
			DBApp.insert("x6q", record_x6q);
		}
		ArrayList<String[]> tr_x6q =DBApp.validateRecords("x6q");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x6q.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"o1","l72x9","en","x779z","d730r","q76u9","d6h72","ui"};
		DBApp.createTable("rda9", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_rda9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rda9[j] = genRandString();
			}
			DBApp.insert("rda9", record_rda9);
		}
		ArrayList<String[]> tr_rda9 =DBApp.validateRecords("rda9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_rda9.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"x2","z9","m6","ac3","kr473","el38","g06r","iv","t5"};
		DBApp.createTable("pu527", cols0);
		for(int i=0;i<37;i++)
		{
			String [] record_pu527 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pu527[j] = genRandString();
			}
			DBApp.insert("pu527", record_pu527);
		}
		ArrayList<String[]> tr_pu527 =DBApp.validateRecords("pu527");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_pu527.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"nzd","a7p","wr","l6s","pjf","j2wx4","o4s","s823x"};
		DBApp.createTable("dyyw4", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_dyyw4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dyyw4[j] = genRandString();
			}
			DBApp.insert("dyyw4", record_dyyw4);
		}
		ArrayList<String[]> tr_dyyw4 =DBApp.validateRecords("dyyw4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_dyyw4.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"k8g","ro","w4q4w","c2","um","yqz","yr6w5","u7d","gns","so"};
		DBApp.createTable("t88", cols0);
		for(int i=0;i<42;i++)
		{
			String [] record_t88 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_t88[j] = genRandString();
			}
			DBApp.insert("t88", record_t88);
		}
		ArrayList<String[]> tr_t88 =DBApp.validateRecords("t88");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t88.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"m22","vp","j19k","r43m7"};
		DBApp.createTable("sur7", cols0);
		for(int i=0;i<79;i++)
		{
			String [] record_sur7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_sur7[j] = genRandString();
			}
			DBApp.insert("sur7", record_sur7);
		}
		ArrayList<String[]> tr_sur7 =DBApp.validateRecords("sur7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_sur7.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_55() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"lz4","df","o3","j1b"};
		DBApp.createTable("f41r", cols0);
		for(int i=0;i<63;i++)
		{
			String [] record_f41r = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f41r[j] = genRandString();
			}
			DBApp.insert("f41r", record_f41r);
		}
		ArrayList<String[]> tr_f41r =DBApp.validateRecords("f41r");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_f41r.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"kq","mjt","e7c","r8hqk","vi9"};
		DBApp.createTable("fl", cols1);
		for(int i=0;i<44;i++)
		{
			String [] record_fl = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fl[j] = genRandString();
			}
			DBApp.insert("fl", record_fl);
		}
		ArrayList<String[]> tr_fl =DBApp.validateRecords("fl");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_fl.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"vu","lbq","c8","kh5","u974"};
		DBApp.createTable("t89", cols2);
		for(int i=0;i<61;i++)
		{
			String [] record_t89 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t89[j] = genRandString();
			}
			DBApp.insert("t89", record_t89);
		}
		ArrayList<String[]> tr_t89 =DBApp.validateRecords("t89");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t89.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"v08"};
		DBApp.createTable("gm", cols3);
		for(int i=0;i<22;i++)
		{
			String [] record_gm = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_gm[j] = genRandString();
			}
			DBApp.insert("gm", record_gm);
		}
		ArrayList<String[]> tr_gm =DBApp.validateRecords("gm");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gm.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_56() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"o61yb","ag0p1","i1u0z","ou36","ck6","ryb"};
		DBApp.createTable("uer", cols0);
		for(int i=0;i<61;i++)
		{
			String [] record_uer = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_uer[j] = genRandString();
			}
			DBApp.insert("uer", record_uer);
		}
		ArrayList<String[]> tr_uer =DBApp.validateRecords("uer");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_uer.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"g7b","sb","lwx"};
		DBApp.createTable("bzo", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_bzo = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_bzo[j] = genRandString();
			}
			DBApp.insert("bzo", record_bzo);
		}
		ArrayList<String[]> tr_bzo =DBApp.validateRecords("bzo");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_bzo.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"d25","t65","j8m","iy3h4","a23w","nti","g3j","ld28"};
		DBApp.createTable("k454d", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_k454d = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_k454d[j] = genRandString();
			}
			DBApp.insert("k454d", record_k454d);
		}
		ArrayList<String[]> tr_k454d =DBApp.validateRecords("k454d");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k454d.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"seb","gj","s3"};
		DBApp.createTable("rj62y", cols3);
		for(int i=0;i<38;i++)
		{
			String [] record_rj62y = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_rj62y[j] = genRandString();
			}
			DBApp.insert("rj62y", record_rj62y);
		}
		ArrayList<String[]> tr_rj62y =DBApp.validateRecords("rj62y");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_rj62y.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"v79"};
		DBApp.createTable("iev", cols4);
		for(int i=0;i<34;i++)
		{
			String [] record_iev = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_iev[j] = genRandString();
			}
			DBApp.insert("iev", record_iev);
		}
		ArrayList<String[]> tr_iev =DBApp.validateRecords("iev");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_iev.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_57() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"r8y","u3e1","y6","t6x","b8w","xo9","o7p","y69vm"};
		DBApp.createTable("jl", cols0);
		for(int i=0;i<62;i++)
		{
			String [] record_jl = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jl[j] = genRandString();
			}
			DBApp.insert("jl", record_jl);
		}
		ArrayList<String[]> tr_jl =DBApp.validateRecords("jl");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_jl.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"eee6n","l1l","wqdh","r1k"};
		DBApp.createTable("y4w4", cols1);
		for(int i=0;i<4;i++)
		{
			String [] record_y4w4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y4w4[j] = genRandString();
			}
			DBApp.insert("y4w4", record_y4w4);
		}
		ArrayList<String[]> tr_y4w4 =DBApp.validateRecords("y4w4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_y4w4.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"nyq9","g0","a941u","jm3","u92y","ay","x3ij","i60","xv490"};
		DBApp.createTable("v93h6", cols2);
		for(int i=0;i<100;i++)
		{
			String [] record_v93h6 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_v93h6[j] = genRandString();
			}
			DBApp.insert("v93h6", record_v93h6);
		}
		ArrayList<String[]> tr_v93h6 =DBApp.validateRecords("v93h6");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_v93h6.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"vk1q"};
		DBApp.createTable("t0jt", cols3);
		for(int i=0;i<47;i++)
		{
			String [] record_t0jt = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_t0jt[j] = genRandString();
			}
			DBApp.insert("t0jt", record_t0jt);
		}
		ArrayList<String[]> tr_t0jt =DBApp.validateRecords("t0jt");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_t0jt.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"s2","a7","iq3z","h49","i8i4","w3","ss58s","gig3k","p53"};
		DBApp.createTable("an", cols4);
		for(int i=0;i<100;i++)
		{
			String [] record_an = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_an[j] = genRandString();
			}
			DBApp.insert("an", record_an);
		}
		ArrayList<String[]> tr_an =DBApp.validateRecords("an");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_an.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_58() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"wa3","mi","gk8","xe2","e6","k75","pk55","f0"};
		DBApp.createTable("x0t", cols0);
		for(int i=0;i<66;i++)
		{
			String [] record_x0t = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x0t[j] = genRandString();
			}
			DBApp.insert("x0t", record_x0t);
		}
		ArrayList<String[]> tr_x0t =DBApp.validateRecords("x0t");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x0t.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"svf","c48v3","iw17k","ql35s","k6mk","nt6","bb3","i0b"};
		DBApp.createTable("gw", cols1);
		for(int i=0;i<37;i++)
		{
			String [] record_gw = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_gw[j] = genRandString();
			}
			DBApp.insert("gw", record_gw);
		}
		ArrayList<String[]> tr_gw =DBApp.validateRecords("gw");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_gw.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"k75","gzoi"};
		DBApp.createTable("on1qj", cols2);
		for(int i=0;i<41;i++)
		{
			String [] record_on1qj = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_on1qj[j] = genRandString();
			}
			DBApp.insert("on1qj", record_on1qj);
		}
		ArrayList<String[]> tr_on1qj =DBApp.validateRecords("on1qj");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_on1qj.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"gaz","sf","fk6c","s1x9","d5q","m0n5h"};
		DBApp.createTable("r6w0", cols3);
		for(int i=0;i<87;i++)
		{
			String [] record_r6w0 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_r6w0[j] = genRandString();
			}
			DBApp.insert("r6w0", record_r6w0);
		}
		ArrayList<String[]> tr_r6w0 =DBApp.validateRecords("r6w0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_r6w0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"so2"};
		DBApp.createTable("h8", cols4);
		for(int i=0;i<41;i++)
		{
			String [] record_h8 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_h8[j] = genRandString();
			}
			DBApp.insert("h8", record_h8);
		}
		ArrayList<String[]> tr_h8 =DBApp.validateRecords("h8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_h8.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_59() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"p2","hv90q","aw","wek","m8431"};
		DBApp.createTable("kux8", cols0);
		for(int i=0;i<65;i++)
		{
			String [] record_kux8 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_kux8[j] = genRandString();
			}
			DBApp.insert("kux8", record_kux8);
		}
		ArrayList<String[]> tr_kux8 =DBApp.validateRecords("kux8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_kux8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"xx9","kl8r","eo4g2","ru4","r5d","q4","zmw","s6","ctp"};
		DBApp.createTable("y73", cols1);
		for(int i=0;i<71;i++)
		{
			String [] record_y73 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y73[j] = genRandString();
			}
			DBApp.insert("y73", record_y73);
		}
		ArrayList<String[]> tr_y73 =DBApp.validateRecords("y73");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_y73.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_60() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"iax","o5863","o40","hx4j1","ickd9"};
		DBApp.createTable("e5", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_e5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_e5[j] = genRandString();
			}
			DBApp.insert("e5", record_e5);
		}
		ArrayList<String[]> tr_e5 =DBApp.validateRecords("e5");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e5.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"evg","el","rg8","vjd","cje","dl9n","p05","nsc","vc3","nb4"};
		DBApp.createTable("u53n", cols1);
		for(int i=0;i<39;i++)
		{
			String [] record_u53n = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_u53n[j] = genRandString();
			}
			DBApp.insert("u53n", record_u53n);
		}
		ArrayList<String[]> tr_u53n =DBApp.validateRecords("u53n");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_u53n.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_61() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"h4bk","ixn10","vjo","i2oo","th7x0","ufg"};
		DBApp.createTable("lnj", cols0);
		for(int i=0;i<78;i++)
		{
			String [] record_lnj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lnj[j] = genRandString();
			}
			DBApp.insert("lnj", record_lnj);
		}
		ArrayList<String[]> tr_lnj =DBApp.validateRecords("lnj");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_lnj.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"c03"};
		DBApp.createTable("zo2", cols1);
		for(int i=0;i<47;i++)
		{
			String [] record_zo2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_zo2[j] = genRandString();
			}
			DBApp.insert("zo2", record_zo2);
		}
		ArrayList<String[]> tr_zo2 =DBApp.validateRecords("zo2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_zo2.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"nb","kfc"};
		DBApp.createTable("csw", cols2);
		for(int i=0;i<19;i++)
		{
			String [] record_csw = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_csw[j] = genRandString();
			}
			DBApp.insert("csw", record_csw);
		}
		ArrayList<String[]> tr_csw =DBApp.validateRecords("csw");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_csw.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_62() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"b62p","z8","r58","tvo","x759a","wr9"};
		DBApp.createTable("eo2", cols0);
		for(int i=0;i<43;i++)
		{
			String [] record_eo2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_eo2[j] = genRandString();
			}
			DBApp.insert("eo2", record_eo2);
		}
		ArrayList<String[]> tr_eo2 =DBApp.validateRecords("eo2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_eo2.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_63() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"iu1d","clp","d49t8","g46","qmcx","rzn","zrx"};
		DBApp.createTable("n22", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_n22 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n22[j] = genRandString();
			}
			DBApp.insert("n22", record_n22);
		}
		ArrayList<String[]> tr_n22 =DBApp.validateRecords("n22");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_n22.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"hf","i6j","uh2d0"};
		DBApp.createTable("fyy", cols1);
		for(int i=0;i<8;i++)
		{
			String [] record_fyy = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fyy[j] = genRandString();
			}
			DBApp.insert("fyy", record_fyy);
		}
		ArrayList<String[]> tr_fyy =DBApp.validateRecords("fyy");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_fyy.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"v09","dtfy","r9s46","mwgf8","wnp","cim","sf0x","sf94","t739","ph3"};
		DBApp.createTable("q0a3r", cols2);
		for(int i=0;i<67;i++)
		{
			String [] record_q0a3r = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_q0a3r[j] = genRandString();
			}
			DBApp.insert("q0a3r", record_q0a3r);
		}
		ArrayList<String[]> tr_q0a3r =DBApp.validateRecords("q0a3r");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_q0a3r.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"qs8mc","p1","u35","m55","wi","lyfmc","h21","psd"};
		DBApp.createTable("wec73", cols3);
		for(int i=0;i<80;i++)
		{
			String [] record_wec73 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_wec73[j] = genRandString();
			}
			DBApp.insert("wec73", record_wec73);
		}
		ArrayList<String[]> tr_wec73 =DBApp.validateRecords("wec73");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_wec73.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_64() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"du7s4","g21","b69","tp0"};
		DBApp.createTable("ha2", cols0);
		for(int i=0;i<55;i++)
		{
			String [] record_ha2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ha2[j] = genRandString();
			}
			DBApp.insert("ha2", record_ha2);
		}
		ArrayList<String[]> tr_ha2 =DBApp.validateRecords("ha2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ha2.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"p66","l34u","f8","s3m","hs9f","dfe","mg0v","r7g77","bgp"};
		DBApp.createTable("w331n", cols1);
		for(int i=0;i<1;i++)
		{
			String [] record_w331n = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_w331n[j] = genRandString();
			}
			DBApp.insert("w331n", record_w331n);
		}
		ArrayList<String[]> tr_w331n =DBApp.validateRecords("w331n");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_w331n.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"u2","q2o","nc9zh","pd","z8x","n0","en","g4f","sj"};
		DBApp.createTable("o5h", cols2);
		for(int i=0;i<5;i++)
		{
			String [] record_o5h = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_o5h[j] = genRandString();
			}
			DBApp.insert("o5h", record_o5h);
		}
		ArrayList<String[]> tr_o5h =DBApp.validateRecords("o5h");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_o5h.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"u6b1","l9","u4","hjwh"};
		DBApp.createTable("dzal", cols3);
		for(int i=0;i<1;i++)
		{
			String [] record_dzal = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_dzal[j] = genRandString();
			}
			DBApp.insert("dzal", record_dzal);
		}
		ArrayList<String[]> tr_dzal =DBApp.validateRecords("dzal");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_dzal.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_65() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"g02q","zoyh6","i67","jcl","bg4d8","iat","mdk","bx","sq43l","exx35"};
		DBApp.createTable("bt6p7", cols0);
		for(int i=0;i<10;i++)
		{
			String [] record_bt6p7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_bt6p7[j] = genRandString();
			}
			DBApp.insert("bt6p7", record_bt6p7);
		}
		ArrayList<String[]> tr_bt6p7 =DBApp.validateRecords("bt6p7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_bt6p7.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_66() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"qrd"};
		DBApp.createTable("os7", cols0);
		for(int i=0;i<94;i++)
		{
			String [] record_os7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_os7[j] = genRandString();
			}
			DBApp.insert("os7", record_os7);
		}
		ArrayList<String[]> tr_os7 =DBApp.validateRecords("os7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_os7.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_67() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"ui","s6s","v2o","klj31","ro0","s822f","s87fk","x3d","lu"};
		DBApp.createTable("uu6ya", cols0);
		for(int i=0;i<41;i++)
		{
			String [] record_uu6ya = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_uu6ya[j] = genRandString();
			}
			DBApp.insert("uu6ya", record_uu6ya);
		}
		ArrayList<String[]> tr_uu6ya =DBApp.validateRecords("uu6ya");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_uu6ya.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_68() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"dt"};
		DBApp.createTable("zul", cols0);
		for(int i=0;i<29;i++)
		{
			String [] record_zul = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_zul[j] = genRandString();
			}
			DBApp.insert("zul", record_zul);
		}
		ArrayList<String[]> tr_zul =DBApp.validateRecords("zul");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_zul.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"y96","q1","eykup","vw5","f6j","v36"};
		DBApp.createTable("tgo", cols1);
		for(int i=0;i<88;i++)
		{
			String [] record_tgo = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_tgo[j] = genRandString();
			}
			DBApp.insert("tgo", record_tgo);
		}
		ArrayList<String[]> tr_tgo =DBApp.validateRecords("tgo");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_tgo.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_69() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"p51","ef","h0","f988","csi70","hj2","fa5","g256","kh4i","l5f9"};
		DBApp.createTable("qo8", cols0);
		for(int i=0;i<7;i++)
		{
			String [] record_qo8 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qo8[j] = genRandString();
			}
			DBApp.insert("qo8", record_qo8);
		}
		ArrayList<String[]> tr_qo8 =DBApp.validateRecords("qo8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_qo8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"i461","v3e"};
		DBApp.createTable("p6jq2", cols1);
		for(int i=0;i<34;i++)
		{
			String [] record_p6jq2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_p6jq2[j] = genRandString();
			}
			DBApp.insert("p6jq2", record_p6jq2);
		}
		ArrayList<String[]> tr_p6jq2 =DBApp.validateRecords("p6jq2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_p6jq2.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_70() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"d9b"};
		DBApp.createTable("k07", cols0);
		for(int i=0;i<96;i++)
		{
			String [] record_k07 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k07[j] = genRandString();
			}
			DBApp.insert("k07", record_k07);
		}
		ArrayList<String[]> tr_k07 =DBApp.validateRecords("k07");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k07.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"n5x1","gwwal"};
		DBApp.createTable("e0k57", cols1);
		for(int i=0;i<49;i++)
		{
			String [] record_e0k57 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e0k57[j] = genRandString();
			}
			DBApp.insert("e0k57", record_e0k57);
		}
		ArrayList<String[]> tr_e0k57 =DBApp.validateRecords("e0k57");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e0k57.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"qi","dwy7k","rh","qj45","y6"};
		DBApp.createTable("r2a8", cols2);
		for(int i=0;i<15;i++)
		{
			String [] record_r2a8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_r2a8[j] = genRandString();
			}
			DBApp.insert("r2a8", record_r2a8);
		}
		ArrayList<String[]> tr_r2a8 =DBApp.validateRecords("r2a8");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_r2a8.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"ha0","mfo","nh","a7tan"};
		DBApp.createTable("x58", cols3);
		for(int i=0;i<39;i++)
		{
			String [] record_x58 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_x58[j] = genRandString();
			}
			DBApp.insert("x58", record_x58);
		}
		ArrayList<String[]> tr_x58 =DBApp.validateRecords("x58");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x58.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_71() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"rf080"};
		DBApp.createTable("tm35", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_tm35 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_tm35[j] = genRandString();
			}
			DBApp.insert("tm35", record_tm35);
		}
		ArrayList<String[]> tr_tm35 =DBApp.validateRecords("tm35");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_tm35.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"uo468","q3","u0","r2cz"};
		DBApp.createTable("c9820", cols1);
		for(int i=0;i<94;i++)
		{
			String [] record_c9820 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c9820[j] = genRandString();
			}
			DBApp.insert("c9820", record_c9820);
		}
		ArrayList<String[]> tr_c9820 =DBApp.validateRecords("c9820");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_c9820.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"gub8f","eor5x","htl","h8j03","b3","jm","e2nk","g4v","ji0ja"};
		DBApp.createTable("oe92", cols2);
		for(int i=0;i<99;i++)
		{
			String [] record_oe92 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_oe92[j] = genRandString();
			}
			DBApp.insert("oe92", record_oe92);
		}
		ArrayList<String[]> tr_oe92 =DBApp.validateRecords("oe92");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_oe92.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"vh52","vb24","fz","ae","l9","ej0e","uls","a33","t3w"};
		DBApp.createTable("wo", cols3);
		for(int i=0;i<75;i++)
		{
			String [] record_wo = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_wo[j] = genRandString();
			}
			DBApp.insert("wo", record_wo);
		}
		ArrayList<String[]> tr_wo =DBApp.validateRecords("wo");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_wo.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"l5q","d9qg"};
		DBApp.createTable("e79zd", cols4);
		for(int i=0;i<23;i++)
		{
			String [] record_e79zd = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_e79zd[j] = genRandString();
			}
			DBApp.insert("e79zd", record_e79zd);
		}
		ArrayList<String[]> tr_e79zd =DBApp.validateRecords("e79zd");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_e79zd.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_72() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"x1i8d","k8","n8i","q4by2","rb2","b06"};
		DBApp.createTable("k76", cols0);
		for(int i=0;i<53;i++)
		{
			String [] record_k76 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k76[j] = genRandString();
			}
			DBApp.insert("k76", record_k76);
		}
		ArrayList<String[]> tr_k76 =DBApp.validateRecords("k76");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_k76.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"r9x","k0hr8","e92p7","m6e","i290n","y0","gaa4q"};
		DBApp.createTable("ny", cols1);
		for(int i=0;i<9;i++)
		{
			String [] record_ny = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ny[j] = genRandString();
			}
			DBApp.insert("ny", record_ny);
		}
		ArrayList<String[]> tr_ny =DBApp.validateRecords("ny");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ny.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"y6byo","z8v","vudy"};
		DBApp.createTable("a95", cols2);
		for(int i=0;i<23;i++)
		{
			String [] record_a95 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_a95[j] = genRandString();
			}
			DBApp.insert("a95", record_a95);
		}
		ArrayList<String[]> tr_a95 =DBApp.validateRecords("a95");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_a95.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"ka8d","syh","fz08q","d02","cdl","ivu","cmn"};
		DBApp.createTable("ebgpl", cols3);
		for(int i=0;i<98;i++)
		{
			String [] record_ebgpl = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ebgpl[j] = genRandString();
			}
			DBApp.insert("ebgpl", record_ebgpl);
		}
		ArrayList<String[]> tr_ebgpl =DBApp.validateRecords("ebgpl");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ebgpl.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_73() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"sg9","t2","mjz","r8","vcbo","l56","c9344","qd9","n16"};
		DBApp.createTable("lf26", cols0);
		for(int i=0;i<68;i++)
		{
			String [] record_lf26 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lf26[j] = genRandString();
			}
			DBApp.insert("lf26", record_lf26);
		}
		ArrayList<String[]> tr_lf26 =DBApp.validateRecords("lf26");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_lf26.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"rnqqp","cvxei","yi16","r29","xe6","bs","z093","n83"};
		DBApp.createTable("h0", cols1);
		for(int i=0;i<27;i++)
		{
			String [] record_h0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h0[j] = genRandString();
			}
			DBApp.insert("h0", record_h0);
		}
		ArrayList<String[]> tr_h0 =DBApp.validateRecords("h0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_h0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"jpv","lr89","vd453","eh2","lx31k","l46","ssp","rg4","j01","aj7"};
		DBApp.createTable("ex80w", cols2);
		for(int i=0;i<13;i++)
		{
			String [] record_ex80w = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ex80w[j] = genRandString();
			}
			DBApp.insert("ex80w", record_ex80w);
		}
		ArrayList<String[]> tr_ex80w =DBApp.validateRecords("ex80w");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ex80w.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"yun","p36","dcbx2","cri3b"};
		DBApp.createTable("n76x7", cols3);
		for(int i=0;i<96;i++)
		{
			String [] record_n76x7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_n76x7[j] = genRandString();
			}
			DBApp.insert("n76x7", record_n76x7);
		}
		ArrayList<String[]> tr_n76x7 =DBApp.validateRecords("n76x7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_n76x7.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols4 = {"kuv","a9581","l9jx9"};
		DBApp.createTable("cdzcd", cols4);
		for(int i=0;i<91;i++)
		{
			String [] record_cdzcd = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_cdzcd[j] = genRandString();
			}
			DBApp.insert("cdzcd", record_cdzcd);
		}
		ArrayList<String[]> tr_cdzcd =DBApp.validateRecords("cdzcd");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_cdzcd.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_74() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"lnl","xu","y3","j1z","h2","avw","h6","fg","nf99","hui9d"};
		DBApp.createTable("l0", cols0);
		for(int i=0;i<7;i++)
		{
			String [] record_l0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l0[j] = genRandString();
			}
			DBApp.insert("l0", record_l0);
		}
		ArrayList<String[]> tr_l0 =DBApp.validateRecords("l0");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l0.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"ld0","t1d","h3o9","ed3","c5","tci4","nn4r","a9x1z"};
		DBApp.createTable("ru0gk", cols1);
		for(int i=0;i<85;i++)
		{
			String [] record_ru0gk = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ru0gk[j] = genRandString();
			}
			DBApp.insert("ru0gk", record_ru0gk);
		}
		ArrayList<String[]> tr_ru0gk =DBApp.validateRecords("ru0gk");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_ru0gk.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"ap","quj","p7n24","ri"};
		DBApp.createTable("tax4", cols2);
		for(int i=0;i<51;i++)
		{
			String [] record_tax4 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_tax4[j] = genRandString();
			}
			DBApp.insert("tax4", record_tax4);
		}
		ArrayList<String[]> tr_tax4 =DBApp.validateRecords("tax4");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_tax4.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"xxy"};
		DBApp.createTable("l8upd", cols3);
		for(int i=0;i<71;i++)
		{
			String [] record_l8upd = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_l8upd[j] = genRandString();
			}
			DBApp.insert("l8upd", record_l8upd);
		}
		ArrayList<String[]> tr_l8upd =DBApp.validateRecords("l8upd");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_l8upd.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_75() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"c4561","g320","b5l","l3n","dv3"};
		DBApp.createTable("x4y", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_x4y = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x4y[j] = genRandString();
			}
			DBApp.insert("x4y", record_x4y);
		}
		ArrayList<String[]> tr_x4y =DBApp.validateRecords("x4y");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_x4y.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSound_76() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"p1g","j1"};
		DBApp.createTable("s6", cols0);
		for(int i=0;i<82;i++)
		{
			String [] record_s6 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_s6[j] = genRandString();
			}
			DBApp.insert("s6", record_s6);
		}
		ArrayList<String[]> tr_s6 =DBApp.validateRecords("s6");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_s6.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols1 = {"z3r5j","c1","p7m0h","c2","is0","emedh"};
		DBApp.createTable("by9", cols1);
		for(int i=0;i<58;i++)
		{
			String [] record_by9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_by9[j] = genRandString();
			}
			DBApp.insert("by9", record_by9);
		}
		ArrayList<String[]> tr_by9 =DBApp.validateRecords("by9");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_by9.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols2 = {"gh","pnb","wa2uf","mk06","yx8li","x28g0","mbz32","xy7m"};
		DBApp.createTable("lm7", cols2);
		for(int i=0;i<76;i++)
		{
			String [] record_lm7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_lm7[j] = genRandString();
			}
			DBApp.insert("lm7", record_lm7);
		}
		ArrayList<String[]> tr_lm7 =DBApp.validateRecords("lm7");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_lm7.size(),  0);
		//--------------------------------------------------------------------------
		String[] cols3 = {"gcw","yc1","fg6","d8v","p1b","gyr6","ux"};
		DBApp.createTable("w2", cols3);
		for(int i=0;i<20;i++)
		{
			String [] record_w2 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_w2[j] = genRandString();
			}
			DBApp.insert("w2", record_w2);
		}
		ArrayList<String[]> tr_w2 =DBApp.validateRecords("w2");
		assertEquals("Validating a non-affected table should report zero-sized missing records list.", tr_w2.size(),  0);
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"xtaz","s1","mdz","ngl2w","m7w","bvuh","n04e","iqh"};
		DBApp.createTable("f8g", cols0);
		for(int i=0;i<9;i++)
		{
			String [] record_f8g = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f8g[j] = genRandString();
			}
			DBApp.insert("f8g", record_f8g);
		}
		DBApp.validateRecords("f8g");
		String tableTrace0 = DBApp.getLastTrace("f8g");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"gr73","ynj","mmj8n","v4f","pu","h7","y3ew","b6"};
		DBApp.createTable("fl0", cols1);
		for(int i=0;i<71;i++)
		{
			String [] record_fl0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fl0[j] = genRandString();
			}
			DBApp.insert("fl0", record_fl0);
		}
		DBApp.validateRecords("fl0");
		String tableTrace1 = DBApp.getLastTrace("fl0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"y1q","ry","mr682","qya9c","z3d","gnj94","l3n83","jy0","ub","pf7"};
		DBApp.createTable("rm67", cols2);
		for(int i=0;i<54;i++)
		{
			String [] record_rm67 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_rm67[j] = genRandString();
			}
			DBApp.insert("rm67", record_rm67);
		}
		DBApp.validateRecords("rm67");
		String tableTrace2 = DBApp.getLastTrace("rm67");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"n6c","xg9k","yl52","a2","x8882","gq279"};
		DBApp.createTable("rw4x", cols0);
		for(int i=0;i<100;i++)
		{
			String [] record_rw4x = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rw4x[j] = genRandString();
			}
			DBApp.insert("rw4x", record_rw4x);
		}
		DBApp.validateRecords("rw4x");
		String tableTrace0 = DBApp.getLastTrace("rw4x");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ch3g","x8g","pze","b698w","pj17","h7","uw4","m44z9","d3w"};
		DBApp.createTable("f96i", cols1);
		for(int i=0;i<82;i++)
		{
			String [] record_f96i = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_f96i[j] = genRandString();
			}
			DBApp.insert("f96i", record_f96i);
		}
		DBApp.validateRecords("f96i");
		String tableTrace1 = DBApp.getLastTrace("f96i");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"t27"};
		DBApp.createTable("yq", cols2);
		for(int i=0;i<31;i++)
		{
			String [] record_yq = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_yq[j] = genRandString();
			}
			DBApp.insert("yq", record_yq);
		}
		DBApp.validateRecords("yq");
		String tableTrace2 = DBApp.getLastTrace("yq");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"h3v","pud","r9f","xkby","t06","p0w","t2s","jd0n5","e90ss","s6"};
		DBApp.createTable("q6", cols0);
		for(int i=0;i<92;i++)
		{
			String [] record_q6 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_q6[j] = genRandString();
			}
			DBApp.insert("q6", record_q6);
		}
		DBApp.validateRecords("q6");
		String tableTrace0 = DBApp.getLastTrace("q6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"v3p","e074a","ekv","x22","w0b53","u22nb","ib20"};
		DBApp.createTable("fy", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_fy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_fy[j] = genRandString();
			}
			DBApp.insert("fy", record_fy);
		}
		DBApp.validateRecords("fy");
		String tableTrace0 = DBApp.getLastTrace("fy");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xt3p"};
		DBApp.createTable("y7s", cols1);
		for(int i=0;i<84;i++)
		{
			String [] record_y7s = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y7s[j] = genRandString();
			}
			DBApp.insert("y7s", record_y7s);
		}
		DBApp.validateRecords("y7s");
		String tableTrace1 = DBApp.getLastTrace("y7s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"bg","mne7","r1i9","h5w0v","m0410","jr","ju2","ih5","hs"};
		DBApp.createTable("e5", cols0);
		for(int i=0;i<36;i++)
		{
			String [] record_e5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_e5[j] = genRandString();
			}
			DBApp.insert("e5", record_e5);
		}
		DBApp.validateRecords("e5");
		String tableTrace0 = DBApp.getLastTrace("e5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"lr63","lzuj","gw"};
		DBApp.createTable("g3", cols0);
		for(int i=0;i<30;i++)
		{
			String [] record_g3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g3[j] = genRandString();
			}
			DBApp.insert("g3", record_g3);
		}
		DBApp.validateRecords("g3");
		String tableTrace0 = DBApp.getLastTrace("g3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z09","a40","t0","wt2","v28","hyn","qx6ub","o2"};
		DBApp.createTable("mu1", cols1);
		for(int i=0;i<39;i++)
		{
			String [] record_mu1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mu1[j] = genRandString();
			}
			DBApp.insert("mu1", record_mu1);
		}
		DBApp.validateRecords("mu1");
		String tableTrace1 = DBApp.getLastTrace("mu1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"b0v84","xv2","kw35","q8l","a53wm","pxcd"};
		DBApp.createTable("aea", cols0);
		for(int i=0;i<29;i++)
		{
			String [] record_aea = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_aea[j] = genRandString();
			}
			DBApp.insert("aea", record_aea);
		}
		DBApp.validateRecords("aea");
		String tableTrace0 = DBApp.getLastTrace("aea");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"zh2","b4"};
		DBApp.createTable("x9s", cols1);
		for(int i=0;i<25;i++)
		{
			String [] record_x9s = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x9s[j] = genRandString();
			}
			DBApp.insert("x9s", record_x9s);
		}
		DBApp.validateRecords("x9s");
		String tableTrace1 = DBApp.getLastTrace("x9s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"r5u","j4","hb1","os9","k967","x5","m203","ilx","f8","qf"};
		DBApp.createTable("y9", cols2);
		for(int i=0;i<22;i++)
		{
			String [] record_y9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_y9[j] = genRandString();
			}
			DBApp.insert("y9", record_y9);
		}
		DBApp.validateRecords("y9");
		String tableTrace2 = DBApp.getLastTrace("y9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"w4vv6","a3a","gyf","u67k","zfr5c","wpr1","l96q3","fon61","t260p","sunz"};
		DBApp.createTable("u76gz", cols3);
		for(int i=0;i<38;i++)
		{
			String [] record_u76gz = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_u76gz[j] = genRandString();
			}
			DBApp.insert("u76gz", record_u76gz);
		}
		DBApp.validateRecords("u76gz");
		String tableTrace3 = DBApp.getLastTrace("u76gz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"w5","ig","g1i","i2","z3","k2","f10","tb8i6","z3pk"};
		DBApp.createTable("u8v0", cols0);
		for(int i=0;i<29;i++)
		{
			String [] record_u8v0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_u8v0[j] = genRandString();
			}
			DBApp.insert("u8v0", record_u8v0);
		}
		DBApp.validateRecords("u8v0");
		String tableTrace0 = DBApp.getLastTrace("u8v0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z9","s44","kr5","lpv","kvm","pxs","vgosd","j2jw"};
		DBApp.createTable("f57", cols1);
		for(int i=0;i<56;i++)
		{
			String [] record_f57 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_f57[j] = genRandString();
			}
			DBApp.insert("f57", record_f57);
		}
		DBApp.validateRecords("f57");
		String tableTrace1 = DBApp.getLastTrace("f57");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"i8z","g6c9","y5y","ntm","t631","pvo","u7","wk64","h3a","bfo"};
		DBApp.createTable("e16", cols2);
		for(int i=0;i<44;i++)
		{
			String [] record_e16 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e16[j] = genRandString();
			}
			DBApp.insert("e16", record_e16);
		}
		DBApp.validateRecords("e16");
		String tableTrace2 = DBApp.getLastTrace("e16");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"wiua","ljb"};
		DBApp.createTable("n53", cols0);
		for(int i=0;i<23;i++)
		{
			String [] record_n53 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n53[j] = genRandString();
			}
			DBApp.insert("n53", record_n53);
		}
		DBApp.validateRecords("n53");
		String tableTrace0 = DBApp.getLastTrace("n53");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"dmri","r8vn","ih1","tdu7","y0","vz","we7","i238","zhaa4"};
		DBApp.createTable("abz", cols1);
		for(int i=0;i<78;i++)
		{
			String [] record_abz = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_abz[j] = genRandString();
			}
			DBApp.insert("abz", record_abz);
		}
		DBApp.validateRecords("abz");
		String tableTrace1 = DBApp.getLastTrace("abz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ts3","jxnr"};
		DBApp.createTable("i102", cols2);
		for(int i=0;i<81;i++)
		{
			String [] record_i102 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i102[j] = genRandString();
			}
			DBApp.insert("i102", record_i102);
		}
		DBApp.validateRecords("i102");
		String tableTrace2 = DBApp.getLastTrace("i102");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"un7","m3wn","x3076"};
		DBApp.createTable("c40s", cols3);
		for(int i=0;i<24;i++)
		{
			String [] record_c40s = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_c40s[j] = genRandString();
			}
			DBApp.insert("c40s", record_c40s);
		}
		DBApp.validateRecords("c40s");
		String tableTrace3 = DBApp.getLastTrace("c40s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"fqfy6","znk","yq23","ch8c","q6","zl4","c66","hfat6","jl5","ka92"};
		DBApp.createTable("mj", cols4);
		for(int i=0;i<39;i++)
		{
			String [] record_mj = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_mj[j] = genRandString();
			}
			DBApp.insert("mj", record_mj);
		}
		DBApp.validateRecords("mj");
		String tableTrace4 = DBApp.getLastTrace("mj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"p4"};
		DBApp.createTable("aw130", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_aw130 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_aw130[j] = genRandString();
			}
			DBApp.insert("aw130", record_aw130);
		}
		DBApp.validateRecords("aw130");
		String tableTrace0 = DBApp.getLastTrace("aw130");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ows0w","mm","i42","o5rk7","wc17","hs29w","v8760"};
		DBApp.createTable("ub", cols1);
		for(int i=0;i<23;i++)
		{
			String [] record_ub = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ub[j] = genRandString();
			}
			DBApp.insert("ub", record_ub);
		}
		DBApp.validateRecords("ub");
		String tableTrace1 = DBApp.getLastTrace("ub");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"u6dfr","rl0","u05f","b9l"};
		DBApp.createTable("ut05", cols2);
		for(int i=0;i<64;i++)
		{
			String [] record_ut05 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ut05[j] = genRandString();
			}
			DBApp.insert("ut05", record_ut05);
		}
		DBApp.validateRecords("ut05");
		String tableTrace2 = DBApp.getLastTrace("ut05");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"ma5"};
		DBApp.createTable("kq1", cols0);
		for(int i=0;i<55;i++)
		{
			String [] record_kq1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_kq1[j] = genRandString();
			}
			DBApp.insert("kq1", record_kq1);
		}
		DBApp.validateRecords("kq1");
		String tableTrace0 = DBApp.getLastTrace("kq1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"y92j4","b0k","kh","kcgds","l7","a86","k9e"};
		DBApp.createTable("ik", cols1);
		for(int i=0;i<4;i++)
		{
			String [] record_ik = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ik[j] = genRandString();
			}
			DBApp.insert("ik", record_ik);
		}
		DBApp.validateRecords("ik");
		String tableTrace1 = DBApp.getLastTrace("ik");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"x4c","hwu","g29","s1o0","a4v26","m9de","z4qm3","h6"};
		DBApp.createTable("o5d", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_o5d = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_o5d[j] = genRandString();
			}
			DBApp.insert("o5d", record_o5d);
		}
		DBApp.validateRecords("o5d");
		String tableTrace2 = DBApp.getLastTrace("o5d");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"f4v","k39","bv3"};
		DBApp.createTable("xa2p", cols0);
		for(int i=0;i<99;i++)
		{
			String [] record_xa2p = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xa2p[j] = genRandString();
			}
			DBApp.insert("xa2p", record_xa2p);
		}
		DBApp.validateRecords("xa2p");
		String tableTrace0 = DBApp.getLastTrace("xa2p");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ux4","h2"};
		DBApp.createTable("ga2bw", cols1);
		for(int i=0;i<73;i++)
		{
			String [] record_ga2bw = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ga2bw[j] = genRandString();
			}
			DBApp.insert("ga2bw", record_ga2bw);
		}
		DBApp.validateRecords("ga2bw");
		String tableTrace1 = DBApp.getLastTrace("ga2bw");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"nl7","k4qz","k8","j0h7","fp","k03"};
		DBApp.createTable("ih", cols2);
		for(int i=0;i<98;i++)
		{
			String [] record_ih = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ih[j] = genRandString();
			}
			DBApp.insert("ih", record_ih);
		}
		DBApp.validateRecords("ih");
		String tableTrace2 = DBApp.getLastTrace("ih");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"v8n46","h9","i915","vm5","p50","wij7","eb2"};
		DBApp.createTable("zr48", cols3);
		for(int i=0;i<1;i++)
		{
			String [] record_zr48 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_zr48[j] = genRandString();
			}
			DBApp.insert("zr48", record_zr48);
		}
		DBApp.validateRecords("zr48");
		String tableTrace3 = DBApp.getLastTrace("zr48");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"d7gq"};
		DBApp.createTable("re2", cols0);
		for(int i=0;i<27;i++)
		{
			String [] record_re2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_re2[j] = genRandString();
			}
			DBApp.insert("re2", record_re2);
		}
		DBApp.validateRecords("re2");
		String tableTrace0 = DBApp.getLastTrace("re2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"l243b","yre","e8nyv","p4","r7y","r11t5","n12","d5549"};
		DBApp.createTable("b07hi", cols1);
		for(int i=0;i<88;i++)
		{
			String [] record_b07hi = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b07hi[j] = genRandString();
			}
			DBApp.insert("b07hi", record_b07hi);
		}
		DBApp.validateRecords("b07hi");
		String tableTrace1 = DBApp.getLastTrace("b07hi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a3yl","aa0k","r0"};
		DBApp.createTable("c2", cols2);
		for(int i=0;i<62;i++)
		{
			String [] record_c2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_c2[j] = genRandString();
			}
			DBApp.insert("c2", record_c2);
		}
		DBApp.validateRecords("c2");
		String tableTrace2 = DBApp.getLastTrace("c2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"s0","l2c","lzz2","u5xu","i31"};
		DBApp.createTable("s4zme", cols3);
		for(int i=0;i<72;i++)
		{
			String [] record_s4zme = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_s4zme[j] = genRandString();
			}
			DBApp.insert("s4zme", record_s4zme);
		}
		DBApp.validateRecords("s4zme");
		String tableTrace3 = DBApp.getLastTrace("s4zme");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"il70"};
		DBApp.createTable("d8", cols0);
		for(int i=0;i<43;i++)
		{
			String [] record_d8 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_d8[j] = genRandString();
			}
			DBApp.insert("d8", record_d8);
		}
		DBApp.validateRecords("d8");
		String tableTrace0 = DBApp.getLastTrace("d8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"by7k","d0p9v","k5gt","rdk9","s95j1","d9","c3l5","c30wn"};
		DBApp.createTable("feh6k", cols1);
		for(int i=0;i<40;i++)
		{
			String [] record_feh6k = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_feh6k[j] = genRandString();
			}
			DBApp.insert("feh6k", record_feh6k);
		}
		DBApp.validateRecords("feh6k");
		String tableTrace1 = DBApp.getLastTrace("feh6k");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"e0","oo5l9","l43","l9dp0","ur","p7m91","e3"};
		DBApp.createTable("yvq", cols0);
		for(int i=0;i<67;i++)
		{
			String [] record_yvq = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yvq[j] = genRandString();
			}
			DBApp.insert("yvq", record_yvq);
		}
		DBApp.validateRecords("yvq");
		String tableTrace0 = DBApp.getLastTrace("yvq");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"wcwy","u746x","us","x5x","c68","u6r","hpbx3","m4","z976","r5a5"};
		DBApp.createTable("iuob5", cols1);
		for(int i=0;i<4;i++)
		{
			String [] record_iuob5 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_iuob5[j] = genRandString();
			}
			DBApp.insert("iuob5", record_iuob5);
		}
		DBApp.validateRecords("iuob5");
		String tableTrace1 = DBApp.getLastTrace("iuob5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"xa","hcx","cn"};
		DBApp.createTable("ttb7s", cols0);
		for(int i=0;i<79;i++)
		{
			String [] record_ttb7s = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ttb7s[j] = genRandString();
			}
			DBApp.insert("ttb7s", record_ttb7s);
		}
		DBApp.validateRecords("ttb7s");
		String tableTrace0 = DBApp.getLastTrace("ttb7s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"z0d","q0","px5","o4k66","v5e","l91aa","mc"};
		DBApp.createTable("c62", cols0);
		for(int i=0;i<8;i++)
		{
			String [] record_c62 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c62[j] = genRandString();
			}
			DBApp.insert("c62", record_c62);
		}
		DBApp.validateRecords("c62");
		String tableTrace0 = DBApp.getLastTrace("c62");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"z3j"};
		DBApp.createTable("bg6", cols0);
		for(int i=0;i<60;i++)
		{
			String [] record_bg6 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_bg6[j] = genRandString();
			}
			DBApp.insert("bg6", record_bg6);
		}
		DBApp.validateRecords("bg6");
		String tableTrace0 = DBApp.getLastTrace("bg6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"c7","jo"};
		DBApp.createTable("qdc", cols1);
		for(int i=0;i<59;i++)
		{
			String [] record_qdc = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_qdc[j] = genRandString();
			}
			DBApp.insert("qdc", record_qdc);
		}
		DBApp.validateRecords("qdc");
		String tableTrace1 = DBApp.getLastTrace("qdc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"q32"};
		DBApp.createTable("lez", cols2);
		for(int i=0;i<66;i++)
		{
			String [] record_lez = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_lez[j] = genRandString();
			}
			DBApp.insert("lez", record_lez);
		}
		DBApp.validateRecords("lez");
		String tableTrace2 = DBApp.getLastTrace("lez");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"z06b","r5u79","qg2","g5tp","n50","e1i89","bo2"};
		DBApp.createTable("ca8o", cols0);
		for(int i=0;i<54;i++)
		{
			String [] record_ca8o = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ca8o[j] = genRandString();
			}
			DBApp.insert("ca8o", record_ca8o);
		}
		DBApp.validateRecords("ca8o");
		String tableTrace0 = DBApp.getLastTrace("ca8o");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d38"};
		DBApp.createTable("si", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_si = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_si[j] = genRandString();
			}
			DBApp.insert("si", record_si);
		}
		DBApp.validateRecords("si");
		String tableTrace1 = DBApp.getLastTrace("si");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"f42","d4","zl90c"};
		DBApp.createTable("mbj1", cols2);
		for(int i=0;i<20;i++)
		{
			String [] record_mbj1 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_mbj1[j] = genRandString();
			}
			DBApp.insert("mbj1", record_mbj1);
		}
		DBApp.validateRecords("mbj1");
		String tableTrace2 = DBApp.getLastTrace("mbj1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"f98","m93t","vx8jh","v7","bz2","r7n","xw"};
		DBApp.createTable("l4bv", cols0);
		for(int i=0;i<62;i++)
		{
			String [] record_l4bv = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l4bv[j] = genRandString();
			}
			DBApp.insert("l4bv", record_l4bv);
		}
		DBApp.validateRecords("l4bv");
		String tableTrace0 = DBApp.getLastTrace("l4bv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"n3","xc2","bk","il38j","w4","hfz8","c69id","ac30","jf6"};
		DBApp.createTable("k1", cols1);
		for(int i=0;i<24;i++)
		{
			String [] record_k1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_k1[j] = genRandString();
			}
			DBApp.insert("k1", record_k1);
		}
		DBApp.validateRecords("k1");
		String tableTrace1 = DBApp.getLastTrace("k1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"nqj3","eh6","g9","iy2","l8","pt135"};
		DBApp.createTable("f9rac", cols0);
		for(int i=0;i<31;i++)
		{
			String [] record_f9rac = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f9rac[j] = genRandString();
			}
			DBApp.insert("f9rac", record_f9rac);
		}
		DBApp.validateRecords("f9rac");
		String tableTrace0 = DBApp.getLastTrace("f9rac");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"f0qko","e1","cn9","wu","b2so","bwf"};
		DBApp.createTable("m48", cols1);
		for(int i=0;i<43;i++)
		{
			String [] record_m48 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m48[j] = genRandString();
			}
			DBApp.insert("m48", record_m48);
		}
		DBApp.validateRecords("m48");
		String tableTrace1 = DBApp.getLastTrace("m48");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"lm","l5fq8","f2","mmc3","pvw"};
		DBApp.createTable("d5z36", cols2);
		for(int i=0;i<69;i++)
		{
			String [] record_d5z36 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_d5z36[j] = genRandString();
			}
			DBApp.insert("d5z36", record_d5z36);
		}
		DBApp.validateRecords("d5z36");
		String tableTrace2 = DBApp.getLastTrace("d5z36");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"pf","gf24","z43"};
		DBApp.createTable("qfz", cols0);
		for(int i=0;i<51;i++)
		{
			String [] record_qfz = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qfz[j] = genRandString();
			}
			DBApp.insert("qfz", record_qfz);
		}
		DBApp.validateRecords("qfz");
		String tableTrace0 = DBApp.getLastTrace("qfz");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"z7m","b36","jfh","ge2"};
		DBApp.createTable("v43", cols0);
		for(int i=0;i<72;i++)
		{
			String [] record_v43 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v43[j] = genRandString();
			}
			DBApp.insert("v43", record_v43);
		}
		DBApp.validateRecords("v43");
		String tableTrace0 = DBApp.getLastTrace("v43");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"ze5","r4","w39","g4","aby1i","f8s","pmq","tee6i"};
		DBApp.createTable("h7z3", cols0);
		for(int i=0;i<39;i++)
		{
			String [] record_h7z3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h7z3[j] = genRandString();
			}
			DBApp.insert("h7z3", record_h7z3);
		}
		DBApp.validateRecords("h7z3");
		String tableTrace0 = DBApp.getLastTrace("h7z3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"p0h","j76l","csr","i3","src","a6xk","orx1"};
		DBApp.createTable("i6u", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_i6u = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i6u[j] = genRandString();
			}
			DBApp.insert("i6u", record_i6u);
		}
		DBApp.validateRecords("i6u");
		String tableTrace1 = DBApp.getLastTrace("i6u");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"hba4","jvvb9","j0648","ybhx","eda","gq09","b56d","g6847"};
		DBApp.createTable("r28", cols2);
		for(int i=0;i<35;i++)
		{
			String [] record_r28 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_r28[j] = genRandString();
			}
			DBApp.insert("r28", record_r28);
		}
		DBApp.validateRecords("r28");
		String tableTrace2 = DBApp.getLastTrace("r28");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"u51fr","f2fk","b4r","s347p"};
		DBApp.createTable("f0u", cols3);
		for(int i=0;i<82;i++)
		{
			String [] record_f0u = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_f0u[j] = genRandString();
			}
			DBApp.insert("f0u", record_f0u);
		}
		DBApp.validateRecords("f0u");
		String tableTrace3 = DBApp.getLastTrace("f0u");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"ve3","a556","vjxh","yike","g4k","fgo"};
		DBApp.createTable("b21", cols0);
		for(int i=0;i<75;i++)
		{
			String [] record_b21 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_b21[j] = genRandString();
			}
			DBApp.insert("b21", record_b21);
		}
		DBApp.validateRecords("b21");
		String tableTrace0 = DBApp.getLastTrace("b21");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"tc035","h8","et","nia0i","yc"};
		DBApp.createTable("w98", cols1);
		for(int i=0;i<32;i++)
		{
			String [] record_w98 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_w98[j] = genRandString();
			}
			DBApp.insert("w98", record_w98);
		}
		DBApp.validateRecords("w98");
		String tableTrace1 = DBApp.getLastTrace("w98");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"efa","my0","h0s","y6"};
		DBApp.createTable("x3o", cols0);
		for(int i=0;i<39;i++)
		{
			String [] record_x3o = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x3o[j] = genRandString();
			}
			DBApp.insert("x3o", record_x3o);
		}
		DBApp.validateRecords("x3o");
		String tableTrace0 = DBApp.getLastTrace("x3o");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"ly","d21","g914d","le","ie2","xo835","qal","hinig"};
		DBApp.createTable("jk7mn", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_jk7mn = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jk7mn[j] = genRandString();
			}
			DBApp.insert("jk7mn", record_jk7mn);
		}
		DBApp.validateRecords("jk7mn");
		String tableTrace0 = DBApp.getLastTrace("jk7mn");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ieq3"};
		DBApp.createTable("x47", cols1);
		for(int i=0;i<33;i++)
		{
			String [] record_x47 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x47[j] = genRandString();
			}
			DBApp.insert("x47", record_x47);
		}
		DBApp.validateRecords("x47");
		String tableTrace1 = DBApp.getLastTrace("x47");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"f038b","n807p","gb","y9e","z47","am","cr3"};
		DBApp.createTable("uj0", cols2);
		for(int i=0;i<29;i++)
		{
			String [] record_uj0 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_uj0[j] = genRandString();
			}
			DBApp.insert("uj0", record_uj0);
		}
		DBApp.validateRecords("uj0");
		String tableTrace2 = DBApp.getLastTrace("uj0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"v54"};
		DBApp.createTable("k50", cols3);
		for(int i=0;i<55;i++)
		{
			String [] record_k50 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_k50[j] = genRandString();
			}
			DBApp.insert("k50", record_k50);
		}
		DBApp.validateRecords("k50");
		String tableTrace3 = DBApp.getLastTrace("k50");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"c8","l10","hv2"};
		DBApp.createTable("p2", cols4);
		for(int i=0;i<11;i++)
		{
			String [] record_p2 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_p2[j] = genRandString();
			}
			DBApp.insert("p2", record_p2);
		}
		DBApp.validateRecords("p2");
		String tableTrace4 = DBApp.getLastTrace("p2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"s307c","k3e","m2w2","y81j4","cp6","y0","r0s"};
		DBApp.createTable("gl1", cols0);
		for(int i=0;i<19;i++)
		{
			String [] record_gl1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_gl1[j] = genRandString();
			}
			DBApp.insert("gl1", record_gl1);
		}
		DBApp.validateRecords("gl1");
		String tableTrace0 = DBApp.getLastTrace("gl1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"catzo","y3o8","uqf89","vgv","c4470"};
		DBApp.createTable("go", cols1);
		for(int i=0;i<75;i++)
		{
			String [] record_go = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_go[j] = genRandString();
			}
			DBApp.insert("go", record_go);
		}
		DBApp.validateRecords("go");
		String tableTrace1 = DBApp.getLastTrace("go");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"o0nc","ws9","fsm","fvgj","a6q3","lsc","w2t"};
		DBApp.createTable("mj", cols0);
		for(int i=0;i<34;i++)
		{
			String [] record_mj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mj[j] = genRandString();
			}
			DBApp.insert("mj", record_mj);
		}
		DBApp.validateRecords("mj");
		String tableTrace0 = DBApp.getLastTrace("mj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t3j","doq","w23","w2uxn","w89","w11","oh6pt","o4s8v","xb92","q836"};
		DBApp.createTable("s9f7t", cols1);
		for(int i=0;i<74;i++)
		{
			String [] record_s9f7t = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_s9f7t[j] = genRandString();
			}
			DBApp.insert("s9f7t", record_s9f7t);
		}
		DBApp.validateRecords("s9f7t");
		String tableTrace1 = DBApp.getLastTrace("s9f7t");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ix834","s544o","yb1","aj5","i365","y3i"};
		DBApp.createTable("js6q", cols2);
		for(int i=0;i<61;i++)
		{
			String [] record_js6q = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_js6q[j] = genRandString();
			}
			DBApp.insert("js6q", record_js6q);
		}
		DBApp.validateRecords("js6q");
		String tableTrace2 = DBApp.getLastTrace("js6q");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"lm7","xus","c9jg","zd3"};
		DBApp.createTable("m6h0", cols3);
		for(int i=0;i<30;i++)
		{
			String [] record_m6h0 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_m6h0[j] = genRandString();
			}
			DBApp.insert("m6h0", record_m6h0);
		}
		DBApp.validateRecords("m6h0");
		String tableTrace3 = DBApp.getLastTrace("m6h0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ndk5","m1j","h70","ng333","i521n","t4dy","qdwa"};
		DBApp.createTable("a1", cols4);
		for(int i=0;i<92;i++)
		{
			String [] record_a1 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_a1[j] = genRandString();
			}
			DBApp.insert("a1", record_a1);
		}
		DBApp.validateRecords("a1");
		String tableTrace4 = DBApp.getLastTrace("a1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"p519"};
		DBApp.createTable("wif", cols0);
		for(int i=0;i<20;i++)
		{
			String [] record_wif = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_wif[j] = genRandString();
			}
			DBApp.insert("wif", record_wif);
		}
		DBApp.validateRecords("wif");
		String tableTrace0 = DBApp.getLastTrace("wif");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"va9","k3bb","w9","xy1","vv8z"};
		DBApp.createTable("jr", cols1);
		for(int i=0;i<32;i++)
		{
			String [] record_jr = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_jr[j] = genRandString();
			}
			DBApp.insert("jr", record_jr);
		}
		DBApp.validateRecords("jr");
		String tableTrace1 = DBApp.getLastTrace("jr");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"hi0o0","i99","h6qkv","hi","h2ge","yc","m5h"};
		DBApp.createTable("bv23", cols0);
		for(int i=0;i<20;i++)
		{
			String [] record_bv23 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_bv23[j] = genRandString();
			}
			DBApp.insert("bv23", record_bv23);
		}
		DBApp.validateRecords("bv23");
		String tableTrace0 = DBApp.getLastTrace("bv23");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z47q","as6m","vk5","a1h","r2m","zpzvu"};
		DBApp.createTable("hv52s", cols1);
		for(int i=0;i<42;i++)
		{
			String [] record_hv52s = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hv52s[j] = genRandString();
			}
			DBApp.insert("hv52s", record_hv52s);
		}
		DBApp.validateRecords("hv52s");
		String tableTrace1 = DBApp.getLastTrace("hv52s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"h10","f2k"};
		DBApp.createTable("mk7cp", cols0);
		for(int i=0;i<35;i++)
		{
			String [] record_mk7cp = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mk7cp[j] = genRandString();
			}
			DBApp.insert("mk7cp", record_mk7cp);
		}
		DBApp.validateRecords("mk7cp");
		String tableTrace0 = DBApp.getLastTrace("mk7cp");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"sa","scocc","pj","yw","q5","b46j","dx0","rok","d5t","b0"};
		DBApp.createTable("r35dp", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_r35dp = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_r35dp[j] = genRandString();
			}
			DBApp.insert("r35dp", record_r35dp);
		}
		DBApp.validateRecords("r35dp");
		String tableTrace1 = DBApp.getLastTrace("r35dp");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"q34a","o38","v3q","s5","j7y"};
		DBApp.createTable("f929", cols2);
		for(int i=0;i<18;i++)
		{
			String [] record_f929 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_f929[j] = genRandString();
			}
			DBApp.insert("f929", record_f929);
		}
		DBApp.validateRecords("f929");
		String tableTrace2 = DBApp.getLastTrace("f929");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"t8","uec","h29","of9","dk"};
		DBApp.createTable("p256", cols0);
		for(int i=0;i<17;i++)
		{
			String [] record_p256 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_p256[j] = genRandString();
			}
			DBApp.insert("p256", record_p256);
		}
		DBApp.validateRecords("p256");
		String tableTrace0 = DBApp.getLastTrace("p256");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"v3m","oe","kw1","vcd","c12","ch5","o4v94","t65"};
		DBApp.createTable("rq1k", cols0);
		for(int i=0;i<32;i++)
		{
			String [] record_rq1k = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rq1k[j] = genRandString();
			}
			DBApp.insert("rq1k", record_rq1k);
		}
		DBApp.validateRecords("rq1k");
		String tableTrace0 = DBApp.getLastTrace("rq1k");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"zn0","bl3vd","un8"};
		DBApp.createTable("uo83e", cols1);
		for(int i=0;i<70;i++)
		{
			String [] record_uo83e = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uo83e[j] = genRandString();
			}
			DBApp.insert("uo83e", record_uo83e);
		}
		DBApp.validateRecords("uo83e");
		String tableTrace1 = DBApp.getLastTrace("uo83e");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"ex3o","x3v","d09b"};
		DBApp.createTable("ey5z", cols0);
		for(int i=0;i<28;i++)
		{
			String [] record_ey5z = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ey5z[j] = genRandString();
			}
			DBApp.insert("ey5z", record_ey5z);
		}
		DBApp.validateRecords("ey5z");
		String tableTrace0 = DBApp.getLastTrace("ey5z");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"glk","r8dh9"};
		DBApp.createTable("fso70", cols0);
		for(int i=0;i<61;i++)
		{
			String [] record_fso70 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_fso70[j] = genRandString();
			}
			DBApp.insert("fso70", record_fso70);
		}
		DBApp.validateRecords("fso70");
		String tableTrace0 = DBApp.getLastTrace("fso70");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"n182"};
		DBApp.createTable("hrw3r", cols1);
		for(int i=0;i<84;i++)
		{
			String [] record_hrw3r = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hrw3r[j] = genRandString();
			}
			DBApp.insert("hrw3r", record_hrw3r);
		}
		DBApp.validateRecords("hrw3r");
		String tableTrace1 = DBApp.getLastTrace("hrw3r");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"ozyl","zx","k9","nlsp4"};
		DBApp.createTable("c9mj", cols0);
		for(int i=0;i<47;i++)
		{
			String [] record_c9mj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c9mj[j] = genRandString();
			}
			DBApp.insert("c9mj", record_c9mj);
		}
		DBApp.validateRecords("c9mj");
		String tableTrace0 = DBApp.getLastTrace("c9mj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ea6","i99u","oe4"};
		DBApp.createTable("i6", cols1);
		for(int i=0;i<78;i++)
		{
			String [] record_i6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i6[j] = genRandString();
			}
			DBApp.insert("i6", record_i6);
		}
		DBApp.validateRecords("i6");
		String tableTrace1 = DBApp.getLastTrace("i6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"m4","ab","bz0","be0","i2g","ts"};
		DBApp.createTable("skby1", cols2);
		for(int i=0;i<91;i++)
		{
			String [] record_skby1 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_skby1[j] = genRandString();
			}
			DBApp.insert("skby1", record_skby1);
		}
		DBApp.validateRecords("skby1");
		String tableTrace2 = DBApp.getLastTrace("skby1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"uv3ea","h7qc5","d9w","p50"};
		DBApp.createTable("pe", cols0);
		for(int i=0;i<44;i++)
		{
			String [] record_pe = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pe[j] = genRandString();
			}
			DBApp.insert("pe", record_pe);
		}
		DBApp.validateRecords("pe");
		String tableTrace0 = DBApp.getLastTrace("pe");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"sne","bff","jjx0","x9h9","scs"};
		DBApp.createTable("u0j", cols0);
		for(int i=0;i<25;i++)
		{
			String [] record_u0j = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_u0j[j] = genRandString();
			}
			DBApp.insert("u0j", record_u0j);
		}
		DBApp.validateRecords("u0j");
		String tableTrace0 = DBApp.getLastTrace("u0j");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"fuj5g","z7tk","skc5","tx","qr85s","udb","t22"};
		DBApp.createTable("bs", cols0);
		for(int i=0;i<94;i++)
		{
			String [] record_bs = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_bs[j] = genRandString();
			}
			DBApp.insert("bs", record_bs);
		}
		DBApp.validateRecords("bs");
		String tableTrace0 = DBApp.getLastTrace("bs");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"wc1","sg","c575","q2","l2gr","rsb","dgoa","vbce8","rvk0c"};
		DBApp.createTable("y2abc", cols0);
		for(int i=0;i<52;i++)
		{
			String [] record_y2abc = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_y2abc[j] = genRandString();
			}
			DBApp.insert("y2abc", record_y2abc);
		}
		DBApp.validateRecords("y2abc");
		String tableTrace0 = DBApp.getLastTrace("y2abc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w96"};
		DBApp.createTable("a9bq", cols1);
		for(int i=0;i<38;i++)
		{
			String [] record_a9bq = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_a9bq[j] = genRandString();
			}
			DBApp.insert("a9bq", record_a9bq);
		}
		DBApp.validateRecords("a9bq");
		String tableTrace1 = DBApp.getLastTrace("a9bq");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"ozwm","i0s4","q9w69"};
		DBApp.createTable("w584g", cols0);
		for(int i=0;i<61;i++)
		{
			String [] record_w584g = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_w584g[j] = genRandString();
			}
			DBApp.insert("w584g", record_w584g);
		}
		DBApp.validateRecords("w584g");
		String tableTrace0 = DBApp.getLastTrace("w584g");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"db7g7","dr","h87"};
		DBApp.createTable("dr001", cols1);
		for(int i=0;i<62;i++)
		{
			String [] record_dr001 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dr001[j] = genRandString();
			}
			DBApp.insert("dr001", record_dr001);
		}
		DBApp.validateRecords("dr001");
		String tableTrace1 = DBApp.getLastTrace("dr001");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"vt","jm0","y69s0"};
		DBApp.createTable("k2ce6", cols0);
		for(int i=0;i<96;i++)
		{
			String [] record_k2ce6 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k2ce6[j] = genRandString();
			}
			DBApp.insert("k2ce6", record_k2ce6);
		}
		DBApp.validateRecords("k2ce6");
		String tableTrace0 = DBApp.getLastTrace("k2ce6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t7e"};
		DBApp.createTable("d7", cols1);
		for(int i=0;i<39;i++)
		{
			String [] record_d7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_d7[j] = genRandString();
			}
			DBApp.insert("d7", record_d7);
		}
		DBApp.validateRecords("d7");
		String tableTrace1 = DBApp.getLastTrace("d7");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"mn4q4","i3","m5w","n680","ai8","u9","sl","xkt","kgvq","m282"};
		DBApp.createTable("lu98o", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_lu98o = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_lu98o[j] = genRandString();
			}
			DBApp.insert("lu98o", record_lu98o);
		}
		DBApp.validateRecords("lu98o");
		String tableTrace2 = DBApp.getLastTrace("lu98o");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"m35","au","k38","n39r","s7yg"};
		DBApp.createTable("z9", cols3);
		for(int i=0;i<21;i++)
		{
			String [] record_z9 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_z9[j] = genRandString();
			}
			DBApp.insert("z9", record_z9);
		}
		DBApp.validateRecords("z9");
		String tableTrace3 = DBApp.getLastTrace("z9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"q37r","m7c","ft9j","ei","j83","vq","jilb4","h84","nzmr","gg06"};
		DBApp.createTable("d38", cols4);
		for(int i=0;i<62;i++)
		{
			String [] record_d38 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_d38[j] = genRandString();
			}
			DBApp.insert("d38", record_d38);
		}
		DBApp.validateRecords("d38");
		String tableTrace4 = DBApp.getLastTrace("d38");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"oe4","g9c","h6os8","y0","quq","q8","x28","u1"};
		DBApp.createTable("m4e", cols0);
		for(int i=0;i<22;i++)
		{
			String [] record_m4e = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m4e[j] = genRandString();
			}
			DBApp.insert("m4e", record_m4e);
		}
		DBApp.validateRecords("m4e");
		String tableTrace0 = DBApp.getLastTrace("m4e");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"j39z","wf2","q8","y3p","ai3"};
		DBApp.createTable("c81", cols1);
		for(int i=0;i<64;i++)
		{
			String [] record_c81 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c81[j] = genRandString();
			}
			DBApp.insert("c81", record_c81);
		}
		DBApp.validateRecords("c81");
		String tableTrace1 = DBApp.getLastTrace("c81");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"tsg"};
		DBApp.createTable("h20a", cols0);
		for(int i=0;i<44;i++)
		{
			String [] record_h20a = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h20a[j] = genRandString();
			}
			DBApp.insert("h20a", record_h20a);
		}
		DBApp.validateRecords("h20a");
		String tableTrace0 = DBApp.getLastTrace("h20a");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"r4","qp0t1","npo"};
		DBApp.createTable("b03", cols1);
		for(int i=0;i<22;i++)
		{
			String [] record_b03 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b03[j] = genRandString();
			}
			DBApp.insert("b03", record_b03);
		}
		DBApp.validateRecords("b03");
		String tableTrace1 = DBApp.getLastTrace("b03");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"mc6","qt3n","dmcd","i27","aue","s9","uxl","u55q","v9p"};
		DBApp.createTable("f5", cols2);
		for(int i=0;i<22;i++)
		{
			String [] record_f5 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_f5[j] = genRandString();
			}
			DBApp.insert("f5", record_f5);
		}
		DBApp.validateRecords("f5");
		String tableTrace2 = DBApp.getLastTrace("f5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"t0p1g","c8","n8","rq5","nz8b","g64","z6t","s103","mo","x3w7"};
		DBApp.createTable("a13x", cols3);
		for(int i=0;i<15;i++)
		{
			String [] record_a13x = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a13x[j] = genRandString();
			}
			DBApp.insert("a13x", record_a13x);
		}
		DBApp.validateRecords("a13x");
		String tableTrace3 = DBApp.getLastTrace("a13x");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ey0"};
		DBApp.createTable("j83", cols4);
		for(int i=0;i<28;i++)
		{
			String [] record_j83 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_j83[j] = genRandString();
			}
			DBApp.insert("j83", record_j83);
		}
		DBApp.validateRecords("j83");
		String tableTrace4 = DBApp.getLastTrace("j83");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"p25g8","fex","m7y3","xqr","ea3","j3odr","ht","n05w","oqef","sx3"};
		DBApp.createTable("fb0gy", cols0);
		for(int i=0;i<83;i++)
		{
			String [] record_fb0gy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_fb0gy[j] = genRandString();
			}
			DBApp.insert("fb0gy", record_fb0gy);
		}
		DBApp.validateRecords("fb0gy");
		String tableTrace0 = DBApp.getLastTrace("fb0gy");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"yjp","xq68r","b5","im7","mym78","cez","kz932"};
		DBApp.createTable("o1p", cols1);
		for(int i=0;i<28;i++)
		{
			String [] record_o1p = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_o1p[j] = genRandString();
			}
			DBApp.insert("o1p", record_o1p);
		}
		DBApp.validateRecords("o1p");
		String tableTrace1 = DBApp.getLastTrace("o1p");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"dze3"};
		DBApp.createTable("s26s", cols2);
		for(int i=0;i<84;i++)
		{
			String [] record_s26s = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_s26s[j] = genRandString();
			}
			DBApp.insert("s26s", record_s26s);
		}
		DBApp.validateRecords("s26s");
		String tableTrace2 = DBApp.getLastTrace("s26s");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"q3f","u1rx","l2wp4","r783","m2594","pi","fp","o7z"};
		DBApp.createTable("mxrv4", cols3);
		for(int i=0;i<17;i++)
		{
			String [] record_mxrv4 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_mxrv4[j] = genRandString();
			}
			DBApp.insert("mxrv4", record_mxrv4);
		}
		DBApp.validateRecords("mxrv4");
		String tableTrace3 = DBApp.getLastTrace("mxrv4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"saxya","u8w46","uf6","ng8"};
		DBApp.createTable("g9", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_g9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g9[j] = genRandString();
			}
			DBApp.insert("g9", record_g9);
		}
		DBApp.validateRecords("g9");
		String tableTrace0 = DBApp.getLastTrace("g9");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"iz","k31rk","r0"};
		DBApp.createTable("u70", cols1);
		for(int i=0;i<62;i++)
		{
			String [] record_u70 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_u70[j] = genRandString();
			}
			DBApp.insert("u70", record_u70);
		}
		DBApp.validateRecords("u70");
		String tableTrace1 = DBApp.getLastTrace("u70");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"cxeh","jzb","gd0b","t37fi","qhfd","ukk3","sr"};
		DBApp.createTable("it54", cols2);
		for(int i=0;i<30;i++)
		{
			String [] record_it54 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_it54[j] = genRandString();
			}
			DBApp.insert("it54", record_it54);
		}
		DBApp.validateRecords("it54");
		String tableTrace2 = DBApp.getLastTrace("it54");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"cg84","m6","odex6","e12j","y9","fz5"};
		DBApp.createTable("c7", cols3);
		for(int i=0;i<73;i++)
		{
			String [] record_c7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_c7[j] = genRandString();
			}
			DBApp.insert("c7", record_c7);
		}
		DBApp.validateRecords("c7");
		String tableTrace3 = DBApp.getLastTrace("c7");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ydo","gqi4","r1","n2c"};
		DBApp.createTable("b9e", cols4);
		for(int i=0;i<31;i++)
		{
			String [] record_b9e = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_b9e[j] = genRandString();
			}
			DBApp.insert("b9e", record_b9e);
		}
		DBApp.validateRecords("b9e");
		String tableTrace4 = DBApp.getLastTrace("b9e");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"w1l","uo"};
		DBApp.createTable("tl", cols0);
		for(int i=0;i<89;i++)
		{
			String [] record_tl = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_tl[j] = genRandString();
			}
			DBApp.insert("tl", record_tl);
		}
		DBApp.validateRecords("tl");
		String tableTrace0 = DBApp.getLastTrace("tl");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"f49","cm28d","z95ji","bx","yg84m"};
		DBApp.createTable("zi6", cols1);
		for(int i=0;i<57;i++)
		{
			String [] record_zi6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_zi6[j] = genRandString();
			}
			DBApp.insert("zi6", record_zi6);
		}
		DBApp.validateRecords("zi6");
		String tableTrace1 = DBApp.getLastTrace("zi6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"o51","z4d","b52","r266","yy5","aeo1","z0"};
		DBApp.createTable("m2", cols2);
		for(int i=0;i<77;i++)
		{
			String [] record_m2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m2[j] = genRandString();
			}
			DBApp.insert("m2", record_m2);
		}
		DBApp.validateRecords("m2");
		String tableTrace2 = DBApp.getLastTrace("m2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"oa","bc018","wz2","s3f5","nup","r5b","h9","g1","b09j9"};
		DBApp.createTable("q0ax", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_q0ax = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_q0ax[j] = genRandString();
			}
			DBApp.insert("q0ax", record_q0ax);
		}
		DBApp.validateRecords("q0ax");
		String tableTrace0 = DBApp.getLastTrace("q0ax");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"rb","px8sq","q09x","k9l3","r894g","t1x"};
		DBApp.createTable("m656", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_m656 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m656[j] = genRandString();
			}
			DBApp.insert("m656", record_m656);
		}
		DBApp.validateRecords("m656");
		String tableTrace0 = DBApp.getLastTrace("m656");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"viw","z871","vu4","u8anp"};
		DBApp.createTable("r3", cols1);
		for(int i=0;i<20;i++)
		{
			String [] record_r3 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_r3[j] = genRandString();
			}
			DBApp.insert("r3", record_r3);
		}
		DBApp.validateRecords("r3");
		String tableTrace1 = DBApp.getLastTrace("r3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"gm20","aky7","zq3x6","xyg","d3q4"};
		DBApp.createTable("i96", cols2);
		for(int i=0;i<96;i++)
		{
			String [] record_i96 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i96[j] = genRandString();
			}
			DBApp.insert("i96", record_i96);
		}
		DBApp.validateRecords("i96");
		String tableTrace2 = DBApp.getLastTrace("i96");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a2y4","v5b4","a09u","el","l6bw","j0","w3p","i0","e6z"};
		DBApp.createTable("cw999", cols3);
		for(int i=0;i<32;i++)
		{
			String [] record_cw999 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_cw999[j] = genRandString();
			}
			DBApp.insert("cw999", record_cw999);
		}
		DBApp.validateRecords("cw999");
		String tableTrace3 = DBApp.getLastTrace("cw999");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"li1","r7p","o373q","d2l"};
		DBApp.createTable("v7l", cols0);
		for(int i=0;i<50;i++)
		{
			String [] record_v7l = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v7l[j] = genRandString();
			}
			DBApp.insert("v7l", record_v7l);
		}
		DBApp.validateRecords("v7l");
		String tableTrace0 = DBApp.getLastTrace("v7l");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g09","ddi","f4bn4","dt","r3"};
		DBApp.createTable("ml", cols1);
		for(int i=0;i<18;i++)
		{
			String [] record_ml = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ml[j] = genRandString();
			}
			DBApp.insert("ml", record_ml);
		}
		DBApp.validateRecords("ml");
		String tableTrace1 = DBApp.getLastTrace("ml");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"w3","a53","jw7","en1q","v8u79","so4","z0"};
		DBApp.createTable("ufg", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_ufg = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ufg[j] = genRandString();
			}
			DBApp.insert("ufg", record_ufg);
		}
		DBApp.validateRecords("ufg");
		String tableTrace0 = DBApp.getLastTrace("ufg");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z6f","n1b28","f8","n9052","w3w","qz","r6"};
		DBApp.createTable("er0", cols1);
		for(int i=0;i<11;i++)
		{
			String [] record_er0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_er0[j] = genRandString();
			}
			DBApp.insert("er0", record_er0);
		}
		DBApp.validateRecords("er0");
		String tableTrace1 = DBApp.getLastTrace("er0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"f27","ei7","ni9e","fjrq","f4w","e5y","y2a","p29a","n07"};
		DBApp.createTable("mhx", cols0);
		for(int i=0;i<46;i++)
		{
			String [] record_mhx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mhx[j] = genRandString();
			}
			DBApp.insert("mhx", record_mhx);
		}
		DBApp.validateRecords("mhx");
		String tableTrace0 = DBApp.getLastTrace("mhx");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"wj1","smr","pcg61"};
		DBApp.createTable("gkm", cols1);
		for(int i=0;i<75;i++)
		{
			String [] record_gkm = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_gkm[j] = genRandString();
			}
			DBApp.insert("gkm", record_gkm);
		}
		DBApp.validateRecords("gkm");
		String tableTrace1 = DBApp.getLastTrace("gkm");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"d53","s0","soo"};
		DBApp.createTable("g2b5w", cols2);
		for(int i=0;i<7;i++)
		{
			String [] record_g2b5w = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_g2b5w[j] = genRandString();
			}
			DBApp.insert("g2b5w", record_g2b5w);
		}
		DBApp.validateRecords("g2b5w");
		String tableTrace2 = DBApp.getLastTrace("g2b5w");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"h1","rr6","s51","t87n","y10","po5b"};
		DBApp.createTable("t51", cols3);
		for(int i=0;i<79;i++)
		{
			String [] record_t51 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_t51[j] = genRandString();
			}
			DBApp.insert("t51", record_t51);
		}
		DBApp.validateRecords("t51");
		String tableTrace3 = DBApp.getLastTrace("t51");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"v74","gob","ix65","mx93n","mtk6q","luz1","j0k","ni","pv"};
		DBApp.createTable("f5o64", cols4);
		for(int i=0;i<17;i++)
		{
			String [] record_f5o64 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_f5o64[j] = genRandString();
			}
			DBApp.insert("f5o64", record_f5o64);
		}
		DBApp.validateRecords("f5o64");
		String tableTrace4 = DBApp.getLastTrace("f5o64");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"l4d21","eil2","n482"};
		DBApp.createTable("ebc0", cols0);
		for(int i=0;i<27;i++)
		{
			String [] record_ebc0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ebc0[j] = genRandString();
			}
			DBApp.insert("ebc0", record_ebc0);
		}
		DBApp.validateRecords("ebc0");
		String tableTrace0 = DBApp.getLastTrace("ebc0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xf24","fn04","bx5x3","da","qy","pxf","v58","ut","w8ms"};
		DBApp.createTable("rp4", cols1);
		for(int i=0;i<86;i++)
		{
			String [] record_rp4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_rp4[j] = genRandString();
			}
			DBApp.insert("rp4", record_rp4);
		}
		DBApp.validateRecords("rp4");
		String tableTrace1 = DBApp.getLastTrace("rp4");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"x305"};
		DBApp.createTable("knc", cols2);
		for(int i=0;i<97;i++)
		{
			String [] record_knc = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_knc[j] = genRandString();
			}
			DBApp.insert("knc", record_knc);
		}
		DBApp.validateRecords("knc");
		String tableTrace2 = DBApp.getLastTrace("knc");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"kqbe","b10bo","z46i0"};
		DBApp.createTable("mk", cols0);
		for(int i=0;i<75;i++)
		{
			String [] record_mk = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mk[j] = genRandString();
			}
			DBApp.insert("mk", record_mk);
		}
		DBApp.validateRecords("mk");
		String tableTrace0 = DBApp.getLastTrace("mk");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vx956","tsc","z4gq7","er5j8","nivk","y6n12","lt9","n93uv","eue3"};
		DBApp.createTable("x6t", cols1);
		for(int i=0;i<41;i++)
		{
			String [] record_x6t = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x6t[j] = genRandString();
			}
			DBApp.insert("x6t", record_x6t);
		}
		DBApp.validateRecords("x6t");
		String tableTrace1 = DBApp.getLastTrace("x6t");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ne3","bn","lyi4p","l75","ral67","j108","a43","anr","m4","n42o"};
		DBApp.createTable("vd2w", cols2);
		for(int i=0;i<56;i++)
		{
			String [] record_vd2w = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_vd2w[j] = genRandString();
			}
			DBApp.insert("vd2w", record_vd2w);
		}
		DBApp.validateRecords("vd2w");
		String tableTrace2 = DBApp.getLastTrace("vd2w");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"e48","d8d","e93","hqlo"};
		DBApp.createTable("pukwa", cols3);
		for(int i=0;i<14;i++)
		{
			String [] record_pukwa = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_pukwa[j] = genRandString();
			}
			DBApp.insert("pukwa", record_pukwa);
		}
		DBApp.validateRecords("pukwa");
		String tableTrace3 = DBApp.getLastTrace("pukwa");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"qc","h3q6","dp46","g86","e6","m9b1","b3o"};
		DBApp.createTable("da", cols4);
		for(int i=0;i<63;i++)
		{
			String [] record_da = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_da[j] = genRandString();
			}
			DBApp.insert("da", record_da);
		}
		DBApp.validateRecords("da");
		String tableTrace4 = DBApp.getLastTrace("da");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_55() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"w1a","l9733","s07","r1","o8b","sv64","n9m0"};
		DBApp.createTable("x52", cols0);
		for(int i=0;i<82;i++)
		{
			String [] record_x52 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x52[j] = genRandString();
			}
			DBApp.insert("x52", record_x52);
		}
		DBApp.validateRecords("x52");
		String tableTrace0 = DBApp.getLastTrace("x52");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_56() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"q34jq","w10","pv9","br","hr","vv8","u6ww","g1z","xv2q","i2"};
		DBApp.createTable("l5p5", cols0);
		for(int i=0;i<45;i++)
		{
			String [] record_l5p5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l5p5[j] = genRandString();
			}
			DBApp.insert("l5p5", record_l5p5);
		}
		DBApp.validateRecords("l5p5");
		String tableTrace0 = DBApp.getLastTrace("l5p5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"phn","ovp6","d0b","w9u8","vo","a1","vw3l","e9t"};
		DBApp.createTable("s64n", cols1);
		for(int i=0;i<57;i++)
		{
			String [] record_s64n = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_s64n[j] = genRandString();
			}
			DBApp.insert("s64n", record_s64n);
		}
		DBApp.validateRecords("s64n");
		String tableTrace1 = DBApp.getLastTrace("s64n");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_57() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"mg9","okzd"};
		DBApp.createTable("gv5", cols0);
		for(int i=0;i<67;i++)
		{
			String [] record_gv5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_gv5[j] = genRandString();
			}
			DBApp.insert("gv5", record_gv5);
		}
		DBApp.validateRecords("gv5");
		String tableTrace0 = DBApp.getLastTrace("gv5");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"sn5","cvd"};
		DBApp.createTable("vo", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_vo = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_vo[j] = genRandString();
			}
			DBApp.insert("vo", record_vo);
		}
		DBApp.validateRecords("vo");
		String tableTrace1 = DBApp.getLastTrace("vo");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"vkumf"};
		DBApp.createTable("y8ll2", cols2);
		for(int i=0;i<76;i++)
		{
			String [] record_y8ll2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_y8ll2[j] = genRandString();
			}
			DBApp.insert("y8ll2", record_y8ll2);
		}
		DBApp.validateRecords("y8ll2");
		String tableTrace2 = DBApp.getLastTrace("y8ll2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"q5","yio4","o2v","coz1n","gjlu7","kwz","nwa","cr2"};
		DBApp.createTable("a75", cols3);
		for(int i=0;i<4;i++)
		{
			String [] record_a75 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a75[j] = genRandString();
			}
			DBApp.insert("a75", record_a75);
		}
		DBApp.validateRecords("a75");
		String tableTrace3 = DBApp.getLastTrace("a75");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_58() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"r06zj","oh","w3p7q","ze3","d9"};
		DBApp.createTable("yx", cols0);
		for(int i=0;i<38;i++)
		{
			String [] record_yx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yx[j] = genRandString();
			}
			DBApp.insert("yx", record_yx);
		}
		DBApp.validateRecords("yx");
		String tableTrace0 = DBApp.getLastTrace("yx");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"i91","y81w5","c6","n5l","k8s","pk4n6","gg7","n44"};
		DBApp.createTable("iye68", cols1);
		for(int i=0;i<15;i++)
		{
			String [] record_iye68 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_iye68[j] = genRandString();
			}
			DBApp.insert("iye68", record_iye68);
		}
		DBApp.validateRecords("iye68");
		String tableTrace1 = DBApp.getLastTrace("iye68");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_59() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"b83"};
		DBApp.createTable("ql", cols0);
		for(int i=0;i<3;i++)
		{
			String [] record_ql = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ql[j] = genRandString();
			}
			DBApp.insert("ql", record_ql);
		}
		DBApp.validateRecords("ql");
		String tableTrace0 = DBApp.getLastTrace("ql");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"o2w2","e6d5","j9n1","a33","z2y2"};
		DBApp.createTable("kv2", cols1);
		for(int i=0;i<43;i++)
		{
			String [] record_kv2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_kv2[j] = genRandString();
			}
			DBApp.insert("kv2", record_kv2);
		}
		DBApp.validateRecords("kv2");
		String tableTrace1 = DBApp.getLastTrace("kv2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"udxx","i92","iz7d","y8s","ci4","y5id","e68o4","o11u","r4747","wg9"};
		DBApp.createTable("a3o8", cols2);
		for(int i=0;i<59;i++)
		{
			String [] record_a3o8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_a3o8[j] = genRandString();
			}
			DBApp.insert("a3o8", record_a3o8);
		}
		DBApp.validateRecords("a3o8");
		String tableTrace2 = DBApp.getLastTrace("a3o8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"q50","k2095","lfy85","oy"};
		DBApp.createTable("n8f", cols3);
		for(int i=0;i<39;i++)
		{
			String [] record_n8f = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_n8f[j] = genRandString();
			}
			DBApp.insert("n8f", record_n8f);
		}
		DBApp.validateRecords("n8f");
		String tableTrace3 = DBApp.getLastTrace("n8f");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_60() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"fnm","o7"};
		DBApp.createTable("s96q", cols0);
		for(int i=0;i<79;i++)
		{
			String [] record_s96q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_s96q[j] = genRandString();
			}
			DBApp.insert("s96q", record_s96q);
		}
		DBApp.validateRecords("s96q");
		String tableTrace0 = DBApp.getLastTrace("s96q");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"c35","njs","nh8","q7","vwmb8"};
		DBApp.createTable("fc13", cols1);
		for(int i=0;i<83;i++)
		{
			String [] record_fc13 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fc13[j] = genRandString();
			}
			DBApp.insert("fc13", record_fc13);
		}
		DBApp.validateRecords("fc13");
		String tableTrace1 = DBApp.getLastTrace("fc13");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_61() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"l44","o68","p0m8u"};
		DBApp.createTable("b48fi", cols0);
		for(int i=0;i<74;i++)
		{
			String [] record_b48fi = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_b48fi[j] = genRandString();
			}
			DBApp.insert("b48fi", record_b48fi);
		}
		DBApp.validateRecords("b48fi");
		String tableTrace0 = DBApp.getLastTrace("b48fi");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ufl","u28","r2ly","n0zm","zadd","v01","bht","lh7","v7yza"};
		DBApp.createTable("y74", cols1);
		for(int i=0;i<32;i++)
		{
			String [] record_y74 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y74[j] = genRandString();
			}
			DBApp.insert("y74", record_y74);
		}
		DBApp.validateRecords("y74");
		String tableTrace1 = DBApp.getLastTrace("y74");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_62() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"amb","yfw","q8","osi","l2s"};
		DBApp.createTable("xrcm", cols0);
		for(int i=0;i<39;i++)
		{
			String [] record_xrcm = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xrcm[j] = genRandString();
			}
			DBApp.insert("xrcm", record_xrcm);
		}
		DBApp.validateRecords("xrcm");
		String tableTrace0 = DBApp.getLastTrace("xrcm");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ehd","w7o","bo","e4","mxhg","qe169","dp","grw9","jx","k08"};
		DBApp.createTable("pro5q", cols1);
		for(int i=0;i<9;i++)
		{
			String [] record_pro5q = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pro5q[j] = genRandString();
			}
			DBApp.insert("pro5q", record_pro5q);
		}
		DBApp.validateRecords("pro5q");
		String tableTrace1 = DBApp.getLastTrace("pro5q");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"wuar","m4o4","l7"};
		DBApp.createTable("c9cl", cols2);
		for(int i=0;i<96;i++)
		{
			String [] record_c9cl = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_c9cl[j] = genRandString();
			}
			DBApp.insert("c9cl", record_c9cl);
		}
		DBApp.validateRecords("c9cl");
		String tableTrace2 = DBApp.getLastTrace("c9cl");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"ig","sz2","lpxs","l67","xw9","o37c1","me5","e98y1"};
		DBApp.createTable("a86", cols3);
		for(int i=0;i<15;i++)
		{
			String [] record_a86 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a86[j] = genRandString();
			}
			DBApp.insert("a86", record_a86);
		}
		DBApp.validateRecords("a86");
		String tableTrace3 = DBApp.getLastTrace("a86");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols4 = {"eyxc","l3jb","ac1","b7hlg","zyug2","e17p2","rn1","f3"};
		DBApp.createTable("ah7", cols4);
		for(int i=0;i<61;i++)
		{
			String [] record_ah7 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_ah7[j] = genRandString();
			}
			DBApp.insert("ah7", record_ah7);
		}
		DBApp.validateRecords("ah7");
		String tableTrace4 = DBApp.getLastTrace("ah7");
		assertTrue("Validating a table should appear in the last trace.", tableTrace4.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace4.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_63() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"c434","cs489"};
		DBApp.createTable("c12y", cols0);
		for(int i=0;i<28;i++)
		{
			String [] record_c12y = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c12y[j] = genRandString();
			}
			DBApp.insert("c12y", record_c12y);
		}
		DBApp.validateRecords("c12y");
		String tableTrace0 = DBApp.getLastTrace("c12y");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"h3","ix8","i34im","h8","rp17","e62"};
		DBApp.createTable("psna6", cols1);
		for(int i=0;i<21;i++)
		{
			String [] record_psna6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_psna6[j] = genRandString();
			}
			DBApp.insert("psna6", record_psna6);
		}
		DBApp.validateRecords("psna6");
		String tableTrace1 = DBApp.getLastTrace("psna6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"t3x","mtf","ah85","vp4","f129l","hv1","sy74k","yf23","ac","in5"};
		DBApp.createTable("a8", cols2);
		for(int i=0;i<73;i++)
		{
			String [] record_a8 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_a8[j] = genRandString();
			}
			DBApp.insert("a8", record_a8);
		}
		DBApp.validateRecords("a8");
		String tableTrace2 = DBApp.getLastTrace("a8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"no","unp9k","m92","tzv","a0gf","s2"};
		DBApp.createTable("f6z", cols3);
		for(int i=0;i<12;i++)
		{
			String [] record_f6z = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_f6z[j] = genRandString();
			}
			DBApp.insert("f6z", record_f6z);
		}
		DBApp.validateRecords("f6z");
		String tableTrace3 = DBApp.getLastTrace("f6z");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_64() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"dre0","u6u89","sxi","z9","b1ox1","n96q","bnx4n","g2m","k8c7","q3"};
		DBApp.createTable("ldx", cols0);
		for(int i=0;i<96;i++)
		{
			String [] record_ldx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ldx[j] = genRandString();
			}
			DBApp.insert("ldx", record_ldx);
		}
		DBApp.validateRecords("ldx");
		String tableTrace0 = DBApp.getLastTrace("ldx");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"cgk","cze","iu4a0","vt3s9","u2","ch01"};
		DBApp.createTable("sop", cols1);
		for(int i=0;i<1;i++)
		{
			String [] record_sop = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_sop[j] = genRandString();
			}
			DBApp.insert("sop", record_sop);
		}
		DBApp.validateRecords("sop");
		String tableTrace1 = DBApp.getLastTrace("sop");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ayc","y5b","ihi2","vk","nri1t","xz"};
		DBApp.createTable("ef", cols2);
		for(int i=0;i<11;i++)
		{
			String [] record_ef = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ef[j] = genRandString();
			}
			DBApp.insert("ef", record_ef);
		}
		DBApp.validateRecords("ef");
		String tableTrace2 = DBApp.getLastTrace("ef");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"l6p","q1h9"};
		DBApp.createTable("a3", cols3);
		for(int i=0;i<54;i++)
		{
			String [] record_a3 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a3[j] = genRandString();
			}
			DBApp.insert("a3", record_a3);
		}
		DBApp.validateRecords("a3");
		String tableTrace3 = DBApp.getLastTrace("a3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_65() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"q7","j05","tn80i","jg2","a95","t3h0"};
		DBApp.createTable("d2", cols0);
		for(int i=0;i<76;i++)
		{
			String [] record_d2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_d2[j] = genRandString();
			}
			DBApp.insert("d2", record_d2);
		}
		DBApp.validateRecords("d2");
		String tableTrace0 = DBApp.getLastTrace("d2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_66() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"e9x"};
		DBApp.createTable("z9i", cols0);
		for(int i=0;i<24;i++)
		{
			String [] record_z9i = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z9i[j] = genRandString();
			}
			DBApp.insert("z9i", record_z9i);
		}
		DBApp.validateRecords("z9i");
		String tableTrace0 = DBApp.getLastTrace("z9i");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"am","lrp2","uu5","s994","oi70y","jlrs6","db6"};
		DBApp.createTable("aa1bv", cols1);
		for(int i=0;i<92;i++)
		{
			String [] record_aa1bv = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_aa1bv[j] = genRandString();
			}
			DBApp.insert("aa1bv", record_aa1bv);
		}
		DBApp.validateRecords("aa1bv");
		String tableTrace1 = DBApp.getLastTrace("aa1bv");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"hp4","lr"};
		DBApp.createTable("ght", cols2);
		for(int i=0;i<90;i++)
		{
			String [] record_ght = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ght[j] = genRandString();
			}
			DBApp.insert("ght", record_ght);
		}
		DBApp.validateRecords("ght");
		String tableTrace2 = DBApp.getLastTrace("ght");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_67() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"mq9"};
		DBApp.createTable("qaq", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_qaq = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qaq[j] = genRandString();
			}
			DBApp.insert("qaq", record_qaq);
		}
		DBApp.validateRecords("qaq");
		String tableTrace0 = DBApp.getLastTrace("qaq");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"o9gl","ft9","umh","sw","a8ps","su","n1"};
		DBApp.createTable("t7m", cols1);
		for(int i=0;i<62;i++)
		{
			String [] record_t7m = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_t7m[j] = genRandString();
			}
			DBApp.insert("t7m", record_t7m);
		}
		DBApp.validateRecords("t7m");
		String tableTrace1 = DBApp.getLastTrace("t7m");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"n15","nl52t","j21","q92zz","jf537"};
		DBApp.createTable("wm0", cols2);
		for(int i=0;i<12;i++)
		{
			String [] record_wm0 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_wm0[j] = genRandString();
			}
			DBApp.insert("wm0", record_wm0);
		}
		DBApp.validateRecords("wm0");
		String tableTrace2 = DBApp.getLastTrace("wm0");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"evr0","yx","h47","h1w","sd0"};
		DBApp.createTable("x6", cols3);
		for(int i=0;i<23;i++)
		{
			String [] record_x6 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_x6[j] = genRandString();
			}
			DBApp.insert("x6", record_x6);
		}
		DBApp.validateRecords("x6");
		String tableTrace3 = DBApp.getLastTrace("x6");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_68() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"g9n","w1","qt36x"};
		DBApp.createTable("cnf", cols0);
		for(int i=0;i<59;i++)
		{
			String [] record_cnf = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_cnf[j] = genRandString();
			}
			DBApp.insert("cnf", record_cnf);
		}
		DBApp.validateRecords("cnf");
		String tableTrace0 = DBApp.getLastTrace("cnf");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_69() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"le","r1l","q4em5"};
		DBApp.createTable("d2j9l", cols0);
		for(int i=0;i<58;i++)
		{
			String [] record_d2j9l = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_d2j9l[j] = genRandString();
			}
			DBApp.insert("d2j9l", record_d2j9l);
		}
		DBApp.validateRecords("d2j9l");
		String tableTrace0 = DBApp.getLastTrace("d2j9l");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w92p","k223","azv6","k0","j13g","v76","i93x","t8"};
		DBApp.createTable("t245n", cols1);
		for(int i=0;i<28;i++)
		{
			String [] record_t245n = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_t245n[j] = genRandString();
			}
			DBApp.insert("t245n", record_t245n);
		}
		DBApp.validateRecords("t245n");
		String tableTrace1 = DBApp.getLastTrace("t245n");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_70() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"l29","u8","hxg","rsv","vyi6","khu","do0","w04ib","gjsmn","jo9"};
		DBApp.createTable("pik", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_pik = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pik[j] = genRandString();
			}
			DBApp.insert("pik", record_pik);
		}
		DBApp.validateRecords("pik");
		String tableTrace0 = DBApp.getLastTrace("pik");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_71() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"ufv50"};
		DBApp.createTable("is", cols0);
		for(int i=0;i<30;i++)
		{
			String [] record_is = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_is[j] = genRandString();
			}
			DBApp.insert("is", record_is);
		}
		DBApp.validateRecords("is");
		String tableTrace0 = DBApp.getLastTrace("is");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"pd","xs2i","fy0nn","h1","q3y7","jj","v3a9j","hw78","n0644","zgr78"};
		DBApp.createTable("s537", cols1);
		for(int i=0;i<35;i++)
		{
			String [] record_s537 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_s537[j] = genRandString();
			}
			DBApp.insert("s537", record_s537);
		}
		DBApp.validateRecords("s537");
		String tableTrace1 = DBApp.getLastTrace("s537");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"j92bp","xo","ni6y"};
		DBApp.createTable("gjs2", cols2);
		for(int i=0;i<90;i++)
		{
			String [] record_gjs2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_gjs2[j] = genRandString();
			}
			DBApp.insert("gjs2", record_gjs2);
		}
		DBApp.validateRecords("gjs2");
		String tableTrace2 = DBApp.getLastTrace("gjs2");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"jv5","h9","zb4fi","h7k56","eo0","g3fs7","n5v","eqs","t67","v4q8"};
		DBApp.createTable("fs", cols3);
		for(int i=0;i<67;i++)
		{
			String [] record_fs = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_fs[j] = genRandString();
			}
			DBApp.insert("fs", record_fs);
		}
		DBApp.validateRecords("fs");
		String tableTrace3 = DBApp.getLastTrace("fs");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_72() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"s5xw9"};
		DBApp.createTable("z218", cols0);
		for(int i=0;i<84;i++)
		{
			String [] record_z218 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z218[j] = genRandString();
			}
			DBApp.insert("z218", record_z218);
		}
		DBApp.validateRecords("z218");
		String tableTrace0 = DBApp.getLastTrace("z218");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"khwx9","utr","rm2t","i8l","wl521"};
		DBApp.createTable("c45", cols1);
		for(int i=0;i<66;i++)
		{
			String [] record_c45 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c45[j] = genRandString();
			}
			DBApp.insert("c45", record_c45);
		}
		DBApp.validateRecords("c45");
		String tableTrace1 = DBApp.getLastTrace("c45");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_73() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"pq","j8x2","mnq6","iogh","ww9","ya9uw"};
		DBApp.createTable("pz3", cols0);
		for(int i=0;i<87;i++)
		{
			String [] record_pz3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pz3[j] = genRandString();
			}
			DBApp.insert("pz3", record_pz3);
		}
		DBApp.validateRecords("pz3");
		String tableTrace0 = DBApp.getLastTrace("pz3");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_74() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"h35","h68","h13jy","b7","t2o3k","znm","h67"};
		DBApp.createTable("juoj", cols0);
		for(int i=0;i<54;i++)
		{
			String [] record_juoj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_juoj[j] = genRandString();
			}
			DBApp.insert("juoj", record_juoj);
		}
		DBApp.validateRecords("juoj");
		String tableTrace0 = DBApp.getLastTrace("juoj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_75() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"rqlh","ym","r0e2v","q8bl","z0o3","bnd","avmd","fy1","c4q98"};
		DBApp.createTable("lv671", cols0);
		for(int i=0;i<30;i++)
		{
			String [] record_lv671 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lv671[j] = genRandString();
			}
			DBApp.insert("lv671", record_lv671);
		}
		DBApp.validateRecords("lv671");
		String tableTrace0 = DBApp.getLastTrace("lv671");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ppk","t12w3","md5"};
		DBApp.createTable("b8", cols1);
		for(int i=0;i<32;i++)
		{
			String [] record_b8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b8[j] = genRandString();
			}
			DBApp.insert("b8", record_b8);
		}
		DBApp.validateRecords("b8");
		String tableTrace1 = DBApp.getLastTrace("b8");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationSoundTrace_76() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"ie4"};
		DBApp.createTable("dg", cols0);
		for(int i=0;i<43;i++)
		{
			String [] record_dg = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dg[j] = genRandString();
			}
			DBApp.insert("dg", record_dg);
		}
		DBApp.validateRecords("dg");
		String tableTrace0 = DBApp.getLastTrace("dg");
		assertTrue("Validating a table should appear in the last trace.", tableTrace0.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace0.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols1 = {"s72q3","a07","x8w","kk6","wqqq","y3ne5","m8im","v1","ke9","r87i"};
		DBApp.createTable("x1", cols1);
		for(int i=0;i<7;i++)
		{
			String [] record_x1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x1[j] = genRandString();
			}
			DBApp.insert("x1", record_x1);
		}
		DBApp.validateRecords("x1");
		String tableTrace1 = DBApp.getLastTrace("x1");
		assertTrue("Validating a table should appear in the last trace.", tableTrace1.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace1.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols2 = {"tw","a37","ab917","i2f","g1","f29","t8a","si3","bs71","gm5"};
		DBApp.createTable("mj", cols2);
		for(int i=0;i<68;i++)
		{
			String [] record_mj = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_mj[j] = genRandString();
			}
			DBApp.insert("mj", record_mj);
		}
		DBApp.validateRecords("mj");
		String tableTrace2 = DBApp.getLastTrace("mj");
		assertTrue("Validating a table should appear in the last trace.", tableTrace2.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace2.contains("0 records missing."));
		//--------------------------------------------------------------------------
		String[] cols3 = {"xzj","ufw71","zsn","je159"};
		DBApp.createTable("u0v", cols3);
		for(int i=0;i<43;i++)
		{
			String [] record_u0v = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_u0v[j] = genRandString();
			}
			DBApp.insert("u0v", record_u0v);
		}
		DBApp.validateRecords("u0v");
		String tableTrace3 = DBApp.getLastTrace("u0v");
		assertTrue("Validating a table should appear in the last trace.", tableTrace3.contains("Validating records"));
		assertTrue("Validating a non-affected table should report zero missing records.", tableTrace3.contains("0 records missing."));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("g316e", cols0);
		String [][] records_g316e = new String[177][cols0.length];
		for(int i=0;i<177;i++)
		{
			records_g316e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g316e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g316e", records_g316e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7, p12, q2, r10, s1]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5, o8, p13, q3, r11, s2]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6, o9, p14, q4, r12, s3]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6, n7, o10, p15, q5, r13, s4]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8, o11, p0, q6, r14, s5]
		int pageCount0 = (int)Math.ceil(177.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g316e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g316e"+ File.separator+i+".db");
				dir_g316e.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("g316e");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rw", cols1);
		String [][] records_rw = new String[134][cols1.length];
		for(int i=0;i<134;i++)
		{
			records_rw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rw", records_rw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		int pageCount1 = (int)Math.ceil(134.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rw"+ File.separator+i+".db");
				dir_rw.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("rw");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p6z", cols2);
		String [][] records_p6z = new String[298][cols2.length];
		for(int i=0;i<298;i++)
		{
			records_p6z[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p6z[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p6z", records_p6z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6]
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		int pageCount2 = (int)Math.ceil(298.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p6z = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p6z"+ File.separator+i+".db");
				dir_p6z.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("p6z");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xk9gh", cols3);
		String [][] records_xk9gh = new String[302][cols3.length];
		for(int i=0;i<302;i++)
		{
			records_xk9gh[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xk9gh[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xk9gh", records_xk9gh[i]);
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
		int pageCount3 = (int)Math.ceil(302.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xk9gh = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xk9gh"+ File.separator+i+".db");
				dir_xk9gh.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("xk9gh");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g0gn", cols4);
		String [][] records_g0gn = new String[178][cols4.length];
		for(int i=0;i<178;i++)
		{
			records_g0gn[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g0gn[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g0gn", records_g0gn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6]
		int pageCount4 = (int)Math.ceil(178.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_g0gn = new File(FileManager.directory.getAbsolutePath()+ File.separator + "g0gn"+ File.separator+i+".db");
				dir_g0gn.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("g0gn");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s3ucn", cols0);
		String [][] records_s3ucn = new String[300][cols0.length];
		for(int i=0;i<300;i++)
		{
			records_s3ucn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s3ucn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s3ucn", records_s3ucn[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record295: [a295, b1, c1, d3, e0, f1, g1, h7, i7, j5, k9, l7, m9, n1, o10]
		//record296: [a296, b0, c2, d0, e1, f2, g2, h0, i8, j6, k10, l8, m10, n2, o11]
		//record297: [a297, b1, c0, d1, e2, f3, g3, h1, i0, j7, k0, l9, m11, n3, o12]
		//record298: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1, l10, m12, n4, o13]
		//record299: [a299, b1, c2, d3, e4, f5, g5, h3, i2, j9, k2, l11, m0, n5, o14]
		int pageCount0 = (int)Math.ceil(300.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s3ucn = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s3ucn"+ File.separator+i+".db");
				dir_s3ucn.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("s3ucn");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("t6jk2", cols0);
		String [][] records_t6jk2 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_t6jk2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t6jk2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t6jk2", records_t6jk2[i]);
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
		int pageCount0 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t6jk2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t6jk2"+ File.separator+i+".db");
				dir_t6jk2.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("t6jk2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o0g", cols1);
		String [][] records_o0g = new String[235][cols1.length];
		for(int i=0;i<235;i++)
		{
			records_o0g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o0g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o0g", records_o0g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6]
		//record231: [a231, b1, c0, d3, e1, f3, g0]
		//record232: [a232, b0, c1, d0, e2, f4, g1]
		//record233: [a233, b1, c2, d1, e3, f5, g2]
		//record234: [a234, b0, c0, d2, e4, f0, g3]
		int pageCount1 = (int)Math.ceil(235.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o0g = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o0g"+ File.separator+i+".db");
				dir_o0g.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("o0g");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("i567", cols2);
		String [][] records_i567 = new String[257][cols2.length];
		for(int i=0;i<257;i++)
		{
			records_i567[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i567[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i567", records_i567[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6, n1]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3]
		//record256: [a256, b0, c1, d0, e1, f4, g4, h0, i4, j6, k3, l4, m9, n4]
		int pageCount2 = (int)Math.ceil(257.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i567 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i567"+ File.separator+i+".db");
				dir_i567.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("i567");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("k371", cols0);
		String [][] records_k371 = new String[135][cols0.length];
		for(int i=0;i<135;i++)
		{
			records_k371[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k371[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k371", records_k371[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4]
		int pageCount0 = (int)Math.ceil(135.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k371 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k371"+ File.separator+i+".db");
				dir_k371.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("k371");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("o3", cols1);
		String [][] records_o3 = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			records_o3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o3", records_o3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		int pageCount1 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_o3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "o3"+ File.separator+i+".db");
				dir_o3.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("o3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("cx9", cols2);
		String [][] records_cx9 = new String[335][cols2.length];
		for(int i=0;i<335;i++)
		{
			records_cx9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cx9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cx9", records_cx9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8, o0, p10]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9, o1, p11]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10, o2, p12]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3, p13]
		//record334: [a334, b0, c1, d2, e4, f4, g5, h6, i1, j4, k4, l10, m9, n12, o4, p14]
		int pageCount2 = (int)Math.ceil(335.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cx9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cx9"+ File.separator+i+".db");
				dir_cx9.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("cx9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("p181", cols3);
		String [][] records_p181 = new String[231][cols3.length];
		for(int i=0;i<231;i++)
		{
			records_p181[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p181[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p181", records_p181[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6, l10, m5, n2, o1, p2, q5]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7, k7, l11, m6, n3, o2, p3, q6]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8, k8, l0, m7, n4, o3, p4, q7]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9, k9, l1, m8, n5, o4, p5, q8]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6, o5, p6, q9]
		int pageCount3 = (int)Math.ceil(231.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_p181 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "p181"+ File.separator+i+".db");
				dir_p181.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("p181");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ec82b", cols0);
		String [][] records_ec82b = new String[291][cols0.length];
		for(int i=0;i<291;i++)
		{
			records_ec82b[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ec82b[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ec82b", records_ec82b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record286: [a286, b0, c1, d2, e1, f4, g6]
		//record287: [a287, b1, c2, d3, e2, f5, g0]
		//record288: [a288, b0, c0, d0, e3, f0, g1]
		//record289: [a289, b1, c1, d1, e4, f1, g2]
		//record290: [a290, b0, c2, d2, e0, f2, g3]
		int pageCount0 = (int)Math.ceil(291.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ec82b = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ec82b"+ File.separator+i+".db");
				dir_ec82b.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("ec82b");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ko1fl", cols1);
		String [][] records_ko1fl = new String[437][cols1.length];
		for(int i=0;i<437;i++)
		{
			records_ko1fl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ko1fl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ko1fl", records_ko1fl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record432: [a432, b0, c0, d0, e2, f0, g5]
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		int pageCount1 = (int)Math.ceil(437.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ko1fl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ko1fl"+ File.separator+i+".db");
				dir_ko1fl.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("ko1fl");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("og8", cols0);
		String [][] records_og8 = new String[482][cols0.length];
		for(int i=0;i<482;i++)
		{
			records_og8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_og8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("og8", records_og8[i]);
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
		int pageCount0 = (int)Math.ceil(482.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_og8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "og8"+ File.separator+i+".db");
				dir_og8.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("og8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k7a38", cols1);
		String [][] records_k7a38 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_k7a38[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k7a38[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k7a38", records_k7a38[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		int pageCount1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k7a38 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k7a38"+ File.separator+i+".db");
				dir_k7a38.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("k7a38");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("va372", cols0);
		String [][] records_va372 = new String[295][cols0.length];
		for(int i=0;i<295;i++)
		{
			records_va372[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_va372[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("va372", records_va372[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record290: [a290, b0, c2, d2, e0, f2, g3, h2, i2, j0, k4, l2, m4]
		//record291: [a291, b1, c0, d3, e1, f3, g4, h3, i3, j1, k5, l3, m5]
		//record292: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6]
		//record293: [a293, b1, c2, d1, e3, f5, g6, h5, i5, j3, k7, l5, m7]
		//record294: [a294, b0, c0, d2, e4, f0, g0, h6, i6, j4, k8, l6, m8]
		int pageCount0 = (int)Math.ceil(295.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_va372 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "va372"+ File.separator+i+".db");
				dir_va372.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("va372");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t4zl7", cols0);
		String [][] records_t4zl7 = new String[467][cols0.length];
		for(int i=0;i<467;i++)
		{
			records_t4zl7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t4zl7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t4zl7", records_t4zl7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record462: [a462, b0, c0, d2, e2, f0, g0, h6, i3, j2, k0, l6, m7, n0, o12]
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7, m8, n1, o13]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8, m9, n2, o14]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9, m10, n3, o0]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11, n4, o1]
		int pageCount0 = (int)Math.ceil(467.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t4zl7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t4zl7"+ File.separator+i+".db");
				dir_t4zl7.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("t4zl7");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v1r2", cols1);
		String [][] records_v1r2 = new String[478][cols1.length];
		for(int i=0;i<478;i++)
		{
			records_v1r2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v1r2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v1r2", records_v1r2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11, o8]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6, n12, o9]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13, o10]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0, o11]
		//record477: [a477, b1, c0, d1, e2, f3, g1, h5, i0, j7, k4, l9, m9, n1, o12]
		int pageCount1 = (int)Math.ceil(478.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v1r2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v1r2"+ File.separator+i+".db");
				dir_v1r2.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("v1r2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ik4aa", cols2);
		String [][] records_ik4aa = new String[154][cols2.length];
		for(int i=0;i<154;i++)
		{
			records_ik4aa[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ik4aa[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ik4aa", records_ik4aa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8]
		//record153: [a153, b1, c0, d1, e3, f3, g6, h1, i0]
		int pageCount2 = (int)Math.ceil(154.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ik4aa = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ik4aa"+ File.separator+i+".db");
				dir_ik4aa.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("ik4aa");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("jxd", cols0);
		String [][] records_jxd = new String[153][cols0.length];
		for(int i=0;i<153;i++)
		{
			records_jxd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jxd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jxd", records_jxd[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record148: [a148, b0, c1, d0, e3, f4, g1, h4, i4, j8, k5, l4, m5, n8, o13, p4, q12, r4, s15, t8]
		//record149: [a149, b1, c2, d1, e4, f5, g2, h5, i5, j9, k6, l5, m6, n9, o14, p5, q13, r5, s16, t9]
		//record150: [a150, b0, c0, d2, e0, f0, g3, h6, i6, j0, k7, l6, m7, n10, o0, p6, q14, r6, s17, t10]
		//record151: [a151, b1, c1, d3, e1, f1, g4, h7, i7, j1, k8, l7, m8, n11, o1, p7, q15, r7, s18, t11]
		//record152: [a152, b0, c2, d0, e2, f2, g5, h0, i8, j2, k9, l8, m9, n12, o2, p8, q16, r8, s0, t12]
		int pageCount0 = (int)Math.ceil(153.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_jxd = new File(FileManager.directory.getAbsolutePath()+ File.separator + "jxd"+ File.separator+i+".db");
				dir_jxd.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("jxd");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xot6f", cols1);
		String [][] records_xot6f = new String[216][cols1.length];
		for(int i=0;i<216;i++)
		{
			records_xot6f[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xot6f[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xot6f", records_xot6f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3]
		//record212: [a212, b0, c2, d0, e2, f2, g2, h4]
		//record213: [a213, b1, c0, d1, e3, f3, g3, h5]
		//record214: [a214, b0, c1, d2, e4, f4, g4, h6]
		//record215: [a215, b1, c2, d3, e0, f5, g5, h7]
		int pageCount1 = (int)Math.ceil(216.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xot6f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xot6f"+ File.separator+i+".db");
				dir_xot6f.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("xot6f");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zg", cols2);
		String [][] records_zg = new String[450][cols2.length];
		for(int i=0;i<450;i++)
		{
			records_zg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zg", records_zg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9]
		int pageCount2 = (int)Math.ceil(450.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zg = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zg"+ File.separator+i+".db");
				dir_zg.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("zg");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n5k", cols3);
		String [][] records_n5k = new String[113][cols3.length];
		for(int i=0;i<113;i++)
		{
			records_n5k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n5k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n5k", records_n5k[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0]
		int pageCount3 = (int)Math.ceil(113.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n5k = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n5k"+ File.separator+i+".db");
				dir_n5k.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("n5k");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("r81", cols0);
		String [][] records_r81 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			records_r81[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r81[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r81", records_r81[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5, p1]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6, p2]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5]
		int pageCount0 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r81 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r81"+ File.separator+i+".db");
				dir_r81.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("r81");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v6", cols1);
		String [][] records_v6 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			records_v6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v6", records_v6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3]
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7]
		int pageCount1 = (int)Math.ceil(68.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_v6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "v6"+ File.separator+i+".db");
				dir_v6.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("v6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("df79", cols0);
		String [][] records_df79 = new String[136][cols0.length];
		for(int i=0;i<136;i++)
		{
			records_df79[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_df79[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("df79", records_df79[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1, n5, o11, p3, q12, r5]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2, n6, o12, p4, q13, r6]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3, n7, o13, p5, q14, r7]
		//record134: [a134, b0, c2, d2, e4, f2, g1, h6, i8, j4, k2, l2, m4, n8, o14, p6, q15, r8]
		//record135: [a135, b1, c0, d3, e0, f3, g2, h7, i0, j5, k3, l3, m5, n9, o0, p7, q16, r9]
		int pageCount0 = (int)Math.ceil(136.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_df79 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "df79"+ File.separator+i+".db");
				dir_df79.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("df79");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("wl", cols1);
		String [][] records_wl = new String[164][cols1.length];
		for(int i=0;i<164;i++)
		{
			records_wl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wl", records_wl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record159: [a159, b1, c0, d3, e4, f3, g5, h7, i6, j9, k5, l3, m3, n5, o9]
		//record160: [a160, b0, c1, d0, e0, f4, g6, h0, i7, j0, k6, l4, m4, n6, o10]
		//record161: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7, o11]
		//record162: [a162, b0, c0, d2, e2, f0, g1, h2, i0, j2, k8, l6, m6, n8, o12]
		//record163: [a163, b1, c1, d3, e3, f1, g2, h3, i1, j3, k9, l7, m7, n9, o13]
		int pageCount1 = (int)Math.ceil(164.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wl"+ File.separator+i+".db");
				dir_wl.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("wl");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("x86", cols2);
		String [][] records_x86 = new String[199][cols2.length];
		for(int i=0;i<199;i++)
		{
			records_x86[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x86[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x86", records_x86[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12, o14, p2, q7]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13, o0, p3, q8]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0, o1, p4, q9]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5, i8, j7, k10, l5, m2, n1, o2, p5, q10]
		//record198: [a198, b0, c0, d2, e3, f0, g2, h6, i0, j8, k0, l6, m3, n2, o3, p6, q11]
		int pageCount2 = (int)Math.ceil(199.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x86 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x86"+ File.separator+i+".db");
				dir_x86.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("x86");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("twhg", cols0);
		String [][] records_twhg = new String[191][cols0.length];
		for(int i=0;i<191;i++)
		{
			records_twhg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_twhg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("twhg", records_twhg[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record186: [a186, b0, c0, d2, e1, f0, g4, h2, i6, j6, k10, l6, m4, n4]
		//record187: [a187, b1, c1, d3, e2, f1, g5, h3, i7, j7, k0, l7, m5, n5]
		//record188: [a188, b0, c2, d0, e3, f2, g6, h4, i8, j8, k1, l8, m6, n6]
		//record189: [a189, b1, c0, d1, e4, f3, g0, h5, i0, j9, k2, l9, m7, n7]
		//record190: [a190, b0, c1, d2, e0, f4, g1, h6, i1, j0, k3, l10, m8, n8]
		int pageCount0 = (int)Math.ceil(191.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_twhg = new File(FileManager.directory.getAbsolutePath()+ File.separator + "twhg"+ File.separator+i+".db");
				dir_twhg.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("twhg");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gimt", cols1);
		String [][] records_gimt = new String[358][cols1.length];
		for(int i=0;i<358;i++)
		{
			records_gimt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gimt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gimt", records_gimt[i]);
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
		int pageCount1 = (int)Math.ceil(358.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gimt = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gimt"+ File.separator+i+".db");
				dir_gimt.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("gimt");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("n13", cols2);
		String [][] records_n13 = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			records_n13[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n13[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n13", records_n13[i]);
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
		int pageCount2 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n13 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n13"+ File.separator+i+".db");
				dir_n13.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("n13");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("bj", cols3);
		String [][] records_bj = new String[350][cols3.length];
		for(int i=0;i<350;i++)
		{
			records_bj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_bj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("bj", records_bj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record345: [a345, b1, c0, d1, e0, f3, g2, h1, i3, j5, k4, l9, m7, n9, o0, p9, q5]
		//record346: [a346, b0, c1, d2, e1, f4, g3, h2, i4, j6, k5, l10, m8, n10, o1, p10, q6]
		//record347: [a347, b1, c2, d3, e2, f5, g4, h3, i5, j7, k6, l11, m9, n11, o2, p11, q7]
		//record348: [a348, b0, c0, d0, e3, f0, g5, h4, i6, j8, k7, l0, m10, n12, o3, p12, q8]
		//record349: [a349, b1, c1, d1, e4, f1, g6, h5, i7, j9, k8, l1, m11, n13, o4, p13, q9]
		int pageCount3 = (int)Math.ceil(350.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bj"+ File.separator+i+".db");
				dir_bj.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("bj");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z0n", cols4);
		String [][] records_z0n = new String[198][cols4.length];
		for(int i=0;i<198;i++)
		{
			records_z0n[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_z0n[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("z0n", records_z0n[i]);
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
		int pageCount4 = (int)Math.ceil(198.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z0n = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z0n"+ File.separator+i+".db");
				dir_z0n.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("z0n");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("h832", cols0);
		String [][] records_h832 = new String[319][cols0.length];
		for(int i=0;i<319;i++)
		{
			records_h832[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h832[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h832", records_h832[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4]
		//record317: [a317, b1, c2, d1, e2, f5, g2, h5, i2, j7, k9, l5, m5]
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8, k10, l6, m6]
		int pageCount0 = (int)Math.ceil(319.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h832 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h832"+ File.separator+i+".db");
				dir_h832.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("h832");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("yrh", cols0);
		String [][] records_yrh = new String[235][cols0.length];
		for(int i=0;i<235;i++)
		{
			records_yrh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yrh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yrh", records_yrh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6]
		int pageCount0 = (int)Math.ceil(235.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yrh = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yrh"+ File.separator+i+".db");
				dir_yrh.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("yrh");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("hv8", cols1);
		String [][] records_hv8 = new String[234][cols1.length];
		for(int i=0;i<234;i++)
		{
			records_hv8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hv8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hv8", records_hv8[i]);
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
		int pageCount1 = (int)Math.ceil(234.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_hv8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "hv8"+ File.separator+i+".db");
				dir_hv8.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("hv8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ygb2", cols2);
		String [][] records_ygb2 = new String[254][cols2.length];
		for(int i=0;i<254;i++)
		{
			records_ygb2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ygb2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ygb2", records_ygb2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record249: [a249, b1, c0, d1, e4, f3, g4]
		//record250: [a250, b0, c1, d2, e0, f4, g5]
		//record251: [a251, b1, c2, d3, e1, f5, g6]
		//record252: [a252, b0, c0, d0, e2, f0, g0]
		//record253: [a253, b1, c1, d1, e3, f1, g1]
		int pageCount2 = (int)Math.ceil(254.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ygb2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ygb2"+ File.separator+i+".db");
				dir_ygb2.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("ygb2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ra7", cols3);
		String [][] records_ra7 = new String[231][cols3.length];
		for(int i=0;i<231;i++)
		{
			records_ra7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ra7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ra7", records_ra7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record226: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6]
		//record227: [a227, b1, c2, d3, e2, f5, g3, h3, i2, j7]
		//record228: [a228, b0, c0, d0, e3, f0, g4, h4, i3, j8]
		//record229: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9]
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0]
		int pageCount3 = (int)Math.ceil(231.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ra7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ra7"+ File.separator+i+".db");
				dir_ra7.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("ra7");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("u2ic", cols4);
		String [][] records_u2ic = new String[440][cols4.length];
		for(int i=0;i<440;i++)
		{
			records_u2ic[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u2ic[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u2ic", records_u2ic[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1, o0, p3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2, o1, p4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3, o2, p5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4, o3, p6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5, o4, p7]
		int pageCount4 = (int)Math.ceil(440.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u2ic = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u2ic"+ File.separator+i+".db");
				dir_u2ic.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("u2ic");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("epa", cols0);
		String [][] records_epa = new String[343][cols0.length];
		for(int i=0;i<343;i++)
		{
			records_epa[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_epa[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("epa", records_epa[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0, n2, o8, p2, q15]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1, n3, o9, p3, q16]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2, n4, o10, p4, q0]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3, n5, o11, p5, q1]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4, n6, o12, p6, q2]
		int pageCount0 = (int)Math.ceil(343.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_epa = new File(FileManager.directory.getAbsolutePath()+ File.separator + "epa"+ File.separator+i+".db");
				dir_epa.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("epa");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a8ue0", cols1);
		String [][] records_a8ue0 = new String[169][cols1.length];
		for(int i=0;i<169;i++)
		{
			records_a8ue0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a8ue0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a8ue0", records_a8ue0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record164: [a164, b0, c2, d0, e4, f2, g3]
		//record165: [a165, b1, c0, d1, e0, f3, g4]
		//record166: [a166, b0, c1, d2, e1, f4, g5]
		//record167: [a167, b1, c2, d3, e2, f5, g6]
		//record168: [a168, b0, c0, d0, e3, f0, g0]
		int pageCount1 = (int)Math.ceil(169.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a8ue0 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a8ue0"+ File.separator+i+".db");
				dir_a8ue0.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("a8ue0");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i86", cols0);
		String [][] records_i86 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_i86[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i86[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i86", records_i86[i]);
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
		int pageCount0 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i86 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i86"+ File.separator+i+".db");
				dir_i86.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("i86");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("t4z", cols1);
		String [][] records_t4z = new String[329][cols1.length];
		for(int i=0;i<329;i++)
		{
			records_t4z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t4z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t4z", records_t4z[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9, p4]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3, o10, p5]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4, o11, p6]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12, p7]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6, o13, p8]
		int pageCount1 = (int)Math.ceil(329.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t4z = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t4z"+ File.separator+i+".db");
				dir_t4z.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("t4z");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b3", cols2);
		String [][] records_b3 = new String[453][cols2.length];
		for(int i=0;i<453;i++)
		{
			records_b3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b3", records_b3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record448: [a448, b0, c1, d0, e3, f4, g0]
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		//record450: [a450, b0, c0, d2, e0, f0, g2]
		//record451: [a451, b1, c1, d3, e1, f1, g3]
		//record452: [a452, b0, c2, d0, e2, f2, g4]
		int pageCount2 = (int)Math.ceil(453.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b3"+ File.separator+i+".db");
				dir_b3.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("b3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ydy5", cols3);
		String [][] records_ydy5 = new String[292][cols3.length];
		for(int i=0;i<292;i++)
		{
			records_ydy5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ydy5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ydy5", records_ydy5[i]);
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
		int pageCount3 = (int)Math.ceil(292.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ydy5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ydy5"+ File.separator+i+".db");
				dir_ydy5.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("ydy5");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("lm2", cols0);
		String [][] records_lm2 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_lm2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lm2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lm2", records_lm2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16, s15, t14]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16, t15]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17, t16]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18, t17]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0, t18]
		int pageCount0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_lm2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "lm2"+ File.separator+i+".db");
				dir_lm2.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("lm2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("c71", cols1);
		String [][] records_c71 = new String[141][cols1.length];
		for(int i=0;i<141;i++)
		{
			records_c71[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c71[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c71", records_c71[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record136: [a136, b0, c1, d0, e1, f4, g3, h0, i1, j6, k4, l4, m6, n10, o1, p8, q0]
		//record137: [a137, b1, c2, d1, e2, f5, g4, h1, i2, j7, k5, l5, m7, n11, o2, p9, q1]
		//record138: [a138, b0, c0, d2, e3, f0, g5, h2, i3, j8, k6, l6, m8, n12, o3, p10, q2]
		//record139: [a139, b1, c1, d3, e4, f1, g6, h3, i4, j9, k7, l7, m9, n13, o4, p11, q3]
		//record140: [a140, b0, c2, d0, e0, f2, g0, h4, i5, j0, k8, l8, m10, n0, o5, p12, q4]
		int pageCount1 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_c71 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "c71"+ File.separator+i+".db");
				dir_c71.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("c71");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("fhw", cols2);
		String [][] records_fhw = new String[141][cols2.length];
		for(int i=0;i<141;i++)
		{
			records_fhw[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fhw[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fhw", records_fhw[i]);
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
		int pageCount2 = (int)Math.ceil(141.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fhw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fhw"+ File.separator+i+".db");
				dir_fhw.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("fhw");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l44w", cols3);
		String [][] records_l44w = new String[313][cols3.length];
		for(int i=0;i<313;i++)
		{
			records_l44w[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l44w[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l44w", records_l44w[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8, m9, n0]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10, n1]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11, n2]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12, n3]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0, n4]
		int pageCount3 = (int)Math.ceil(313.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_l44w = new File(FileManager.directory.getAbsolutePath()+ File.separator + "l44w"+ File.separator+i+".db");
				dir_l44w.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("l44w");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("xyu", cols0);
		String [][] records_xyu = new String[419][cols0.length];
		for(int i=0;i<419;i++)
		{
			records_xyu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xyu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xyu", records_xyu[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record414: [a414, b0, c0, d2, e4, f0, g1, h6, i0, j4]
		//record415: [a415, b1, c1, d3, e0, f1, g2, h7, i1, j5]
		//record416: [a416, b0, c2, d0, e1, f2, g3, h0, i2, j6]
		//record417: [a417, b1, c0, d1, e2, f3, g4, h1, i3, j7]
		//record418: [a418, b0, c1, d2, e3, f4, g5, h2, i4, j8]
		int pageCount0 = (int)Math.ceil(419.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_xyu = new File(FileManager.directory.getAbsolutePath()+ File.separator + "xyu"+ File.separator+i+".db");
				dir_xyu.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("xyu");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("nh156", cols1);
		String [][] records_nh156 = new String[222][cols1.length];
		for(int i=0;i<222;i++)
		{
			records_nh156[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nh156[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nh156", records_nh156[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0]
		//record221: [a221, b1, c2, d1, e1, f5, g4, h5, i5, j1]
		int pageCount1 = (int)Math.ceil(222.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_nh156 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "nh156"+ File.separator+i+".db");
				dir_nh156.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("nh156");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("cz", cols0);
		String [][] records_cz = new String[332][cols0.length];
		for(int i=0;i<332;i++)
		{
			records_cz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cz", records_cz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12, p7, q4]
		//record328: [a328, b0, c1, d0, e3, f4, g6, h0, i4, j8, k9, l4, m3, n6, o13, p8, q5]
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4, n7, o14, p9, q6]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8, o0, p10, q7]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9, o1, p11, q8]
		int pageCount0 = (int)Math.ceil(332.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cz = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cz"+ File.separator+i+".db");
				dir_cz.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("cz");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u7g", cols0);
		String [][] records_u7g = new String[298][cols0.length];
		for(int i=0;i<298;i++)
		{
			records_u7g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u7g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u7g", records_u7g[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record293: [a293, b1, c2, d1, e3, f5, g6]
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		//record296: [a296, b0, c2, d0, e1, f2, g2]
		//record297: [a297, b1, c0, d1, e2, f3, g3]
		int pageCount0 = (int)Math.ceil(298.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u7g = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u7g"+ File.separator+i+".db");
				dir_u7g.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("u7g");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("kx", cols1);
		String [][] records_kx = new String[211][cols1.length];
		for(int i=0;i<211;i++)
		{
			records_kx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kx", records_kx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2, m11, n10, o11, p14, q2, r8, s16, t6]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3, m12, n11, o12, p15, q3, r9, s17, t7]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4, m0, n12, o13, p0, q4, r10, s18, t8]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5, m1, n13, o14, p1, q5, r11, s0, t9]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6, m2, n0, o0, p2, q6, r12, s1, t10]
		int pageCount1 = (int)Math.ceil(211.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_kx = new File(FileManager.directory.getAbsolutePath()+ File.separator + "kx"+ File.separator+i+".db");
				dir_kx.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("kx");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("yw3sh", cols2);
		String [][] records_yw3sh = new String[134][cols2.length];
		for(int i=0;i<134;i++)
		{
			records_yw3sh[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_yw3sh[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("yw3sh", records_yw3sh[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9, m12, n3, o9, p1, q10, r3, s15]
		//record130: [a130, b0, c1, d2, e0, f4, g4, h2, i4, j0, k9, l10, m0, n4, o10, p2, q11, r4, s16]
		//record131: [a131, b1, c2, d3, e1, f5, g5, h3, i5, j1, k10, l11, m1, n5, o11, p3, q12, r5, s17]
		//record132: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0, m2, n6, o12, p4, q13, r6, s18]
		//record133: [a133, b1, c1, d1, e3, f1, g0, h5, i7, j3, k1, l1, m3, n7, o13, p5, q14, r7, s0]
		int pageCount2 = (int)Math.ceil(134.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_yw3sh = new File(FileManager.directory.getAbsolutePath()+ File.separator + "yw3sh"+ File.separator+i+".db");
				dir_yw3sh.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("yw3sh");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("is3s", cols0);
		String [][] records_is3s = new String[140][cols0.length];
		for(int i=0;i<140;i++)
		{
			records_is3s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_is3s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("is3s", records_is3s[i]);
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
		int pageCount0 = (int)Math.ceil(140.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_is3s = new File(FileManager.directory.getAbsolutePath()+ File.separator + "is3s"+ File.separator+i+".db");
				dir_is3s.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("is3s");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q7cj", cols1);
		String [][] records_q7cj = new String[458][cols1.length];
		for(int i=0;i<458;i++)
		{
			records_q7cj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q7cj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q7cj", records_q7cj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1]
		int pageCount1 = (int)Math.ceil(458.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q7cj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q7cj"+ File.separator+i+".db");
				dir_q7cj.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("q7cj");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("n9", cols2);
		String [][] records_n9 = new String[458][cols2.length];
		for(int i=0;i<458;i++)
		{
			records_n9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n9", records_n9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0]
		//record457: [a457, b1, c1, d1, e2, f1, g2, h1, i7, j7, k6, l1]
		int pageCount2 = (int)Math.ceil(458.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_n9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "n9"+ File.separator+i+".db");
				dir_n9.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("n9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("x6", cols3);
		String [][] records_x6 = new String[313][cols3.length];
		for(int i=0;i<313;i++)
		{
			records_x6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x6", records_x6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record308: [a308, b0, c2, d0, e3, f2, g0, h4, i2, j8, k0, l8, m9, n0, o8, p4, q2]
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10, n1, o9, p5, q3]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11, n2, o10, p6, q4]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12, n3, o11, p7, q5]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0, n4, o12, p8, q6]
		int pageCount3 = (int)Math.ceil(313.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x6"+ File.separator+i+".db");
				dir_x6.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("x6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y085", cols0);
		String [][] records_y085 = new String[205][cols0.length];
		for(int i=0;i<205;i++)
		{
			records_y085[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y085[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y085", records_y085[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record200: [a200, b0, c2, d0, e0, f2, g4, h0]
		//record201: [a201, b1, c0, d1, e1, f3, g5, h1]
		//record202: [a202, b0, c1, d2, e2, f4, g6, h2]
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4]
		int pageCount0 = (int)Math.ceil(205.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y085 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y085"+ File.separator+i+".db");
				dir_y085.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("y085");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w5", cols1);
		String [][] records_w5 = new String[256][cols1.length];
		for(int i=0;i<256;i++)
		{
			records_w5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w5", records_w5[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9, l11, m4, n13, o11]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10, l0, m5, n0, o12]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0, l1, m6, n1, o13]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1, l2, m7, n2, o14]
		//record255: [a255, b1, c0, d3, e0, f3, g3, h7, i3, j5, k2, l3, m8, n3, o0]
		int pageCount1 = (int)Math.ceil(256.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w5"+ File.separator+i+".db");
				dir_w5.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("w5");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h1", cols2);
		String [][] records_h1 = new String[115][cols2.length];
		for(int i=0;i<115;i++)
		{
			records_h1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h1", records_h1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9, n1]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10, n2]
		int pageCount2 = (int)Math.ceil(115.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h1"+ File.separator+i+".db");
				dir_h1.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("h1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ek", cols3);
		String [][] records_ek = new String[212][cols3.length];
		for(int i=0;i<212;i++)
		{
			records_ek[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ek[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ek", records_ek[i]);
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
		int pageCount3 = (int)Math.ceil(212.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ek = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ek"+ File.separator+i+".db");
				dir_ek.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("ek");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("r8", cols4);
		String [][] records_r8 = new String[110][cols4.length];
		for(int i=0;i<110;i++)
		{
			records_r8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r8", records_r8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10]
		int pageCount4 = (int)Math.ceil(110.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r8"+ File.separator+i+".db");
				dir_r8.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("r8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qav", cols0);
		String [][] records_qav = new String[305][cols0.length];
		for(int i=0;i<305;i++)
		{
			records_qav[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qav[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qav", records_qav[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record300: [a300, b0, c0, d0, e0, f0, g6, h4, i3, j0, k3, l0]
		//record301: [a301, b1, c1, d1, e1, f1, g0, h5, i4, j1, k4, l1]
		//record302: [a302, b0, c2, d2, e2, f2, g1, h6, i5, j2, k5, l2]
		//record303: [a303, b1, c0, d3, e3, f3, g2, h7, i6, j3, k6, l3]
		//record304: [a304, b0, c1, d0, e4, f4, g3, h0, i7, j4, k7, l4]
		int pageCount0 = (int)Math.ceil(305.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_qav = new File(FileManager.directory.getAbsolutePath()+ File.separator + "qav"+ File.separator+i+".db");
				dir_qav.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("qav");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("on", cols1);
		String [][] records_on = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			records_on[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_on[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("on", records_on[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record12: [a12, b0, c0, d0, e2, f0, g5]
		//record13: [a13, b1, c1, d1, e3, f1, g6]
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		int pageCount1 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_on = new File(FileManager.directory.getAbsolutePath()+ File.separator + "on"+ File.separator+i+".db");
				dir_on.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("on");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("iu3", cols2);
		String [][] records_iu3 = new String[333][cols2.length];
		for(int i=0;i<333;i++)
		{
			records_iu3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_iu3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("iu3", records_iu3[i]);
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
		int pageCount2 = (int)Math.ceil(333.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_iu3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "iu3"+ File.separator+i+".db");
				dir_iu3.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("iu3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("om3y9", cols3);
		String [][] records_om3y9 = new String[210][cols3.length];
		for(int i=0;i<210;i++)
		{
			records_om3y9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_om3y9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("om3y9", records_om3y9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2]
		int pageCount3 = (int)Math.ceil(210.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_om3y9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "om3y9"+ File.separator+i+".db");
				dir_om3y9.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("om3y9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("h4crw", cols0);
		String [][] records_h4crw = new String[435][cols0.length];
		for(int i=0;i<435;i++)
		{
			records_h4crw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h4crw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h4crw", records_h4crw[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record430: [a430, b0, c1, d2, e0, f4, g3, h6, i7, j0, k1, l10, m1, n10, o10, p14, q5, r16]
		//record431: [a431, b1, c2, d3, e1, f5, g4, h7, i8, j1, k2, l11, m2, n11, o11, p15, q6, r17]
		//record432: [a432, b0, c0, d0, e2, f0, g5, h0, i0, j2, k3, l0, m3, n12, o12, p0, q7, r0]
		//record433: [a433, b1, c1, d1, e3, f1, g6, h1, i1, j3, k4, l1, m4, n13, o13, p1, q8, r1]
		//record434: [a434, b0, c2, d2, e4, f2, g0, h2, i2, j4, k5, l2, m5, n0, o14, p2, q9, r2]
		int pageCount0 = (int)Math.ceil(435.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h4crw = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h4crw"+ File.separator+i+".db");
				dir_h4crw.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("h4crw");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("phv6", cols0);
		String [][] records_phv6 = new String[115][cols0.length];
		for(int i=0;i<115;i++)
		{
			records_phv6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_phv6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("phv6", records_phv6[i]);
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
		int pageCount0 = (int)Math.ceil(115.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_phv6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "phv6"+ File.separator+i+".db");
				dir_phv6.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("phv6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t83t", cols0);
		String [][] records_t83t = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_t83t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t83t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t83t", records_t83t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		int pageCount0 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_t83t = new File(FileManager.directory.getAbsolutePath()+ File.separator + "t83t"+ File.separator+i+".db");
				dir_t83t.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("t83t");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("caht9", cols1);
		String [][] records_caht9 = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			records_caht9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_caht9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("caht9", records_caht9[i]);
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
		int pageCount1 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_caht9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "caht9"+ File.separator+i+".db");
				dir_caht9.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("caht9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nh583", cols2);
		String [][] records_nh583 = new String[63][cols2.length];
		for(int i=0;i<63;i++)
		{
			records_nh583[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nh583[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nh583", records_nh583[i]);
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
		int pageCount2 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_nh583 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "nh583"+ File.separator+i+".db");
				dir_nh583.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("nh583");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("db", cols3);
		String [][] records_db = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			records_db[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_db[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("db", records_db[i]);
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
		int pageCount3 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_db = new File(FileManager.directory.getAbsolutePath()+ File.separator + "db"+ File.separator+i+".db");
				dir_db.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("db");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u1j", cols4);
		String [][] records_u1j = new String[97][cols4.length];
		for(int i=0;i<97;i++)
		{
			records_u1j[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u1j[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u1j", records_u1j[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record92: [a92, b0, c2, d0, e2, f2, g1, h4, i2, j2, k4, l8, m1, n8, o2]
		//record93: [a93, b1, c0, d1, e3, f3, g2, h5, i3, j3, k5, l9, m2, n9, o3]
		//record94: [a94, b0, c1, d2, e4, f4, g3, h6, i4, j4, k6, l10, m3, n10, o4]
		//record95: [a95, b1, c2, d3, e0, f5, g4, h7, i5, j5, k7, l11, m4, n11, o5]
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0, i6, j6, k8, l0, m5, n12, o6]
		int pageCount4 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_u1j = new File(FileManager.directory.getAbsolutePath()+ File.separator + "u1j"+ File.separator+i+".db");
				dir_u1j.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("u1j");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("k48", cols0);
		String [][] records_k48 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			records_k48[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k48[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k48", records_k48[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record86: [a86, b0, c2, d2, e1, f2, g2, h6]
		//record87: [a87, b1, c0, d3, e2, f3, g3, h7]
		//record88: [a88, b0, c1, d0, e3, f4, g4, h0]
		//record89: [a89, b1, c2, d1, e4, f5, g5, h1]
		//record90: [a90, b0, c0, d2, e0, f0, g6, h2]
		int pageCount0 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_k48 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "k48"+ File.separator+i+".db");
				dir_k48.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("k48");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("q6r4", cols1);
		String [][] records_q6r4 = new String[148][cols1.length];
		for(int i=0;i<148;i++)
		{
			records_q6r4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_q6r4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("q6r4", records_q6r4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3, o8, p15, q7, r17, s10]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4, o9, p0, q8, r0, s11]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10, p1, q9, r1, s12]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10, r2, s13]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12, p3, q11, r3, s14]
		int pageCount1 = (int)Math.ceil(148.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q6r4 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q6r4"+ File.separator+i+".db");
				dir_q6r4.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("q6r4");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q42", cols2);
		String [][] records_q42 = new String[299][cols2.length];
		for(int i=0;i<299;i++)
		{
			records_q42[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q42[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q42", records_q42[i]);
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
		int pageCount2 = (int)Math.ceil(299.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q42 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q42"+ File.separator+i+".db");
				dir_q42.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("q42");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gc2", cols3);
		String [][] records_gc2 = new String[81][cols3.length];
		for(int i=0;i<81;i++)
		{
			records_gc2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gc2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gc2", records_gc2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record76: [a76, b0, c1, d0, e1, f4, g6]
		//record77: [a77, b1, c2, d1, e2, f5, g0]
		//record78: [a78, b0, c0, d2, e3, f0, g1]
		//record79: [a79, b1, c1, d3, e4, f1, g2]
		//record80: [a80, b0, c2, d0, e0, f2, g3]
		int pageCount3 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gc2 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gc2"+ File.separator+i+".db");
				dir_gc2.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("gc2");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fu7x5", cols0);
		String [][] records_fu7x5 = new String[207][cols0.length];
		for(int i=0;i<207;i++)
		{
			records_fu7x5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fu7x5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fu7x5", records_fu7x5[i]);
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
		int pageCount0 = (int)Math.ceil(207.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_fu7x5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "fu7x5"+ File.separator+i+".db");
				dir_fu7x5.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("fu7x5");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z12", cols0);
		String [][] records_z12 = new String[385][cols0.length];
		for(int i=0;i<385;i++)
		{
			records_z12[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z12[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z12", records_z12[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record380: [a380, b0, c2, d0, e0, f2, g2]
		//record381: [a381, b1, c0, d1, e1, f3, g3]
		//record382: [a382, b0, c1, d2, e2, f4, g4]
		//record383: [a383, b1, c2, d3, e3, f5, g5]
		//record384: [a384, b0, c0, d0, e4, f0, g6]
		int pageCount0 = (int)Math.ceil(385.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z12 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z12"+ File.separator+i+".db");
				dir_z12.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("z12");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a9h49", cols1);
		String [][] records_a9h49 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			records_a9h49[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a9h49[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a9h49", records_a9h49[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		int pageCount1 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a9h49 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a9h49"+ File.separator+i+".db");
				dir_a9h49.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("a9h49");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rd8", cols2);
		String [][] records_rd8 = new String[155][cols2.length];
		for(int i=0;i<155;i++)
		{
			records_rd8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rd8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rd8", records_rd8[i]);
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
		int pageCount2 = (int)Math.ceil(155.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rd8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rd8"+ File.separator+i+".db");
				dir_rd8.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("rd8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("h7", cols3);
		String [][] records_h7 = new String[221][cols3.length];
		for(int i=0;i<221;i++)
		{
			records_h7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h7", records_h7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record216: [a216, b0, c0, d0, e1, f0, g6, h0, i0, j6, k7, l0, m8, n6, o6]
		//record217: [a217, b1, c1, d1, e2, f1, g0, h1, i1, j7, k8, l1, m9, n7, o7]
		//record218: [a218, b0, c2, d2, e3, f2, g1, h2, i2, j8, k9, l2, m10, n8, o8]
		//record219: [a219, b1, c0, d3, e4, f3, g2, h3, i3, j9, k10, l3, m11, n9, o9]
		//record220: [a220, b0, c1, d0, e0, f4, g3, h4, i4, j0, k0, l4, m12, n10, o10]
		int pageCount3 = (int)Math.ceil(221.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h7"+ File.separator+i+".db");
				dir_h7.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("h7");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("r42", cols0);
		String [][] records_r42 = new String[440][cols0.length];
		for(int i=0;i<440;i++)
		{
			records_r42[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r42[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r42", records_r42[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7]
		int pageCount0 = (int)Math.ceil(440.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r42 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r42"+ File.separator+i+".db");
				dir_r42.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("r42");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x7n", cols1);
		String [][] records_x7n = new String[481][cols1.length];
		for(int i=0;i<481;i++)
		{
			records_x7n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x7n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x7n", records_x7n[i]);
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
		int pageCount1 = (int)Math.ceil(481.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x7n = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x7n"+ File.separator+i+".db");
				dir_x7n.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("x7n");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mp5", cols2);
		String [][] records_mp5 = new String[299][cols2.length];
		for(int i=0;i<299;i++)
		{
			records_mp5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mp5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mp5", records_mp5[i]);
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
		int pageCount2 = (int)Math.ceil(299.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_mp5 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "mp5"+ File.separator+i+".db");
				dir_mp5.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("mp5");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a6", cols0);
		String [][] records_a6 = new String[461][cols0.length];
		for(int i=0;i<461;i++)
		{
			records_a6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a6", records_a6[i]);
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
		int pageCount0 = (int)Math.ceil(461.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a6"+ File.separator+i+".db");
				dir_a6.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("a6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zq49", cols1);
		String [][] records_zq49 = new String[230][cols1.length];
		for(int i=0;i<230;i++)
		{
			records_zq49[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zq49[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zq49", records_zq49[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record225: [a225, b1, c0, d1, e0, f3, g1]
		//record226: [a226, b0, c1, d2, e1, f4, g2]
		//record227: [a227, b1, c2, d3, e2, f5, g3]
		//record228: [a228, b0, c0, d0, e3, f0, g4]
		//record229: [a229, b1, c1, d1, e4, f1, g5]
		int pageCount1 = (int)Math.ceil(230.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zq49 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zq49"+ File.separator+i+".db");
				dir_zq49.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("zq49");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("bxx", cols0);
		String [][] records_bxx = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_bxx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bxx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bxx", records_bxx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		int pageCount0 = (int)Math.ceil(46.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bxx = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bxx"+ File.separator+i+".db");
				dir_bxx.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("bxx");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("cj3", cols1);
		String [][] records_cj3 = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			records_cj3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cj3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cj3", records_cj3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record64: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0, q13, r10, s7]
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5, p1, q14, r11, s8]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6, p2, q15, r12, s9]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3, q16, r13, s10]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0, r14, s11]
		int pageCount1 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_cj3 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "cj3"+ File.separator+i+".db");
				dir_cj3.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("cj3");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("b53", cols2);
		String [][] records_b53 = new String[452][cols2.length];
		for(int i=0;i<452;i++)
		{
			records_b53[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b53[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b53", records_b53[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13, o12, p15]
		//record448: [a448, b0, c1, d0, e3, f4, g0, h0, i7, j8, k8, l4, m6, n0, o13, p0]
		//record449: [a449, b1, c2, d1, e4, f5, g1, h1, i8, j9, k9, l5, m7, n1, o14, p1]
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2, o0, p2]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3, o1, p3]
		int pageCount2 = (int)Math.ceil(452.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b53 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b53"+ File.separator+i+".db");
				dir_b53.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("b53");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d6789", cols0);
		String [][] records_d6789 = new String[97][cols0.length];
		for(int i=0;i<97;i++)
		{
			records_d6789[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d6789[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d6789", records_d6789[i]);
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
		int pageCount0 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d6789 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d6789"+ File.separator+i+".db");
				dir_d6789.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("d6789");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bf10d", cols1);
		String [][] records_bf10d = new String[202][cols1.length];
		for(int i=0;i<202;i++)
		{
			records_bf10d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bf10d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bf10d", records_bf10d[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record197: [a197, b1, c2, d1, e2, f5, g1]
		//record198: [a198, b0, c0, d2, e3, f0, g2]
		//record199: [a199, b1, c1, d3, e4, f1, g3]
		//record200: [a200, b0, c2, d0, e0, f2, g4]
		//record201: [a201, b1, c0, d1, e1, f3, g5]
		int pageCount1 = (int)Math.ceil(202.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_bf10d = new File(FileManager.directory.getAbsolutePath()+ File.separator + "bf10d"+ File.separator+i+".db");
				dir_bf10d.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("bf10d");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i1", cols2);
		String [][] records_i1 = new String[398][cols2.length];
		for(int i=0;i<398;i++)
		{
			records_i1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i1", records_i1[i]);
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
		int pageCount2 = (int)Math.ceil(398.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i1"+ File.separator+i+".db");
				dir_i1.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("i1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ux6s1", cols3);
		String [][] records_ux6s1 = new String[219][cols3.length];
		for(int i=0;i<219;i++)
		{
			records_ux6s1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ux6s1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ux6s1", records_ux6s1[i]);
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
		int pageCount3 = (int)Math.ceil(219.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_ux6s1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "ux6s1"+ File.separator+i+".db");
				dir_ux6s1.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("ux6s1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("j82", cols4);
		String [][] records_j82 = new String[455][cols4.length];
		for(int i=0;i<455;i++)
		{
			records_j82[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j82[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j82", records_j82[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record450: [a450, b0, c0, d2, e0, f0, g2, h2, i0, j0, k10, l6, m8, n2, o0, p2, q8, r0, s13, t10]
		//record451: [a451, b1, c1, d3, e1, f1, g3, h3, i1, j1, k0, l7, m9, n3, o1, p3, q9, r1, s14, t11]
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4, o2, p4, q10, r2, s15, t12]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5, o3, p5, q11, r3, s16, t13]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6, q12, r4, s17, t14]
		int pageCount4 = (int)Math.ceil(455.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_j82 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "j82"+ File.separator+i+".db");
				dir_j82.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("j82");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x6", cols0);
		String [][] records_x6 = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_x6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x6[i][j] = cols0[j]+((i%(j+1)));
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
		//record14: [a14, b0, c2, d2, e4, f2, g0]
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		int pageCount0 = (int)Math.ceil(19.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x6"+ File.separator+i+".db");
				dir_x6.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("x6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r6p", cols1);
		String [][] records_r6p = new String[210][cols1.length];
		for(int i=0;i<210;i++)
		{
			records_r6p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r6p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r6p", records_r6p[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1]
		int pageCount1 = (int)Math.ceil(210.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_r6p = new File(FileManager.directory.getAbsolutePath()+ File.separator + "r6p"+ File.separator+i+".db");
				dir_r6p.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("r6p");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d68", cols2);
		String [][] records_d68 = new String[334][cols2.length];
		for(int i=0;i<334;i++)
		{
			records_d68[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d68[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d68", records_d68[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4, n7, o14]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8, o0]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9, o1]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10, o2]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3]
		int pageCount2 = (int)Math.ceil(334.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d68 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d68"+ File.separator+i+".db");
				dir_d68.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("d68");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("s5hb", cols3);
		String [][] records_s5hb = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			records_s5hb[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s5hb[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s5hb", records_s5hb[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record59: [a59, b1, c2, d3, e4, f5, g3, h3, i5, j9, k4, l11, m7, n3]
		//record60: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0, m8, n4]
		//record61: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5]
		//record62: [a62, b0, c2, d2, e2, f2, g6, h6, i8, j2, k7, l2, m10, n6]
		//record63: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7]
		int pageCount3 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_s5hb = new File(FileManager.directory.getAbsolutePath()+ File.separator + "s5hb"+ File.separator+i+".db");
				dir_s5hb.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("s5hb");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("pt8", cols0);
		String [][] records_pt8 = new String[328][cols0.length];
		for(int i=0;i<328;i++)
		{
			records_pt8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pt8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pt8", records_pt8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1, o8, p3]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9, p4]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3, o10, p5]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4, o11, p6]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12, p7]
		int pageCount0 = (int)Math.ceil(328.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pt8 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pt8"+ File.separator+i+".db");
				dir_pt8.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("pt8");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w5kvl", cols0);
		String [][] records_w5kvl = new String[243][cols0.length];
		for(int i=0;i<243;i++)
		{
			records_w5kvl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w5kvl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w5kvl", records_w5kvl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record238: [a238, b0, c1, d2, e3, f4, g0]
		//record239: [a239, b1, c2, d3, e4, f5, g1]
		//record240: [a240, b0, c0, d0, e0, f0, g2]
		//record241: [a241, b1, c1, d1, e1, f1, g3]
		//record242: [a242, b0, c2, d2, e2, f2, g4]
		int pageCount0 = (int)Math.ceil(243.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w5kvl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w5kvl"+ File.separator+i+".db");
				dir_w5kvl.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("w5kvl");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("x1p", cols1);
		String [][] records_x1p = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_x1p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x1p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x1p", records_x1p[i]);
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
		int pageCount1 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x1p = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x1p"+ File.separator+i+".db");
				dir_x1p.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("x1p");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a7", cols2);
		String [][] records_a7 = new String[334][cols2.length];
		for(int i=0;i<334;i++)
		{
			records_a7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a7[i][j] = cols2[j]+((i%(j+1)));
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
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		//record330: [a330, b0, c0, d2, e0, f0, g1]
		//record331: [a331, b1, c1, d3, e1, f1, g2]
		//record332: [a332, b0, c2, d0, e2, f2, g3]
		//record333: [a333, b1, c0, d1, e3, f3, g4]
		int pageCount2 = (int)Math.ceil(334.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a7 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a7"+ File.separator+i+".db");
				dir_a7.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("a7");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("q522", cols3);
		String [][] records_q522 = new String[180][cols3.length];
		for(int i=0;i<180;i++)
		{
			records_q522[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q522[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q522", records_q522[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7]
		//record179: [a179, b1, c2, d3, e4, f5, g4, h3, i8]
		int pageCount3 = (int)Math.ceil(180.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_q522 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "q522"+ File.separator+i+".db");
				dir_q522.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("q522");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x48", cols4);
		String [][] records_x48 = new String[469][cols4.length];
		for(int i=0;i<469;i++)
		{
			records_x48[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x48[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x48", records_x48[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8, m9]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9, m10]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6, l0, m0]
		int pageCount4 = (int)Math.ceil(469.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_x48 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "x48"+ File.separator+i+".db");
				dir_x48.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("x48");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wf", cols0);
		String [][] records_wf = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_wf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wf", records_wf[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		int pageCount0 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_wf = new File(FileManager.directory.getAbsolutePath()+ File.separator + "wf"+ File.separator+i+".db");
				dir_wf.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("wf");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w83", cols1);
		String [][] records_w83 = new String[470][cols1.length];
		for(int i=0;i<470;i++)
		{
			records_w83[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w83[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w83", records_w83[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5]
		//record468: [a468, b0, c0, d0, e3, f0, g6, h4, i0, j8, k6]
		//record469: [a469, b1, c1, d1, e4, f1, g0, h5, i1, j9, k7]
		int pageCount1 = (int)Math.ceil(470.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_w83 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "w83"+ File.separator+i+".db");
				dir_w83.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("w83");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("a9f", cols2);
		String [][] records_a9f = new String[249][cols2.length];
		for(int i=0;i<249;i++)
		{
			records_a9f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a9f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a9f", records_a9f[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record244: [a244, b0, c1, d0, e4, f4, g6, h4, i1, j4, k2, l4, m10, n6, o4, p4, q6]
		//record245: [a245, b1, c2, d1, e0, f5, g0, h5, i2, j5, k3, l5, m11, n7, o5, p5, q7]
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3, j6, k4, l6, m12, n8, o6, p6, q8]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4, j7, k5, l7, m0, n9, o7, p7, q9]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5, j8, k6, l8, m1, n10, o8, p8, q10]
		int pageCount2 = (int)Math.ceil(249.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_a9f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "a9f"+ File.separator+i+".db");
				dir_a9f.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("a9f");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y5n", cols0);
		String [][] records_y5n = new String[105][cols0.length];
		for(int i=0;i<105;i++)
		{
			records_y5n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y5n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y5n", records_y5n[i]);
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
		int pageCount0 = (int)Math.ceil(105.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_y5n = new File(FileManager.directory.getAbsolutePath()+ File.separator + "y5n"+ File.separator+i+".db");
				dir_y5n.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("y5n");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("d6", cols1);
		String [][] records_d6 = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			records_d6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d6", records_d6[i]);
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
		int pageCount1 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_d6 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "d6"+ File.separator+i+".db");
				dir_d6.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("d6");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zr", cols2);
		String [][] records_zr = new String[165][cols2.length];
		for(int i=0;i<165;i++)
		{
			records_zr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zr", records_zr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record160: [a160, b0, c1, d0, e0, f4, g6]
		//record161: [a161, b1, c2, d1, e1, f5, g0]
		//record162: [a162, b0, c0, d2, e2, f0, g1]
		//record163: [a163, b1, c1, d3, e3, f1, g2]
		//record164: [a164, b0, c2, d0, e4, f2, g3]
		int pageCount2 = (int)Math.ceil(165.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_zr = new File(FileManager.directory.getAbsolutePath()+ File.separator + "zr"+ File.separator+i+".db");
				dir_zr.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("zr");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m2b", cols3);
		String [][] records_m2b = new String[197][cols3.length];
		for(int i=0;i<197;i++)
		{
			records_m2b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m2b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m2b", records_m2b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record192: [a192, b0, c0, d0, e2, f0, g3, h0, i3, j2, k5, l0, m10, n10]
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1, i4, j3, k6, l1, m11, n11]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2, i5, j4, k7, l2, m12, n12]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3, i6, j5, k8, l3, m0, n13]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4, i7, j6, k9, l4, m1, n0]
		int pageCount3 = (int)Math.ceil(197.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_m2b = new File(FileManager.directory.getAbsolutePath()+ File.separator + "m2b"+ File.separator+i+".db");
				dir_m2b.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("m2b");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("gp7qj", cols0);
		String [][] records_gp7qj = new String[360][cols0.length];
		for(int i=0;i<360;i++)
		{
			records_gp7qj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gp7qj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gp7qj", records_gp7qj[i]);
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
		int pageCount0 = (int)Math.ceil(360.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_gp7qj = new File(FileManager.directory.getAbsolutePath()+ File.separator + "gp7qj"+ File.separator+i+".db");
				dir_gp7qj.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("gp7qj");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b1", cols1);
		String [][] records_b1 = new String[289][cols1.length];
		for(int i=0;i<289;i++)
		{
			records_b1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1", records_b1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record284: [a284, b0, c2, d0, e4, f2, g4]
		//record285: [a285, b1, c0, d1, e0, f3, g5]
		//record286: [a286, b0, c1, d2, e1, f4, g6]
		//record287: [a287, b1, c2, d3, e2, f5, g0]
		//record288: [a288, b0, c0, d0, e3, f0, g1]
		int pageCount1 = (int)Math.ceil(289.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_b1 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "b1"+ File.separator+i+".db");
				dir_b1.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("b1");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i0e", cols2);
		String [][] records_i0e = new String[468][cols2.length];
		for(int i=0;i<468;i++)
		{
			records_i0e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i0e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i0e", records_i0e[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11]
		int pageCount2 = (int)Math.ceil(468.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_i0e = new File(FileManager.directory.getAbsolutePath()+ File.separator + "i0e"+ File.separator+i+".db");
				dir_i0e.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("i0e");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("c0sxl", cols3);
		String [][] records_c0sxl = new String[148][cols3.length];
		for(int i=0;i<148;i++)
		{
			records_c0sxl[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c0sxl[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c0sxl", records_c0sxl[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8, j3, k0, l11, m0, n3, o8, p15, q7, r17]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0, j4, k1, l0, m1, n4, o9, p0, q8, r0]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1, j5, k2, l1, m2, n5, o10, p1, q9, r1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2, j6, k3, l2, m3, n6, o11, p2, q10, r2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3, j7, k4, l3, m4, n7, o12, p3, q11, r3]
		int pageCount3 = (int)Math.ceil(148.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_c0sxl = new File(FileManager.directory.getAbsolutePath()+ File.separator + "c0sxl"+ File.separator+i+".db");
				dir_c0sxl.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("c0sxl");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableValidationAffected_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z743", cols0);
		String [][] records_z743 = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			records_z743[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z743[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z743", records_z743[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3, n13]
		//record56: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		//record57: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9, m5, n1]
		int pageCount0 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		int randDel0 = 0;
		for(int i = 0; i < pageCount0-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_z743 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "z743"+ File.separator+i+".db");
				dir_z743.delete();
				randDel0++;
			}
		}
		ArrayList<String[]> tableValidate0 = DBApp.validateRecords("z743");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate0.size(), (randDel0*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h9", cols1);
		String [][] records_h9 = new String[251][cols1.length];
		for(int i=0;i<251;i++)
		{
			records_h9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h9", records_h9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record246: [a246, b0, c0, d2, e1, f0, g1, h6, i3]
		//record247: [a247, b1, c1, d3, e2, f1, g2, h7, i4]
		//record248: [a248, b0, c2, d0, e3, f2, g3, h0, i5]
		//record249: [a249, b1, c0, d1, e4, f3, g4, h1, i6]
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7]
		int pageCount1 = (int)Math.ceil(251.0/DBApp.dataPageSize);
		int randDel1 = 0;
		for(int i = 0; i < pageCount1-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_h9 = new File(FileManager.directory.getAbsolutePath()+ File.separator + "h9"+ File.separator+i+".db");
				dir_h9.delete();
				randDel1++;
			}
		}
		ArrayList<String[]> tableValidate1 = DBApp.validateRecords("h9");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate1.size(), (randDel1*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rj4f", cols2);
		String [][] records_rj4f = new String[121][cols2.length];
		for(int i=0;i<121;i++)
		{
			records_rj4f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rj4f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rj4f", records_rj4f[i]);
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
		int pageCount2 = (int)Math.ceil(121.0/DBApp.dataPageSize);
		int randDel2 = 0;
		for(int i = 0; i < pageCount2-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_rj4f = new File(FileManager.directory.getAbsolutePath()+ File.separator + "rj4f"+ File.separator+i+".db");
				dir_rj4f.delete();
				randDel2++;
			}
		}
		ArrayList<String[]> tableValidate2 = DBApp.validateRecords("rj4f");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate2.size(), (randDel2*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("pv3l", cols3);
		String [][] records_pv3l = new String[371][cols3.length];
		for(int i=0;i<371;i++)
		{
			records_pv3l[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_pv3l[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("pv3l", records_pv3l[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record366: [a366, b0, c0, d2, e1, f0, g2, h6, i6, j6, k3, l6, m2, n2, o6, p14, q9, r6, s5, t6]
		//record367: [a367, b1, c1, d3, e2, f1, g3, h7, i7, j7, k4, l7, m3, n3, o7, p15, q10, r7, s6, t7]
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8, m4, n4, o8, p0, q11, r8, s7, t8]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5, n5, o9, p1, q12, r9, s8, t9]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6, n6, o10, p2, q13, r10, s9, t10]
		int pageCount3 = (int)Math.ceil(371.0/DBApp.dataPageSize);
		int randDel3 = 0;
		for(int i = 0; i < pageCount3-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_pv3l = new File(FileManager.directory.getAbsolutePath()+ File.separator + "pv3l"+ File.separator+i+".db");
				dir_pv3l.delete();
				randDel3++;
			}
		}
		ArrayList<String[]> tableValidate3 = DBApp.validateRecords("pv3l");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate3.size(), (randDel3*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("eij", cols4);
		String [][] records_eij = new String[71][cols4.length];
		for(int i=0;i<71;i++)
		{
			records_eij[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_eij[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("eij", records_eij[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6, p2, q15]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3, q16]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1]
		//record70: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2]
		int pageCount4 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		int randDel4 = 0;
		for(int i = 0; i < pageCount4-1; i++)
		{
			if(genRandNum(4)>3)
			{
				File dir_eij = new File(FileManager.directory.getAbsolutePath()+ File.separator + "eij"+ File.separator+i+".db");
				dir_eij.delete();
				randDel4++;
			}
		}
		ArrayList<String[]> tableValidate4 = DBApp.validateRecords("eij");
		assertEquals("Validating a table with some pages deleted should report the correct size of missing records list.", tableValidate4.size(), (randDel4*DBApp.dataPageSize));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1948);
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