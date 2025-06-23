package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_07
{

	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_58() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("s303", cols0);
		DBApp.createBitMapIndex("s303","b");
		DBApp.createBitMapIndex("s303","d");
		DBApp.createBitMapIndex("s303","h");
		DBApp.createBitMapIndex("s303","j");
		DBApp.createBitMapIndex("s303","k");
		DBApp.createBitMapIndex("s303","l");
		DBApp.createBitMapIndex("s303","m");
		DBApp.createBitMapIndex("s303","o");
		DBApp.createBitMapIndex("s303","p");
		String [][] records_s303 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_s303[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s303[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s303", records_s303[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//last 15 records:
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "b", "b0"), ("101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "d", "d1"), ("010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "h", "h6"), ("000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "j", "j8"), ("000000001000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "k", "k6"), ("000000100000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "l", "l8"), ("000000001000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "m", "m9"), ("000000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s303", "o", "o11"), ("000000000001000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s303", "p", "p16"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w1", cols1);
		DBApp.createBitMapIndex("w1","d");
		DBApp.createBitMapIndex("w1","g");
		String [][] records_w1 = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			records_w1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w1", records_w1[i]);
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
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w1", "d", "d1"), ("010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w1", "g", "g7"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_59() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sj", cols0);
		DBApp.createBitMapIndex("sj","b");
		DBApp.createBitMapIndex("sj","d");
		DBApp.createBitMapIndex("sj","g");
		String [][] records_sj = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_sj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sj", records_sj[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sj", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sj", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("sj", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d5zb1", cols1);
		DBApp.createBitMapIndex("d5zb1","b");
		DBApp.createBitMapIndex("d5zb1","g");
		DBApp.createBitMapIndex("d5zb1","j");
		DBApp.createBitMapIndex("d5zb1","l");
		String [][] records_d5zb1 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_d5zb1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d5zb1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d5zb1", records_d5zb1[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//last 15 records:
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d5zb1", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d5zb1", "g", "g0"), ("10000001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d5zb1", "j", "j2"), ("00100000000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("d5zb1", "l", "l12"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xf", cols2);
		DBApp.createBitMapIndex("xf","b");
		DBApp.createBitMapIndex("xf","c");
		DBApp.createBitMapIndex("xf","f");
		DBApp.createBitMapIndex("xf","h");
		String [][] records_xf = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_xf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xf", records_xf[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6]
		//last 15 records:
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xf", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xf", "c", "c1"), ("0100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xf", "f", "f1"), ("0100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xf", "h", "h8"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zy9", cols3);
		DBApp.createBitMapIndex("zy9","b");
		DBApp.createBitMapIndex("zy9","c");
		DBApp.createBitMapIndex("zy9","d");
		DBApp.createBitMapIndex("zy9","f");
		DBApp.createBitMapIndex("zy9","g");
		String [][] records_zy9 = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			records_zy9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zy9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zy9", records_zy9[i]);
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
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zy9", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zy9", "c", "c1"), ("01001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zy9", "d", "d0"), ("10001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zy9", "f", "f2"), ("00100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zy9", "g", "g7"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yx8", cols4);
		DBApp.createBitMapIndex("yx8","b");
		DBApp.createBitMapIndex("yx8","d");
		DBApp.createBitMapIndex("yx8","e");
		DBApp.createBitMapIndex("yx8","g");
		String [][] records_yx8 = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			records_yx8[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_yx8[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("yx8", records_yx8[i]);
		}
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yx8", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yx8", "d", "d0"), ("1000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yx8", "e", "e2"), ("0010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yx8", "g", "g7"), ("0000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_60() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("mlhs", cols0);
		DBApp.createBitMapIndex("mlhs","d");
		DBApp.createBitMapIndex("mlhs","e");
		DBApp.createBitMapIndex("mlhs","g");
		DBApp.createBitMapIndex("mlhs","h");
		DBApp.createBitMapIndex("mlhs","i");
		DBApp.createBitMapIndex("mlhs","j");
		DBApp.createBitMapIndex("mlhs","q");
		DBApp.createBitMapIndex("mlhs","s");
		String [][] records_mlhs = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_mlhs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mlhs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mlhs", records_mlhs[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14]
		//last 15 records:
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8, s7]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11, s10]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13, s12]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15, s14]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16, s15]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "d", "d1"), ("010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "e", "e1"), ("010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "g", "g2"), ("001000000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "h", "h1"), ("010000000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "i", "i6"), ("000000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "j", "j6"), ("000000100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mlhs", "q", "q1"), ("010000000000000000100000000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mlhs", "s", "s19"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("if0", cols1);
		DBApp.createBitMapIndex("if0","b");
		DBApp.createBitMapIndex("if0","d");
		DBApp.createBitMapIndex("if0","e");
		DBApp.createBitMapIndex("if0","f");
		DBApp.createBitMapIndex("if0","g");
		DBApp.createBitMapIndex("if0","h");
		DBApp.createBitMapIndex("if0","j");
		DBApp.createBitMapIndex("if0","l");
		DBApp.createBitMapIndex("if0","m");
		DBApp.createBitMapIndex("if0","p");
		DBApp.createBitMapIndex("if0","q");
		String [][] records_if0 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			records_if0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_if0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("if0", records_if0[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//last 15 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "b", "b0"), ("1010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "d", "d0"), ("1000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "e", "e3"), ("0001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "f", "f0"), ("1000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "g", "g2"), ("0010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "h", "h2"), ("0010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "j", "j4"), ("0000100000000010000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "l", "l1"), ("0100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "m", "m10"), ("0000000000100000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("if0", "p", "p7"), ("0000000100000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("if0", "q", "q17"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_61() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y1y6", cols0);
		DBApp.createBitMapIndex("y1y6","c");
		DBApp.createBitMapIndex("y1y6","g");
		String [][] records_y1y6 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			records_y1y6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y1y6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y1y6", records_y1y6[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y1y6", "c", "c0"), ("100100100100100100100100100100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y1y6", "g", "g7"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_62() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s84", cols0);
		DBApp.createBitMapIndex("s84","b");
		DBApp.createBitMapIndex("s84","d");
		DBApp.createBitMapIndex("s84","g");
		DBApp.createBitMapIndex("s84","i");
		DBApp.createBitMapIndex("s84","j");
		DBApp.createBitMapIndex("s84","k");
		String [][] records_s84 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_s84[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s84[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s84", records_s84[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//last 15 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s84", "b", "b0"), ("101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s84", "d", "d1"), ("010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s84", "g", "g5"), ("000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s84", "i", "i7"), ("000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s84", "j", "j0"), ("100000000010000000001000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s84", "k", "k11"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yl", cols1);
		DBApp.createBitMapIndex("yl","d");
		DBApp.createBitMapIndex("yl","e");
		DBApp.createBitMapIndex("yl","f");
		DBApp.createBitMapIndex("yl","i");
		String [][] records_yl = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			records_yl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yl", records_yl[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//last 15 records:
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yl", "d", "d2"), ("00100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yl", "e", "e2"), ("00100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yl", "f", "f4"), ("00001000001000001000001000001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yl", "i", "i9"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ptp73", cols2);
		DBApp.createBitMapIndex("ptp73","b");
		DBApp.createBitMapIndex("ptp73","f");
		DBApp.createBitMapIndex("ptp73","g");
		String [][] records_ptp73 = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			records_ptp73[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ptp73[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ptp73", records_ptp73[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ptp73", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ptp73", "f", "f2"), ("0010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ptp73", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s08mx", cols3);
		DBApp.createBitMapIndex("s08mx","d");
		DBApp.createBitMapIndex("s08mx","g");
		String [][] records_s08mx = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			records_s08mx[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s08mx[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s08mx", records_s08mx[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s08mx", "d", "d2"), ("001000100010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s08mx", "g", "g7"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_63() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uw9", cols0);
		DBApp.createBitMapIndex("uw9","d");
		DBApp.createBitMapIndex("uw9","g");
		String [][] records_uw9 = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			records_uw9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uw9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uw9", records_uw9[i]);
		}
		//first 14 records:
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
		//last 14 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uw9", "d", "d0"), ("10001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uw9", "g", "g7"), ("00000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("of3i", cols1);
		DBApp.createBitMapIndex("of3i","b");
		DBApp.createBitMapIndex("of3i","c");
		DBApp.createBitMapIndex("of3i","i");
		DBApp.createBitMapIndex("of3i","j");
		DBApp.createBitMapIndex("of3i","k");
		DBApp.createBitMapIndex("of3i","n");
		DBApp.createBitMapIndex("of3i","p");
		DBApp.createBitMapIndex("of3i","q");
		String [][] records_of3i = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			records_of3i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_of3i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("of3i", records_of3i[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//last 15 records:
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "b", "b0"), ("101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "c", "c0"), ("100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "i", "i5"), ("000001000000001000000001000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "j", "j5"), ("000001000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "k", "k3"), ("000100000000001000000000010000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "n", "n10"), ("000000000010000000000000100000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("of3i", "p", "p1"), ("010000000000000001000000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("of3i", "q", "q17"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_64() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("r38u", cols0);
		DBApp.createBitMapIndex("r38u","b");
		DBApp.createBitMapIndex("r38u","c");
		DBApp.createBitMapIndex("r38u","d");
		DBApp.createBitMapIndex("r38u","f");
		DBApp.createBitMapIndex("r38u","h");
		DBApp.createBitMapIndex("r38u","j");
		DBApp.createBitMapIndex("r38u","k");
		DBApp.createBitMapIndex("r38u","l");
		String [][] records_r38u = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_r38u[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r38u[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r38u", records_r38u[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "b", "b0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "d", "d2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "f", "f3"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "h", "h5"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "j", "j8"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r38u", "k", "k5"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r38u", "l", "l12"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e131p", cols1);
		DBApp.createBitMapIndex("e131p","c");
		DBApp.createBitMapIndex("e131p","d");
		DBApp.createBitMapIndex("e131p","e");
		DBApp.createBitMapIndex("e131p","f");
		DBApp.createBitMapIndex("e131p","g");
		String [][] records_e131p = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			records_e131p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e131p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e131p", records_e131p[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e131p", "c", "c0"), ("1001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e131p", "d", "d0"), ("1000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e131p", "e", "e2"), ("0010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e131p", "f", "f3"), ("0001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e131p", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_65() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("y2g", cols0);
		DBApp.createBitMapIndex("y2g","b");
		DBApp.createBitMapIndex("y2g","g");
		DBApp.createBitMapIndex("y2g","k");
		DBApp.createBitMapIndex("y2g","l");
		DBApp.createBitMapIndex("y2g","o");
		DBApp.createBitMapIndex("y2g","p");
		DBApp.createBitMapIndex("y2g","q");
		String [][] records_y2g = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_y2g[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y2g[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y2g", records_y2g[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//last 15 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "g", "g3"), ("000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "k", "k6"), ("000000100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "l", "l8"), ("000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "o", "o0"), ("100000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2g", "p", "p9"), ("000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y2g", "q", "q17"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("xx5m5", cols1);
		DBApp.createBitMapIndex("xx5m5","c");
		DBApp.createBitMapIndex("xx5m5","e");
		DBApp.createBitMapIndex("xx5m5","f");
		DBApp.createBitMapIndex("xx5m5","g");
		DBApp.createBitMapIndex("xx5m5","h");
		DBApp.createBitMapIndex("xx5m5","i");
		DBApp.createBitMapIndex("xx5m5","k");
		DBApp.createBitMapIndex("xx5m5","m");
		DBApp.createBitMapIndex("xx5m5","n");
		DBApp.createBitMapIndex("xx5m5","p");
		String [][] records_xx5m5 = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			records_xx5m5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xx5m5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xx5m5", records_xx5m5[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "c", "c1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "e", "e0"), ("1000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "f", "f4"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "g", "g1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "h", "h0"), ("1000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "i", "i2"), ("0010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "k", "k1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "m", "m1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xx5m5", "n", "n6"), ("0000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xx5m5", "p", "p16"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("a3c", cols2);
		DBApp.createBitMapIndex("a3c","d");
		DBApp.createBitMapIndex("a3c","i");
		DBApp.createBitMapIndex("a3c","j");
		DBApp.createBitMapIndex("a3c","k");
		DBApp.createBitMapIndex("a3c","l");
		DBApp.createBitMapIndex("a3c","o");
		DBApp.createBitMapIndex("a3c","p");
		String [][] records_a3c = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			records_a3c[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a3c[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a3c", records_a3c[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "d", "d0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "i", "i4"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "j", "j6"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "k", "k1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "l", "l10"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3c", "o", "o6"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a3c", "p", "p16"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vq7", cols3);
		DBApp.createBitMapIndex("vq7","c");
		DBApp.createBitMapIndex("vq7","e");
		DBApp.createBitMapIndex("vq7","f");
		DBApp.createBitMapIndex("vq7","g");
		String [][] records_vq7 = new String[4][cols3.length];
		for(int i=0;i<4;i++)
		{
			records_vq7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vq7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vq7", records_vq7[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vq7", "c", "c0"), ("1001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vq7", "e", "e2"), ("0010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vq7", "f", "f4"), ("0000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vq7", "g", "g7"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mif", cols4);
		DBApp.createBitMapIndex("mif","b");
		DBApp.createBitMapIndex("mif","e");
		DBApp.createBitMapIndex("mif","g");
		String [][] records_mif = new String[5][cols4.length];
		for(int i=0;i<5;i++)
		{
			records_mif[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mif[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mif", records_mif[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mif", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mif", "e", "e0"), ("10000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mif", "g", "g7"), ("00000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_66() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wqz", cols0);
		DBApp.createBitMapIndex("wqz","c");
		DBApp.createBitMapIndex("wqz","e");
		DBApp.createBitMapIndex("wqz","h");
		String [][] records_wqz = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			records_wqz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wqz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wqz", records_wqz[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6]
		//last 15 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wqz", "c", "c1"), ("010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wqz", "e", "e1"), ("010000100001000010000100001000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wqz", "h", "h8"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_67() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xq", cols0);
		DBApp.createBitMapIndex("xq","b");
		DBApp.createBitMapIndex("xq","c");
		DBApp.createBitMapIndex("xq","d");
		DBApp.createBitMapIndex("xq","e");
		DBApp.createBitMapIndex("xq","g");
		String [][] records_xq = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			records_xq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xq", records_xq[i]);
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
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		//record48: [a48, b0, c0, d0, e3, f0, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq", "b", "b0"), ("1010101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq", "c", "c0"), ("1001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq", "d", "d1"), ("0100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq", "e", "e3"), ("0001000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xq", "g", "g7"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("y0", cols1);
		DBApp.createBitMapIndex("y0","b");
		DBApp.createBitMapIndex("y0","e");
		DBApp.createBitMapIndex("y0","f");
		DBApp.createBitMapIndex("y0","h");
		DBApp.createBitMapIndex("y0","k");
		DBApp.createBitMapIndex("y0","p");
		DBApp.createBitMapIndex("y0","q");
		DBApp.createBitMapIndex("y0","r");
		DBApp.createBitMapIndex("y0","s");
		String [][] records_y0 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			records_y0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y0", records_y0[i]);
		}
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "b", "b0"), ("101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "e", "e3"), ("000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "f", "f1"), ("010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "h", "h2"), ("001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "k", "k2"), ("001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "p", "p11"), ("000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "q", "q7"), ("000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0", "r", "r8"), ("000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y0", "s", "s19"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("j6", cols2);
		DBApp.createBitMapIndex("j6","c");
		DBApp.createBitMapIndex("j6","d");
		DBApp.createBitMapIndex("j6","f");
		DBApp.createBitMapIndex("j6","g");
		DBApp.createBitMapIndex("j6","m");
		DBApp.createBitMapIndex("j6","n");
		DBApp.createBitMapIndex("j6","o");
		DBApp.createBitMapIndex("j6","p");
		String [][] records_j6 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			records_j6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j6", records_j6[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//last 15 records:
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "c", "c0"), ("100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "d", "d2"), ("001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "f", "f3"), ("000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "g", "g4"), ("000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "m", "m3"), ("000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "n", "n5"), ("000001000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j6", "o", "o11"), ("000000000001000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j6", "p", "p16"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("fd9", cols3);
		DBApp.createBitMapIndex("fd9","b");
		DBApp.createBitMapIndex("fd9","d");
		DBApp.createBitMapIndex("fd9","e");
		DBApp.createBitMapIndex("fd9","f");
		DBApp.createBitMapIndex("fd9","g");
		DBApp.createBitMapIndex("fd9","j");
		String [][] records_fd9 = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			records_fd9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_fd9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("fd9", records_fd9[i]);
		}
		//first 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//last 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fd9", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fd9", "d", "d2"), ("0010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fd9", "e", "e3"), ("0001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fd9", "f", "f0"), ("1000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fd9", "g", "g1"), ("0100000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fd9", "j", "j10"), ("0000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_68() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xjpt", cols0);
		DBApp.createBitMapIndex("xjpt","c");
		DBApp.createBitMapIndex("xjpt","f");
		DBApp.createBitMapIndex("xjpt","g");
		DBApp.createBitMapIndex("xjpt","h");
		String [][] records_xjpt = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			records_xjpt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xjpt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xjpt", records_xjpt[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6]
		//last 15 records:
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xjpt", "c", "c1"), ("0100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xjpt", "f", "f0"), ("1000001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xjpt", "g", "g2"), ("0010000001000000100000010000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xjpt", "h", "h8"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w6", cols1);
		DBApp.createBitMapIndex("w6","c");
		DBApp.createBitMapIndex("w6","d");
		DBApp.createBitMapIndex("w6","e");
		DBApp.createBitMapIndex("w6","g");
		String [][] records_w6 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_w6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w6", records_w6[i]);
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
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w6", "c", "c0"), ("100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w6", "d", "d1"), ("010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w6", "e", "e2"), ("001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w6", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_69() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("veu", cols0);
		DBApp.createBitMapIndex("veu","b");
		DBApp.createBitMapIndex("veu","f");
		DBApp.createBitMapIndex("veu","g");
		String [][] records_veu = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_veu[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_veu[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("veu", records_veu[i]);
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
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		//record38: [a38, b0, c2, d2, e3, f2, g3]
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		//record44: [a44, b0, c2, d0, e4, f2, g2]
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		//record46: [a46, b0, c1, d2, e1, f4, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("veu", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("veu", "f", "f0"), ("10000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("veu", "g", "g7"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_70() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("atw", cols0);
		DBApp.createBitMapIndex("atw","b");
		DBApp.createBitMapIndex("atw","c");
		DBApp.createBitMapIndex("atw","e");
		DBApp.createBitMapIndex("atw","f");
		DBApp.createBitMapIndex("atw","g");
		DBApp.createBitMapIndex("atw","j");
		DBApp.createBitMapIndex("atw","k");
		DBApp.createBitMapIndex("atw","l");
		DBApp.createBitMapIndex("atw","q");
		String [][] records_atw = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_atw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_atw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("atw", records_atw[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//last 15 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "b", "b0"), ("10101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "c", "c0"), ("10010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "e", "e2"), ("00100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "f", "f0"), ("10000010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "g", "g4"), ("00001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "j", "j2"), ("00100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "k", "k8"), ("00000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atw", "l", "l5"), ("00000100000000000100000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("atw", "q", "q17"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("ks90", cols1);
		DBApp.createBitMapIndex("ks90","f");
		DBApp.createBitMapIndex("ks90","g");
		DBApp.createBitMapIndex("ks90","i");
		DBApp.createBitMapIndex("ks90","j");
		DBApp.createBitMapIndex("ks90","k");
		DBApp.createBitMapIndex("ks90","m");
		DBApp.createBitMapIndex("ks90","o");
		DBApp.createBitMapIndex("ks90","p");
		String [][] records_ks90 = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			records_ks90[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ks90[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ks90", records_ks90[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//last 15 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "f", "f0"), ("100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "g", "g5"), ("000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "i", "i3"), ("000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "j", "j8"), ("000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "k", "k1"), ("010000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "m", "m5"), ("000001000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ks90", "o", "o7"), ("000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ks90", "p", "p16"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gvm", cols2);
		DBApp.createBitMapIndex("gvm","e");
		DBApp.createBitMapIndex("gvm","f");
		DBApp.createBitMapIndex("gvm","g");
		String [][] records_gvm = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			records_gvm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gvm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gvm", records_gvm[i]);
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
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gvm", "e", "e2"), ("00100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gvm", "f", "f3"), ("00010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gvm", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wx94", cols3);
		DBApp.createBitMapIndex("wx94","b");
		DBApp.createBitMapIndex("wx94","c");
		DBApp.createBitMapIndex("wx94","g");
		String [][] records_wx94 = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			records_wx94[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wx94[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wx94", records_wx94[i]);
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
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wx94", "b", "b0"), ("101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wx94", "c", "c0"), ("100100100100100100100100100100100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wx94", "g", "g7"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_71() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g9", cols0);
		DBApp.createBitMapIndex("g9","b");
		DBApp.createBitMapIndex("g9","g");
		String [][] records_g9 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_g9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g9", records_g9[i]);
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
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		//record19: [a19, b1, c1, d3, e4, f1, g5]
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		//record21: [a21, b1, c0, d1, e1, f3, g0]
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		//record24: [a24, b0, c0, d0, e4, f0, g3]
		//record25: [a25, b1, c1, d1, e0, f1, g4]
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		//record29: [a29, b1, c2, d1, e4, f5, g1]
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		//record32: [a32, b0, c2, d0, e2, f2, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g9", "b", "b0"), ("101010101010101010101010101010101"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g9", "g", "g7"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_72() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("o0", cols0);
		DBApp.createBitMapIndex("o0","d");
		DBApp.createBitMapIndex("o0","e");
		DBApp.createBitMapIndex("o0","g");
		DBApp.createBitMapIndex("o0","h");
		DBApp.createBitMapIndex("o0","i");
		DBApp.createBitMapIndex("o0","k");
		DBApp.createBitMapIndex("o0","m");
		DBApp.createBitMapIndex("o0","n");
		DBApp.createBitMapIndex("o0","o");
		DBApp.createBitMapIndex("o0","p");
		DBApp.createBitMapIndex("o0","q");
		String [][] records_o0 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_o0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0", records_o0[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//last 15 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "d", "d2"), ("001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "e", "e3"), ("000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "g", "g1"), ("010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "h", "h0"), ("100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "i", "i6"), ("000000100000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "k", "k5"), ("000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "m", "m7"), ("000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "n", "n6"), ("000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "o", "o5"), ("000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0", "p", "p4"), ("000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o0", "q", "q17"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("f6", cols1);
		DBApp.createBitMapIndex("f6","b");
		DBApp.createBitMapIndex("f6","c");
		DBApp.createBitMapIndex("f6","g");
		DBApp.createBitMapIndex("f6","h");
		DBApp.createBitMapIndex("f6","i");
		DBApp.createBitMapIndex("f6","j");
		DBApp.createBitMapIndex("f6","k");
		DBApp.createBitMapIndex("f6","p");
		DBApp.createBitMapIndex("f6","q");
		String [][] records_f6 = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			records_f6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f6", records_f6[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "b", "b0"), ("1010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "c", "c0"), ("1001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "g", "g5"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "h", "h5"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "i", "i5"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "j", "j3"), ("0001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "k", "k4"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f6", "p", "p7"), ("0000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f6", "q", "q17"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("v9f1", cols2);
		DBApp.createBitMapIndex("v9f1","c");
		DBApp.createBitMapIndex("v9f1","d");
		DBApp.createBitMapIndex("v9f1","g");
		DBApp.createBitMapIndex("v9f1","i");
		DBApp.createBitMapIndex("v9f1","j");
		DBApp.createBitMapIndex("v9f1","l");
		String [][] records_v9f1 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			records_v9f1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v9f1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v9f1", records_v9f1[i]);
		}
		//first 13 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//last 13 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v9f1", "c", "c1"), ("0100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v9f1", "d", "d2"), ("0010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v9f1", "g", "g2"), ("0010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v9f1", "i", "i5"), ("0000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v9f1", "j", "j1"), ("0100000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v9f1", "l", "l12"), ("0000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_73() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l1a7", cols0);
		DBApp.createBitMapIndex("l1a7","b");
		DBApp.createBitMapIndex("l1a7","c");
		DBApp.createBitMapIndex("l1a7","e");
		DBApp.createBitMapIndex("l1a7","f");
		DBApp.createBitMapIndex("l1a7","g");
		String [][] records_l1a7 = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_l1a7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l1a7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l1a7", records_l1a7[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1]
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		//record18: [a18, b0, c0, d2, e3, f0, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l1a7", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l1a7", "c", "c0"), ("1001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l1a7", "e", "e3"), ("0001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l1a7", "f", "f2"), ("0010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l1a7", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("bwuk", cols1);
		DBApp.createBitMapIndex("bwuk","b");
		DBApp.createBitMapIndex("bwuk","d");
		DBApp.createBitMapIndex("bwuk","g");
		DBApp.createBitMapIndex("bwuk","h");
		DBApp.createBitMapIndex("bwuk","i");
		String [][] records_bwuk = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			records_bwuk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bwuk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bwuk", records_bwuk[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//last 15 records:
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bwuk", "b", "b0"), ("101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bwuk", "d", "d0"), ("100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bwuk", "g", "g1"), ("010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bwuk", "h", "h2"), ("001000000010000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bwuk", "i", "i9"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("k2e3d", cols2);
		DBApp.createBitMapIndex("k2e3d","b");
		DBApp.createBitMapIndex("k2e3d","f");
		DBApp.createBitMapIndex("k2e3d","g");
		DBApp.createBitMapIndex("k2e3d","h");
		DBApp.createBitMapIndex("k2e3d","j");
		DBApp.createBitMapIndex("k2e3d","k");
		DBApp.createBitMapIndex("k2e3d","l");
		DBApp.createBitMapIndex("k2e3d","m");
		DBApp.createBitMapIndex("k2e3d","o");
		DBApp.createBitMapIndex("k2e3d","p");
		DBApp.createBitMapIndex("k2e3d","q");
		DBApp.createBitMapIndex("k2e3d","s");
		String [][] records_k2e3d = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			records_k2e3d[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k2e3d[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k2e3d", records_k2e3d[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14]
		//last 15 records:
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13, s12]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15, s14]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16, s15]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7, s5]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8, s6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "b", "b0"), ("101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "f", "f4"), ("000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "g", "g2"), ("001000000100000010000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "h", "h0"), ("100000001000000010000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "j", "j3"), ("000100000000010000000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "k", "k4"), ("000010000000000100000000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "l", "l8"), ("000000001000000000001000000000001000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "m", "m11"), ("000000000001000000000000100000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "o", "o6"), ("000000100000000000000100000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "p", "p13"), ("000000000000010000000000000001000000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2e3d", "q", "q13"), ("000000000000010000000000000000100000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k2e3d", "s", "s19"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o9x", cols3);
		DBApp.createBitMapIndex("o9x","b");
		DBApp.createBitMapIndex("o9x","c");
		DBApp.createBitMapIndex("o9x","e");
		DBApp.createBitMapIndex("o9x","f");
		DBApp.createBitMapIndex("o9x","g");
		DBApp.createBitMapIndex("o9x","h");
		DBApp.createBitMapIndex("o9x","i");
		DBApp.createBitMapIndex("o9x","j");
		String [][] records_o9x = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			records_o9x[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o9x[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o9x", records_o9x[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//last 15 records:
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "b", "b0"), ("10101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "c", "c1"), ("01001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "e", "e2"), ("00100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "f", "f2"), ("00100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "g", "g5"), ("00000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "h", "h5"), ("00000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9x", "i", "i4"), ("00001000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o9x", "j", "j10"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("duq", cols4);
		DBApp.createBitMapIndex("duq","e");
		DBApp.createBitMapIndex("duq","f");
		DBApp.createBitMapIndex("duq","g");
		DBApp.createBitMapIndex("duq","h");
		DBApp.createBitMapIndex("duq","j");
		DBApp.createBitMapIndex("duq","k");
		DBApp.createBitMapIndex("duq","l");
		String [][] records_duq = new String[19][cols4.length];
		for(int i=0;i<19;i++)
		{
			records_duq[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_duq[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("duq", records_duq[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//last 15 records:
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "e", "e0"), ("1000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "f", "f4"), ("0000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "g", "g1"), ("0100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "h", "h4"), ("0000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "j", "j0"), ("1000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("duq", "k", "k2"), ("0010000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("duq", "l", "l12"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_74() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("jn3", cols0);
		DBApp.createBitMapIndex("jn3","c");
		DBApp.createBitMapIndex("jn3","e");
		DBApp.createBitMapIndex("jn3","g");
		DBApp.createBitMapIndex("jn3","j");
		DBApp.createBitMapIndex("jn3","k");
		DBApp.createBitMapIndex("jn3","l");
		DBApp.createBitMapIndex("jn3","m");
		DBApp.createBitMapIndex("jn3","n");
		String [][] records_jn3 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_jn3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jn3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jn3", records_jn3[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "c", "c0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "e", "e3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "g", "g1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "j", "j6"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "k", "k3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "l", "l5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jn3", "m", "m7"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jn3", "n", "n14"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("qx", cols1);
		DBApp.createBitMapIndex("qx","b");
		DBApp.createBitMapIndex("qx","f");
		DBApp.createBitMapIndex("qx","i");
		DBApp.createBitMapIndex("qx","m");
		String [][] records_qx = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_qx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qx", records_qx[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//last 15 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qx", "b", "b0"), ("101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qx", "f", "f3"), ("000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qx", "i", "i6"), ("000000100000000100000000100000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qx", "m", "m13"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("s3re", cols2);
		DBApp.createBitMapIndex("s3re","c");
		DBApp.createBitMapIndex("s3re","d");
		DBApp.createBitMapIndex("s3re","e");
		DBApp.createBitMapIndex("s3re","f");
		DBApp.createBitMapIndex("s3re","g");
		DBApp.createBitMapIndex("s3re","h");
		DBApp.createBitMapIndex("s3re","i");
		DBApp.createBitMapIndex("s3re","j");
		DBApp.createBitMapIndex("s3re","l");
		DBApp.createBitMapIndex("s3re","m");
		DBApp.createBitMapIndex("s3re","n");
		DBApp.createBitMapIndex("s3re","o");
		DBApp.createBitMapIndex("s3re","p");
		String [][] records_s3re = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			records_s3re[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s3re[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s3re", records_s3re[i]);
		}
		//first 14 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		//last 14 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "c", "c0"), ("10010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "d", "d0"), ("10001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "e", "e2"), ("00100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "f", "f0"), ("10000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "g", "g4"), ("00001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "h", "h1"), ("01000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "i", "i3"), ("00010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "j", "j5"), ("00000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "l", "l2"), ("00100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "m", "m3"), ("00010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "n", "n10"), ("00000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s3re", "o", "o12"), ("00000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s3re", "p", "p16"), ("00000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("tfpc", cols3);
		DBApp.createBitMapIndex("tfpc","d");
		DBApp.createBitMapIndex("tfpc","i");
		DBApp.createBitMapIndex("tfpc","j");
		DBApp.createBitMapIndex("tfpc","k");
		String [][] records_tfpc = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			records_tfpc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tfpc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tfpc", records_tfpc[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//last 15 records:
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tfpc", "d", "d2"), ("001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tfpc", "i", "i6"), ("000000100000000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tfpc", "j", "j6"), ("000000100000000010000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tfpc", "k", "k11"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("d9jr3", cols4);
		DBApp.createBitMapIndex("d9jr3","c");
		DBApp.createBitMapIndex("d9jr3","e");
		DBApp.createBitMapIndex("d9jr3","f");
		DBApp.createBitMapIndex("d9jr3","h");
		DBApp.createBitMapIndex("d9jr3","j");
		DBApp.createBitMapIndex("d9jr3","m");
		DBApp.createBitMapIndex("d9jr3","p");
		DBApp.createBitMapIndex("d9jr3","q");
		DBApp.createBitMapIndex("d9jr3","r");
		DBApp.createBitMapIndex("d9jr3","s");
		DBApp.createBitMapIndex("d9jr3","t");
		String [][] records_d9jr3 = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			records_d9jr3[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_d9jr3[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("d9jr3", records_d9jr3[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5, t5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6, t6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7, t7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8, t8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9, s9, t9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10, s10, t10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11, t11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13, t13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14, t14]
		//last 15 records:
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15, s14, t13]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16, s15, t14]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16, t15]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17, t16]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18, t17]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0, t18]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1, t19]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2, t0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3, t1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4, t2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7, s5, t3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8, s6, t4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9, s7, t5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10, s8, t6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11, s9, t7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "c", "c1"), ("010010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "e", "e3"), ("000100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "f", "f3"), ("000100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "h", "h6"), ("000000100000001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "j", "j1"), ("010000000001000000000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "m", "m4"), ("000010000000000001000000000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "p", "p10"), ("000000000010000000000000001000000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "q", "q8"), ("000000001000000000000000010000000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "r", "r9"), ("000000000100000000000000000100000000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9jr3", "s", "s14"), ("000000000000001000000000000000000100000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("d9jr3", "t", "t20"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_75() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ho", cols0);
		DBApp.createBitMapIndex("ho","g");
		DBApp.createBitMapIndex("ho","m");
		String [][] records_ho = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_ho[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ho[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ho", records_ho[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//last 15 records:
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho", "g", "g5"), ("00000100000010000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ho", "m", "m13"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("br", cols1);
		DBApp.createBitMapIndex("br","b");
		DBApp.createBitMapIndex("br","d");
		DBApp.createBitMapIndex("br","e");
		DBApp.createBitMapIndex("br","g");
		DBApp.createBitMapIndex("br","h");
		DBApp.createBitMapIndex("br","i");
		DBApp.createBitMapIndex("br","l");
		String [][] records_br = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			records_br[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_br[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("br", records_br[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "b", "b0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "d", "d2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "e", "e1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "g", "g4"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "h", "h0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("br", "i", "i0"), ("10"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("br", "l", "l12"), ("00"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_76() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n8c", cols0);
		DBApp.createBitMapIndex("n8c","c");
		DBApp.createBitMapIndex("n8c","d");
		DBApp.createBitMapIndex("n8c","f");
		DBApp.createBitMapIndex("n8c","j");
		DBApp.createBitMapIndex("n8c","k");
		String [][] records_n8c = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			records_n8c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n8c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n8c", records_n8c[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//last 15 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8c", "c", "c0"), ("1001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8c", "d", "d1"), ("0100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8c", "f", "f2"), ("0010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8c", "j", "j2"), ("0010000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n8c", "k", "k11"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m78ux", cols0);
		String [][] records_m78ux = new String[131][cols0.length];
		for(int i=0;i<131;i++)
		{
			records_m78ux[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m78ux[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m78ux", records_m78ux[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record126: [a126, b0, c0, d2, e1, f0, g0]
		//record127: [a127, b1, c1, d3, e2, f1, g1]
		//record128: [a128, b0, c2, d0, e3, f2, g2]
		//record129: [a129, b1, c0, d1, e4, f3, g3]
		//record130: [a130, b0, c1, d2, e0, f4, g4]
		String[] ConditionColumns0 = {"d","b","e"};
		String[] ConditionColumnsValues0 = {"d3","b1","e1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("m78ux", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a51, b1, c0, d3, e1, f3, g2]
		//record3: [a71, b1, c2, d3, e1, f5, g1]
		//record4: [a91, b1, c1, d3, e1, f1, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a71","b1","c2","d3","e1","f5","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a91","b1","c1","d3","e1","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("s45g6", cols1);
		String [][] records_s45g6 = new String[240][cols1.length];
		for(int i=0;i<240;i++)
		{
			records_s45g6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s45g6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s45g6", records_s45g6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record235: [a235, b1, c1, d3, e0, f1, g4, h3, i1, j5, k4, l7, m1, n11, o10, p11, q14, r1]
		//record236: [a236, b0, c2, d0, e1, f2, g5, h4, i2, j6, k5, l8, m2, n12, o11, p12, q15, r2]
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9, m3, n13, o12, p13, q16, r3]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10, m4, n0, o13, p14, q0, r4]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14, p15, q1, r5]
		String[] ConditionColumns1 = {"r","b","i","k","l","c","n","h","o"};
		String[] ConditionColumnsValues1 = {"r11","b1","i2","k3","l11","c2","n5","h7","o2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("s45g6", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8","n5","o2","p15","q13","r11"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d87", cols2);
		String [][] records_d87 = new String[450][cols2.length];
		for(int i=0;i<450;i++)
		{
			records_d87[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d87[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d87", records_d87[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record445: [a445, b1, c1, d1, e0, f1, g4]
		//record446: [a446, b0, c2, d2, e1, f2, g5]
		//record447: [a447, b1, c0, d3, e2, f3, g6]
		//record448: [a448, b0, c1, d0, e3, f4, g0]
		//record449: [a449, b1, c2, d1, e4, f5, g1]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("d87", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 150, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 150);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("s819o", cols0);
		String [][] records_s819o = new String[493][cols0.length];
		for(int i=0;i<493;i++)
		{
			records_s819o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s819o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s819o", records_s819o[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7, n12, o8, p8, q12, r2, s13, t8]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8, n13, o9, p9, q13, r3, s14, t9]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9, n0, o10, p10, q14, r4, s15, t10]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10, n1, o11, p11, q15, r5, s16, t11]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11, n2, o12, p12, q16, r6, s17, t12]
		String[] ConditionColumns0 = {"b","m"};
		String[] ConditionColumnsValues0 = {"b0","m12"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("s819o", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		//record1: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0, t18]
		//record2: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0, q13, r10, s7, t4]
		//record3: [a90, b0, c0, d2, e0, f0, g6, h2, i0, j0, k2, l6, m12, n6, o0, p10, q5, r0, s14, t10]
		//record4: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6, k6, l8, m12, n4, o11, p4, q14, r8, s2, t16]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12","q12","r12","s12","t12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8","p6","q4","r2","s0","t18"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a64","b0","c1","d0","e4","f4","g1","h0","i1","j4","k9","l4","m12","n8","o4","p0","q13","r10","s7","t4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a90","b0","c0","d2","e0","f0","g6","h2","i0","j0","k2","l6","m12","n6","o0","p10","q5","r0","s14","t10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a116","b0","c2","d0","e1","f2","g4","h4","i8","j6","k6","l8","m12","n4","o11","p4","q14","r8","s2","t16"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u6n", cols1);
		String [][] records_u6n = new String[209][cols1.length];
		for(int i=0;i<209;i++)
		{
			records_u6n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u6n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u6n", records_u6n[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record204: [a204, b0, c0, d0, e4, f0, g1]
		//record205: [a205, b1, c1, d1, e0, f1, g2]
		//record206: [a206, b0, c2, d2, e1, f2, g3]
		//record207: [a207, b1, c0, d3, e2, f3, g4]
		//record208: [a208, b0, c1, d0, e3, f4, g5]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("u6n", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 52, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 52);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zcl2", cols2);
		String [][] records_zcl2 = new String[233][cols2.length];
		for(int i=0;i<233;i++)
		{
			records_zcl2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_zcl2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("zcl2", records_zcl2[i]);
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
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("zcl2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 39, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 39);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zm36", cols3);
		String [][] records_zm36 = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			records_zm36[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zm36[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zm36", records_zm36[i]);
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
		String[] ConditionColumns3 = {"g","e","f"};
		String[] ConditionColumnsValues3 = {"g5","e1","f2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("zm36", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a26, b0, c2, d2, e1, f2, g5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("yt3h", cols0);
		String [][] records_yt3h = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_yt3h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yt3h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yt3h", records_yt3h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("yt3h", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0]
		//record3: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1","p1","q1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9","p9","q9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4","n3","o2","p1","q0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10","p9","q8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9","m7","n5","o3","p1","q16"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v12b", cols1);
		String [][] records_v12b = new String[330][cols1.length];
		for(int i=0;i<330;i++)
		{
			records_v12b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v12b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v12b", records_v12b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record325: [a325, b1, c1, d1, e0, f1, g3]
		//record326: [a326, b0, c2, d2, e1, f2, g4]
		//record327: [a327, b1, c0, d3, e2, f3, g5]
		//record328: [a328, b0, c1, d0, e3, f4, g6]
		//record329: [a329, b1, c2, d1, e4, f5, g0]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("v12b", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 83, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a9, b1, c0, d1, e4, f3, g2]
		//record3: [a13, b1, c1, d1, e3, f1, g6]
		//record4: [a17, b1, c2, d1, e2, f5, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 83);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("o8", cols2);
		String [][] records_o8 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			records_o8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o8", records_o8[i]);
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
		String[] ConditionColumns2 = {"c","d","g","h","b"};
		String[] ConditionColumnsValues2 = {"c2","d0","g1","h0","b0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("o8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f3p5b", cols3);
		String [][] records_f3p5b = new String[198][cols3.length];
		for(int i=0;i<198;i++)
		{
			records_f3p5b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f3p5b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f3p5b", records_f3p5b[i]);
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
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("f3p5b", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 66, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 66);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hl", cols4);
		String [][] records_hl = new String[406][cols4.length];
		for(int i=0;i<406;i++)
		{
			records_hl[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_hl[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("hl", records_hl[i]);
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
		String[] ConditionColumns4 = {"e","c"};
		String[] ConditionColumnsValues4 = {"e2","c2"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("hl", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 27, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a17, b1, c2, d1, e2, f5, g3]
		//record2: [a32, b0, c2, d0, e2, f2, g4]
		//record3: [a47, b1, c2, d3, e2, f5, g5]
		//record4: [a62, b0, c2, d2, e2, f2, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 27);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a62","b0","c2","d2","e2","f2","g6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ez", cols0);
		String [][] records_ez = new String[437][cols0.length];
		for(int i=0;i<437;i++)
		{
			records_ez[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ez[i][j] = cols0[j]+((i%(j+1)));
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
		//record432: [a432, b0, c0, d0, e2, f0, g5]
		//record433: [a433, b1, c1, d1, e3, f1, g6]
		//record434: [a434, b0, c2, d2, e4, f2, g0]
		//record435: [a435, b1, c0, d3, e0, f3, g1]
		//record436: [a436, b0, c1, d0, e1, f4, g2]
		String[] ConditionColumns0 = {"e","g","c"};
		String[] ConditionColumnsValues0 = {"e4","g3","c0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ez", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a24, b0, c0, d0, e4, f0, g3]
		//record1: [a129, b1, c0, d1, e4, f3, g3]
		//record2: [a234, b0, c0, d2, e4, f0, g3]
		//record3: [a339, b1, c0, d3, e4, f3, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a129","b1","c0","d1","e4","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a234","b0","c0","d2","e4","f0","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a339","b1","c0","d3","e4","f3","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("vkx", cols1);
		String [][] records_vkx = new String[244][cols1.length];
		for(int i=0;i<244;i++)
		{
			records_vkx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vkx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vkx", records_vkx[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11, m5, n1, o14, p15, q1, r5, s11]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0, m6, n2, o0, p0, q2, r6, s12]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1, m7, n3, o1, p1, q3, r7, s13]
		//record242: [a242, b0, c2, d2, e2, f2, g4, h2, i8, j2, k0, l2, m8, n4, o2, p2, q4, r8, s14]
		//record243: [a243, b1, c0, d3, e3, f3, g5, h3, i0, j3, k1, l3, m9, n5, o3, p3, q5, r9, s15]
		String[] ConditionColumns1 = {"r"};
		String[] ConditionColumnsValues1 = {"r2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vkx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2, s0]
		//record3: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0, o11, p8, q5, r2, s18]
		//record4: [a74, b0, c2, d2, e4, f2, g4, h2, i2, j4, k8, l2, m9, n4, o14, p10, q6, r2, s17]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 14);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2","p2","q2","r2","s2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5","p4","q3","r2","s1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8","p6","q4","r2","s0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0","o11","p8","q5","r2","s18"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a74","b0","c2","d2","e4","f2","g4","h2","i2","j4","k8","l2","m9","n4","o14","p10","q6","r2","s17"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("nu2s", cols2);
		String [][] records_nu2s = new String[242][cols2.length];
		for(int i=0;i<242;i++)
		{
			records_nu2s[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nu2s[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nu2s", records_nu2s[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record237: [a237, b1, c0, d1, e2, f3, g6, h5, i3, j7, k6, l9]
		//record238: [a238, b0, c1, d2, e3, f4, g0, h6, i4, j8, k7, l10]
		//record239: [a239, b1, c2, d3, e4, f5, g1, h7, i5, j9, k8, l11]
		//record240: [a240, b0, c0, d0, e0, f0, g2, h0, i6, j0, k9, l0]
		//record241: [a241, b1, c1, d1, e1, f1, g3, h1, i7, j1, k10, l1]
		String[] ConditionColumns2 = {"g","j","f","k"};
		String[] ConditionColumnsValues2 = {"g6","j2","f0","k0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("nu2s", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a132, b0, c0, d0, e2, f0, g6, h4, i6, j2, k0, l0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a132","b0","c0","d0","e2","f0","g6","h4","i6","j2","k0","l0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e7b", cols3);
		String [][] records_e7b = new String[500][cols3.length];
		for(int i=0;i<500;i++)
		{
			records_e7b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e7b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e7b", records_e7b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record495: [a495, b1, c0, d3, e0, f3, g5, h7, i0, j5, k0, l3, m1]
		//record496: [a496, b0, c1, d0, e1, f4, g6, h0, i1, j6, k1, l4, m2]
		//record497: [a497, b1, c2, d1, e2, f5, g0, h1, i2, j7, k2, l5, m3]
		//record498: [a498, b0, c0, d2, e3, f0, g1, h2, i3, j8, k3, l6, m4]
		//record499: [a499, b1, c1, d3, e4, f1, g2, h3, i4, j9, k4, l7, m5]
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("e7b", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 100, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record2: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11]
		//record3: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3]
		//record4: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 100);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("w26ib", cols4);
		String [][] records_w26ib = new String[468][cols4.length];
		for(int i=0;i<468;i++)
		{
			records_w26ib[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_w26ib[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("w26ib", records_w26ib[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4, j3, k1, l7, m8, n1, o13, p15, q4]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5, j4, k2, l8, m9, n2, o14, p0, q5]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6, j5, k3, l9, m10, n3, o0, p1, q6]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7, j6, k4, l10, m11, n4, o1, p2, q7]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8, j7, k5, l11, m12, n5, o2, p3, q8]
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a99"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("w26ib", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3, m8, n1, o9, p3, q14]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9","k0","l3","m8","n1","o9","p3","q14"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("pp2", cols0);
		String [][] records_pp2 = new String[373][cols0.length];
		for(int i=0;i<373;i++)
		{
			records_pp2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pp2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pp2", records_pp2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record368: [a368, b0, c2, d0, e3, f2, g4, h0, i8, j8, k5, l8, m4, n4, o8, p0, q11]
		//record369: [a369, b1, c0, d1, e4, f3, g5, h1, i0, j9, k6, l9, m5, n5, o9, p1, q12]
		//record370: [a370, b0, c1, d2, e0, f4, g6, h2, i1, j0, k7, l10, m6, n6, o10, p2, q13]
		//record371: [a371, b1, c2, d3, e1, f5, g0, h3, i2, j1, k8, l11, m7, n7, o11, p3, q14]
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8, n8, o12, p4, q15]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k10"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("pp2", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 33, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15]
		//record3: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9, p6, q3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 33);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10","m10","n10","o10","p10","q10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6","p5","q4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2","p0","q15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3","i7","j3","k10","l7","m4","n1","o13","p11","q9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2","n12","o9","p6","q3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wh04", cols1);
		String [][] records_wh04 = new String[208][cols1.length];
		for(int i=0;i<208;i++)
		{
			records_wh04[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wh04[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wh04", records_wh04[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record203: [a203, b1, c2, d3, e3, f5, g0, h3, i5]
		//record204: [a204, b0, c0, d0, e4, f0, g1, h4, i6]
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0]
		String[] ConditionColumns1 = {"d","i"};
		String[] ConditionColumnsValues1 = {"d1","i3"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("wh04", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record1: [a57, b1, c0, d1, e2, f3, g1, h1, i3]
		//record2: [a93, b1, c0, d1, e3, f3, g2, h5, i3]
		//record3: [a129, b1, c0, d1, e4, f3, g3, h1, i3]
		//record4: [a165, b1, c0, d1, e0, f3, g4, h5, i3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a57","b1","c0","d1","e2","f3","g1","h1","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a93","b1","c0","d1","e3","f3","g2","h5","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a129","b1","c0","d1","e4","f3","g3","h1","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a165","b1","c0","d1","e0","f3","g4","h5","i3"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("d4930", cols2);
		String [][] records_d4930 = new String[475][cols2.length];
		for(int i=0;i<475;i++)
		{
			records_d4930[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d4930[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d4930", records_d4930[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record470: [a470, b0, c2, d2, e0, f2, g1, h6, i2]
		//record471: [a471, b1, c0, d3, e1, f3, g2, h7, i3]
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6]
		String[] ConditionColumns2 = {"f","i","h"};
		String[] ConditionColumnsValues2 = {"f2","i2","h0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("d4930", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 6, records:
		//record0: [a56, b0, c2, d0, e1, f2, g0, h0, i2]
		//record1: [a128, b0, c2, d0, e3, f2, g2, h0, i2]
		//record2: [a200, b0, c2, d0, e0, f2, g4, h0, i2]
		//record3: [a272, b0, c2, d0, e2, f2, g6, h0, i2]
		//record4: [a344, b0, c2, d0, e4, f2, g1, h0, i2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 6);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a128","b0","c2","d0","e3","f2","g2","h0","i2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a200","b0","c2","d0","e0","f2","g4","h0","i2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a272","b0","c2","d0","e2","f2","g6","h0","i2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a344","b0","c2","d0","e4","f2","g1","h0","i2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("sj", cols0);
		String [][] records_sj = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			records_sj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sj", records_sj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record76: [a76, b0, c1, d0, e1, f4, g6, h4, i4, j6, k10, l4, m11, n6, o1, p12, q8, r4]
		//record77: [a77, b1, c2, d1, e2, f5, g0, h5, i5, j7, k0, l5, m12, n7, o2, p13, q9, r5]
		//record78: [a78, b0, c0, d2, e3, f0, g1, h6, i6, j8, k1, l6, m0, n8, o3, p14, q10, r6]
		//record79: [a79, b1, c1, d3, e4, f1, g2, h7, i7, j9, k2, l7, m1, n9, o4, p15, q11, r7]
		//record80: [a80, b0, c2, d0, e0, f2, g3, h0, i8, j0, k3, l8, m2, n10, o5, p0, q12, r8]
		String[] ConditionColumns0 = {"r","e","p","l","i","h","j"};
		String[] ConditionColumnsValues0 = {"r7","e2","p7","l7","i7","h7","j7"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("sj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7","o7","p7","q7","r7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("cs", cols1);
		String [][] records_cs = new String[393][cols1.length];
		for(int i=0;i<393;i++)
		{
			records_cs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cs", records_cs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record388: [a388, b0, c1, d0, e3, f4, g3, h4, i1, j8, k3, l4, m11, n10]
		//record389: [a389, b1, c2, d1, e4, f5, g4, h5, i2, j9, k4, l5, m12, n11]
		//record390: [a390, b0, c0, d2, e0, f0, g5, h6, i3, j0, k5, l6, m0, n12]
		//record391: [a391, b1, c1, d3, e1, f1, g6, h7, i4, j1, k6, l7, m1, n13]
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0]
		String[] ConditionColumns1 = {"k","d","g","h"};
		String[] ConditionColumnsValues1 = {"k3","d2","g3","h2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("cs", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a234","b0","c0","d2","e4","f0","g3","h2","i0","j4","k3","l6","m0","n10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g34", cols2);
		String [][] records_g34 = new String[344][cols2.length];
		for(int i=0;i<344;i++)
		{
			records_g34[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_g34[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("g34", records_g34[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record339: [a339, b1, c0, d3, e4, f3, g3]
		//record340: [a340, b0, c1, d0, e0, f4, g4]
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		//record342: [a342, b0, c0, d2, e2, f0, g6]
		//record343: [a343, b1, c1, d3, e3, f1, g0]
		String[] ConditionColumns2 = {"d"};
		String[] ConditionColumnsValues2 = {"d3"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("g34", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 86, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a15, b1, c0, d3, e0, f3, g1]
		//record4: [a19, b1, c1, d3, e4, f1, g5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 86);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("l1w53", cols0);
		String [][] records_l1w53 = new String[130][cols0.length];
		for(int i=0;i<130;i++)
		{
			records_l1w53[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l1w53[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l1w53", records_l1w53[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record125: [a125, b1, c2, d1, e0, f5, g6, h5, i8, j5, k4, l5]
		//record126: [a126, b0, c0, d2, e1, f0, g0, h6, i0, j6, k5, l6]
		//record127: [a127, b1, c1, d3, e2, f1, g1, h7, i1, j7, k6, l7]
		//record128: [a128, b0, c2, d0, e3, f2, g2, h0, i2, j8, k7, l8]
		//record129: [a129, b1, c0, d1, e4, f3, g3, h1, i3, j9, k8, l9]
		String[] ConditionColumns0 = {"c","h","k","f"};
		String[] ConditionColumnsValues0 = {"c0","h3","k0","f3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("l1w53", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a99, b1, c0, d3, e4, f3, g1, h3, i0, j9, k0, l3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a99","b1","c0","d3","e4","f3","g1","h3","i0","j9","k0","l3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("guk4i", cols1);
		String [][] records_guk4i = new String[314][cols1.length];
		for(int i=0;i<314;i++)
		{
			records_guk4i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_guk4i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("guk4i", records_guk4i[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1]
		String[] ConditionColumns1 = {"h"};
		String[] ConditionColumnsValues1 = {"h2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("guk4i", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 39, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 39);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("s0", cols2);
		String [][] records_s0 = new String[377][cols2.length];
		for(int i=0;i<377;i++)
		{
			records_s0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s0", records_s0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record372: [a372, b0, c0, d0, e2, f0, g1, h4, i3, j2, k9, l0, m8, n8, o12, p4]
		//record373: [a373, b1, c1, d1, e3, f1, g2, h5, i4, j3, k10, l1, m9, n9, o13, p5]
		//record374: [a374, b0, c2, d2, e4, f2, g3, h6, i5, j4, k0, l2, m10, n10, o14, p6]
		//record375: [a375, b1, c0, d3, e0, f3, g4, h7, i6, j5, k1, l3, m11, n11, o0, p7]
		//record376: [a376, b0, c1, d0, e1, f4, g5, h0, i7, j6, k2, l4, m12, n12, o1, p8]
		String[] ConditionColumns2 = {"n"};
		String[] ConditionColumnsValues2 = {"n4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("s0", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 27, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record1: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0]
		//record3: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14]
		//record4: [a60, b0, c0, d0, e0, f0, g4, h4, i6, j0, k5, l0, m8, n4, o0, p12]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 27);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3","p2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2","p0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2","l10","m7","n4","o1","p14"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a60","b0","c0","d0","e0","f0","g4","h4","i6","j0","k5","l0","m8","n4","o0","p12"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("c7", cols0);
		String [][] records_c7 = new String[112][cols0.length];
		for(int i=0;i<112;i++)
		{
			records_c7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c7", records_c7[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13]
		//record110: [a110, b0, c2, d2, e0, f2, g5, h6, i2, j0, k0, l2, m6, n12, o5, p14]
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("c7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 13, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record2: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record3: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		//record4: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 13);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3","p3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6","p5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0","k8","l6","m4","n2","o0","p14"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0","n11","o9","p7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("i5r", cols1);
		String [][] records_i5r = new String[113][cols1.length];
		for(int i=0;i<113;i++)
		{
			records_i5r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i5r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i5r", records_i5r[i]);
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
		String[] ConditionColumns1 = {"r","i"};
		String[] ConditionColumnsValues1 = {"r14","i5"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("i5r", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14]
		//record1: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13]
		//record2: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5, p2, q16, r14, s12]
		//record3: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0, r14, s11]
		//record4: [a86, b0, c2, d2, e1, f2, g2, h6, i5, j6, k9, l2, m8, n2, o11, p6, q1, r14, s10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14","p14","q14","r14","s14"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2","p0","q15","r14","s13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2","m11","n8","o5","p2","q16","r14","s12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a68","b0","c2","d0","e3","f2","g5","h4","i5","j8","k2","l8","m3","n12","o8","p4","q0","r14","s11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a86","b0","c2","d2","e1","f2","g2","h6","i5","j6","k9","l2","m8","n2","o11","p6","q1","r14","s10"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s01", cols2);
		String [][] records_s01 = new String[212][cols2.length];
		for(int i=0;i<212;i++)
		{
			records_s01[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s01[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s01", records_s01[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5]
		//record210: [a210, b0, c0, d2, e0, f0, g0, h2, i3, j0, k1, l6]
		//record211: [a211, b1, c1, d3, e1, f1, g1, h3, i4, j1, k2, l7]
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g6"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("s01", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 30, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record3: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 30);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6","h3","i0","j7","k5","l3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x64", cols3);
		String [][] records_x64 = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			records_x64[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x64[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x64", records_x64[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("x64", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o3d", cols0);
		String [][] records_o3d = new String[244][cols0.length];
		for(int i=0;i<244;i++)
		{
			records_o3d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o3d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o3d", records_o3d[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("o3d", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 49, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a16, b0, c1, d0, e1, f4, g2]
		//record4: [a21, b1, c0, d1, e1, f3, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 49);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vzn", cols0);
		String [][] records_vzn = new String[263][cols0.length];
		for(int i=0;i<263;i++)
		{
			records_vzn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vzn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vzn", records_vzn[i]);
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
		String[] ConditionColumns0 = {"d"};
		String[] ConditionColumnsValues0 = {"d1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("vzn", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 66, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a9, b1, c0, d1, e4, f3, g2]
		//record3: [a13, b1, c1, d1, e3, f1, g6]
		//record4: [a17, b1, c2, d1, e2, f5, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 66);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("c734a", cols1);
		String [][] records_c734a = new String[110][cols1.length];
		for(int i=0;i<110;i++)
		{
			records_c734a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c734a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c734a", records_c734a[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//last 5 records:
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9, m1, n7, o0, p9, q3, r15, s10]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10, m2, n8, o1, p10, q4, r16, s11]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3, n9, o2, p11, q5, r17, s12]
		//record108: [a108, b0, c0, d0, e3, f0, g3, h4, i0, j8, k9, l0, m4, n10, o3, p12, q6, r0, s13]
		//record109: [a109, b1, c1, d1, e4, f1, g4, h5, i1, j9, k10, l1, m5, n11, o4, p13, q7, r1, s14]
		String[] ConditionColumns1 = {"q"};
		String[] ConditionColumnsValues1 = {"q2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("c734a", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 7, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0]
		//record2: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17]
		//record3: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1, n11, o8, p5, q2, r17, s15]
		//record4: [a70, b0, c1, d2, e0, f4, g0, h6, i7, j0, k4, l10, m5, n0, o10, p6, q2, r16, s13]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 7);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2","p2","q2","r2","s2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6","n5","o4","p3","q2","r1","s0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0","m10","n8","o6","p4","q2","r0","s17"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a53","b1","c2","d1","e3","f5","g4","h5","i8","j3","k9","l5","m1","n11","o8","p5","q2","r17","s15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a70","b0","c1","d2","e0","f4","g0","h6","i7","j0","k4","l10","m5","n0","o10","p6","q2","r16","s13"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jy", cols2);
		String [][] records_jy = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			records_jy[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jy[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jy", records_jy[i]);
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
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("jy", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 16, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a6, b0, c0, d2, e1, f0, g6]
		//record3: [a9, b1, c0, d1, e4, f3, g2]
		//record4: [a12, b0, c0, d0, e2, f0, g5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 16);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("q5kxr", cols3);
		String [][] records_q5kxr = new String[448][cols3.length];
		for(int i=0;i<448;i++)
		{
			records_q5kxr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q5kxr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q5kxr", records_q5kxr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1, n9, o8, p11]
		//record444: [a444, b0, c0, d0, e4, f0, g3, h4, i3, j4, k4, l0, m2, n10, o9, p12]
		//record445: [a445, b1, c1, d1, e0, f1, g4, h5, i4, j5, k5, l1, m3, n11, o10, p13]
		//record446: [a446, b0, c2, d2, e1, f2, g5, h6, i5, j6, k6, l2, m4, n12, o11, p14]
		//record447: [a447, b1, c0, d3, e2, f3, g6, h7, i6, j7, k7, l3, m5, n13, o12, p15]
		String[] ConditionColumns3 = {"l"};
		String[] ConditionColumnsValues3 = {"l4"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("q5kxr", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 37, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record1: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12]
		//record3: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8]
		//record4: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7, p4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 37);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4","o4","p4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7","j6","k5","l4","m3","n2","o1","p0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13","p12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10","p8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10","o7","p4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("uhw", cols0);
		String [][] records_uhw = new String[473][cols0.length];
		for(int i=0;i<473;i++)
		{
			records_uhw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uhw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uhw", records_uhw[i]);
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
		String[] ConditionColumns0 = {"l","o","j","e","h"};
		String[] ConditionColumnsValues0 = {"l4","o7","j2","e2","h4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("uhw", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 4, records:
		//record0: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0, n10, o7]
		//record1: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4, o7]
		//record2: [a292, b0, c1, d0, e2, f4, g5, h4, i4, j2, k6, l4, m6, n12, o7]
		//record3: [a412, b0, c1, d0, e2, f4, g6, h4, i7, j2, k5, l4, m9, n6, o7]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 4);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3","h4","i7","j2","k8","l4","m0","n10","o7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a172","b0","c1","d0","e2","f4","g4","h4","i1","j2","k7","l4","m3","n4","o7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a292","b0","c1","d0","e2","f4","g5","h4","i4","j2","k6","l4","m6","n12","o7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a412","b0","c1","d0","e2","f4","g6","h4","i7","j2","k5","l4","m9","n6","o7"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("pfoh4", cols1);
		String [][] records_pfoh4 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_pfoh4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pfoh4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pfoh4", records_pfoh4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		String[] ConditionColumns1 = {"i","b","h"};
		String[] ConditionColumnsValues1 = {"i0","b1","h5"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("pfoh4", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d1h2b", cols2);
		String [][] records_d1h2b = new String[425][cols2.length];
		for(int i=0;i<425;i++)
		{
			records_d1h2b[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_d1h2b[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("d1h2b", records_d1h2b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record420: [a420, b0, c0, d0, e0, f0, g0, h4, i6, j0, k2, l0]
		//record421: [a421, b1, c1, d1, e1, f1, g1, h5, i7, j1, k3, l1]
		//record422: [a422, b0, c2, d2, e2, f2, g2, h6, i8, j2, k4, l2]
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4]
		String[] ConditionColumns2 = {"d","l","h"};
		String[] ConditionColumnsValues2 = {"d1","l9","h1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("d1h2b", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 18, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9]
		//record1: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record2: [a57, b1, c0, d1, e2, f3, g1, h1, i3, j7, k2, l9]
		//record3: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9]
		//record4: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 18);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0","l9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a57","b1","c0","d1","e2","f3","g1","h1","i3","j7","k2","l9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1","f3","g4","h1","i0","j1","k4","l9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a105","b1","c0","d1","e0","f3","g0","h1","i6","j5","k6","l9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("enm", cols3);
		String [][] records_enm = new String[157][cols3.length];
		for(int i=0;i<157;i++)
		{
			records_enm[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_enm[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("enm", records_enm[i]);
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
		String[] ConditionColumns3 = {"c"};
		String[] ConditionColumnsValues3 = {"c1"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("enm", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 52, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 52);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ky", cols4);
		String [][] records_ky = new String[32][cols4.length];
		for(int i=0;i<32;i++)
		{
			records_ky[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ky[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ky", records_ky[i]);
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
		String[] ConditionColumns4 = {"a"};
		String[] ConditionColumnsValues4 = {"a30"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("ky", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a30, b0, c0, d2, e0, f0, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h62", cols0);
		String [][] records_h62 = new String[477][cols0.length];
		for(int i=0;i<477;i++)
		{
			records_h62[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h62[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h62", records_h62[i]);
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
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("h62", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 159, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a11, b1, c2, d3, e1, f5, g4]
		//record4: [a14, b0, c2, d2, e4, f2, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 159);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dclr", cols0);
		String [][] records_dclr = new String[108][cols0.length];
		for(int i=0;i<108;i++)
		{
			records_dclr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dclr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dclr", records_dclr[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record103: [a103, b1, c1, d3, e3, f1, g5, h7, i4, j3, k4, l7, m12]
		//record104: [a104, b0, c2, d0, e4, f2, g6, h0, i5, j4, k5, l8, m0]
		//record105: [a105, b1, c0, d1, e0, f3, g0, h1, i6, j5, k6, l9, m1]
		//record106: [a106, b0, c1, d2, e1, f4, g1, h2, i7, j6, k7, l10, m2]
		//record107: [a107, b1, c2, d3, e2, f5, g2, h3, i8, j7, k8, l11, m3]
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("dclr", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 54, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record2: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record3: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record4: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 54);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m6274", cols1);
		String [][] records_m6274 = new String[118][cols1.length];
		for(int i=0;i<118;i++)
		{
			records_m6274[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m6274[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m6274", records_m6274[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5]
		//record116: [a116, b0, c2, d0, e1, f2, g4, h4, i8, j6]
		//record117: [a117, b1, c0, d1, e2, f3, g5, h5, i0, j7]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("m6274", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 20, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record4: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 20);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w6995", cols2);
		String [][] records_w6995 = new String[136][cols2.length];
		for(int i=0;i<136;i++)
		{
			records_w6995[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w6995[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w6995", records_w6995[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record131: [a131, b1, c2, d3, e1, f5, g5]
		//record132: [a132, b0, c0, d0, e2, f0, g6]
		//record133: [a133, b1, c1, d1, e3, f1, g0]
		//record134: [a134, b0, c2, d2, e4, f2, g1]
		//record135: [a135, b1, c0, d3, e0, f3, g2]
		String[] ConditionColumns2 = {"c","f","e"};
		String[] ConditionColumnsValues2 = {"c0","f0","e1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("w6995", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 5, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6]
		//record1: [a36, b0, c0, d0, e1, f0, g1]
		//record2: [a66, b0, c0, d2, e1, f0, g3]
		//record3: [a96, b0, c0, d0, e1, f0, g5]
		//record4: [a126, b0, c0, d2, e1, f0, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 5);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a66","b0","c0","d2","e1","f0","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a96","b0","c0","d0","e1","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a126","b0","c0","d2","e1","f0","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h9s", cols0);
		String [][] records_h9s = new String[301][cols0.length];
		for(int i=0;i<301;i++)
		{
			records_h9s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h9s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h9s", records_h9s[i]);
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
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c2"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("h9s", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 100, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a5, b1, c2, d1, e0, f5, g5]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a11, b1, c2, d3, e1, f5, g4]
		//record4: [a14, b0, c2, d2, e4, f2, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 100);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qqx", cols1);
		String [][] records_qqx = new String[365][cols1.length];
		for(int i=0;i<365;i++)
		{
			records_qqx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qqx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qqx", records_qqx[i]);
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
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("qqx", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 183, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a2, b0, c2, d2, e2, f2, g2]
		//record2: [a4, b0, c1, d0, e4, f4, g4]
		//record3: [a6, b0, c0, d2, e1, f0, g6]
		//record4: [a8, b0, c2, d0, e3, f2, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 183);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j74o8", cols2);
		String [][] records_j74o8 = new String[395][cols2.length];
		for(int i=0;i<395;i++)
		{
			records_j74o8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j74o8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j74o8", records_j74o8[i]);
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
		String[] ConditionColumns2 = {"b","c","d"};
		String[] ConditionColumnsValues2 = {"b0","c1","d0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("j74o8", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 33, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a16, b0, c1, d0, e1, f4, g2]
		//record2: [a28, b0, c1, d0, e3, f4, g0]
		//record3: [a40, b0, c1, d0, e0, f4, g5]
		//record4: [a52, b0, c1, d0, e2, f4, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 33);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a52","b0","c1","d0","e2","f4","g3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tc", cols3);
		String [][] records_tc = new String[177][cols3.length];
		for(int i=0;i<177;i++)
		{
			records_tc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tc", records_tc[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record172: [a172, b0, c1, d0, e2, f4, g4, h4, i1, j2, k7, l4, m3, n4]
		//record173: [a173, b1, c2, d1, e3, f5, g5, h5, i2, j3, k8, l5, m4, n5]
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9, l6, m5, n6]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10, l7, m6, n7]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0, l8, m7, n8]
		String[] ConditionColumns3 = {"m","c"};
		String[] ConditionColumnsValues3 = {"m5","c2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("tc", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 5, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record1: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record2: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13]
		//record3: [a122, b0, c2, d2, e2, f2, g3, h2, i5, j2, k1, l2, m5, n10]
		//record4: [a161, b1, c2, d1, e1, f5, g0, h1, i8, j1, k7, l5, m5, n7]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 5);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5","m5","n5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5","n2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6","h3","i2","j3","k6","l11","m5","n13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a122","b0","c2","d2","e2","f2","g3","h2","i5","j2","k1","l2","m5","n10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a161","b1","c2","d1","e1","f5","g0","h1","i8","j1","k7","l5","m5","n7"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sd25", cols4);
		String [][] records_sd25 = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			records_sd25[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_sd25[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("sd25", records_sd25[i]);
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
		String[] ConditionColumns4 = {"g","b","k"};
		String[] ConditionColumnsValues4 = {"g6","b0","k6"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("sd25", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xq", cols0);
		String [][] records_xq = new String[384][cols0.length];
		for(int i=0;i<384;i++)
		{
			records_xq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xq", records_xq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record379: [a379, b1, c1, d3, e4, f1, g1, h3, i1, j9, k5]
		//record380: [a380, b0, c2, d0, e0, f2, g2, h4, i2, j0, k6]
		//record381: [a381, b1, c0, d1, e1, f3, g3, h5, i3, j1, k7]
		//record382: [a382, b0, c1, d2, e2, f4, g4, h6, i4, j2, k8]
		//record383: [a383, b1, c2, d3, e3, f5, g5, h7, i5, j3, k9]
		String[] ConditionColumns0 = {"h"};
		String[] ConditionColumnsValues0 = {"h2"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("xq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 48, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record1: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record2: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record4: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 48);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ak4x", cols1);
		String [][] records_ak4x = new String[101][cols1.length];
		for(int i=0;i<101;i++)
		{
			records_ak4x[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ak4x[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ak4x", records_ak4x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record96: [a96, b0, c0, d0, e1, f0, g5, h0]
		//record97: [a97, b1, c1, d1, e2, f1, g6, h1]
		//record98: [a98, b0, c2, d2, e3, f2, g0, h2]
		//record99: [a99, b1, c0, d3, e4, f3, g1, h3]
		//record100: [a100, b0, c1, d0, e0, f4, g2, h4]
		String[] ConditionColumns1 = {"f"};
		String[] ConditionColumnsValues1 = {"f0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ak4x", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 17, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 17);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("su", cols2);
		String [][] records_su = new String[135][cols2.length];
		for(int i=0;i<135;i++)
		{
			records_su[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_su[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("su", records_su[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record130: [a130, b0, c1, d2, e0, f4, g4]
		//record131: [a131, b1, c2, d3, e1, f5, g5]
		//record132: [a132, b0, c0, d0, e2, f0, g6]
		//record133: [a133, b1, c1, d1, e3, f1, g0]
		//record134: [a134, b0, c2, d2, e4, f2, g1]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("su", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 45, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 45);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s87", cols3);
		String [][] records_s87 = new String[385][cols3.length];
		for(int i=0;i<385;i++)
		{
			records_s87[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s87[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s87", records_s87[i]);
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
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("s87", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 77, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a12, b0, c0, d0, e2, f0, g5]
		//record3: [a17, b1, c2, d1, e2, f5, g3]
		//record4: [a22, b0, c1, d2, e2, f4, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 77);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("hi1", cols0);
		String [][] records_hi1 = new String[472][cols0.length];
		for(int i=0;i<472;i++)
		{
			records_hi1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hi1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hi1", records_hi1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record467: [a467, b1, c2, d3, e2, f5, g5]
		//record468: [a468, b0, c0, d0, e3, f0, g6]
		//record469: [a469, b1, c1, d1, e4, f1, g0]
		//record470: [a470, b0, c2, d2, e0, f2, g1]
		//record471: [a471, b1, c0, d3, e1, f3, g2]
		String[] ConditionColumns0 = {"a","f","d"};
		String[] ConditionColumnsValues0 = {"a32","f2","d0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("hi1", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a32, b0, c2, d0, e2, f2, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uo5c", cols1);
		String [][] records_uo5c = new String[356][cols1.length];
		for(int i=0;i<356;i++)
		{
			records_uo5c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uo5c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uo5c", records_uo5c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record351: [a351, b1, c0, d3, e1, f3, g1]
		//record352: [a352, b0, c1, d0, e2, f4, g2]
		//record353: [a353, b1, c2, d1, e3, f5, g3]
		//record354: [a354, b0, c0, d2, e4, f0, g4]
		//record355: [a355, b1, c1, d3, e0, f1, g5]
		String[] ConditionColumns1 = {"b"};
		String[] ConditionColumnsValues1 = {"b1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("uo5c", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 178, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 178);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v3", cols2);
		String [][] records_v3 = new String[467][cols2.length];
		for(int i=0;i<467;i++)
		{
			records_v3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v3", records_v3[i]);
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
		String[] ConditionColumns2 = {"b"};
		String[] ConditionColumnsValues2 = {"b1"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("v3", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 233, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 233);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qc89", cols3);
		String [][] records_qc89 = new String[435][cols3.length];
		for(int i=0;i<435;i++)
		{
			records_qc89[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qc89[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qc89", records_qc89[i]);
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
		String[] ConditionColumns3 = {"f"};
		String[] ConditionColumnsValues3 = {"f4"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("qc89", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 72, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a10, b0, c1, d2, e0, f4, g3]
		//record2: [a16, b0, c1, d0, e1, f4, g2]
		//record3: [a22, b0, c1, d2, e2, f4, g1]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 72);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k158c", cols4);
		String [][] records_k158c = new String[440][cols4.length];
		for(int i=0;i<440;i++)
		{
			records_k158c[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_k158c[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("k158c", records_k158c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record435: [a435, b1, c0, d3, e0, f3, g1, h3, i3, j5, k6, l3, m6, n1]
		//record436: [a436, b0, c1, d0, e1, f4, g2, h4, i4, j6, k7, l4, m7, n2]
		//record437: [a437, b1, c2, d1, e2, f5, g3, h5, i5, j7, k8, l5, m8, n3]
		//record438: [a438, b0, c0, d2, e3, f0, g4, h6, i6, j8, k9, l6, m9, n4]
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5]
		String[] ConditionColumns4 = {"n"};
		String[] ConditionColumnsValues4 = {"n9"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("k158c", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 31, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9]
		//record4: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 31);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3","m12","n9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a65","b1","c2","d1","e0","f5","g2","h1","i2","j5","k10","l5","m0","n9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("m6", cols0);
		String [][] records_m6 = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			records_m6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m6", records_m6[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record81: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6]
		//record82: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5, l10, m4, n12, o7]
		//record83: [a83, b1, c2, d3, e3, f5, g6, h3, i2, j3, k6, l11, m5, n13, o8]
		//record84: [a84, b0, c0, d0, e4, f0, g0, h4, i3, j4, k7, l0, m6, n0, o9]
		//record85: [a85, b1, c1, d1, e0, f1, g1, h5, i4, j5, k8, l1, m7, n1, o10]
		String[] ConditionColumns0 = {"m"};
		String[] ConditionColumnsValues0 = {"m11"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("m6", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 6, records:
		//record0: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record2: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record3: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2, m11, n8, o5]
		//record4: [a63, b1, c0, d3, e3, f3, g0, h7, i0, j3, k8, l3, m11, n7, o3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 6);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2","h5","i1","j7","k4","l1","m11","n9","o7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a50","b0","c2","d2","e0","f2","g1","h2","i5","j0","k6","l2","m11","n8","o5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7","i0","j3","k8","l3","m11","n7","o3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ahq", cols0);
		String [][] records_ahq = new String[148][cols0.length];
		for(int i=0;i<148;i++)
		{
			records_ahq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ahq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ahq", records_ahq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record143: [a143, b1, c2, d3, e3, f5, g3, h7, i8]
		//record144: [a144, b0, c0, d0, e4, f0, g4, h0, i0]
		//record145: [a145, b1, c1, d1, e0, f1, g5, h1, i1]
		//record146: [a146, b0, c2, d2, e1, f2, g6, h2, i2]
		//record147: [a147, b1, c0, d3, e2, f3, g0, h3, i3]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("ahq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 25, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 25);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i2t", cols1);
		String [][] records_i2t = new String[390][cols1.length];
		for(int i=0;i<390;i++)
		{
			records_i2t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i2t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i2t", records_i2t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record385: [a385, b1, c1, d1, e0, f1, g0]
		//record386: [a386, b0, c2, d2, e1, f2, g1]
		//record387: [a387, b1, c0, d3, e2, f3, g2]
		//record388: [a388, b0, c1, d0, e3, f4, g3]
		//record389: [a389, b1, c2, d1, e4, f5, g4]
		String[] ConditionColumns1 = {"g","b"};
		String[] ConditionColumnsValues1 = {"g4","b0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("i2t", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 28, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4]
		//record1: [a18, b0, c0, d2, e3, f0, g4]
		//record2: [a32, b0, c2, d0, e2, f2, g4]
		//record3: [a46, b0, c1, d2, e1, f4, g4]
		//record4: [a60, b0, c0, d0, e0, f0, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 28);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a60","b0","c0","d0","e0","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("k35", cols2);
		String [][] records_k35 = new String[457][cols2.length];
		for(int i=0;i<457;i++)
		{
			records_k35[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k35[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k35", records_k35[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record452: [a452, b0, c2, d0, e2, f2, g4, h4, i2, j2, k1, l8, m10, n4, o2, p4, q10, r2]
		//record453: [a453, b1, c0, d1, e3, f3, g5, h5, i3, j3, k2, l9, m11, n5, o3, p5, q11, r3]
		//record454: [a454, b0, c1, d2, e4, f4, g6, h6, i4, j4, k3, l10, m12, n6, o4, p6, q12, r4]
		//record455: [a455, b1, c2, d3, e0, f5, g0, h7, i5, j5, k4, l11, m0, n7, o5, p7, q13, r5]
		//record456: [a456, b0, c0, d0, e1, f0, g1, h0, i6, j6, k5, l0, m1, n8, o6, p8, q14, r6]
		String[] ConditionColumns2 = {"g","c","p","m","n","o","d","l","r"};
		String[] ConditionColumnsValues2 = {"g2","c0","p9","m9","n9","o9","d1","l9","r9"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("k35", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9","n9","o9","p9","q9","r9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p0b", cols3);
		String [][] records_p0b = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			records_p0b[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p0b[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p0b", records_p0b[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1]
		//record50: [a50, b0, c2, d2, e0, f2, g1, h2, i5, j0, k6, l2]
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3]
		String[] ConditionColumns3 = {"e","k","c","a"};
		String[] ConditionColumnsValues3 = {"e1","k7","c0","a51"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("p0b", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bi", cols4);
		String [][] records_bi = new String[68][cols4.length];
		for(int i=0;i<68;i++)
		{
			records_bi[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bi[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bi", records_bi[i]);
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
		String[] ConditionColumns4 = {"d","h","c"};
		String[] ConditionColumnsValues4 = {"d2","h6","c0"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("bi", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 3, records:
		//record0: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record1: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0]
		//record2: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 3);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a30","b0","c0","d2","e0","f0","g2","h6","i3","j0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u0l9", cols0);
		String [][] records_u0l9 = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_u0l9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u0l9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u0l9", records_u0l9[i]);
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
		String[] ConditionColumns0 = {"b","c","a"};
		String[] ConditionColumnsValues0 = {"b0","c1","a40"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("u0l9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5, h0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("oz1zt", cols1);
		String [][] records_oz1zt = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			records_oz1zt[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_oz1zt[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("oz1zt", records_oz1zt[i]);
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
		String[] ConditionColumns1 = {"c"};
		String[] ConditionColumnsValues1 = {"c2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("oz1zt", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 24, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record1: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record3: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record4: [a14, b0, c2, d2, e4, f2, g0, h6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 24);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("q0x", cols0);
		String [][] records_q0x = new String[334][cols0.length];
		for(int i=0;i<334;i++)
		{
			records_q0x[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q0x[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q0x", records_q0x[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record329: [a329, b1, c2, d1, e4, f5, g0, h1, i5, j9, k10, l5, m4, n7, o14, p9]
		//record330: [a330, b0, c0, d2, e0, f0, g1, h2, i6, j0, k0, l6, m5, n8, o0, p10]
		//record331: [a331, b1, c1, d3, e1, f1, g2, h3, i7, j1, k1, l7, m6, n9, o1, p11]
		//record332: [a332, b0, c2, d0, e2, f2, g3, h4, i8, j2, k2, l8, m7, n10, o2, p12]
		//record333: [a333, b1, c0, d1, e3, f3, g4, h5, i0, j3, k3, l9, m8, n11, o3, p13]
		String[] ConditionColumns0 = {"a"};
		String[] ConditionColumnsValues0 = {"a3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("q0x", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3","p3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("jnj", cols0);
		String [][] records_jnj = new String[444][cols0.length];
		for(int i=0;i<444;i++)
		{
			records_jnj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jnj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jnj", records_jnj[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record439: [a439, b1, c1, d3, e4, f1, g5, h7, i7, j9, k10, l7, m10, n5]
		//record440: [a440, b0, c2, d0, e0, f2, g6, h0, i8, j0, k0, l8, m11, n6]
		//record441: [a441, b1, c0, d1, e1, f3, g0, h1, i0, j1, k1, l9, m12, n7]
		//record442: [a442, b0, c1, d2, e2, f4, g1, h2, i1, j2, k2, l10, m0, n8]
		//record443: [a443, b1, c2, d3, e3, f5, g2, h3, i2, j3, k3, l11, m1, n9]
		String[] ConditionColumns0 = {"i"};
		String[] ConditionColumnsValues0 = {"i4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("jnj", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 49, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record2: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record3: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record4: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 49);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4","n4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3","h7","i4","j1","k9","l7","m5","n3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("x736", cols1);
		String [][] records_x736 = new String[397][cols1.length];
		for(int i=0;i<397;i++)
		{
			records_x736[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x736[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x736", records_x736[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record392: [a392, b0, c2, d0, e2, f2, g0, h0, i5, j2, k7, l8, m2, n0, o2, p8, q1, r14]
		//record393: [a393, b1, c0, d1, e3, f3, g1, h1, i6, j3, k8, l9, m3, n1, o3, p9, q2, r15]
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10, m4, n2, o4, p10, q3, r16]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10, l11, m5, n3, o5, p11, q4, r17]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0, l0, m6, n4, o6, p12, q5, r0]
		String[] ConditionColumns1 = {"i"};
		String[] ConditionColumnsValues1 = {"i2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("x736", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 44, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record2: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2]
		//record3: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record4: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 44);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2","p2","q2","r2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11","m11","n11","o11","p11","q11","r11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5","p4","q3","r2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3","n1","o14","p13","q12","r11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8","p6","q4","r2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m6y", cols0);
		String [][] records_m6y = new String[459][cols0.length];
		for(int i=0;i<459;i++)
		{
			records_m6y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m6y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m6y", records_m6y[i]);
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
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("m6y", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 92, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a11, b1, c2, d3, e1, f5, g4]
		//record3: [a16, b0, c1, d0, e1, f4, g2]
		//record4: [a21, b1, c0, d1, e1, f3, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 92);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vwp", cols0);
		String [][] records_vwp = new String[158][cols0.length];
		for(int i=0;i<158;i++)
		{
			records_vwp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vwp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vwp", records_vwp[i]);
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
		String[] ConditionColumns0 = {"t"};
		String[] ConditionColumnsValues0 = {"t1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("vwp", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 8, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record1: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2, t1]
		//record2: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3, t1]
		//record3: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6, l1, m9, n5, o1, p13, q10, r7, s4, t1]
		//record4: [a81, b1, c0, d1, e1, f3, g4, h1, i0, j1, k4, l9, m3, n11, o6, p1, q13, r9, s5, t1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 8);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1","p1","q1","r1","s1","t1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0","h5","i3","j1","k10","l9","m8","n7","o6","p5","q4","r3","s2","t1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1","k8","l5","m2","n13","o11","p9","q7","r5","s3","t1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5","h5","i7","j1","k6","l1","m9","n5","o1","p13","q10","r7","s4","t1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a81","b1","c0","d1","e1","f3","g4","h1","i0","j1","k4","l9","m3","n11","o6","p1","q13","r9","s5","t1"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e7m9", cols1);
		String [][] records_e7m9 = new String[355][cols1.length];
		for(int i=0;i<355;i++)
		{
			records_e7m9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e7m9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e7m9", records_e7m9[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record350: [a350, b0, c2, d2, e0, f2, g0, h6, i8, j0, k9, l2]
		//record351: [a351, b1, c0, d3, e1, f3, g1, h7, i0, j1, k10, l3]
		//record352: [a352, b0, c1, d0, e2, f4, g2, h0, i1, j2, k0, l4]
		//record353: [a353, b1, c2, d1, e3, f5, g3, h1, i2, j3, k1, l5]
		//record354: [a354, b0, c0, d2, e4, f0, g4, h2, i3, j4, k2, l6]
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("e7m9", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 89, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record1: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record2: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record3: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record4: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 89);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10","l10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("l4", cols2);
		String [][] records_l4 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			records_l4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l4", records_l4[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		String[] ConditionColumns2 = {"c"};
		String[] ConditionColumnsValues2 = {"c0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("l4", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record2: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 3);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0","j0","k0","l0","m0","n0","o0","p0","q0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3","p3","q3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6","h6","i6","j6","k6","l6","m6","n6","o6","p6","q6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v2", cols3);
		String [][] records_v2 = new String[325][cols3.length];
		for(int i=0;i<325;i++)
		{
			records_v2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v2", records_v2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record320: [a320, b0, c2, d0, e0, f2, g5]
		//record321: [a321, b1, c0, d1, e1, f3, g6]
		//record322: [a322, b0, c1, d2, e2, f4, g0]
		//record323: [a323, b1, c2, d3, e3, f5, g1]
		//record324: [a324, b0, c0, d0, e4, f0, g2]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("v2", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 81, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 81);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x43", cols4);
		String [][] records_x43 = new String[228][cols4.length];
		for(int i=0;i<228;i++)
		{
			records_x43[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_x43[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("x43", records_x43[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record223: [a223, b1, c1, d3, e3, f1, g6]
		//record224: [a224, b0, c2, d0, e4, f2, g0]
		//record225: [a225, b1, c0, d1, e0, f3, g1]
		//record226: [a226, b0, c1, d2, e1, f4, g2]
		//record227: [a227, b1, c2, d3, e2, f5, g3]
		String[] ConditionColumns4 = {"a","f","d"};
		String[] ConditionColumnsValues4 = {"a165","f3","d1"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("x43", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a165, b1, c0, d1, e0, f3, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a165","b1","c0","d1","e0","f3","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("qe", cols0);
		String [][] records_qe = new String[314][cols0.length];
		for(int i=0;i<314;i++)
		{
			records_qe[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qe[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qe", records_qe[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record309: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10, n1, o9]
		//record310: [a310, b0, c1, d2, e0, f4, g2, h6, i4, j0, k2, l10, m11, n2, o10]
		//record311: [a311, b1, c2, d3, e1, f5, g3, h7, i5, j1, k3, l11, m12, n3, o11]
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0, n4, o12]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5, l1, m1, n5, o13]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qe", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record2: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record3: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record4: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 29);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1","h1","i1","j1","k1","l1","m1","n1","o1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9","o8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a34","b0","c1","d2","e4","f4","g6","h2","i7","j4","k1","l10","m8","n6","o4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a45","b1","c0","d1","e0","f3","g3","h5","i0","j5","k1","l9","m6","n3","o0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("umd2", cols1);
		String [][] records_umd2 = new String[169][cols1.length];
		for(int i=0;i<169;i++)
		{
			records_umd2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_umd2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("umd2", records_umd2[i]);
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
		String[] ConditionColumns1 = {"m"};
		String[] ConditionColumnsValues1 = {"m2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("umd2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 13, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record2: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record3: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11]
		//record4: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2, n12, o9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 13);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1","h7","i6","j5","k4","l3","m2","n1","o0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0","h4","i1","j8","k6","l4","m2","n0","o13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5","j1","k8","l5","m2","n13","o11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a54","b0","c0","d2","e4","f0","g5","h6","i0","j4","k10","l6","m2","n12","o9"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gcq", cols0);
		String [][] records_gcq = new String[428][cols0.length];
		for(int i=0;i<428;i++)
		{
			records_gcq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gcq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gcq", records_gcq[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record423: [a423, b1, c0, d3, e3, f3, g3, h7, i0, j3, k5, l3]
		//record424: [a424, b0, c1, d0, e4, f4, g4, h0, i1, j4, k6, l4]
		//record425: [a425, b1, c2, d1, e0, f5, g5, h1, i2, j5, k7, l5]
		//record426: [a426, b0, c0, d2, e1, f0, g6, h2, i3, j6, k8, l6]
		//record427: [a427, b1, c1, d3, e2, f1, g0, h3, i4, j7, k9, l7]
		String[] ConditionColumns0 = {"f"};
		String[] ConditionColumnsValues0 = {"f5"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("gcq", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 71, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record1: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record2: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record3: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record4: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 71);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5","l5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4","h3","i2","j1","k0","l11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("z3y", cols1);
		String [][] records_z3y = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_z3y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z3y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z3y", records_z3y[i]);
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
		String[] ConditionColumns1 = {"o","g","f","h","l","m"};
		String[] ConditionColumnsValues1 = {"o2","g4","f2","h0","l8","m6"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("z3y", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4","o2","p0","q15","r14"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("q60h", cols2);
		String [][] records_q60h = new String[317][cols2.length];
		for(int i=0;i<317;i++)
		{
			records_q60h[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q60h[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q60h", records_q60h[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6]
		String[] ConditionColumns2 = {"g","c","e"};
		String[] ConditionColumnsValues2 = {"g5","c1","e4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("q60h", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 3, records:
		//record0: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record1: [a124, b0, c1, d0, e4, f4, g5, h4, i7, j4]
		//record2: [a229, b1, c1, d1, e4, f1, g5, h5, i4, j9]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 3);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a124","b0","c1","d0","e4","f4","g5","h4","i7","j4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a229","b1","c1","d1","e4","f1","g5","h5","i4","j9"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("av8", cols3);
		String [][] records_av8 = new String[172][cols3.length];
		for(int i=0;i<172;i++)
		{
			records_av8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_av8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("av8", records_av8[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//last 5 records:
		//record167: [a167, b1, c2, d3, e2, f5, g6, h7, i5, j7, k2, l11, m11, n13, o2, p7, q14, r5, s15, t7]
		//record168: [a168, b0, c0, d0, e3, f0, g0, h0, i6, j8, k3, l0, m12, n0, o3, p8, q15, r6, s16, t8]
		//record169: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1, o4, p9, q16, r7, s17, t9]
		//record170: [a170, b0, c2, d2, e0, f2, g2, h2, i8, j0, k5, l2, m1, n2, o5, p10, q0, r8, s18, t10]
		//record171: [a171, b1, c0, d3, e1, f3, g3, h3, i0, j1, k6, l3, m2, n3, o6, p11, q1, r9, s0, t11]
		String[] ConditionColumns3 = {"g","q","s","t"};
		String[] ConditionColumnsValues3 = {"g3","q4","s5","t17"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("av8", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a157, b1, c1, d1, e2, f1, g3, h5, i4, j7, k3, l1, m1, n3, o7, p13, q4, r13, s5, t17]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a157","b1","c1","d1","e2","f1","g3","h5","i4","j7","k3","l1","m1","n3","o7","p13","q4","r13","s5","t17"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sp5", cols4);
		String [][] records_sp5 = new String[240][cols4.length];
		for(int i=0;i<240;i++)
		{
			records_sp5[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_sp5[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("sp5", records_sp5[i]);
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
		String[] ConditionColumns4 = {"f"};
		String[] ConditionColumnsValues4 = {"f5"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("sp5", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 40, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5]
		//record1: [a11, b1, c2, d3, e1, f5, g4]
		//record2: [a17, b1, c2, d1, e2, f5, g3]
		//record3: [a23, b1, c2, d3, e3, f5, g2]
		//record4: [a29, b1, c2, d1, e4, f5, g1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 40);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a11","b1","c2","d3","e1","f5","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("q3", cols0);
		String [][] records_q3 = new String[477][cols0.length];
		for(int i=0;i<477;i++)
		{
			records_q3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q3", records_q3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record472: [a472, b0, c1, d0, e2, f4, g3, h0, i4, j2, k10, l4, m4, n10]
		//record473: [a473, b1, c2, d1, e3, f5, g4, h1, i5, j3, k0, l5, m5, n11]
		//record474: [a474, b0, c0, d2, e4, f0, g5, h2, i6, j4, k1, l6, m6, n12]
		//record475: [a475, b1, c1, d3, e0, f1, g6, h3, i7, j5, k2, l7, m7, n13]
		//record476: [a476, b0, c2, d0, e1, f2, g0, h4, i8, j6, k3, l8, m8, n0]
		String[] ConditionColumns0 = {"l"};
		String[] ConditionColumnsValues0 = {"l8"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("q3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 40, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record1: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record2: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record3: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2]
		//record4: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 40);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a32","b0","c2","d0","e2","f2","g4","h0","i5","j2","k10","l8","m6","n4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a44","b0","c2","d0","e4","f2","g2","h4","i8","j4","k0","l8","m5","n2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o00", cols1);
		String [][] records_o00 = new String[218][cols1.length];
		for(int i=0;i<218;i++)
		{
			records_o00[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o00[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o00", records_o00[i]);
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
		String[] ConditionColumns1 = {"d"};
		String[] ConditionColumnsValues1 = {"d0"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("o00", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 55, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a8, b0, c2, d0, e3, f2, g1]
		//record3: [a12, b0, c0, d0, e2, f0, g5]
		//record4: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 55);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nykxy", cols2);
		String [][] records_nykxy = new String[296][cols2.length];
		for(int i=0;i<296;i++)
		{
			records_nykxy[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nykxy[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nykxy", records_nykxy[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record291: [a291, b1, c0, d3, e1, f3, g4]
		//record292: [a292, b0, c1, d0, e2, f4, g5]
		//record293: [a293, b1, c2, d1, e3, f5, g6]
		//record294: [a294, b0, c0, d2, e4, f0, g0]
		//record295: [a295, b1, c1, d3, e0, f1, g1]
		String[] ConditionColumns2 = {"d","g"};
		String[] ConditionColumnsValues2 = {"d3","g3"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("nykxy", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 11, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a59, b1, c2, d3, e4, f5, g3]
		//record3: [a87, b1, c0, d3, e2, f3, g3]
		//record4: [a115, b1, c1, d3, e0, f1, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 11);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a59","b1","c2","d3","e4","f5","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a87","b1","c0","d3","e2","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a115","b1","c1","d3","e0","f1","g3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m11", cols3);
		String [][] records_m11 = new String[220][cols3.length];
		for(int i=0;i<220;i++)
		{
			records_m11[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_m11[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("m11", records_m11[i]);
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
		String[] ConditionColumns3 = {"d","a"};
		String[] ConditionColumnsValues3 = {"d3","a203"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("m11", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a203, b1, c2, d3, e3, f5, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a203","b1","c2","d3","e3","f5","g0"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j2956", cols0);
		String [][] records_j2956 = new String[314][cols0.length];
		for(int i=0;i<314;i++)
		{
			records_j2956[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j2956[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j2956", records_j2956[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record309: [a309, b1, c0, d1, e4, f3, g1]
		//record310: [a310, b0, c1, d2, e0, f4, g2]
		//record311: [a311, b1, c2, d3, e1, f5, g3]
		//record312: [a312, b0, c0, d0, e2, f0, g4]
		//record313: [a313, b1, c1, d1, e3, f1, g5]
		String[] ConditionColumns0 = {"g","d","e"};
		String[] ConditionColumnsValues0 = {"g0","d3","e3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("j2956", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a63, b1, c0, d3, e3, f3, g0]
		//record1: [a203, b1, c2, d3, e3, f5, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a203","b1","c2","d3","e3","f5","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n10", cols1);
		String [][] records_n10 = new String[198][cols1.length];
		for(int i=0;i<198;i++)
		{
			records_n10[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n10[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n10", records_n10[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record193: [a193, b1, c1, d1, e3, f1, g4, h1]
		//record194: [a194, b0, c2, d2, e4, f2, g5, h2]
		//record195: [a195, b1, c0, d3, e0, f3, g6, h3]
		//record196: [a196, b0, c1, d0, e1, f4, g0, h4]
		//record197: [a197, b1, c2, d1, e2, f5, g1, h5]
		String[] ConditionColumns1 = {"d","e"};
		String[] ConditionColumnsValues1 = {"d3","e3"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("n10", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 10, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record1: [a23, b1, c2, d3, e3, f5, g2, h7]
		//record2: [a43, b1, c1, d3, e3, f1, g1, h3]
		//record3: [a63, b1, c0, d3, e3, f3, g0, h7]
		//record4: [a83, b1, c2, d3, e3, f5, g6, h3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 10);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a43","b1","c1","d3","e3","f1","g1","h3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a63","b1","c0","d3","e3","f3","g0","h7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a83","b1","c2","d3","e3","f5","g6","h3"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("fwt", cols0);
		String [][] records_fwt = new String[179][cols0.length];
		for(int i=0;i<179;i++)
		{
			records_fwt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fwt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fwt", records_fwt[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record174: [a174, b0, c0, d2, e4, f0, g6, h6, i3, j4, k9]
		//record175: [a175, b1, c1, d3, e0, f1, g0, h7, i4, j5, k10]
		//record176: [a176, b0, c2, d0, e1, f2, g1, h0, i5, j6, k0]
		//record177: [a177, b1, c0, d1, e2, f3, g2, h1, i6, j7, k1]
		//record178: [a178, b0, c1, d2, e3, f4, g3, h2, i7, j8, k2]
		String[] ConditionColumns0 = {"e"};
		String[] ConditionColumnsValues0 = {"e4"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("fwt", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 35, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 35);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ypu0", cols1);
		String [][] records_ypu0 = new String[406][cols1.length];
		for(int i=0;i<406;i++)
		{
			records_ypu0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ypu0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ypu0", records_ypu0[i]);
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
		String[] ConditionColumns1 = {"f","d"};
		String[] ConditionColumnsValues1 = {"f1","d1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("ypu0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 34, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a13, b1, c1, d1, e3, f1, g6]
		//record2: [a25, b1, c1, d1, e0, f1, g4]
		//record3: [a37, b1, c1, d1, e2, f1, g2]
		//record4: [a49, b1, c1, d1, e4, f1, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 34);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a37","b1","c1","d1","e2","f1","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("n270", cols2);
		String [][] records_n270 = new String[328][cols2.length];
		for(int i=0;i<328;i++)
		{
			records_n270[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n270[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n270", records_n270[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record323: [a323, b1, c2, d3, e3, f5, g1, h3, i8, j3, k4, l11, m11, n1, o8]
		//record324: [a324, b0, c0, d0, e4, f0, g2, h4, i0, j4, k5, l0, m12, n2, o9]
		//record325: [a325, b1, c1, d1, e0, f1, g3, h5, i1, j5, k6, l1, m0, n3, o10]
		//record326: [a326, b0, c2, d2, e1, f2, g4, h6, i2, j6, k7, l2, m1, n4, o11]
		//record327: [a327, b1, c0, d3, e2, f3, g5, h7, i3, j7, k8, l3, m2, n5, o12]
		String[] ConditionColumns2 = {"f"};
		String[] ConditionColumnsValues2 = {"f2"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("n270", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 55, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//record3: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		//record4: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 55);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1","n0","o14"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a20","b0","c2","d0","e0","f2","g6","h4","i2","j0","k9","l8","m7","n6","o5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4","l2","m0","n12","o11"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p6t", cols0);
		String [][] records_p6t = new String[346][cols0.length];
		for(int i=0;i<346;i++)
		{
			records_p6t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p6t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p6t", records_p6t[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//last 5 records:
		//record341: [a341, b1, c2, d1, e1, f5, g5]
		//record342: [a342, b0, c0, d2, e2, f0, g6]
		//record343: [a343, b1, c1, d3, e3, f1, g0]
		//record344: [a344, b0, c2, d0, e4, f2, g1]
		//record345: [a345, b1, c0, d1, e0, f3, g2]
		String[] ConditionColumns0 = {"f","d"};
		String[] ConditionColumnsValues0 = {"f3","d3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("p6t", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 29, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a15, b1, c0, d3, e0, f3, g1]
		//record2: [a27, b1, c0, d3, e2, f3, g6]
		//record3: [a39, b1, c0, d3, e4, f3, g4]
		//record4: [a51, b1, c0, d3, e1, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 29);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a15","b1","c0","d3","e0","f3","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a27","b1","c0","d3","e2","f3","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h43o1", cols1);
		String [][] records_h43o1 = new String[323][cols1.length];
		for(int i=0;i<323;i++)
		{
			records_h43o1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h43o1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h43o1", records_h43o1[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//last 5 records:
		//record318: [a318, b0, c0, d2, e3, f0, g3, h6, i3, j8]
		//record319: [a319, b1, c1, d3, e4, f1, g4, h7, i4, j9]
		//record320: [a320, b0, c2, d0, e0, f2, g5, h0, i5, j0]
		//record321: [a321, b1, c0, d1, e1, f3, g6, h1, i6, j1]
		//record322: [a322, b0, c1, d2, e2, f4, g0, h2, i7, j2]
		String[] ConditionColumns1 = {"a"};
		String[] ConditionColumnsValues1 = {"a266"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("h43o1", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 1, records:
		//record0: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 1);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a266","b0","c2","d2","e1","f2","g0","h2","i5","j6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("av7qe", cols2);
		String [][] records_av7qe = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			records_av7qe[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_av7qe[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("av7qe", records_av7qe[i]);
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
		String[] ConditionColumns2 = {"g"};
		String[] ConditionColumnsValues2 = {"g3"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("av7qe", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l59", cols3);
		String [][] records_l59 = new String[468][cols3.length];
		for(int i=0;i<468;i++)
		{
			records_l59[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l59[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l59", records_l59[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record463: [a463, b1, c1, d3, e3, f1, g1, h7, i4]
		//record464: [a464, b0, c2, d0, e4, f2, g2, h0, i5]
		//record465: [a465, b1, c0, d1, e0, f3, g3, h1, i6]
		//record466: [a466, b0, c1, d2, e1, f4, g4, h2, i7]
		//record467: [a467, b1, c2, d3, e2, f5, g5, h3, i8]
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d0"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("l59", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 117, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//record2: [a8, b0, c2, d0, e3, f2, g1, h0, i8]
		//record3: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record4: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 117);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0","h0","i0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a16","b0","c1","d0","e1","f4","g2","h0","i7"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q6202", cols0);
		String [][] records_q6202 = new String[493][cols0.length];
		for(int i=0;i<493;i++)
		{
			records_q6202[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q6202[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q6202", records_q6202[i]);
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
		String[] ConditionColumns0 = {"g"};
		String[] ConditionColumnsValues0 = {"g0"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("q6202", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 71, records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a7, b1, c1, d3, e2, f1, g0]
		//record2: [a14, b0, c2, d2, e4, f2, g0]
		//record3: [a21, b1, c0, d1, e1, f3, g0]
		//record4: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 71);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a0","b0","c0","d0","e0","f0","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a21","b1","c0","d1","e1","f3","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a28","b0","c1","d0","e3","f4","g0"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("gy38", cols1);
		String [][] records_gy38 = new String[317][cols1.length];
		for(int i=0;i<317;i++)
		{
			records_gy38[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gy38[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gy38", records_gy38[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//last 5 records:
		//record312: [a312, b0, c0, d0, e2, f0, g4, h0, i6, j2, k4, l0, m0, n4]
		//record313: [a313, b1, c1, d1, e3, f1, g5, h1, i7, j3, k5, l1, m1, n5]
		//record314: [a314, b0, c2, d2, e4, f2, g6, h2, i8, j4, k6, l2, m2, n6]
		//record315: [a315, b1, c0, d3, e0, f3, g0, h3, i0, j5, k7, l3, m3, n7]
		//record316: [a316, b0, c1, d0, e1, f4, g1, h4, i1, j6, k8, l4, m4, n8]
		String[] ConditionColumns1 = {"e","n","d"};
		String[] ConditionColumnsValues1 = {"e4","n1","d1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("gy38", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 3, records:
		//record0: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record1: [a169, b1, c1, d1, e4, f1, g1, h1, i7, j9, k4, l1, m0, n1]
		//record2: [a309, b1, c0, d1, e4, f3, g1, h5, i3, j9, k1, l9, m10, n1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 3);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3","n1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a169","b1","c1","d1","e4","f1","g1","h1","i7","j9","k4","l1","m0","n1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a309","b1","c0","d1","e4","f3","g1","h5","i3","j9","k1","l9","m10","n1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vi", cols2);
		String [][] records_vi = new String[234][cols2.length];
		for(int i=0;i<234;i++)
		{
			records_vi[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vi[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vi", records_vi[i]);
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
		String[] ConditionColumns2 = {"k"};
		String[] ConditionColumnsValues2 = {"k2"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("vi", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 22, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record1: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13, t13]
		//record2: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5, t4]
		//record3: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16, t15]
		//record4: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10, s8, t6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 22);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2","p2","q2","r2","s2","t2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13","p13","q13","r13","s13","t13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9","p8","q7","r6","s5","t4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a35","b1","c2","d3","e0","f5","g0","h3","i8","j5","k2","l11","m9","n7","o5","p3","q1","r17","s16","t15"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a46","b0","c1","d2","e1","f4","g4","h6","i1","j6","k2","l10","m7","n4","o1","p14","q12","r10","s8","t6"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("my", cols3);
		String [][] records_my = new String[116][cols3.length];
		for(int i=0;i<116;i++)
		{
			records_my[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_my[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("my", records_my[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record111: [a111, b1, c0, d3, e1, f3, g6, h7, i3, j1, k1, l3, m7, n13, o6, p15, q9]
		//record112: [a112, b0, c1, d0, e2, f4, g0, h0, i4, j2, k2, l4, m8, n0, o7, p0, q10]
		//record113: [a113, b1, c2, d1, e3, f5, g1, h1, i5, j3, k3, l5, m9, n1, o8, p1, q11]
		//record114: [a114, b0, c0, d2, e4, f0, g2, h2, i6, j4, k4, l6, m10, n2, o9, p2, q12]
		//record115: [a115, b1, c1, d3, e0, f1, g3, h3, i7, j5, k5, l7, m11, n3, o10, p3, q13]
		String[] ConditionColumns3 = {"d","b","p"};
		String[] ConditionColumnsValues3 = {"d0","b0","p8"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("my", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 7, records:
		//record0: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8]
		//record1: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7]
		//record2: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		//record3: [a56, b0, c2, d0, e1, f2, g0, h0, i2, j6, k1, l8, m4, n0, o11, p8, q5]
		//record4: [a72, b0, c0, d0, e2, f0, g2, h0, i0, j2, k6, l0, m7, n2, o12, p8, q4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 7);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8","p8","q8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11","n10","o9","p8","q7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10","p8","q6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a56","b0","c2","d0","e1","f2","g0","h0","i2","j6","k1","l8","m4","n0","o11","p8","q5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a72","b0","c0","d0","e2","f0","g2","h0","i0","j2","k6","l0","m7","n2","o12","p8","q4"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("owwhv", cols4);
		String [][] records_owwhv = new String[493][cols4.length];
		for(int i=0;i<493;i++)
		{
			records_owwhv[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_owwhv[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("owwhv", records_owwhv[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record488: [a488, b0, c2, d0, e3, f2, g5, h0, i2, j8, k4, l8, m7]
		//record489: [a489, b1, c0, d1, e4, f3, g6, h1, i3, j9, k5, l9, m8]
		//record490: [a490, b0, c1, d2, e0, f4, g0, h2, i4, j0, k6, l10, m9]
		//record491: [a491, b1, c2, d3, e1, f5, g1, h3, i5, j1, k7, l11, m10]
		//record492: [a492, b0, c0, d0, e2, f0, g2, h4, i6, j2, k8, l0, m11]
		String[] ConditionColumns4 = {"j"};
		String[] ConditionColumnsValues4 = {"j9"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("owwhv", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 49, records:
		//record0: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record1: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record2: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		//record3: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record4: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 49);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a29","b1","c2","d1","e4","f5","g1","h5","i2","j9","k7","l5","m3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a39","b1","c0","d3","e4","f3","g4","h7","i3","j9","k6","l3","m0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a49","b1","c1","d1","e4","f1","g0","h1","i4","j9","k5","l1","m10"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b1dq3", cols0);
		String [][] records_b1dq3 = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_b1dq3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b1dq3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b1dq3", records_b1dq3[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4]
		//last 5 records:
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		String[] ConditionColumns0 = {"e","h","c","i"};
		String[] ConditionColumnsValues0 = {"e1","h1","c2","i5"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("b1dq3", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 1, records:
		//record0: [a41, b1, c2, d1, e1, f5, g6, h1, i5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 1);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a41","b1","c2","d1","e1","f5","g6","h1","i5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f1g", cols1);
		String [][] records_f1g = new String[419][cols1.length];
		for(int i=0;i<419;i++)
		{
			records_f1g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f1g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f1g", records_f1g[i]);
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
		String[] ConditionColumns1 = {"b","f","e"};
		String[] ConditionColumnsValues1 = {"b1","f1","e1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("f1g", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 14, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a31, b1, c1, d3, e1, f1, g3]
		//record2: [a61, b1, c1, d1, e1, f1, g5]
		//record3: [a91, b1, c1, d3, e1, f1, g0]
		//record4: [a121, b1, c1, d1, e1, f1, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 14);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a31","b1","c1","d3","e1","f1","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a91","b1","c1","d3","e1","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a121","b1","c1","d1","e1","f1","g2"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("e07", cols2);
		String [][] records_e07 = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			records_e07[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e07[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e07", records_e07[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//last 5 records:
		//record65: [a65, b1, c2, d1, e0, f5, g2, h1, i2, j5, k10, l5, m0, n9, o5, p1, q14, r11]
		//record66: [a66, b0, c0, d2, e1, f0, g3, h2, i3, j6, k0, l6, m1, n10, o6, p2, q15, r12]
		//record67: [a67, b1, c1, d3, e2, f1, g4, h3, i4, j7, k1, l7, m2, n11, o7, p3, q16, r13]
		//record68: [a68, b0, c2, d0, e3, f2, g5, h4, i5, j8, k2, l8, m3, n12, o8, p4, q0, r14]
		//record69: [a69, b1, c0, d1, e4, f3, g6, h5, i6, j9, k3, l9, m4, n13, o9, p5, q1, r15]
		String[] ConditionColumns2 = {"i","c","j"};
		String[] ConditionColumnsValues2 = {"i4","c1","j0"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("e07", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a40","b0","c1","d0","e0","f4","g5","h0","i4","j0","k7","l4","m1","n12","o10","p8","q6","r4"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n6y7k", cols3);
		String [][] records_n6y7k = new String[326][cols3.length];
		for(int i=0;i<326;i++)
		{
			records_n6y7k[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n6y7k[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n6y7k", records_n6y7k[i]);
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
		String[] ConditionColumns3 = {"e"};
		String[] ConditionColumnsValues3 = {"e3"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("n6y7k", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 65, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3]
		//record1: [a8, b0, c2, d0, e3, f2, g1]
		//record2: [a13, b1, c1, d1, e3, f1, g6]
		//record3: [a18, b0, c0, d2, e3, f0, g4]
		//record4: [a23, b1, c2, d3, e3, f5, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 65);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c03", cols4);
		String [][] records_c03 = new String[3][cols4.length];
		for(int i=0;i<3;i++)
		{
			records_c03[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_c03[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("c03", records_c03[i]);
		}
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		String[] ConditionColumns4 = {"f"};
		String[] ConditionColumnsValues4 = {"f2"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("c03", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 1, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 1);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("qsz", cols0);
		String [][] records_qsz = new String[210][cols0.length];
		for(int i=0;i<210;i++)
		{
			records_qsz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qsz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qsz", records_qsz[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record205: [a205, b1, c1, d1, e0, f1, g2, h5, i7, j5, k7, l1]
		//record206: [a206, b0, c2, d2, e1, f2, g3, h6, i8, j6, k8, l2]
		//record207: [a207, b1, c0, d3, e2, f3, g4, h7, i0, j7, k9, l3]
		//record208: [a208, b0, c1, d0, e3, f4, g5, h0, i1, j8, k10, l4]
		//record209: [a209, b1, c2, d1, e4, f5, g6, h1, i2, j9, k0, l5]
		String[] ConditionColumns0 = {"k"};
		String[] ConditionColumnsValues0 = {"k3"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("qsz", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 19, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record1: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record2: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record3: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		//record4: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 19);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a36","b0","c0","d0","e1","f0","g1","h4","i0","j6","k3","l0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("zbx2", cols1);
		String [][] records_zbx2 = new String[269][cols1.length];
		for(int i=0;i<269;i++)
		{
			records_zbx2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zbx2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zbx2", records_zbx2[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9, p8]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10, p9]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11, p10]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12, p11]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13, p12]
		String[] ConditionColumns1 = {"e"};
		String[] ConditionColumnsValues1 = {"e2"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("zbx2", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 54, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record1: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//record2: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record3: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		//record4: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 54);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2","h2","i2","j2","k2","l2","m2","n2","o2","p2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0","h7","i7","j7","k7","l7","m7","n7","o7","p7"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a17","b1","c2","d1","e2","f5","g3","h1","i8","j7","k6","l5","m4","n3","o2","p1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a22","b0","c1","d2","e2","f4","g1","h6","i4","j2","k0","l10","m9","n8","o7","p6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pf2", cols2);
		String [][] records_pf2 = new String[123][cols2.length];
		for(int i=0;i<123;i++)
		{
			records_pf2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_pf2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("pf2", records_pf2[i]);
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
		String[] ConditionColumns2 = {"f","d","a"};
		String[] ConditionColumnsValues2 = {"f3","d3","a51"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("pf2", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a51, b1, c0, d3, e1, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("c54", cols3);
		String [][] records_c54 = new String[399][cols3.length];
		for(int i=0;i<399;i++)
		{
			records_c54[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c54[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c54", records_c54[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record394: [a394, b0, c1, d2, e4, f4, g2, h2, i7, j4, k9, l10]
		//record395: [a395, b1, c2, d3, e0, f5, g3, h3, i8, j5, k10, l11]
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0, l0]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1, l1]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2, l2]
		String[] ConditionColumns3 = {"h","k","l","f","j"};
		String[] ConditionColumnsValues3 = {"h1","k8","l5","f5","j5"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("c54", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 1, records:
		//record0: [a305, b1, c2, d1, e0, f5, g4, h1, i8, j5, k8, l5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 1);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a305","b1","c2","d1","e0","f5","g4","h1","i8","j5","k8","l5"})));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("du", cols4);
		String [][] records_du = new String[269][cols4.length];
		for(int i=0;i<269;i++)
		{
			records_du[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_du[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("du", records_du[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//last 5 records:
		//record264: [a264, b0, c0, d0, e4, f0, g5, h0, i3, j4, k0, l0, m4, n12, o9]
		//record265: [a265, b1, c1, d1, e0, f1, g6, h1, i4, j5, k1, l1, m5, n13, o10]
		//record266: [a266, b0, c2, d2, e1, f2, g0, h2, i5, j6, k2, l2, m6, n0, o11]
		//record267: [a267, b1, c0, d3, e2, f3, g1, h3, i6, j7, k3, l3, m7, n1, o12]
		//record268: [a268, b0, c1, d0, e3, f4, g2, h4, i7, j8, k4, l4, m8, n2, o13]
		String[] ConditionColumns4 = {"e"};
		String[] ConditionColumnsValues4 = {"e3"};
		ArrayList<String[]> tableSelect4 = DBApp.selectIndex("du", ConditionColumns4, ConditionColumnsValues4);
		//select output size: 54, records:
		//record0: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record1: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record2: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record3: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record4: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect4.size(), 54);
		ArrayList<String> tableSelectStrings4 = new ArrayList<String>();
		for(int i = 0; i < tableSelect4.size(); i++)
			tableSelectStrings4.add(Arrays.toString(tableSelect4.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3","h3","i3","j3","k3","l3","m3","n3","o3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a8","b0","c2","d0","e3","f2","g1","h0","i8","j8","k8","l8","m8","n8","o8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a13","b1","c1","d1","e3","f1","g6","h5","i4","j3","k2","l1","m0","n13","o13"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4","h2","i0","j8","k7","l6","m5","n4","o3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings4.contains(Arrays.toString(new String[]{"a23","b1","c2","d3","e3","f5","g2","h7","i5","j3","k1","l11","m10","n9","o8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("je9", cols0);
		String [][] records_je9 = new String[410][cols0.length];
		for(int i=0;i<410;i++)
		{
			records_je9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_je9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("je9", records_je9[i]);
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
		String[] ConditionColumns0 = {"b"};
		String[] ConditionColumnsValues0 = {"b1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("je9", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 205, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a3, b1, c0, d3, e3, f3, g3]
		//record2: [a5, b1, c2, d1, e0, f5, g5]
		//record3: [a7, b1, c1, d3, e2, f1, g0]
		//record4: [a9, b1, c0, d1, e4, f3, g2]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 205);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a1","b1","c1","d1","e1","f1","g1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a3","b1","c0","d3","e3","f3","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a7","b1","c1","d3","e2","f1","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e63", cols1);
		String [][] records_e63 = new String[401][cols1.length];
		for(int i=0;i<401;i++)
		{
			records_e63[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e63[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e63", records_e63[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record396: [a396, b0, c0, d0, e1, f0, g4, h4, i0, j6, k0]
		//record397: [a397, b1, c1, d1, e2, f1, g5, h5, i1, j7, k1]
		//record398: [a398, b0, c2, d2, e3, f2, g6, h6, i2, j8, k2]
		//record399: [a399, b1, c0, d3, e4, f3, g0, h7, i3, j9, k3]
		//record400: [a400, b0, c1, d0, e0, f4, g1, h0, i4, j0, k4]
		String[] ConditionColumns1 = {"k","f","i","b"};
		String[] ConditionColumnsValues1 = {"k6","f1","i7","b1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("e63", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 2, records:
		//record0: [a61, b1, c1, d1, e1, f1, g5, h5, i7, j1, k6]
		//record1: [a259, b1, c1, d3, e4, f1, g0, h3, i7, j9, k6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 2);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a61","b1","c1","d1","e1","f1","g5","h5","i7","j1","k6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a259","b1","c1","d3","e4","f1","g0","h3","i7","j9","k6"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("w7620", cols2);
		String [][] records_w7620 = new String[235][cols2.length];
		for(int i=0;i<235;i++)
		{
			records_w7620[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w7620[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w7620", records_w7620[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record230: [a230, b0, c2, d2, e0, f2, g6, h6, i5, j0, k10, l2, m9, n6, o5, p6]
		//record231: [a231, b1, c0, d3, e1, f3, g0, h7, i6, j1, k0, l3, m10, n7, o6, p7]
		//record232: [a232, b0, c1, d0, e2, f4, g1, h0, i7, j2, k1, l4, m11, n8, o7, p8]
		//record233: [a233, b1, c2, d1, e3, f5, g2, h1, i8, j3, k2, l5, m12, n9, o8, p9]
		//record234: [a234, b0, c0, d2, e4, f0, g3, h2, i0, j4, k3, l6, m0, n10, o9, p10]
		String[] ConditionColumns2 = {"m"};
		String[] ConditionColumnsValues2 = {"m12"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("w7620", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 18, records:
		//record0: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12]
		//record1: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9]
		//record2: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6]
		//record3: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12, n9, o6, p3]
		//record4: [a64, b0, c1, d0, e4, f4, g1, h0, i1, j4, k9, l4, m12, n8, o4, p0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 18);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1","l0","m12","n12","o12","p12"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a25","b1","c1","d1","e0","f1","g4","h1","i7","j5","k3","l1","m12","n11","o10","p9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a38","b0","c2","d2","e3","f2","g3","h6","i2","j8","k5","l2","m12","n10","o8","p6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a51","b1","c0","d3","e1","f3","g2","h3","i6","j1","k7","l3","m12","n9","o6","p3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a64","b0","c1","d0","e4","f4","g1","h0","i1","j4","k9","l4","m12","n8","o4","p0"})));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d79", cols3);
		String [][] records_d79 = new String[155][cols3.length];
		for(int i=0;i<155;i++)
		{
			records_d79[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_d79[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("d79", records_d79[i]);
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
		String[] ConditionColumns3 = {"d"};
		String[] ConditionColumnsValues3 = {"d2"};
		ArrayList<String[]> tableSelect3 = DBApp.selectIndex("d79", ConditionColumns3, ConditionColumnsValues3);
		//select output size: 39, records:
		//record0: [a2, b0, c2, d2, e2, f2, g2]
		//record1: [a6, b0, c0, d2, e1, f0, g6]
		//record2: [a10, b0, c1, d2, e0, f4, g3]
		//record3: [a14, b0, c2, d2, e4, f2, g0]
		//record4: [a18, b0, c0, d2, e3, f0, g4]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect3.size(), 39);
		ArrayList<String> tableSelectStrings3 = new ArrayList<String>();
		for(int i = 0; i < tableSelect3.size(); i++)
			tableSelectStrings3.add(Arrays.toString(tableSelect3.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a2","b0","c2","d2","e2","f2","g2"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a6","b0","c0","d2","e1","f0","g6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings3.contains(Arrays.toString(new String[]{"a18","b0","c0","d2","e3","f0","g4"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s37a5", cols0);
		String [][] records_s37a5 = new String[161][cols0.length];
		for(int i=0;i<161;i++)
		{
			records_s37a5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s37a5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s37a5", records_s37a5[i]);
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
		String[] ConditionColumns0 = {"e","g","b"};
		String[] ConditionColumnsValues0 = {"e2","g5","b1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("s37a5", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 2, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5]
		//record1: [a117, b1, c0, d1, e2, f3, g5]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 2);
		ArrayList<String> tableSelectStrings0 = new ArrayList<String>();
		for(int i = 0; i < tableSelect0.size(); i++)
			tableSelectStrings0.add(Arrays.toString(tableSelect0.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings0.contains(Arrays.toString(new String[]{"a117","b1","c0","d1","e2","f3","g5"})));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vs", cols1);
		String [][] records_vs = new String[255][cols1.length];
		for(int i=0;i<255;i++)
		{
			records_vs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vs", records_vs[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record250: [a250, b0, c1, d2, e0, f4, g5, h2, i7, j0, k8]
		//record251: [a251, b1, c2, d3, e1, f5, g6, h3, i8, j1, k9]
		//record252: [a252, b0, c0, d0, e2, f0, g0, h4, i0, j2, k10]
		//record253: [a253, b1, c1, d1, e3, f1, g1, h5, i1, j3, k0]
		//record254: [a254, b0, c2, d2, e4, f2, g2, h6, i2, j4, k1]
		String[] ConditionColumns1 = {"g"};
		String[] ConditionColumnsValues1 = {"g5"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("vs", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 36, records:
		//record0: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//record1: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record2: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record3: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4]
		//record4: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 36);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a5","b1","c2","d1","e0","f5","g5","h5","i5","j5","k5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a12","b0","c0","d0","e2","f0","g5","h4","i3","j2","k1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a26","b0","c2","d2","e1","f2","g5","h2","i8","j6","k4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a33","b1","c0","d1","e3","f3","g5","h1","i6","j3","k0"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jlic", cols2);
		String [][] records_jlic = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			records_jlic[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jlic[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jlic", records_jlic[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record51: [a51, b1, c0, d3, e1, f3, g2, h3, i6, j1, k7, l3, m12]
		//record52: [a52, b0, c1, d0, e2, f4, g3, h4, i7, j2, k8, l4, m0]
		//record53: [a53, b1, c2, d1, e3, f5, g4, h5, i8, j3, k9, l5, m1]
		//record54: [a54, b0, c0, d2, e4, f0, g5, h6, i0, j4, k10, l6, m2]
		//record55: [a55, b1, c1, d3, e0, f1, g6, h7, i1, j5, k0, l7, m3]
		String[] ConditionColumns2 = {"m","i","a"};
		String[] ConditionColumnsValues2 = {"m8","i2","a47"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("jlic", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 1, records:
		//record0: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 1);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a47","b1","c2","d3","e2","f5","g5","h7","i2","j7","k3","l11","m8"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableSelectionIndexConditionNonIndexed_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b7", cols0);
		String [][] records_b7 = new String[497][cols0.length];
		for(int i=0;i<497;i++)
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
		//record492: [a492, b0, c0, d0, e2, f0, g2]
		//record493: [a493, b1, c1, d1, e3, f1, g3]
		//record494: [a494, b0, c2, d2, e4, f2, g4]
		//record495: [a495, b1, c0, d3, e0, f3, g5]
		//record496: [a496, b0, c1, d0, e1, f4, g6]
		String[] ConditionColumns0 = {"c"};
		String[] ConditionColumnsValues0 = {"c1"};
		ArrayList<String[]> tableSelect0 = DBApp.selectIndex("b7", ConditionColumns0, ConditionColumnsValues0);
		//select output size: 166, records:
		//record0: [a1, b1, c1, d1, e1, f1, g1]
		//record1: [a4, b0, c1, d0, e4, f4, g4]
		//record2: [a7, b1, c1, d3, e2, f1, g0]
		//record3: [a10, b0, c1, d2, e0, f4, g3]
		//record4: [a13, b1, c1, d1, e3, f1, g6]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect0.size(), 166);
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
		DBApp.createTable("b0", cols1);
		String [][] records_b0 = new String[410][cols1.length];
		for(int i=0;i<410;i++)
		{
			records_b0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b0", records_b0[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//last 5 records:
		//record405: [a405, b1, c0, d1, e0, f3, g6, h5, i0, j5, k9]
		//record406: [a406, b0, c1, d2, e1, f4, g0, h6, i1, j6, k10]
		//record407: [a407, b1, c2, d3, e2, f5, g1, h7, i2, j7, k0]
		//record408: [a408, b0, c0, d0, e3, f0, g2, h0, i3, j8, k1]
		//record409: [a409, b1, c1, d1, e4, f1, g3, h1, i4, j9, k2]
		String[] ConditionColumns1 = {"h","i"};
		String[] ConditionColumnsValues1 = {"h2","i1"};
		ArrayList<String[]> tableSelect1 = DBApp.selectIndex("b0", ConditionColumns1, ConditionColumnsValues1);
		//select output size: 6, records:
		//record0: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record1: [a82, b0, c1, d2, e2, f4, g5, h2, i1, j2, k5]
		//record2: [a154, b0, c1, d2, e4, f4, g0, h2, i1, j4, k0]
		//record3: [a226, b0, c1, d2, e1, f4, g2, h2, i1, j6, k6]
		//record4: [a298, b0, c1, d2, e3, f4, g4, h2, i1, j8, k1]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect1.size(), 6);
		ArrayList<String> tableSelectStrings1 = new ArrayList<String>();
		for(int i = 0; i < tableSelect1.size(); i++)
			tableSelectStrings1.add(Arrays.toString(tableSelect1.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a10","b0","c1","d2","e0","f4","g3","h2","i1","j0","k10"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a82","b0","c1","d2","e2","f4","g5","h2","i1","j2","k5"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a154","b0","c1","d2","e4","f4","g0","h2","i1","j4","k0"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a226","b0","c1","d2","e1","f4","g2","h2","i1","j6","k6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings1.contains(Arrays.toString(new String[]{"a298","b0","c1","d2","e3","f4","g4","h2","i1","j8","k1"})));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("w5c", cols2);
		String [][] records_w5c = new String[343][cols2.length];
		for(int i=0;i<343;i++)
		{
			records_w5c[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_w5c[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("w5c", records_w5c[i]);
		}
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//last 5 records:
		//record338: [a338, b0, c2, d2, e3, f2, g2, h2, i5, j8, k8, l2, m0]
		//record339: [a339, b1, c0, d3, e4, f3, g3, h3, i6, j9, k9, l3, m1]
		//record340: [a340, b0, c1, d0, e0, f4, g4, h4, i7, j0, k10, l4, m2]
		//record341: [a341, b1, c2, d1, e1, f5, g5, h5, i8, j1, k0, l5, m3]
		//record342: [a342, b0, c0, d2, e2, f0, g6, h6, i0, j2, k1, l6, m4]
		String[] ConditionColumns2 = {"e"};
		String[] ConditionColumnsValues2 = {"e4"};
		ArrayList<String[]> tableSelect2 = DBApp.selectIndex("w5c", ConditionColumns2, ConditionColumnsValues2);
		//select output size: 68, records:
		//record0: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record1: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record2: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1]
		//record3: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record4: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11]
		assertEquals("Conditional Index selection when no columns are indexed should return the correct number of records.", tableSelect2.size(), 68);
		ArrayList<String> tableSelectStrings2 = new ArrayList<String>();
		for(int i = 0; i < tableSelect2.size(); i++)
			tableSelectStrings2.add(Arrays.toString(tableSelect2.get(i)));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a4","b0","c1","d0","e4","f4","g4","h4","i4","j4","k4","l4","m4"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a9","b1","c0","d1","e4","f3","g2","h1","i0","j9","k9","l9","m9"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a14","b0","c2","d2","e4","f2","g0","h6","i5","j4","k3","l2","m1"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a19","b1","c1","d3","e4","f1","g5","h3","i1","j9","k8","l7","m6"})));
		assertTrue("Conditional Index selection when no columns are indexed should return the correct inserted record.", tableSelectStrings2.contains(Arrays.toString(new String[]{"a24","b0","c0","d0","e4","f0","g3","h0","i6","j4","k2","l0","m11"})));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1849);
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