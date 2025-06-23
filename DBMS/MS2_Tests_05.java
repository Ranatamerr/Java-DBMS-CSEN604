package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_05
{

	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lqh21", cols0);
		//To be indexed: [e, g], to be queried: [e2, g7].
		String [][] records_lqh21 = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			records_lqh21[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lqh21[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lqh21", records_lqh21[i]);
		}
		DBApp.createBitMapIndex("lqh21","e");
		DBApp.createBitMapIndex("lqh21","g");
		//first 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		//last 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//record8: [a8, b0, c2, d0, e3, f2, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lqh21", "e", "e2"), ("001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lqh21", "g", "g7"), ("000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("y82", cols1);
		//To be indexed: [b, e, g, k, m, n, o], to be queried: [b0, e0, g4, k3, m7, n11, o15].
		String [][] records_y82 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_y82[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y82[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y82", records_y82[i]);
		}
		DBApp.createBitMapIndex("y82","b");
		DBApp.createBitMapIndex("y82","e");
		DBApp.createBitMapIndex("y82","g");
		DBApp.createBitMapIndex("y82","k");
		DBApp.createBitMapIndex("y82","m");
		DBApp.createBitMapIndex("y82","n");
		DBApp.createBitMapIndex("y82","o");
		//first 11 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//last 11 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "b", "b0"), ("10101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "e", "e0"), ("10000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "g", "g4"), ("00001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "k", "k3"), ("00010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "m", "m7"), ("00000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y82", "n", "n11"), ("00000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y82", "o", "o15"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("j83s0", cols2);
		//To be indexed: [c, j, m, n, o, q], to be queried: [c1, j1, m2, n7, o2, q17].
		String [][] records_j83s0 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			records_j83s0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j83s0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j83s0", records_j83s0[i]);
		}
		DBApp.createBitMapIndex("j83s0","c");
		DBApp.createBitMapIndex("j83s0","j");
		DBApp.createBitMapIndex("j83s0","m");
		DBApp.createBitMapIndex("j83s0","n");
		DBApp.createBitMapIndex("j83s0","o");
		DBApp.createBitMapIndex("j83s0","q");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j83s0", "c", "c1"), ("010010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j83s0", "j", "j1"), ("010000000001000000000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j83s0", "m", "m2"), ("001000000000000100000000000010000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j83s0", "n", "n7"), ("000000010000000000000100000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j83s0", "o", "o2"), ("001000000000000001000000000000001000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j83s0", "q", "q17"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o8q", cols0);
		//To be indexed: [c, e, g], to be queried: [c1, e0, g7].
		String [][] records_o8q = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			records_o8q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o8q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o8q", records_o8q[i]);
		}
		DBApp.createBitMapIndex("o8q","c");
		DBApp.createBitMapIndex("o8q","e");
		DBApp.createBitMapIndex("o8q","g");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8q", "c", "c1"), ("01001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8q", "e", "e0"), ("10000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o8q", "g", "g7"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("v07", cols1);
		//To be indexed: [b, c, g, j, o, p, q], to be queried: [b0, c1, g1, j7, o6, p7, q17].
		String [][] records_v07 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_v07[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v07[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v07", records_v07[i]);
		}
		DBApp.createBitMapIndex("v07","b");
		DBApp.createBitMapIndex("v07","c");
		DBApp.createBitMapIndex("v07","g");
		DBApp.createBitMapIndex("v07","j");
		DBApp.createBitMapIndex("v07","o");
		DBApp.createBitMapIndex("v07","p");
		DBApp.createBitMapIndex("v07","q");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "c", "c1"), ("0100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "g", "g1"), ("0100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "j", "j7"), ("0000000100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "o", "o6"), ("0000001000000000000001000000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v07", "p", "p7"), ("0000000100000000000000010000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v07", "q", "q17"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y6", cols0);
		//To be indexed: [b, c, d, g], to be queried: [b0, c1, d1, g7].
		String [][] records_y6 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_y6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y6", records_y6[i]);
		}
		DBApp.createBitMapIndex("y6","b");
		DBApp.createBitMapIndex("y6","c");
		DBApp.createBitMapIndex("y6","d");
		DBApp.createBitMapIndex("y6","g");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y6", "b", "b0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y6", "c", "c1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y6", "d", "d1"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y6", "g", "g7"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i0", cols1);
		//To be indexed: [c, d, e, g], to be queried: [c0, d2, e3, g7].
		String [][] records_i0 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			records_i0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i0", records_i0[i]);
		}
		DBApp.createBitMapIndex("i0","c");
		DBApp.createBitMapIndex("i0","d");
		DBApp.createBitMapIndex("i0","e");
		DBApp.createBitMapIndex("i0","g");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i0", "c", "c0"), ("1001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i0", "d", "d2"), ("0010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i0", "e", "e3"), ("0001000010000100001000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i0", "g", "g7"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n0u1y", cols2);
		//To be indexed: [b, c, g], to be queried: [b0, c0, g7].
		String [][] records_n0u1y = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			records_n0u1y[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n0u1y[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n0u1y", records_n0u1y[i]);
		}
		DBApp.createBitMapIndex("n0u1y","b");
		DBApp.createBitMapIndex("n0u1y","c");
		DBApp.createBitMapIndex("n0u1y","g");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n0u1y", "b", "b0"), ("1010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n0u1y", "c", "c0"), ("1001001001001001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n0u1y", "g", "g7"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s1", cols3);
		//To be indexed: [c, e, f, g], to be queried: [c1, e1, f2, g7].
		String [][] records_s1 = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			records_s1[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s1[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s1", records_s1[i]);
		}
		DBApp.createBitMapIndex("s1","c");
		DBApp.createBitMapIndex("s1","e");
		DBApp.createBitMapIndex("s1","f");
		DBApp.createBitMapIndex("s1","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s1", "c", "c1"), ("0100100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s1", "e", "e1"), ("0100001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s1", "f", "f2"), ("0010000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s1", "g", "g7"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jykg8", cols0);
		//To be indexed: [e, h, i, j, k, l], to be queried: [e0, h2, i2, j6, k2, l12].
		String [][] records_jykg8 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			records_jykg8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jykg8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jykg8", records_jykg8[i]);
		}
		DBApp.createBitMapIndex("jykg8","e");
		DBApp.createBitMapIndex("jykg8","h");
		DBApp.createBitMapIndex("jykg8","i");
		DBApp.createBitMapIndex("jykg8","j");
		DBApp.createBitMapIndex("jykg8","k");
		DBApp.createBitMapIndex("jykg8","l");
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jykg8", "e", "e0"), ("100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jykg8", "h", "h2"), ("001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jykg8", "i", "i2"), ("001000000001000000001000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jykg8", "j", "j6"), ("000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jykg8", "k", "k2"), ("001000000000010000000000100000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jykg8", "l", "l12"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i8804", cols1);
		//To be indexed: [b, c, h], to be queried: [b0, c0, h8].
		String [][] records_i8804 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_i8804[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i8804[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i8804", records_i8804[i]);
		}
		DBApp.createBitMapIndex("i8804","b");
		DBApp.createBitMapIndex("i8804","c");
		DBApp.createBitMapIndex("i8804","h");
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
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8804", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8804", "c", "c0"), ("1001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i8804", "h", "h8"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gm6", cols0);
		//To be indexed: [d, f, g, l], to be queried: [d1, f2, g1, l12].
		String [][] records_gm6 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			records_gm6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gm6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gm6", records_gm6[i]);
		}
		DBApp.createBitMapIndex("gm6","d");
		DBApp.createBitMapIndex("gm6","f");
		DBApp.createBitMapIndex("gm6","g");
		DBApp.createBitMapIndex("gm6","l");
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
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gm6", "d", "d1"), ("0100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gm6", "f", "f2"), ("0010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gm6", "g", "g1"), ("0100000010000001000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gm6", "l", "l12"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lu", cols1);
		//To be indexed: [b, c, e, f, g], to be queried: [b0, c0, e2, f4, g7].
		String [][] records_lu = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			records_lu[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lu[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lu", records_lu[i]);
		}
		DBApp.createBitMapIndex("lu","b");
		DBApp.createBitMapIndex("lu","c");
		DBApp.createBitMapIndex("lu","e");
		DBApp.createBitMapIndex("lu","f");
		DBApp.createBitMapIndex("lu","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lu", "b", "b0"), ("101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lu", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lu", "e", "e2"), ("001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lu", "f", "f4"), ("000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lu", "g", "g7"), ("000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("fx7", cols2);
		//To be indexed: [b, g, i, k, l, n], to be queried: [b0, g4, i3, k5, l3, n14].
		String [][] records_fx7 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			records_fx7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fx7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fx7", records_fx7[i]);
		}
		DBApp.createBitMapIndex("fx7","b");
		DBApp.createBitMapIndex("fx7","g");
		DBApp.createBitMapIndex("fx7","i");
		DBApp.createBitMapIndex("fx7","k");
		DBApp.createBitMapIndex("fx7","l");
		DBApp.createBitMapIndex("fx7","n");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fx7", "b", "b0"), ("10101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fx7", "g", "g4"), ("00001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fx7", "i", "i3"), ("00010000000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fx7", "k", "k5"), ("00000100000000001000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fx7", "l", "l3"), ("00010000000000010000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fx7", "n", "n14"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p234l", cols3);
		//To be indexed: [d, e, f, g], to be queried: [d0, e0, f4, g7].
		String [][] records_p234l = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			records_p234l[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p234l[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p234l", records_p234l[i]);
		}
		DBApp.createBitMapIndex("p234l","d");
		DBApp.createBitMapIndex("p234l","e");
		DBApp.createBitMapIndex("p234l","f");
		DBApp.createBitMapIndex("p234l","g");
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//record7: [a7, b1, c1, d3, e2, f1, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p234l", "d", "d0"), ("10001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p234l", "e", "e0"), ("10000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p234l", "f", "f4"), ("00001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p234l", "g", "g7"), ("00000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w62", cols0);
		//To be indexed: [b, c, d, e, g], to be queried: [b0, c0, d2, e3, g7].
		String [][] records_w62 = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			records_w62[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w62[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w62", records_w62[i]);
		}
		DBApp.createBitMapIndex("w62","b");
		DBApp.createBitMapIndex("w62","c");
		DBApp.createBitMapIndex("w62","d");
		DBApp.createBitMapIndex("w62","e");
		DBApp.createBitMapIndex("w62","g");
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
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w62", "b", "b0"), ("101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w62", "c", "c0"), ("100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w62", "d", "d2"), ("001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w62", "e", "e3"), ("000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w62", "g", "g7"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t84", cols1);
		//To be indexed: [b, f, g, h, l, m, n, o], to be queried: [b0, f1, g4, h6, l6, m4, n6, o15].
		String [][] records_t84 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			records_t84[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t84[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t84", records_t84[i]);
		}
		DBApp.createBitMapIndex("t84","b");
		DBApp.createBitMapIndex("t84","f");
		DBApp.createBitMapIndex("t84","g");
		DBApp.createBitMapIndex("t84","h");
		DBApp.createBitMapIndex("t84","l");
		DBApp.createBitMapIndex("t84","m");
		DBApp.createBitMapIndex("t84","n");
		DBApp.createBitMapIndex("t84","o");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "f", "f1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "g", "g4"), ("000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "h", "h6"), ("000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "l", "l6"), ("000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "m", "m4"), ("000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t84", "n", "n6"), ("000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t84", "o", "o15"), ("000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xq2", cols2);
		//To be indexed: [b, e, f, h, j, k, l, n, o], to be queried: [b0, e2, f0, h5, j6, k8, l3, n11, o15].
		String [][] records_xq2 = new String[41][cols2.length];
		for(int i=0;i<41;i++)
		{
			records_xq2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xq2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xq2", records_xq2[i]);
		}
		DBApp.createBitMapIndex("xq2","b");
		DBApp.createBitMapIndex("xq2","e");
		DBApp.createBitMapIndex("xq2","f");
		DBApp.createBitMapIndex("xq2","h");
		DBApp.createBitMapIndex("xq2","j");
		DBApp.createBitMapIndex("xq2","k");
		DBApp.createBitMapIndex("xq2","l");
		DBApp.createBitMapIndex("xq2","n");
		DBApp.createBitMapIndex("xq2","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "b", "b0"), ("10101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "e", "e2"), ("00100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "f", "f0"), ("10000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "h", "h5"), ("00000100000001000000010000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "j", "j6"), ("00000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "k", "k8"), ("00000000100000000001000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "l", "l3"), ("00010000000000010000000000010000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq2", "n", "n11"), ("00000000000100000000000001000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xq2", "o", "o15"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("mfr", cols3);
		//To be indexed: [b, c, d, h, j], to be queried: [b0, c0, d0, h3, j10].
		String [][] records_mfr = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			records_mfr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_mfr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("mfr", records_mfr[i]);
		}
		DBApp.createBitMapIndex("mfr","b");
		DBApp.createBitMapIndex("mfr","c");
		DBApp.createBitMapIndex("mfr","d");
		DBApp.createBitMapIndex("mfr","h");
		DBApp.createBitMapIndex("mfr","j");
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
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mfr", "b", "b0"), ("10101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mfr", "c", "c0"), ("10010010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mfr", "d", "d0"), ("10001000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mfr", "h", "h3"), ("00010000000100000001000000010000000100000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mfr", "j", "j10"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m383m", cols0);
		//To be indexed: [b, c, g], to be queried: [b0, c1, g7].
		String [][] records_m383m = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			records_m383m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m383m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m383m", records_m383m[i]);
		}
		DBApp.createBitMapIndex("m383m","b");
		DBApp.createBitMapIndex("m383m","c");
		DBApp.createBitMapIndex("m383m","g");
		//first 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//last 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m383m", "b", "b0"), ("1010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m383m", "c", "c1"), ("0100100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m383m", "g", "g7"), ("0000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a6myw", cols1);
		//To be indexed: [b, c, h, i, j, k], to be queried: [b0, c1, h4, i6, j5, k11].
		String [][] records_a6myw = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_a6myw[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_a6myw[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("a6myw", records_a6myw[i]);
		}
		DBApp.createBitMapIndex("a6myw","b");
		DBApp.createBitMapIndex("a6myw","c");
		DBApp.createBitMapIndex("a6myw","h");
		DBApp.createBitMapIndex("a6myw","i");
		DBApp.createBitMapIndex("a6myw","j");
		DBApp.createBitMapIndex("a6myw","k");
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
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a6myw", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a6myw", "c", "c1"), ("01001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a6myw", "h", "h4"), ("00001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a6myw", "i", "i6"), ("00000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a6myw", "j", "j5"), ("00000100000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a6myw", "k", "k11"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("nm", cols2);
		//To be indexed: [b, c, g, j, k, l, n, o, p, q, r], to be queried: [b0, c0, g3, j7, k5, l9, n1, o13, p12, q6, r18].
		String [][] records_nm = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			records_nm[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nm[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nm", records_nm[i]);
		}
		DBApp.createBitMapIndex("nm","b");
		DBApp.createBitMapIndex("nm","c");
		DBApp.createBitMapIndex("nm","g");
		DBApp.createBitMapIndex("nm","j");
		DBApp.createBitMapIndex("nm","k");
		DBApp.createBitMapIndex("nm","l");
		DBApp.createBitMapIndex("nm","n");
		DBApp.createBitMapIndex("nm","o");
		DBApp.createBitMapIndex("nm","p");
		DBApp.createBitMapIndex("nm","q");
		DBApp.createBitMapIndex("nm","r");
		//first 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//last 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "b", "b0"), ("101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "c", "c0"), ("100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "g", "g3"), ("000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "j", "j7"), ("000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "k", "k5"), ("000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "l", "l9"), ("000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "n", "n1"), ("010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "o", "o13"), ("000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "p", "p12"), ("000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm", "q", "q6"), ("000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nm", "r", "r18"), ("000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mpv8", cols3);
		//To be indexed: [b, c, d, f, g, h, k, m, n, o], to be queried: [b0, c0, d0, f2, g5, h5, k9, m2, n0, o15].
		String [][] records_mpv8 = new String[39][cols3.length];
		for(int i=0;i<39;i++)
		{
			records_mpv8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_mpv8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("mpv8", records_mpv8[i]);
		}
		DBApp.createBitMapIndex("mpv8","b");
		DBApp.createBitMapIndex("mpv8","c");
		DBApp.createBitMapIndex("mpv8","d");
		DBApp.createBitMapIndex("mpv8","f");
		DBApp.createBitMapIndex("mpv8","g");
		DBApp.createBitMapIndex("mpv8","h");
		DBApp.createBitMapIndex("mpv8","k");
		DBApp.createBitMapIndex("mpv8","m");
		DBApp.createBitMapIndex("mpv8","n");
		DBApp.createBitMapIndex("mpv8","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "b", "b0"), ("101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "c", "c0"), ("100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "d", "d0"), ("100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "f", "f2"), ("001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "g", "g5"), ("000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "h", "h5"), ("000001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "k", "k9"), ("000000000100000000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "m", "m2"), ("001000000000000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mpv8", "n", "n0"), ("100000000000001000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mpv8", "o", "o15"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("tr5", cols0);
		//To be indexed: [b, c, d, e, f, i, k, l, m, n, r, s], to be queried: [b0, c0, d1, e0, f0, i5, k5, l4, m4, n0, r12, s19].
		String [][] records_tr5 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_tr5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tr5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tr5", records_tr5[i]);
		}
		DBApp.createBitMapIndex("tr5","b");
		DBApp.createBitMapIndex("tr5","c");
		DBApp.createBitMapIndex("tr5","d");
		DBApp.createBitMapIndex("tr5","e");
		DBApp.createBitMapIndex("tr5","f");
		DBApp.createBitMapIndex("tr5","i");
		DBApp.createBitMapIndex("tr5","k");
		DBApp.createBitMapIndex("tr5","l");
		DBApp.createBitMapIndex("tr5","m");
		DBApp.createBitMapIndex("tr5","n");
		DBApp.createBitMapIndex("tr5","r");
		DBApp.createBitMapIndex("tr5","s");
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
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8, s7]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "b", "b0"), ("1010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "c", "c0"), ("1001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "d", "d1"), ("0100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "e", "e0"), ("1000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "f", "f0"), ("1000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "i", "i5"), ("0000010000000010000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "k", "k5"), ("0000010000000000100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "l", "l4"), ("0000100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "m", "m4"), ("0000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "n", "n0"), ("1000000000000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tr5", "r", "r12"), ("0000000000001000000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tr5", "s", "s19"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("v2", cols1);
		//To be indexed: [b, d, e, h, i, l, o], to be queried: [b0, d1, e3, h2, i1, l7, o15].
		String [][] records_v2 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_v2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v2", records_v2[i]);
		}
		DBApp.createBitMapIndex("v2","b");
		DBApp.createBitMapIndex("v2","d");
		DBApp.createBitMapIndex("v2","e");
		DBApp.createBitMapIndex("v2","h");
		DBApp.createBitMapIndex("v2","i");
		DBApp.createBitMapIndex("v2","l");
		DBApp.createBitMapIndex("v2","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "b", "b0"), ("101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "d", "d1"), ("010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "e", "e3"), ("000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "h", "h2"), ("001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "i", "i1"), ("010000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v2", "l", "l7"), ("000000010000000000010000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v2", "o", "o15"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("cedyt", cols2);
		//To be indexed: [b, d, e, f, g], to be queried: [b0, d0, e3, f2, g7].
		String [][] records_cedyt = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_cedyt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cedyt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cedyt", records_cedyt[i]);
		}
		DBApp.createBitMapIndex("cedyt","b");
		DBApp.createBitMapIndex("cedyt","d");
		DBApp.createBitMapIndex("cedyt","e");
		DBApp.createBitMapIndex("cedyt","f");
		DBApp.createBitMapIndex("cedyt","g");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cedyt", "b", "b0"), ("1010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cedyt", "d", "d0"), ("1000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cedyt", "e", "e3"), ("0001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cedyt", "f", "f2"), ("0010000010000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cedyt", "g", "g7"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pt8", cols3);
		//To be indexed: [b, d, g], to be queried: [b0, d0, g7].
		String [][] records_pt8 = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			records_pt8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_pt8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("pt8", records_pt8[i]);
		}
		DBApp.createBitMapIndex("pt8","b");
		DBApp.createBitMapIndex("pt8","d");
		DBApp.createBitMapIndex("pt8","g");
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
		//record49: [a49, b1, c1, d1, e4, f1, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pt8", "b", "b0"), ("10101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pt8", "d", "d0"), ("10001000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("pt8", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a18", cols4);
		//To be indexed: [c, d, e, h, i], to be queried: [c0, d2, e0, h0, i9].
		String [][] records_a18 = new String[30][cols4.length];
		for(int i=0;i<30;i++)
		{
			records_a18[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_a18[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("a18", records_a18[i]);
		}
		DBApp.createBitMapIndex("a18","c");
		DBApp.createBitMapIndex("a18","d");
		DBApp.createBitMapIndex("a18","e");
		DBApp.createBitMapIndex("a18","h");
		DBApp.createBitMapIndex("a18","i");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a18", "c", "c0"), ("100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a18", "d", "d2"), ("001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a18", "e", "e0"), ("100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a18", "h", "h0"), ("100000001000000010000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a18", "i", "i9"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("dck1", cols0);
		//To be indexed: [d, g, i], to be queried: [d1, g1, i9].
		String [][] records_dck1 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_dck1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dck1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dck1", records_dck1[i]);
		}
		DBApp.createBitMapIndex("dck1","d");
		DBApp.createBitMapIndex("dck1","g");
		DBApp.createBitMapIndex("dck1","i");
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
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dck1", "d", "d1"), ("010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dck1", "g", "g1"), ("010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dck1", "i", "i9"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r2u", cols1);
		//To be indexed: [b, c, d, e, g], to be queried: [b0, c0, d1, e0, g7].
		String [][] records_r2u = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			records_r2u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r2u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r2u", records_r2u[i]);
		}
		DBApp.createBitMapIndex("r2u","b");
		DBApp.createBitMapIndex("r2u","c");
		DBApp.createBitMapIndex("r2u","d");
		DBApp.createBitMapIndex("r2u","e");
		DBApp.createBitMapIndex("r2u","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r2u", "b", "b0"), ("1010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r2u", "c", "c0"), ("1001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r2u", "d", "d1"), ("0100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r2u", "e", "e0"), ("1000010000100001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r2u", "g", "g7"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("a954", cols2);
		//To be indexed: [b, e, f, g, h, j, l], to be queried: [b0, e3, f3, g4, h2, j6, l12].
		String [][] records_a954 = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			records_a954[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_a954[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("a954", records_a954[i]);
		}
		DBApp.createBitMapIndex("a954","b");
		DBApp.createBitMapIndex("a954","e");
		DBApp.createBitMapIndex("a954","f");
		DBApp.createBitMapIndex("a954","g");
		DBApp.createBitMapIndex("a954","h");
		DBApp.createBitMapIndex("a954","j");
		DBApp.createBitMapIndex("a954","l");
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "e", "e3"), ("00010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "f", "f3"), ("00010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "g", "g4"), ("00001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "h", "h2"), ("00100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a954", "j", "j6"), ("00000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a954", "l", "l12"), ("00000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xq7", cols3);
		//To be indexed: [b, c, d, e, f, g], to be queried: [b0, c1, d2, e3, f2, g7].
		String [][] records_xq7 = new String[29][cols3.length];
		for(int i=0;i<29;i++)
		{
			records_xq7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xq7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xq7", records_xq7[i]);
		}
		DBApp.createBitMapIndex("xq7","b");
		DBApp.createBitMapIndex("xq7","c");
		DBApp.createBitMapIndex("xq7","d");
		DBApp.createBitMapIndex("xq7","e");
		DBApp.createBitMapIndex("xq7","f");
		DBApp.createBitMapIndex("xq7","g");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq7", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq7", "c", "c1"), ("01001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq7", "d", "d2"), ("00100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq7", "e", "e3"), ("00010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xq7", "f", "f2"), ("00100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xq7", "g", "g7"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mch3", cols0);
		//To be indexed: [d, e, f, g], to be queried: [d0, e2, f1, g7].
		String [][] records_mch3 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_mch3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mch3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mch3", records_mch3[i]);
		}
		DBApp.createBitMapIndex("mch3","d");
		DBApp.createBitMapIndex("mch3","e");
		DBApp.createBitMapIndex("mch3","f");
		DBApp.createBitMapIndex("mch3","g");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mch3", "d", "d0"), ("1000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mch3", "e", "e2"), ("0010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mch3", "f", "f1"), ("0100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mch3", "g", "g7"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("vl63", cols0);
		//To be indexed: [c, g, h, i, j, k, l], to be queried: [c1, g4, h6, i6, j1, k9, l12].
		String [][] records_vl63 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_vl63[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vl63[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vl63", records_vl63[i]);
		}
		DBApp.createBitMapIndex("vl63","c");
		DBApp.createBitMapIndex("vl63","g");
		DBApp.createBitMapIndex("vl63","h");
		DBApp.createBitMapIndex("vl63","i");
		DBApp.createBitMapIndex("vl63","j");
		DBApp.createBitMapIndex("vl63","k");
		DBApp.createBitMapIndex("vl63","l");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "c", "c1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "g", "g4"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "h", "h6"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "i", "i6"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "j", "j1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vl63", "k", "k9"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vl63", "l", "l12"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("de", cols1);
		//To be indexed: [f, g], to be queried: [f4, g7].
		String [][] records_de = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_de[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_de[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("de", records_de[i]);
		}
		DBApp.createBitMapIndex("de","f");
		DBApp.createBitMapIndex("de","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("de", "f", "f4"), ("0000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("de", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p3x0", cols2);
		//To be indexed: [b, c, d, e, f, j, k, l, n], to be queried: [b0, c1, d2, e2, f4, j3, k5, l9, n14].
		String [][] records_p3x0 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			records_p3x0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p3x0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p3x0", records_p3x0[i]);
		}
		DBApp.createBitMapIndex("p3x0","b");
		DBApp.createBitMapIndex("p3x0","c");
		DBApp.createBitMapIndex("p3x0","d");
		DBApp.createBitMapIndex("p3x0","e");
		DBApp.createBitMapIndex("p3x0","f");
		DBApp.createBitMapIndex("p3x0","j");
		DBApp.createBitMapIndex("p3x0","k");
		DBApp.createBitMapIndex("p3x0","l");
		DBApp.createBitMapIndex("p3x0","n");
		//first 13 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//last 13 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "c", "c1"), ("0100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "d", "d2"), ("0010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "e", "e2"), ("0010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "f", "f4"), ("0000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "j", "j3"), ("0001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "k", "k5"), ("0000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p3x0", "l", "l9"), ("0000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p3x0", "n", "n14"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("zyu", cols3);
		//To be indexed: [b, c, f, g, h, i, m, o, q], to be queried: [b0, c1, f1, g5, h1, i0, m9, o0, q17].
		String [][] records_zyu = new String[26][cols3.length];
		for(int i=0;i<26;i++)
		{
			records_zyu[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zyu[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zyu", records_zyu[i]);
		}
		DBApp.createBitMapIndex("zyu","b");
		DBApp.createBitMapIndex("zyu","c");
		DBApp.createBitMapIndex("zyu","f");
		DBApp.createBitMapIndex("zyu","g");
		DBApp.createBitMapIndex("zyu","h");
		DBApp.createBitMapIndex("zyu","i");
		DBApp.createBitMapIndex("zyu","m");
		DBApp.createBitMapIndex("zyu","o");
		DBApp.createBitMapIndex("zyu","q");
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
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "c", "c1"), ("01001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "f", "f1"), ("01000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "g", "g5"), ("00000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "h", "h1"), ("01000000010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "i", "i0"), ("10000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "m", "m9"), ("00000000010000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zyu", "o", "o0"), ("10000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zyu", "q", "q17"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("esy", cols0);
		//To be indexed: [b, d, e, g, i, n], to be queried: [b0, d0, e2, g3, i7, n14].
		String [][] records_esy = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_esy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_esy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("esy", records_esy[i]);
		}
		DBApp.createBitMapIndex("esy","b");
		DBApp.createBitMapIndex("esy","d");
		DBApp.createBitMapIndex("esy","e");
		DBApp.createBitMapIndex("esy","g");
		DBApp.createBitMapIndex("esy","i");
		DBApp.createBitMapIndex("esy","n");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("esy", "b", "b0"), ("101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("esy", "d", "d0"), ("100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("esy", "e", "e2"), ("001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("esy", "g", "g3"), ("000100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("esy", "i", "i7"), ("000000010000000010000000010000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("esy", "n", "n14"), ("000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x5", cols0);
		//To be indexed: [b, d, f, h, k, l, m, o], to be queried: [b0, d2, f1, h4, k6, l3, m6, o15].
		String [][] records_x5 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			records_x5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x5", records_x5[i]);
		}
		DBApp.createBitMapIndex("x5","b");
		DBApp.createBitMapIndex("x5","d");
		DBApp.createBitMapIndex("x5","f");
		DBApp.createBitMapIndex("x5","h");
		DBApp.createBitMapIndex("x5","k");
		DBApp.createBitMapIndex("x5","l");
		DBApp.createBitMapIndex("x5","m");
		DBApp.createBitMapIndex("x5","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "b", "b0"), ("1010101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "d", "d2"), ("0010001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "f", "f1"), ("0100000100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "h", "h4"), ("0000100000001000000010000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "k", "k6"), ("0000001000000000010000000000100000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "l", "l3"), ("0001000000000001000000000001000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x5", "m", "m6"), ("0000001000000000000100000000000010000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("x5", "o", "o15"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qv9", cols0);
		//To be indexed: [b, c, d, f, i, j, n], to be queried: [b0, c0, d1, f2, i5, j8, n14].
		String [][] records_qv9 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_qv9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qv9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qv9", records_qv9[i]);
		}
		DBApp.createBitMapIndex("qv9","b");
		DBApp.createBitMapIndex("qv9","c");
		DBApp.createBitMapIndex("qv9","d");
		DBApp.createBitMapIndex("qv9","f");
		DBApp.createBitMapIndex("qv9","i");
		DBApp.createBitMapIndex("qv9","j");
		DBApp.createBitMapIndex("qv9","n");
		//first 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//last 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "c", "c0"), ("1001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "f", "f2"), ("0010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "i", "i5"), ("0000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qv9", "j", "j8"), ("0000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qv9", "n", "n14"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w08f7", cols1);
		//To be indexed: [b, c, d, f, g], to be queried: [b0, c1, d2, f2, g7].
		String [][] records_w08f7 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			records_w08f7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w08f7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w08f7", records_w08f7[i]);
		}
		DBApp.createBitMapIndex("w08f7","b");
		DBApp.createBitMapIndex("w08f7","c");
		DBApp.createBitMapIndex("w08f7","d");
		DBApp.createBitMapIndex("w08f7","f");
		DBApp.createBitMapIndex("w08f7","g");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w08f7", "b", "b0"), ("101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w08f7", "c", "c1"), ("010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w08f7", "d", "d2"), ("001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w08f7", "f", "f2"), ("001000001000001000001000001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w08f7", "g", "g7"), ("000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dtn", cols2);
		//To be indexed: [b, g, h], to be queried: [b0, g5, h8].
		String [][] records_dtn = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			records_dtn[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dtn[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dtn", records_dtn[i]);
		}
		DBApp.createBitMapIndex("dtn","b");
		DBApp.createBitMapIndex("dtn","g");
		DBApp.createBitMapIndex("dtn","h");
		//first 13 records:
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
		//last 13 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dtn", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dtn", "g", "g5"), ("0000010000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dtn", "h", "h8"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("y0l89", cols3);
		//To be indexed: [d, e, i, j, l, n, o, p, q, t], to be queried: [d0, e3, i2, j8, l9, n11, o1, p12, q10, t20].
		String [][] records_y0l89 = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			records_y0l89[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y0l89[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y0l89", records_y0l89[i]);
		}
		DBApp.createBitMapIndex("y0l89","d");
		DBApp.createBitMapIndex("y0l89","e");
		DBApp.createBitMapIndex("y0l89","i");
		DBApp.createBitMapIndex("y0l89","j");
		DBApp.createBitMapIndex("y0l89","l");
		DBApp.createBitMapIndex("y0l89","n");
		DBApp.createBitMapIndex("y0l89","o");
		DBApp.createBitMapIndex("y0l89","p");
		DBApp.createBitMapIndex("y0l89","q");
		DBApp.createBitMapIndex("y0l89","t");
		//first 13 records:
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
		//last 13 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "d", "d0"), ("1000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "e", "e3"), ("0001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "i", "i2"), ("0010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "j", "j8"), ("0000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "l", "l9"), ("0000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "n", "n11"), ("0000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "o", "o1"), ("0100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "p", "p12"), ("0000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y0l89", "q", "q10"), ("0000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y0l89", "t", "t20"), ("0000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j5tz", cols0);
		//To be indexed: [c, e, g], to be queried: [c1, e0, g7].
		String [][] records_j5tz = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			records_j5tz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_j5tz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("j5tz", records_j5tz[i]);
		}
		DBApp.createBitMapIndex("j5tz","c");
		DBApp.createBitMapIndex("j5tz","e");
		DBApp.createBitMapIndex("j5tz","g");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j5tz", "c", "c1"), ("0100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j5tz", "e", "e0"), ("1000010000100001000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j5tz", "g", "g7"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("cm", cols1);
		//To be indexed: [b, c, i], to be queried: [b0, c0, i9].
		String [][] records_cm = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			records_cm[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cm[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cm", records_cm[i]);
		}
		DBApp.createBitMapIndex("cm","b");
		DBApp.createBitMapIndex("cm","c");
		DBApp.createBitMapIndex("cm","i");
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
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cm", "b", "b0"), ("1010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cm", "c", "c0"), ("1001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cm", "i", "i9"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ovc0", cols0);
		//To be indexed: [b, c, d, e, i, j, k, n, o], to be queried: [b0, c1, d2, e0, i1, j8, k7, n6, o15].
		String [][] records_ovc0 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_ovc0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ovc0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ovc0", records_ovc0[i]);
		}
		DBApp.createBitMapIndex("ovc0","b");
		DBApp.createBitMapIndex("ovc0","c");
		DBApp.createBitMapIndex("ovc0","d");
		DBApp.createBitMapIndex("ovc0","e");
		DBApp.createBitMapIndex("ovc0","i");
		DBApp.createBitMapIndex("ovc0","j");
		DBApp.createBitMapIndex("ovc0","k");
		DBApp.createBitMapIndex("ovc0","n");
		DBApp.createBitMapIndex("ovc0","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "b", "b0"), ("10101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "c", "c1"), ("01001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "d", "d2"), ("00100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "e", "e0"), ("10000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "i", "i1"), ("01000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "j", "j8"), ("00000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "k", "k7"), ("00000001000000000010000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ovc0", "n", "n6"), ("00000010000000000000100000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ovc0", "o", "o15"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("v63", cols1);
		//To be indexed: [d, e, f, g, h, i, j, m], to be queried: [d0, e2, f0, g1, h3, i4, j4, m13].
		String [][] records_v63 = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			records_v63[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v63[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v63", records_v63[i]);
		}
		DBApp.createBitMapIndex("v63","d");
		DBApp.createBitMapIndex("v63","e");
		DBApp.createBitMapIndex("v63","f");
		DBApp.createBitMapIndex("v63","g");
		DBApp.createBitMapIndex("v63","h");
		DBApp.createBitMapIndex("v63","i");
		DBApp.createBitMapIndex("v63","j");
		DBApp.createBitMapIndex("v63","m");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "d", "d0"), ("10001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "e", "e2"), ("00100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "f", "f0"), ("10000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "g", "g1"), ("01000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "h", "h3"), ("00010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "i", "i4"), ("00001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v63", "j", "j4"), ("00001000000000100000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v63", "m", "m13"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("mke4", cols2);
		//To be indexed: [b, e, g, h, j, l, n, o, p, r, s], to be queried: [b0, e0, g2, h5, j0, l7, n2, o5, p2, r11, s19].
		String [][] records_mke4 = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			records_mke4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mke4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mke4", records_mke4[i]);
		}
		DBApp.createBitMapIndex("mke4","b");
		DBApp.createBitMapIndex("mke4","e");
		DBApp.createBitMapIndex("mke4","g");
		DBApp.createBitMapIndex("mke4","h");
		DBApp.createBitMapIndex("mke4","j");
		DBApp.createBitMapIndex("mke4","l");
		DBApp.createBitMapIndex("mke4","n");
		DBApp.createBitMapIndex("mke4","o");
		DBApp.createBitMapIndex("mke4","p");
		DBApp.createBitMapIndex("mke4","r");
		DBApp.createBitMapIndex("mke4","s");
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
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "b", "b0"), ("10101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "e", "e0"), ("10000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "g", "g2"), ("00100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "h", "h5"), ("00000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "j", "j0"), ("10000000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "l", "l7"), ("00000001000000000001000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "n", "n2"), ("00100000000000001000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "o", "o5"), ("00000100000000000000100000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "p", "p2"), ("00100000000000000010000000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mke4", "r", "r11"), ("00000000000100000000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mke4", "s", "s19"), ("00000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ux", cols3);
		//To be indexed: [b, f, g, i, l, n, o], to be queried: [b0, f2, g1, i3, l7, n11, o15].
		String [][] records_ux = new String[11][cols3.length];
		for(int i=0;i<11;i++)
		{
			records_ux[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ux[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ux", records_ux[i]);
		}
		DBApp.createBitMapIndex("ux","b");
		DBApp.createBitMapIndex("ux","f");
		DBApp.createBitMapIndex("ux","g");
		DBApp.createBitMapIndex("ux","i");
		DBApp.createBitMapIndex("ux","l");
		DBApp.createBitMapIndex("ux","n");
		DBApp.createBitMapIndex("ux","o");
		//first 11 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//last 11 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "b", "b0"), ("10101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "f", "f2"), ("00100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "g", "g1"), ("01000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "i", "i3"), ("00010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "l", "l7"), ("00000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ux", "n", "n11"), ("00000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ux", "o", "o15"), ("00000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("sfz", cols0);
		//To be indexed: [b, c, d, e, g, k, m, n], to be queried: [b0, c0, d0, e3, g1, k1, m2, n14].
		String [][] records_sfz = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			records_sfz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sfz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sfz", records_sfz[i]);
		}
		DBApp.createBitMapIndex("sfz","b");
		DBApp.createBitMapIndex("sfz","c");
		DBApp.createBitMapIndex("sfz","d");
		DBApp.createBitMapIndex("sfz","e");
		DBApp.createBitMapIndex("sfz","g");
		DBApp.createBitMapIndex("sfz","k");
		DBApp.createBitMapIndex("sfz","m");
		DBApp.createBitMapIndex("sfz","n");
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "b", "b0"), ("1010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "c", "c0"), ("1001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "d", "d0"), ("1000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "e", "e3"), ("0001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "g", "g1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "k", "k1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sfz", "m", "m2"), ("0010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("sfz", "n", "n14"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b9", cols1);
		//To be indexed: [d, h, l, o], to be queried: [d2, h4, l10, o15].
		String [][] records_b9 = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			records_b9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		DBApp.createBitMapIndex("b9","d");
		DBApp.createBitMapIndex("b9","h");
		DBApp.createBitMapIndex("b9","l");
		DBApp.createBitMapIndex("b9","o");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14]
		//last 15 records:
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9", "d", "d2"), ("00100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9", "h", "h4"), ("00001000000010000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9", "l", "l10"), ("00000000001000000000001000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b9", "o", "o15"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tt00l", cols2);
		//To be indexed: [d, e, f, g], to be queried: [d1, e0, f2, g7].
		String [][] records_tt00l = new String[44][cols2.length];
		for(int i=0;i<44;i++)
		{
			records_tt00l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tt00l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tt00l", records_tt00l[i]);
		}
		DBApp.createBitMapIndex("tt00l","d");
		DBApp.createBitMapIndex("tt00l","e");
		DBApp.createBitMapIndex("tt00l","f");
		DBApp.createBitMapIndex("tt00l","g");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		//record43: [a43, b1, c1, d3, e3, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tt00l", "d", "d1"), ("01000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tt00l", "e", "e0"), ("10000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tt00l", "f", "f2"), ("00100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tt00l", "g", "g7"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("vk6", cols0);
		//To be indexed: [d, l, n, q, r, s, t], to be queried: [d1, l9, n1, q2, r6, s4, t20].
		String [][] records_vk6 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_vk6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vk6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vk6", records_vk6[i]);
		}
		DBApp.createBitMapIndex("vk6","d");
		DBApp.createBitMapIndex("vk6","l");
		DBApp.createBitMapIndex("vk6","n");
		DBApp.createBitMapIndex("vk6","q");
		DBApp.createBitMapIndex("vk6","r");
		DBApp.createBitMapIndex("vk6","s");
		DBApp.createBitMapIndex("vk6","t");
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
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11, s11, t11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12, s12, t12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13, s13, t13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14, t14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15, t15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16, t16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17, t17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18, t18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0, t19]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1, t0]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2, t1]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3, t2]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4, t3]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5, t4]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6, t5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "d", "d1"), ("01000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "l", "l9"), ("00000000010000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "n", "n1"), ("01000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "q", "q2"), ("00100000000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "r", "r6"), ("00000010000000000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vk6", "s", "s4"), ("00001000000000000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vk6", "t", "t20"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("nx0a", cols1);
		//To be indexed: [c, d, e, g, i, k, l, n, s, t], to be queried: [c1, d1, e1, g5, i5, k9, l9, n10, s10, t20].
		String [][] records_nx0a = new String[7][cols1.length];
		for(int i=0;i<7;i++)
		{
			records_nx0a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nx0a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nx0a", records_nx0a[i]);
		}
		DBApp.createBitMapIndex("nx0a","c");
		DBApp.createBitMapIndex("nx0a","d");
		DBApp.createBitMapIndex("nx0a","e");
		DBApp.createBitMapIndex("nx0a","g");
		DBApp.createBitMapIndex("nx0a","i");
		DBApp.createBitMapIndex("nx0a","k");
		DBApp.createBitMapIndex("nx0a","l");
		DBApp.createBitMapIndex("nx0a","n");
		DBApp.createBitMapIndex("nx0a","s");
		DBApp.createBitMapIndex("nx0a","t");
		//first 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5, t5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6, t6]
		//last 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3, t3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4, t4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5, t5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6, t6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "c", "c1"), ("0100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "d", "d1"), ("0100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "e", "e1"), ("0100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "g", "g5"), ("0000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "i", "i5"), ("0000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "k", "k9"), ("0000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "l", "l9"), ("0000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "n", "n10"), ("0000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx0a", "s", "s10"), ("0000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nx0a", "t", "t20"), ("0000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("v5p4s", cols2);
		//To be indexed: [c, d, e, g, j, n, p, r], to be queried: [c0, d2, e3, g4, j1, n10, p14, r18].
		String [][] records_v5p4s = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_v5p4s[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v5p4s[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v5p4s", records_v5p4s[i]);
		}
		DBApp.createBitMapIndex("v5p4s","c");
		DBApp.createBitMapIndex("v5p4s","d");
		DBApp.createBitMapIndex("v5p4s","e");
		DBApp.createBitMapIndex("v5p4s","g");
		DBApp.createBitMapIndex("v5p4s","j");
		DBApp.createBitMapIndex("v5p4s","n");
		DBApp.createBitMapIndex("v5p4s","p");
		DBApp.createBitMapIndex("v5p4s","r");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//last 15 records:
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "c", "c0"), ("10010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "d", "d2"), ("00100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "e", "e3"), ("00010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "g", "g4"), ("00001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "j", "j1"), ("01000000000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "n", "n10"), ("00000000001000000000000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5p4s", "p", "p14"), ("00000000000000100000000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v5p4s", "r", "r18"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qj", cols3);
		//To be indexed: [b, d, g], to be queried: [b0, d1, g7].
		String [][] records_qj = new String[3][cols3.length];
		for(int i=0;i<3;i++)
		{
			records_qj[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qj[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qj", records_qj[i]);
		}
		DBApp.createBitMapIndex("qj","b");
		DBApp.createBitMapIndex("qj","d");
		DBApp.createBitMapIndex("qj","g");
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qj", "b", "b0"), ("101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qj", "d", "d1"), ("010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qj", "g", "g7"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u25l9", cols4);
		//To be indexed: [c, e, f, g], to be queried: [c0, e3, f4, g7].
		String [][] records_u25l9 = new String[18][cols4.length];
		for(int i=0;i<18;i++)
		{
			records_u25l9[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_u25l9[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("u25l9", records_u25l9[i]);
		}
		DBApp.createBitMapIndex("u25l9","c");
		DBApp.createBitMapIndex("u25l9","e");
		DBApp.createBitMapIndex("u25l9","f");
		DBApp.createBitMapIndex("u25l9","g");
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
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		//record17: [a17, b1, c2, d1, e2, f5, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u25l9", "c", "c0"), ("100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u25l9", "e", "e3"), ("000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u25l9", "f", "f4"), ("000010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u25l9", "g", "g7"), ("000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("u1", cols0);
		//To be indexed: [b, c, e, i, j, k, n, o, p, q, r, s], to be queried: [b0, c1, e3, i3, j8, k0, n1, o0, p9, q14, r7, s19].
		String [][] records_u1 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_u1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u1", records_u1[i]);
		}
		DBApp.createBitMapIndex("u1","b");
		DBApp.createBitMapIndex("u1","c");
		DBApp.createBitMapIndex("u1","e");
		DBApp.createBitMapIndex("u1","i");
		DBApp.createBitMapIndex("u1","j");
		DBApp.createBitMapIndex("u1","k");
		DBApp.createBitMapIndex("u1","n");
		DBApp.createBitMapIndex("u1","o");
		DBApp.createBitMapIndex("u1","p");
		DBApp.createBitMapIndex("u1","q");
		DBApp.createBitMapIndex("u1","r");
		DBApp.createBitMapIndex("u1","s");
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9, s7]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10, s8]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11, s9]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12, s10]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1, m10, n7, o4, p1, q15, r13, s11]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "b", "b0"), ("10101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "c", "c1"), ("01001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "e", "e3"), ("00010000100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "i", "i3"), ("00010000000010000000010000000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "j", "j8"), ("00000000100000000010000000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "k", "k0"), ("10000000000100000000001000000000010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "n", "n1"), ("01000000000000010000000000000100000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "o", "o0"), ("10000000000000010000000000000010000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "p", "p9"), ("00000000010000000000000001000000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "q", "q14"), ("00000000000000100000000000000001000000000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u1", "r", "r7"), ("00000001000000000000000001000000000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u1", "s", "s19"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vn4vx", cols0);
		//To be indexed: [e, f, g], to be queried: [e0, f0, g7].
		String [][] records_vn4vx = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_vn4vx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vn4vx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vn4vx", records_vn4vx[i]);
		}
		DBApp.createBitMapIndex("vn4vx","e");
		DBApp.createBitMapIndex("vn4vx","f");
		DBApp.createBitMapIndex("vn4vx","g");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vn4vx", "e", "e0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vn4vx", "f", "f0"), ("1"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vn4vx", "g", "g7"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i1b7y", cols1);
		//To be indexed: [b, c, g], to be queried: [b0, c0, g7].
		String [][] records_i1b7y = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			records_i1b7y[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i1b7y[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i1b7y", records_i1b7y[i]);
		}
		DBApp.createBitMapIndex("i1b7y","b");
		DBApp.createBitMapIndex("i1b7y","c");
		DBApp.createBitMapIndex("i1b7y","g");
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1b7y", "b", "b0"), ("101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1b7y", "c", "c0"), ("100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i1b7y", "g", "g7"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("f59", cols2);
		//To be indexed: [b, c, f, g, k, m, o, r], to be queried: [b0, c0, f3, g0, k4, m1, o8, r18].
		String [][] records_f59 = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			records_f59[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f59[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f59", records_f59[i]);
		}
		DBApp.createBitMapIndex("f59","b");
		DBApp.createBitMapIndex("f59","c");
		DBApp.createBitMapIndex("f59","f");
		DBApp.createBitMapIndex("f59","g");
		DBApp.createBitMapIndex("f59","k");
		DBApp.createBitMapIndex("f59","m");
		DBApp.createBitMapIndex("f59","o");
		DBApp.createBitMapIndex("f59","r");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "c", "c0"), ("100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "f", "f3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "g", "g0"), ("100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "k", "k4"), ("000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "m", "m1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f59", "o", "o8"), ("000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f59", "r", "r18"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("cafe1", cols0);
		//To be indexed: [b, c, e, f, g], to be queried: [b0, c0, e0, f2, g7].
		String [][] records_cafe1 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_cafe1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cafe1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cafe1", records_cafe1[i]);
		}
		DBApp.createBitMapIndex("cafe1","b");
		DBApp.createBitMapIndex("cafe1","c");
		DBApp.createBitMapIndex("cafe1","e");
		DBApp.createBitMapIndex("cafe1","f");
		DBApp.createBitMapIndex("cafe1","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cafe1", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cafe1", "c", "c0"), ("10010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cafe1", "e", "e0"), ("10000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cafe1", "f", "f2"), ("00100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cafe1", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bkv", cols0);
		//To be indexed: [b, c, d, e, g], to be queried: [b0, c0, d2, e1, g7].
		String [][] records_bkv = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_bkv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bkv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bkv", records_bkv[i]);
		}
		DBApp.createBitMapIndex("bkv","b");
		DBApp.createBitMapIndex("bkv","c");
		DBApp.createBitMapIndex("bkv","d");
		DBApp.createBitMapIndex("bkv","e");
		DBApp.createBitMapIndex("bkv","g");
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bkv", "b", "b0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bkv", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bkv", "d", "d2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bkv", "e", "e1"), ("01"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bkv", "g", "g7"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uzcl", cols1);
		//To be indexed: [c, j, k, l], to be queried: [c1, j7, k0, l12].
		String [][] records_uzcl = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			records_uzcl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uzcl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uzcl", records_uzcl[i]);
		}
		DBApp.createBitMapIndex("uzcl","c");
		DBApp.createBitMapIndex("uzcl","j");
		DBApp.createBitMapIndex("uzcl","k");
		DBApp.createBitMapIndex("uzcl","l");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uzcl", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uzcl", "j", "j7"), ("0000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uzcl", "k", "k0"), ("1000000000010000000000100000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uzcl", "l", "l12"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_55() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("lvd", cols0);
		//To be indexed: [b, c, g, i, l, m, n, o, q], to be queried: [b0, c0, g1, i1, l1, m6, n10, o1, q17].
		String [][] records_lvd = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_lvd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lvd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lvd", records_lvd[i]);
		}
		DBApp.createBitMapIndex("lvd","b");
		DBApp.createBitMapIndex("lvd","c");
		DBApp.createBitMapIndex("lvd","g");
		DBApp.createBitMapIndex("lvd","i");
		DBApp.createBitMapIndex("lvd","l");
		DBApp.createBitMapIndex("lvd","m");
		DBApp.createBitMapIndex("lvd","n");
		DBApp.createBitMapIndex("lvd","o");
		DBApp.createBitMapIndex("lvd","q");
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
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "c", "c0"), ("100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "g", "g1"), ("010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "i", "i1"), ("010000000010000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "l", "l1"), ("010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "m", "m6"), ("000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "n", "n10"), ("000000000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lvd", "o", "o1"), ("010000000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lvd", "q", "q17"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_56() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ne", cols0);
		//To be indexed: [c, e, j, l], to be queried: [c0, e1, j8, l12].
		String [][] records_ne = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			records_ne[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ne[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ne", records_ne[i]);
		}
		DBApp.createBitMapIndex("ne","c");
		DBApp.createBitMapIndex("ne","e");
		DBApp.createBitMapIndex("ne","j");
		DBApp.createBitMapIndex("ne","l");
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
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ne", "c", "c0"), ("1001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ne", "e", "e1"), ("0100001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ne", "j", "j8"), ("0000000010000000001000000000100000000010000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ne", "l", "l12"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("v6d", cols1);
		//To be indexed: [b, d, e, f, g, j, k, n, o, p], to be queried: [b0, d0, e0, f3, g3, j6, k2, n8, o3, p16].
		String [][] records_v6d = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			records_v6d[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v6d[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v6d", records_v6d[i]);
		}
		DBApp.createBitMapIndex("v6d","b");
		DBApp.createBitMapIndex("v6d","d");
		DBApp.createBitMapIndex("v6d","e");
		DBApp.createBitMapIndex("v6d","f");
		DBApp.createBitMapIndex("v6d","g");
		DBApp.createBitMapIndex("v6d","j");
		DBApp.createBitMapIndex("v6d","k");
		DBApp.createBitMapIndex("v6d","n");
		DBApp.createBitMapIndex("v6d","o");
		DBApp.createBitMapIndex("v6d","p");
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "b", "b0"), ("10101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "d", "d0"), ("10001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "e", "e0"), ("10000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "f", "f3"), ("00010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "g", "g3"), ("00010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "j", "j6"), ("00000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "k", "k2"), ("00100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "n", "n8"), ("00000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v6d", "o", "o3"), ("00010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v6d", "p", "p16"), ("00000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("e5u6", cols2);
		//To be indexed: [e, f, g, h, i, k, l, n, q], to be queried: [e2, f2, g1, h3, i5, k4, l3, n11, q17].
		String [][] records_e5u6 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			records_e5u6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e5u6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e5u6", records_e5u6[i]);
		}
		DBApp.createBitMapIndex("e5u6","e");
		DBApp.createBitMapIndex("e5u6","f");
		DBApp.createBitMapIndex("e5u6","g");
		DBApp.createBitMapIndex("e5u6","h");
		DBApp.createBitMapIndex("e5u6","i");
		DBApp.createBitMapIndex("e5u6","k");
		DBApp.createBitMapIndex("e5u6","l");
		DBApp.createBitMapIndex("e5u6","n");
		DBApp.createBitMapIndex("e5u6","q");
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "e", "e2"), ("00100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "f", "f2"), ("00100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "g", "g1"), ("01000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "h", "h3"), ("00010000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "i", "i5"), ("00000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "k", "k4"), ("00001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "l", "l3"), ("00010000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5u6", "n", "n11"), ("00000000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e5u6", "q", "q17"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_57() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ye7", cols0);
		//To be indexed: [b, c, g, h, j, m, p, q, r], to be queried: [b0, c0, g5, h2, j3, m1, p9, q2, r18].
		String [][] records_ye7 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_ye7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ye7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ye7", records_ye7[i]);
		}
		DBApp.createBitMapIndex("ye7","b");
		DBApp.createBitMapIndex("ye7","c");
		DBApp.createBitMapIndex("ye7","g");
		DBApp.createBitMapIndex("ye7","h");
		DBApp.createBitMapIndex("ye7","j");
		DBApp.createBitMapIndex("ye7","m");
		DBApp.createBitMapIndex("ye7","p");
		DBApp.createBitMapIndex("ye7","q");
		DBApp.createBitMapIndex("ye7","r");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//last 15 records:
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "b", "b0"), ("1010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "c", "c0"), ("1001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "g", "g5"), ("0000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "h", "h2"), ("0010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "j", "j3"), ("0001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "m", "m1"), ("0100000000000010000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "p", "p9"), ("0000000001000000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye7", "q", "q2"), ("0010000000000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ye7", "r", "r18"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_58() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("xebr", cols0);
		//To be indexed: [c, d, g, i, k], to be queried: [c1, d1, g1, i3, k11].
		String [][] records_xebr = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_xebr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xebr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xebr", records_xebr[i]);
		}
		DBApp.createBitMapIndex("xebr","c");
		DBApp.createBitMapIndex("xebr","d");
		DBApp.createBitMapIndex("xebr","g");
		DBApp.createBitMapIndex("xebr","i");
		DBApp.createBitMapIndex("xebr","k");
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
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xebr", "c", "c1"), ("01001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xebr", "d", "d1"), ("01000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xebr", "g", "g1"), ("01000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xebr", "i", "i3"), ("00010000000010000000010000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xebr", "k", "k11"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_59() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ve9o", cols0);
		//To be indexed: [b, d, e, g, h, i, j, n, o, q, r], to be queried: [b0, d1, e2, g2, h1, i2, j7, n4, o1, q0, r18].
		String [][] records_ve9o = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_ve9o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ve9o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ve9o", records_ve9o[i]);
		}
		DBApp.createBitMapIndex("ve9o","b");
		DBApp.createBitMapIndex("ve9o","d");
		DBApp.createBitMapIndex("ve9o","e");
		DBApp.createBitMapIndex("ve9o","g");
		DBApp.createBitMapIndex("ve9o","h");
		DBApp.createBitMapIndex("ve9o","i");
		DBApp.createBitMapIndex("ve9o","j");
		DBApp.createBitMapIndex("ve9o","n");
		DBApp.createBitMapIndex("ve9o","o");
		DBApp.createBitMapIndex("ve9o","q");
		DBApp.createBitMapIndex("ve9o","r");
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "b", "b0"), ("10101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "d", "d1"), ("01000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "e", "e2"), ("00100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "g", "g2"), ("00100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "h", "h1"), ("01000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "i", "i2"), ("00100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "j", "j7"), ("00000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "n", "n4"), ("00001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "o", "o1"), ("01000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ve9o", "q", "q0"), ("10000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ve9o", "r", "r18"), ("00000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("dvre", cols1);
		//To be indexed: [c, d, e, f, i, j, l, m, p, q, r], to be queried: [c0, d1, e1, f3, i3, j2, l2, m11, p6, q7, r18].
		String [][] records_dvre = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			records_dvre[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dvre[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dvre", records_dvre[i]);
		}
		DBApp.createBitMapIndex("dvre","c");
		DBApp.createBitMapIndex("dvre","d");
		DBApp.createBitMapIndex("dvre","e");
		DBApp.createBitMapIndex("dvre","f");
		DBApp.createBitMapIndex("dvre","i");
		DBApp.createBitMapIndex("dvre","j");
		DBApp.createBitMapIndex("dvre","l");
		DBApp.createBitMapIndex("dvre","m");
		DBApp.createBitMapIndex("dvre","p");
		DBApp.createBitMapIndex("dvre","q");
		DBApp.createBitMapIndex("dvre","r");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//last 15 records:
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "c", "c0"), ("1001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "d", "d1"), ("0100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "e", "e1"), ("0100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "f", "f3"), ("0001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "i", "i3"), ("0001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "j", "j2"), ("0010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "l", "l2"), ("0010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "m", "m11"), ("0000000000010000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "p", "p6"), ("0000001000000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dvre", "q", "q7"), ("0000000100000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dvre", "r", "r18"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_60() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("sxg", cols0);
		//To be indexed: [d, f, h, j, l, m, n, r], to be queried: [d0, f0, h2, j2, l0, m10, n12, r18].
		String [][] records_sxg = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			records_sxg[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sxg[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sxg", records_sxg[i]);
		}
		DBApp.createBitMapIndex("sxg","d");
		DBApp.createBitMapIndex("sxg","f");
		DBApp.createBitMapIndex("sxg","h");
		DBApp.createBitMapIndex("sxg","j");
		DBApp.createBitMapIndex("sxg","l");
		DBApp.createBitMapIndex("sxg","m");
		DBApp.createBitMapIndex("sxg","n");
		DBApp.createBitMapIndex("sxg","r");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//last 15 records:
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "d", "d0"), ("100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "f", "f0"), ("100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "h", "h2"), ("001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "j", "j2"), ("001000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "l", "l0"), ("100000000000100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "m", "m10"), ("000000000010000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sxg", "n", "n12"), ("000000000000100000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("sxg", "r", "r18"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wn2", cols1);
		//To be indexed: [b, c, g], to be queried: [b0, c0, g7].
		String [][] records_wn2 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_wn2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wn2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wn2", records_wn2[i]);
		}
		DBApp.createBitMapIndex("wn2","b");
		DBApp.createBitMapIndex("wn2","c");
		DBApp.createBitMapIndex("wn2","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wn2", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wn2", "c", "c0"), ("10010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wn2", "g", "g7"), ("00000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ahj", cols2);
		//To be indexed: [c, e, g, h], to be queried: [c1, e0, g5, h8].
		String [][] records_ahj = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_ahj[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ahj[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ahj", records_ahj[i]);
		}
		DBApp.createBitMapIndex("ahj","c");
		DBApp.createBitMapIndex("ahj","e");
		DBApp.createBitMapIndex("ahj","g");
		DBApp.createBitMapIndex("ahj","h");
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
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ahj", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ahj", "e", "e0"), ("100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ahj", "g", "g5"), ("000001000000100000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ahj", "h", "h8"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("lh8", cols3);
		//To be indexed: [b, e, i, k, m, o, q, r, s], to be queried: [b0, e3, i0, k2, m10, o2, q11, r1, s19].
		String [][] records_lh8 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			records_lh8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lh8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lh8", records_lh8[i]);
		}
		DBApp.createBitMapIndex("lh8","b");
		DBApp.createBitMapIndex("lh8","e");
		DBApp.createBitMapIndex("lh8","i");
		DBApp.createBitMapIndex("lh8","k");
		DBApp.createBitMapIndex("lh8","m");
		DBApp.createBitMapIndex("lh8","o");
		DBApp.createBitMapIndex("lh8","q");
		DBApp.createBitMapIndex("lh8","r");
		DBApp.createBitMapIndex("lh8","s");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "b", "b0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "e", "e3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "i", "i0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "k", "k2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "m", "m10"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "o", "o2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "q", "q11"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lh8", "r", "r1"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lh8", "s", "s19"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_61() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k5d", cols0);
		//To be indexed: [c, e, f, g], to be queried: [c0, e0, f4, g7].
		String [][] records_k5d = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			records_k5d[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k5d[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k5d", records_k5d[i]);
		}
		DBApp.createBitMapIndex("k5d","c");
		DBApp.createBitMapIndex("k5d","e");
		DBApp.createBitMapIndex("k5d","f");
		DBApp.createBitMapIndex("k5d","g");
		//first 13 records:
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
		//last 13 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5d", "c", "c0"), ("1001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5d", "e", "e0"), ("1000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5d", "f", "f4"), ("0000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k5d", "g", "g7"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hq", cols1);
		//To be indexed: [b, c, f, i, j, k, l], to be queried: [b0, c1, f4, i2, j7, k1, l12].
		String [][] records_hq = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			records_hq[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hq[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hq", records_hq[i]);
		}
		DBApp.createBitMapIndex("hq","b");
		DBApp.createBitMapIndex("hq","c");
		DBApp.createBitMapIndex("hq","f");
		DBApp.createBitMapIndex("hq","i");
		DBApp.createBitMapIndex("hq","j");
		DBApp.createBitMapIndex("hq","k");
		DBApp.createBitMapIndex("hq","l");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "b", "b0"), ("101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "c", "c1"), ("010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "f", "f4"), ("000010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "i", "i2"), ("001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "j", "j7"), ("000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hq", "k", "k1"), ("010000000000100000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hq", "l", "l12"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h1qg", cols2);
		//To be indexed: [e, g], to be queried: [e3, g7].
		String [][] records_h1qg = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			records_h1qg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h1qg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h1qg", records_h1qg[i]);
		}
		DBApp.createBitMapIndex("h1qg","e");
		DBApp.createBitMapIndex("h1qg","g");
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
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1qg", "e", "e3"), ("000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h1qg", "g", "g7"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v103", cols3);
		//To be indexed: [d, g], to be queried: [d2, g7].
		String [][] records_v103 = new String[45][cols3.length];
		for(int i=0;i<45;i++)
		{
			records_v103[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v103[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v103", records_v103[i]);
		}
		DBApp.createBitMapIndex("v103","d");
		DBApp.createBitMapIndex("v103","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v103", "d", "d2"), ("001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v103", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zn", cols4);
		//To be indexed: [b, d, e, f, j], to be queried: [b0, d1, e3, f3, j10].
		String [][] records_zn = new String[6][cols4.length];
		for(int i=0;i<6;i++)
		{
			records_zn[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_zn[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("zn", records_zn[i]);
		}
		DBApp.createBitMapIndex("zn","b");
		DBApp.createBitMapIndex("zn","d");
		DBApp.createBitMapIndex("zn","e");
		DBApp.createBitMapIndex("zn","f");
		DBApp.createBitMapIndex("zn","j");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zn", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zn", "d", "d1"), ("010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zn", "e", "e3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zn", "f", "f3"), ("000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zn", "j", "j10"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_62() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("feby1", cols0);
		//To be indexed: [b, d, f, g], to be queried: [b0, d2, f0, g7].
		String [][] records_feby1 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_feby1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_feby1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("feby1", records_feby1[i]);
		}
		DBApp.createBitMapIndex("feby1","b");
		DBApp.createBitMapIndex("feby1","d");
		DBApp.createBitMapIndex("feby1","f");
		DBApp.createBitMapIndex("feby1","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("feby1", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("feby1", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("feby1", "f", "f0"), ("1000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("feby1", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("pf2", cols1);
		//To be indexed: [b, c, e, f, g], to be queried: [b0, c1, e2, f3, g7].
		String [][] records_pf2 = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			records_pf2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pf2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pf2", records_pf2[i]);
		}
		DBApp.createBitMapIndex("pf2","b");
		DBApp.createBitMapIndex("pf2","c");
		DBApp.createBitMapIndex("pf2","e");
		DBApp.createBitMapIndex("pf2","f");
		DBApp.createBitMapIndex("pf2","g");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pf2", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pf2", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pf2", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pf2", "f", "f3"), ("000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("pf2", "g", "g7"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("lub", cols2);
		//To be indexed: [c, d, e, h, k, m], to be queried: [c1, d0, e2, h3, k8, m13].
		String [][] records_lub = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_lub[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lub[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lub", records_lub[i]);
		}
		DBApp.createBitMapIndex("lub","c");
		DBApp.createBitMapIndex("lub","d");
		DBApp.createBitMapIndex("lub","e");
		DBApp.createBitMapIndex("lub","h");
		DBApp.createBitMapIndex("lub","k");
		DBApp.createBitMapIndex("lub","m");
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
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lub", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lub", "d", "d0"), ("100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lub", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lub", "h", "h3"), ("000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lub", "k", "k8"), ("000000001000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lub", "m", "m13"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kr5", cols3);
		//To be indexed: [b, c, f, h], to be queried: [b0, c0, f2, h8].
		String [][] records_kr5 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			records_kr5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kr5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kr5", records_kr5[i]);
		}
		DBApp.createBitMapIndex("kr5","b");
		DBApp.createBitMapIndex("kr5","c");
		DBApp.createBitMapIndex("kr5","f");
		DBApp.createBitMapIndex("kr5","h");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kr5", "b", "b0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kr5", "c", "c0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kr5", "f", "f2"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kr5", "h", "h8"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("r3f1", cols4);
		//To be indexed: [c, f, j, k, l], to be queried: [c0, f2, j0, k2, l12].
		String [][] records_r3f1 = new String[38][cols4.length];
		for(int i=0;i<38;i++)
		{
			records_r3f1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r3f1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r3f1", records_r3f1[i]);
		}
		DBApp.createBitMapIndex("r3f1","c");
		DBApp.createBitMapIndex("r3f1","f");
		DBApp.createBitMapIndex("r3f1","j");
		DBApp.createBitMapIndex("r3f1","k");
		DBApp.createBitMapIndex("r3f1","l");
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
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r3f1", "c", "c0"), ("10010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r3f1", "f", "f2"), ("00100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r3f1", "j", "j0"), ("10000000001000000000100000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r3f1", "k", "k2"), ("00100000000001000000000010000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r3f1", "l", "l12"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_63() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gq", cols0);
		//To be indexed: [b, c, f, g], to be queried: [b0, c0, f2, g7].
		String [][] records_gq = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_gq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gq", records_gq[i]);
		}
		DBApp.createBitMapIndex("gq","b");
		DBApp.createBitMapIndex("gq","c");
		DBApp.createBitMapIndex("gq","f");
		DBApp.createBitMapIndex("gq","g");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gq", "b", "b0"), ("101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gq", "c", "c0"), ("100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gq", "f", "f2"), ("001000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gq", "g", "g7"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("y4ub", cols1);
		//To be indexed: [e, g, h, i], to be queried: [e1, g1, h5, i9].
		String [][] records_y4ub = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_y4ub[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y4ub[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y4ub", records_y4ub[i]);
		}
		DBApp.createBitMapIndex("y4ub","e");
		DBApp.createBitMapIndex("y4ub","g");
		DBApp.createBitMapIndex("y4ub","h");
		DBApp.createBitMapIndex("y4ub","i");
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
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y4ub", "e", "e1"), ("0100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y4ub", "g", "g1"), ("0100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y4ub", "h", "h5"), ("0000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y4ub", "i", "i9"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("oxfb8", cols2);
		//To be indexed: [c, f, g, i, j, k, m, n, o, p, s], to be queried: [c1, f4, g0, i1, j0, k5, m8, n4, o3, p12, s19].
		String [][] records_oxfb8 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_oxfb8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oxfb8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oxfb8", records_oxfb8[i]);
		}
		DBApp.createBitMapIndex("oxfb8","c");
		DBApp.createBitMapIndex("oxfb8","f");
		DBApp.createBitMapIndex("oxfb8","g");
		DBApp.createBitMapIndex("oxfb8","i");
		DBApp.createBitMapIndex("oxfb8","j");
		DBApp.createBitMapIndex("oxfb8","k");
		DBApp.createBitMapIndex("oxfb8","m");
		DBApp.createBitMapIndex("oxfb8","n");
		DBApp.createBitMapIndex("oxfb8","o");
		DBApp.createBitMapIndex("oxfb8","p");
		DBApp.createBitMapIndex("oxfb8","s");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "f", "f4"), ("0000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "g", "g0"), ("1000000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "i", "i1"), ("0100000000100000000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "j", "j0"), ("1000000000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "k", "k5"), ("0000010000000000100000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "m", "m8"), ("0000000010000000000001000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "n", "n4"), ("0000100000000000001000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "o", "o3"), ("0001000000000000001000000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oxfb8", "p", "p12"), ("0000000000001000000000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oxfb8", "s", "s19"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_64() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("g1c0", cols0);
		//To be indexed: [b, c, d, e, h, i, l], to be queried: [b0, c0, d0, e0, h1, i1, l12].
		String [][] records_g1c0 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_g1c0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g1c0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g1c0", records_g1c0[i]);
		}
		DBApp.createBitMapIndex("g1c0","b");
		DBApp.createBitMapIndex("g1c0","c");
		DBApp.createBitMapIndex("g1c0","d");
		DBApp.createBitMapIndex("g1c0","e");
		DBApp.createBitMapIndex("g1c0","h");
		DBApp.createBitMapIndex("g1c0","i");
		DBApp.createBitMapIndex("g1c0","l");
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
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "c", "c0"), ("10010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "d", "d0"), ("10001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "e", "e0"), ("10000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "h", "h1"), ("01000000010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g1c0", "i", "i1"), ("01000000001000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g1c0", "l", "l12"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("kx57b", cols1);
		//To be indexed: [b, d, e, h, i, l, n], to be queried: [b0, d0, e3, h5, i5, l3, n14].
		String [][] records_kx57b = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_kx57b[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kx57b[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kx57b", records_kx57b[i]);
		}
		DBApp.createBitMapIndex("kx57b","b");
		DBApp.createBitMapIndex("kx57b","d");
		DBApp.createBitMapIndex("kx57b","e");
		DBApp.createBitMapIndex("kx57b","h");
		DBApp.createBitMapIndex("kx57b","i");
		DBApp.createBitMapIndex("kx57b","l");
		DBApp.createBitMapIndex("kx57b","n");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "d", "d0"), ("100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "e", "e3"), ("000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "h", "h5"), ("000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "i", "i5"), ("000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kx57b", "l", "l3"), ("000100000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kx57b", "n", "n14"), ("000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_65() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cw0", cols0);
		//To be indexed: [d, k], to be queried: [d2, k11].
		String [][] records_cw0 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_cw0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cw0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cw0", records_cw0[i]);
		}
		DBApp.createBitMapIndex("cw0","d");
		DBApp.createBitMapIndex("cw0","k");
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
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cw0", "d", "d2"), ("0010001000100010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cw0", "k", "k11"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("dew2", cols1);
		//To be indexed: [b, c, d, f, g, i, n, p, q, s], to be queried: [b0, c1, d0, f2, g1, i4, n2, p9, q6, s19].
		String [][] records_dew2 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_dew2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dew2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dew2", records_dew2[i]);
		}
		DBApp.createBitMapIndex("dew2","b");
		DBApp.createBitMapIndex("dew2","c");
		DBApp.createBitMapIndex("dew2","d");
		DBApp.createBitMapIndex("dew2","f");
		DBApp.createBitMapIndex("dew2","g");
		DBApp.createBitMapIndex("dew2","i");
		DBApp.createBitMapIndex("dew2","n");
		DBApp.createBitMapIndex("dew2","p");
		DBApp.createBitMapIndex("dew2","q");
		DBApp.createBitMapIndex("dew2","s");
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "b", "b0"), ("10101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "c", "c1"), ("01001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "d", "d0"), ("10001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "f", "f2"), ("00100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "g", "g1"), ("01000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "i", "i4"), ("00001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "n", "n2"), ("00100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "p", "p9"), ("00000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dew2", "q", "q6"), ("00000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dew2", "s", "s19"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wymj", cols2);
		//To be indexed: [b, c, d, e, f, g, i, k, l, m], to be queried: [b0, c1, d0, e3, f3, g5, i6, k2, l1, m13].
		String [][] records_wymj = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_wymj[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wymj[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wymj", records_wymj[i]);
		}
		DBApp.createBitMapIndex("wymj","b");
		DBApp.createBitMapIndex("wymj","c");
		DBApp.createBitMapIndex("wymj","d");
		DBApp.createBitMapIndex("wymj","e");
		DBApp.createBitMapIndex("wymj","f");
		DBApp.createBitMapIndex("wymj","g");
		DBApp.createBitMapIndex("wymj","i");
		DBApp.createBitMapIndex("wymj","k");
		DBApp.createBitMapIndex("wymj","l");
		DBApp.createBitMapIndex("wymj","m");
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
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "b", "b0"), ("1010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "c", "c1"), ("0100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "d", "d0"), ("1000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "e", "e3"), ("0001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "f", "f3"), ("0001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "g", "g5"), ("0000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "i", "i6"), ("0000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "k", "k2"), ("0010000000000100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wymj", "l", "l1"), ("0100000000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wymj", "m", "m13"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_66() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("zxa06", cols0);
		//To be indexed: [c, d, e, h, i, j, k, l, m, n, o, p, s], to be queried: [c1, d2, e1, h2, i2, j6, k7, l5, m11, n4, o0, p4, s19].
		String [][] records_zxa06 = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			records_zxa06[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zxa06[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zxa06", records_zxa06[i]);
		}
		DBApp.createBitMapIndex("zxa06","c");
		DBApp.createBitMapIndex("zxa06","d");
		DBApp.createBitMapIndex("zxa06","e");
		DBApp.createBitMapIndex("zxa06","h");
		DBApp.createBitMapIndex("zxa06","i");
		DBApp.createBitMapIndex("zxa06","j");
		DBApp.createBitMapIndex("zxa06","k");
		DBApp.createBitMapIndex("zxa06","l");
		DBApp.createBitMapIndex("zxa06","m");
		DBApp.createBitMapIndex("zxa06","n");
		DBApp.createBitMapIndex("zxa06","o");
		DBApp.createBitMapIndex("zxa06","p");
		DBApp.createBitMapIndex("zxa06","s");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "d", "d2"), ("0010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "e", "e1"), ("0100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "h", "h2"), ("0010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "i", "i2"), ("0010000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "j", "j6"), ("0000001000000000100000000010000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "k", "k7"), ("0000000100000000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "l", "l5"), ("0000010000000000010000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "m", "m11"), ("0000000000010000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "n", "n4"), ("0000100000000000001000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "o", "o0"), ("1000000000000001000000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zxa06", "p", "p4"), ("0000100000000000000010000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zxa06", "s", "s19"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("r58", cols1);
		//To be indexed: [c, f, g, h, m], to be queried: [c0, f2, g2, h1, m13].
		String [][] records_r58 = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			records_r58[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r58[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r58", records_r58[i]);
		}
		DBApp.createBitMapIndex("r58","c");
		DBApp.createBitMapIndex("r58","f");
		DBApp.createBitMapIndex("r58","g");
		DBApp.createBitMapIndex("r58","h");
		DBApp.createBitMapIndex("r58","m");
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r58", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r58", "f", "f2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r58", "g", "g2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r58", "h", "h1"), ("01"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r58", "m", "m13"), ("00"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_67() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yh8", cols0);
		//To be indexed: [b, c, g], to be queried: [b0, c1, g7].
		String [][] records_yh8 = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			records_yh8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yh8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yh8", records_yh8[i]);
		}
		DBApp.createBitMapIndex("yh8","b");
		DBApp.createBitMapIndex("yh8","c");
		DBApp.createBitMapIndex("yh8","g");
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yh8", "b", "b0"), ("101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yh8", "c", "c1"), ("010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yh8", "g", "g7"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("vbcb2", cols1);
		//To be indexed: [d, e, f, g, h, i, j, k, l, m], to be queried: [d2, e0, f0, g0, h0, i7, j2, k2, l4, m13].
		String [][] records_vbcb2 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			records_vbcb2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vbcb2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vbcb2", records_vbcb2[i]);
		}
		DBApp.createBitMapIndex("vbcb2","d");
		DBApp.createBitMapIndex("vbcb2","e");
		DBApp.createBitMapIndex("vbcb2","f");
		DBApp.createBitMapIndex("vbcb2","g");
		DBApp.createBitMapIndex("vbcb2","h");
		DBApp.createBitMapIndex("vbcb2","i");
		DBApp.createBitMapIndex("vbcb2","j");
		DBApp.createBitMapIndex("vbcb2","k");
		DBApp.createBitMapIndex("vbcb2","l");
		DBApp.createBitMapIndex("vbcb2","m");
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
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "d", "d2"), ("0010001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "e", "e0"), ("1000010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "f", "f0"), ("1000001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "g", "g0"), ("1000000100000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "h", "h0"), ("1000000010000000100000001000000010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "i", "i7"), ("0000000100000000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "j", "j2"), ("0010000000001000000000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "k", "k2"), ("0010000000000100000000001000000000010000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vbcb2", "l", "l4"), ("0000100000000000100000000000100000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vbcb2", "m", "m13"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ms4p", cols2);
		//To be indexed: [b, c, f, g, l, m, n, o, r], to be queried: [b0, c1, f4, g3, l3, m4, n2, o11, r18].
		String [][] records_ms4p = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_ms4p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ms4p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ms4p", records_ms4p[i]);
		}
		DBApp.createBitMapIndex("ms4p","b");
		DBApp.createBitMapIndex("ms4p","c");
		DBApp.createBitMapIndex("ms4p","f");
		DBApp.createBitMapIndex("ms4p","g");
		DBApp.createBitMapIndex("ms4p","l");
		DBApp.createBitMapIndex("ms4p","m");
		DBApp.createBitMapIndex("ms4p","n");
		DBApp.createBitMapIndex("ms4p","o");
		DBApp.createBitMapIndex("ms4p","r");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10, o10, p10, q10, r10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11, o11, p11, q11, r11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12, o12, p12, q12, r12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13, o13, p13, q13, r13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14]
		//last 15 records:
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "b", "b0"), ("1010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "f", "f4"), ("0000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "g", "g3"), ("0001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "l", "l3"), ("0001000000000001000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "m", "m4"), ("0000100000000000010000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "n", "n2"), ("0010000000000000100000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms4p", "o", "o11"), ("0000000000010000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ms4p", "r", "r18"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_68() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ol3", cols0);
		//To be indexed: [d, g], to be queried: [d0, g7].
		String [][] records_ol3 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			records_ol3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ol3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ol3", records_ol3[i]);
		}
		DBApp.createBitMapIndex("ol3","d");
		DBApp.createBitMapIndex("ol3","g");
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
		//record30: [a30, b0, c0, d2, e0, f0, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ol3", "d", "d0"), ("1000100010001000100010001000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ol3", "g", "g7"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c1", cols1);
		//To be indexed: [d, e, f, g], to be queried: [d0, e2, f2, g7].
		String [][] records_c1 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			records_c1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c1", records_c1[i]);
		}
		DBApp.createBitMapIndex("c1","d");
		DBApp.createBitMapIndex("c1","e");
		DBApp.createBitMapIndex("c1","f");
		DBApp.createBitMapIndex("c1","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c1", "d", "d0"), ("10001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c1", "e", "e2"), ("00100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c1", "f", "f2"), ("00100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c1", "g", "g7"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h2xz9", cols2);
		//To be indexed: [i], to be queried: [i9].
		String [][] records_h2xz9 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			records_h2xz9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h2xz9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h2xz9", records_h2xz9[i]);
		}
		DBApp.createBitMapIndex("h2xz9","i");
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
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8]
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h2xz9", "i", "i9"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("uq11g", cols3);
		//To be indexed: [d, e, f, g, h, i, l, n], to be queried: [d2, e0, f4, g5, h1, i3, l9, n14].
		String [][] records_uq11g = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_uq11g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_uq11g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("uq11g", records_uq11g[i]);
		}
		DBApp.createBitMapIndex("uq11g","d");
		DBApp.createBitMapIndex("uq11g","e");
		DBApp.createBitMapIndex("uq11g","f");
		DBApp.createBitMapIndex("uq11g","g");
		DBApp.createBitMapIndex("uq11g","h");
		DBApp.createBitMapIndex("uq11g","i");
		DBApp.createBitMapIndex("uq11g","l");
		DBApp.createBitMapIndex("uq11g","n");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "e", "e0"), ("1000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "f", "f4"), ("0000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "g", "g5"), ("0000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "h", "h1"), ("0100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "i", "i3"), ("0001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq11g", "l", "l9"), ("0000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uq11g", "n", "n14"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("b61", cols4);
		//To be indexed: [b, f, j, k], to be queried: [b0, f3, j8, k11].
		String [][] records_b61 = new String[46][cols4.length];
		for(int i=0;i<46;i++)
		{
			records_b61[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_b61[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("b61", records_b61[i]);
		}
		DBApp.createBitMapIndex("b61","b");
		DBApp.createBitMapIndex("b61","f");
		DBApp.createBitMapIndex("b61","j");
		DBApp.createBitMapIndex("b61","k");
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
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b61", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b61", "f", "f3"), ("0001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b61", "j", "j8"), ("0000000010000000001000000000100000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b61", "k", "k11"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_69() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("p0", cols0);
		//To be indexed: [b, c, d, f, h, i, j, k, l], to be queried: [b0, c0, d0, f1, h3, i2, j6, k8, l12].
		String [][] records_p0 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_p0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p0", records_p0[i]);
		}
		DBApp.createBitMapIndex("p0","b");
		DBApp.createBitMapIndex("p0","c");
		DBApp.createBitMapIndex("p0","d");
		DBApp.createBitMapIndex("p0","f");
		DBApp.createBitMapIndex("p0","h");
		DBApp.createBitMapIndex("p0","i");
		DBApp.createBitMapIndex("p0","j");
		DBApp.createBitMapIndex("p0","k");
		DBApp.createBitMapIndex("p0","l");
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
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "b", "b0"), ("101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "c", "c0"), ("100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "d", "d0"), ("100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "f", "f1"), ("010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "h", "h3"), ("000100000001000000010000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "i", "i2"), ("001000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "j", "j6"), ("000000100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p0", "k", "k8"), ("000000001000000000010000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p0", "l", "l12"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_70() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a8tl9", cols0);
		//To be indexed: [b, d, f, g, h], to be queried: [b0, d2, f2, g3, h8].
		String [][] records_a8tl9 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			records_a8tl9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a8tl9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a8tl9", records_a8tl9[i]);
		}
		DBApp.createBitMapIndex("a8tl9","b");
		DBApp.createBitMapIndex("a8tl9","d");
		DBApp.createBitMapIndex("a8tl9","f");
		DBApp.createBitMapIndex("a8tl9","g");
		DBApp.createBitMapIndex("a8tl9","h");
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
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a8tl9", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a8tl9", "d", "d2"), ("001000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a8tl9", "f", "f2"), ("001000001000001000001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a8tl9", "g", "g3"), ("000100000010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a8tl9", "h", "h8"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("pz9l4", cols1);
		//To be indexed: [b, c, d, h, i, k], to be queried: [b0, c0, d1, h3, i6, k11].
		String [][] records_pz9l4 = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_pz9l4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pz9l4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pz9l4", records_pz9l4[i]);
		}
		DBApp.createBitMapIndex("pz9l4","b");
		DBApp.createBitMapIndex("pz9l4","c");
		DBApp.createBitMapIndex("pz9l4","d");
		DBApp.createBitMapIndex("pz9l4","h");
		DBApp.createBitMapIndex("pz9l4","i");
		DBApp.createBitMapIndex("pz9l4","k");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pz9l4", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pz9l4", "c", "c0"), ("100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pz9l4", "d", "d1"), ("010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pz9l4", "h", "h3"), ("000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pz9l4", "i", "i6"), ("000000100000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("pz9l4", "k", "k11"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("rg5dg", cols2);
		//To be indexed: [b, f, g, j], to be queried: [b0, f0, g1, j10].
		String [][] records_rg5dg = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			records_rg5dg[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rg5dg[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rg5dg", records_rg5dg[i]);
		}
		DBApp.createBitMapIndex("rg5dg","b");
		DBApp.createBitMapIndex("rg5dg","f");
		DBApp.createBitMapIndex("rg5dg","g");
		DBApp.createBitMapIndex("rg5dg","j");
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rg5dg", "b", "b0"), ("101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rg5dg", "f", "f0"), ("100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rg5dg", "g", "g1"), ("010000001000000100000010000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("rg5dg", "j", "j10"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ms", cols3);
		//To be indexed: [b, d, h, k, l, n, p, q, r, s], to be queried: [b0, d0, h6, k2, l8, n2, p14, q4, r1, s19].
		String [][] records_ms = new String[29][cols3.length];
		for(int i=0;i<29;i++)
		{
			records_ms[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ms[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ms", records_ms[i]);
		}
		DBApp.createBitMapIndex("ms","b");
		DBApp.createBitMapIndex("ms","d");
		DBApp.createBitMapIndex("ms","h");
		DBApp.createBitMapIndex("ms","k");
		DBApp.createBitMapIndex("ms","l");
		DBApp.createBitMapIndex("ms","n");
		DBApp.createBitMapIndex("ms","p");
		DBApp.createBitMapIndex("ms","q");
		DBApp.createBitMapIndex("ms","r");
		DBApp.createBitMapIndex("ms","s");
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
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0, o14, p14, q14, r14, s14]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1, s0]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3, r2, s1]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5, q4, r3, s2]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4, s3]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8, s7]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "d", "d0"), ("10001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "h", "h6"), ("00000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "k", "k2"), ("00100000000001000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "l", "l8"), ("00000000100000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "n", "n2"), ("00100000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "p", "p14"), ("00000000000000100000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "q", "q4"), ("00001000000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ms", "r", "r1"), ("01000000000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ms", "s", "s19"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("wj2i", cols4);
		//To be indexed: [c, e, j, k, l, n, p], to be queried: [c0, e3, j6, k2, l10, n10, p16].
		String [][] records_wj2i = new String[38][cols4.length];
		for(int i=0;i<38;i++)
		{
			records_wj2i[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wj2i[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wj2i", records_wj2i[i]);
		}
		DBApp.createBitMapIndex("wj2i","c");
		DBApp.createBitMapIndex("wj2i","e");
		DBApp.createBitMapIndex("wj2i","j");
		DBApp.createBitMapIndex("wj2i","k");
		DBApp.createBitMapIndex("wj2i","l");
		DBApp.createBitMapIndex("wj2i","n");
		DBApp.createBitMapIndex("wj2i","p");
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
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "c", "c0"), ("10010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "e", "e3"), ("00010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "j", "j6"), ("00000010000000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "k", "k2"), ("00100000000001000000000010000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "l", "l10"), ("00000000001000000000001000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wj2i", "n", "n10"), ("00000000001000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wj2i", "p", "p16"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_71() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("szt3", cols0);
		//To be indexed: [e, f, g, h, i, j, m], to be queried: [e0, f1, g2, h3, i7, j1, m13].
		String [][] records_szt3 = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_szt3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_szt3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("szt3", records_szt3[i]);
		}
		DBApp.createBitMapIndex("szt3","e");
		DBApp.createBitMapIndex("szt3","f");
		DBApp.createBitMapIndex("szt3","g");
		DBApp.createBitMapIndex("szt3","h");
		DBApp.createBitMapIndex("szt3","i");
		DBApp.createBitMapIndex("szt3","j");
		DBApp.createBitMapIndex("szt3","m");
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
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "e", "e0"), ("1000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "f", "f1"), ("0100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "g", "g2"), ("0010000001000000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "h", "h3"), ("0001000000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "i", "i7"), ("0000000100000000100000000100000000100000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("szt3", "j", "j1"), ("0100000000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("szt3", "m", "m13"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("m34", cols1);
		//To be indexed: [b, c, d, f, g, k], to be queried: [b0, c1, d1, f4, g5, k11].
		String [][] records_m34 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			records_m34[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m34[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m34", records_m34[i]);
		}
		DBApp.createBitMapIndex("m34","b");
		DBApp.createBitMapIndex("m34","c");
		DBApp.createBitMapIndex("m34","d");
		DBApp.createBitMapIndex("m34","f");
		DBApp.createBitMapIndex("m34","g");
		DBApp.createBitMapIndex("m34","k");
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m34", "b", "b0"), ("101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m34", "c", "c1"), ("010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m34", "d", "d1"), ("010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m34", "f", "f4"), ("000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m34", "g", "g5"), ("000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m34", "k", "k11"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rw8", cols2);
		//To be indexed: [e, g], to be queried: [e3, g7].
		String [][] records_rw8 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_rw8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_rw8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("rw8", records_rw8[i]);
		}
		DBApp.createBitMapIndex("rw8","e");
		DBApp.createBitMapIndex("rw8","g");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rw8", "e", "e3"), ("00010000100001000010000100001000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("rw8", "g", "g7"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("tg570", cols3);
		//To be indexed: [c, d, e, f, g, j, l, m, p], to be queried: [c1, d0, e3, f2, g1, j7, l5, m3, p16].
		String [][] records_tg570 = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			records_tg570[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tg570[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tg570", records_tg570[i]);
		}
		DBApp.createBitMapIndex("tg570","c");
		DBApp.createBitMapIndex("tg570","d");
		DBApp.createBitMapIndex("tg570","e");
		DBApp.createBitMapIndex("tg570","f");
		DBApp.createBitMapIndex("tg570","g");
		DBApp.createBitMapIndex("tg570","j");
		DBApp.createBitMapIndex("tg570","l");
		DBApp.createBitMapIndex("tg570","m");
		DBApp.createBitMapIndex("tg570","p");
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
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "c", "c1"), ("01001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "d", "d0"), ("10001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "e", "e3"), ("00010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "f", "f2"), ("00100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "g", "g1"), ("01000000100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "j", "j7"), ("00000001000000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "l", "l5"), ("00000100000000000100000000000100000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tg570", "m", "m3"), ("00010000000000001000000000000100000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tg570", "p", "p16"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c4", cols4);
		//To be indexed: [b, d, f, g], to be queried: [b0, d1, f1, g7].
		String [][] records_c4 = new String[14][cols4.length];
		for(int i=0;i<14;i++)
		{
			records_c4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_c4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("c4", records_c4[i]);
		}
		DBApp.createBitMapIndex("c4","b");
		DBApp.createBitMapIndex("c4","d");
		DBApp.createBitMapIndex("c4","f");
		DBApp.createBitMapIndex("c4","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c4", "b", "b0"), ("10101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c4", "d", "d1"), ("01000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c4", "f", "f1"), ("01000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c4", "g", "g7"), ("00000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_72() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("i84r1", cols0);
		//To be indexed: [c, g, i, j], to be queried: [c1, g3, i5, j10].
		String [][] records_i84r1 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			records_i84r1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i84r1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i84r1", records_i84r1[i]);
		}
		DBApp.createBitMapIndex("i84r1","c");
		DBApp.createBitMapIndex("i84r1","g");
		DBApp.createBitMapIndex("i84r1","i");
		DBApp.createBitMapIndex("i84r1","j");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i84r1", "c", "c1"), ("01001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i84r1", "g", "g3"), ("00010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i84r1", "i", "i5"), ("00000100000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i84r1", "j", "j10"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("nx", cols1);
		//To be indexed: [b, c, d, e, g, h, i], to be queried: [b0, c0, d1, e2, g1, h1, i9].
		String [][] records_nx = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_nx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_nx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("nx", records_nx[i]);
		}
		DBApp.createBitMapIndex("nx","b");
		DBApp.createBitMapIndex("nx","c");
		DBApp.createBitMapIndex("nx","d");
		DBApp.createBitMapIndex("nx","e");
		DBApp.createBitMapIndex("nx","g");
		DBApp.createBitMapIndex("nx","h");
		DBApp.createBitMapIndex("nx","i");
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "c", "c0"), ("10010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "d", "d1"), ("01000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "e", "e2"), ("00100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "g", "g1"), ("01000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nx", "h", "h1"), ("01000000010000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nx", "i", "i9"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f60", cols2);
		//To be indexed: [c, d, g], to be queried: [c0, d2, g7].
		String [][] records_f60 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			records_f60[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f60[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f60", records_f60[i]);
		}
		DBApp.createBitMapIndex("f60","c");
		DBApp.createBitMapIndex("f60","d");
		DBApp.createBitMapIndex("f60","g");
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
		//record47: [a47, b1, c2, d3, e2, f5, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f60", "c", "c0"), ("100100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f60", "d", "d2"), ("001000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f60", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_73() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("c713s", cols0);
		//To be indexed: [b, e, f, g, h, i, m, n], to be queried: [b0, e1, f0, g5, h4, i7, m7, n14].
		String [][] records_c713s = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			records_c713s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c713s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c713s", records_c713s[i]);
		}
		DBApp.createBitMapIndex("c713s","b");
		DBApp.createBitMapIndex("c713s","e");
		DBApp.createBitMapIndex("c713s","f");
		DBApp.createBitMapIndex("c713s","g");
		DBApp.createBitMapIndex("c713s","h");
		DBApp.createBitMapIndex("c713s","i");
		DBApp.createBitMapIndex("c713s","m");
		DBApp.createBitMapIndex("c713s","n");
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "b", "b0"), ("10101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "e", "e1"), ("01000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "f", "f0"), ("10000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "g", "g5"), ("00000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "h", "h4"), ("00001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "i", "i7"), ("00000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c713s", "m", "m7"), ("00000001000000000000100000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c713s", "n", "n14"), ("00000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_74() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("cf3", cols0);
		//To be indexed: [b, c, d, h, j, k, l, p], to be queried: [b0, c1, d2, h6, j6, k6, l5, p16].
		String [][] records_cf3 = new String[25][cols0.length];
		for(int i=0;i<25;i++)
		{
			records_cf3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_cf3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("cf3", records_cf3[i]);
		}
		DBApp.createBitMapIndex("cf3","b");
		DBApp.createBitMapIndex("cf3","c");
		DBApp.createBitMapIndex("cf3","d");
		DBApp.createBitMapIndex("cf3","h");
		DBApp.createBitMapIndex("cf3","j");
		DBApp.createBitMapIndex("cf3","k");
		DBApp.createBitMapIndex("cf3","l");
		DBApp.createBitMapIndex("cf3","p");
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "b", "b0"), ("1010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "c", "c1"), ("0100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "d", "d2"), ("0010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "h", "h6"), ("0000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "j", "j6"), ("0000001000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "k", "k6"), ("0000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cf3", "l", "l5"), ("0000010000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cf3", "p", "p16"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("uum", cols1);
		//To be indexed: [d, e, h, k, l, m, n, q], to be queried: [d0, e3, h6, k1, l0, m0, n11, q17].
		String [][] records_uum = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_uum[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_uum[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("uum", records_uum[i]);
		}
		DBApp.createBitMapIndex("uum","d");
		DBApp.createBitMapIndex("uum","e");
		DBApp.createBitMapIndex("uum","h");
		DBApp.createBitMapIndex("uum","k");
		DBApp.createBitMapIndex("uum","l");
		DBApp.createBitMapIndex("uum","m");
		DBApp.createBitMapIndex("uum","n");
		DBApp.createBitMapIndex("uum","q");
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "d", "d0"), ("10001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "e", "e3"), ("00010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "h", "h6"), ("00000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "k", "k1"), ("01000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "l", "l0"), ("10000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "m", "m0"), ("10000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uum", "n", "n11"), ("00000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uum", "q", "q17"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("i94", cols2);
		//To be indexed: [c, d, f, g, i, l, m, p], to be queried: [c0, d0, f4, g5, i5, l7, m10, p16].
		String [][] records_i94 = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_i94[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_i94[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("i94", records_i94[i]);
		}
		DBApp.createBitMapIndex("i94","c");
		DBApp.createBitMapIndex("i94","d");
		DBApp.createBitMapIndex("i94","f");
		DBApp.createBitMapIndex("i94","g");
		DBApp.createBitMapIndex("i94","i");
		DBApp.createBitMapIndex("i94","l");
		DBApp.createBitMapIndex("i94","m");
		DBApp.createBitMapIndex("i94","p");
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "c", "c0"), ("1001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "d", "d0"), ("1000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "f", "f4"), ("0000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "g", "g5"), ("0000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "i", "i5"), ("0000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "l", "l7"), ("0000000100000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i94", "m", "m10"), ("0000000000100000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i94", "p", "p16"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("v0", cols3);
		//To be indexed: [c, e, g, h, i, k, q, t], to be queried: [c0, e1, g5, h6, i0, k0, q13, t20].
		String [][] records_v0 = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			records_v0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v0", records_v0[i]);
		}
		DBApp.createBitMapIndex("v0","c");
		DBApp.createBitMapIndex("v0","e");
		DBApp.createBitMapIndex("v0","g");
		DBApp.createBitMapIndex("v0","h");
		DBApp.createBitMapIndex("v0","i");
		DBApp.createBitMapIndex("v0","k");
		DBApp.createBitMapIndex("v0","q");
		DBApp.createBitMapIndex("v0","t");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "e", "e1"), ("010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "g", "g5"), ("000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "h", "h6"), ("000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "i", "i0"), ("100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "k", "k0"), ("100000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0", "q", "q13"), ("000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v0", "t", "t20"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_75() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s6474", cols0);
		//To be indexed: [e, g, i, k], to be queried: [e1, g5, i1, k11].
		String [][] records_s6474 = new String[49][cols0.length];
		for(int i=0;i<49;i++)
		{
			records_s6474[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s6474[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s6474", records_s6474[i]);
		}
		DBApp.createBitMapIndex("s6474","e");
		DBApp.createBitMapIndex("s6474","g");
		DBApp.createBitMapIndex("s6474","i");
		DBApp.createBitMapIndex("s6474","k");
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
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s6474", "e", "e1"), ("0100001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s6474", "g", "g5"), ("0000010000001000000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s6474", "i", "i1"), ("0100000000100000000100000000100000000100000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s6474", "k", "k11"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o8vy", cols1);
		//To be indexed: [b, c, d, f, h, j, m, n], to be queried: [b0, c1, d2, f1, h1, j3, m7, n14].
		String [][] records_o8vy = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			records_o8vy[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o8vy[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o8vy", records_o8vy[i]);
		}
		DBApp.createBitMapIndex("o8vy","b");
		DBApp.createBitMapIndex("o8vy","c");
		DBApp.createBitMapIndex("o8vy","d");
		DBApp.createBitMapIndex("o8vy","f");
		DBApp.createBitMapIndex("o8vy","h");
		DBApp.createBitMapIndex("o8vy","j");
		DBApp.createBitMapIndex("o8vy","m");
		DBApp.createBitMapIndex("o8vy","n");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "c", "c1"), ("010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "d", "d2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "f", "f1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "h", "h1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "j", "j3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o8vy", "m", "m7"), ("000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o8vy", "n", "n14"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_76() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yzd37", cols0);
		//To be indexed: [b, c, e, h, i], to be queried: [b0, c1, e2, h6, i9].
		String [][] records_yzd37 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_yzd37[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yzd37[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yzd37", records_yzd37[i]);
		}
		DBApp.createBitMapIndex("yzd37","b");
		DBApp.createBitMapIndex("yzd37","c");
		DBApp.createBitMapIndex("yzd37","e");
		DBApp.createBitMapIndex("yzd37","h");
		DBApp.createBitMapIndex("yzd37","i");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yzd37", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yzd37", "c", "c1"), ("010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yzd37", "e", "e2"), ("001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yzd37", "h", "h6"), ("000000100000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yzd37", "i", "i9"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h8z", cols1);
		//To be indexed: [b, c, g], to be queried: [b0, c0, g7].
		String [][] records_h8z = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_h8z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h8z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h8z", records_h8z[i]);
		}
		DBApp.createBitMapIndex("h8z","b");
		DBApp.createBitMapIndex("h8z","c");
		DBApp.createBitMapIndex("h8z","g");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h8z", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h8z", "c", "c0"), ("1001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h8z", "g", "g7"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("n1v7", cols2);
		//To be indexed: [i, k, o, p, r, t], to be queried: [i0, k3, o8, p4, r12, t20].
		String [][] records_n1v7 = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			records_n1v7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n1v7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n1v7", records_n1v7[i]);
		}
		DBApp.createBitMapIndex("n1v7","i");
		DBApp.createBitMapIndex("n1v7","k");
		DBApp.createBitMapIndex("n1v7","o");
		DBApp.createBitMapIndex("n1v7","p");
		DBApp.createBitMapIndex("n1v7","r");
		DBApp.createBitMapIndex("n1v7","t");
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
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13, t12]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n1v7", "i", "i0"), ("10000000010000000010000000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n1v7", "k", "k3"), ("00010000000000100000000001000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n1v7", "o", "o8"), ("00000000100000000000000100000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n1v7", "p", "p4"), ("00001000000000000000100000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n1v7", "r", "r12"), ("00000000000010000000000000000010000000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n1v7", "t", "t20"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("crhg", cols3);
		//To be indexed: [b, c, e, f, g], to be queried: [b0, c1, e2, f3, g7].
		String [][] records_crhg = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			records_crhg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_crhg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("crhg", records_crhg[i]);
		}
		DBApp.createBitMapIndex("crhg","b");
		DBApp.createBitMapIndex("crhg","c");
		DBApp.createBitMapIndex("crhg","e");
		DBApp.createBitMapIndex("crhg","f");
		DBApp.createBitMapIndex("crhg","g");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		//record23: [a23, b1, c2, d3, e3, f5, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("crhg", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("crhg", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("crhg", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("crhg", "f", "f3"), ("000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("crhg", "g", "g7"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("s951s", cols4);
		//To be indexed: [b, c, h, i, k], to be queried: [b0, c0, h3, i5, k11].
		String [][] records_s951s = new String[29][cols4.length];
		for(int i=0;i<29;i++)
		{
			records_s951s[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_s951s[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("s951s", records_s951s[i]);
		}
		DBApp.createBitMapIndex("s951s","b");
		DBApp.createBitMapIndex("s951s","c");
		DBApp.createBitMapIndex("s951s","h");
		DBApp.createBitMapIndex("s951s","i");
		DBApp.createBitMapIndex("s951s","k");
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
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s951s", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s951s", "c", "c0"), ("10010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s951s", "h", "h3"), ("00010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s951s", "i", "i5"), ("00000100000000100000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s951s", "k", "k11"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("iyl", cols0);
		DBApp.createBitMapIndex("iyl","d");
		DBApp.createBitMapIndex("iyl","f");
		DBApp.createBitMapIndex("iyl","j");
		DBApp.createBitMapIndex("iyl","m");
		DBApp.createBitMapIndex("iyl","n");
		DBApp.createBitMapIndex("iyl","p");
		DBApp.createBitMapIndex("iyl","q");
		DBApp.createBitMapIndex("iyl","r");
		DBApp.createBitMapIndex("iyl","s");
		String [][] records_iyl = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_iyl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_iyl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("iyl", records_iyl[i]);
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9, s7]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10, s8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "d", "d1"), ("01000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "f", "f0"), ("10000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "j", "j7"), ("00000001000000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "m", "m6"), ("00000010000000000001000000000000100000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "n", "n1"), ("01000000000000010000000000000100000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "p", "p12"), ("00000000000010000000000000001000000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "q", "q3"), ("00010000000000000000100000000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("iyl", "r", "r7"), ("00000001000000000000000001000000000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("iyl", "s", "s19"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f1a47", cols1);
		DBApp.createBitMapIndex("f1a47","b");
		DBApp.createBitMapIndex("f1a47","c");
		DBApp.createBitMapIndex("f1a47","d");
		DBApp.createBitMapIndex("f1a47","e");
		DBApp.createBitMapIndex("f1a47","f");
		DBApp.createBitMapIndex("f1a47","g");
		DBApp.createBitMapIndex("f1a47","j");
		DBApp.createBitMapIndex("f1a47","k");
		DBApp.createBitMapIndex("f1a47","m");
		DBApp.createBitMapIndex("f1a47","n");
		String [][] records_f1a47 = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			records_f1a47[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f1a47[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f1a47", records_f1a47[i]);
		}
		//first 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//last 10 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "c", "c0"), ("1001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "e", "e3"), ("0001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "f", "f1"), ("0100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "g", "g2"), ("0010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "j", "j8"), ("0000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "k", "k3"), ("0001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1a47", "m", "m8"), ("0000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f1a47", "n", "n14"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z67d6", cols2);
		DBApp.createBitMapIndex("z67d6","b");
		DBApp.createBitMapIndex("z67d6","c");
		DBApp.createBitMapIndex("z67d6","d");
		DBApp.createBitMapIndex("z67d6","f");
		DBApp.createBitMapIndex("z67d6","g");
		String [][] records_z67d6 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			records_z67d6[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z67d6[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z67d6", records_z67d6[i]);
		}
		//first 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		//last 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//record6: [a6, b0, c0, d2, e1, f0, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z67d6", "b", "b0"), ("1010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z67d6", "c", "c1"), ("0100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z67d6", "d", "d2"), ("0010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z67d6", "f", "f3"), ("0001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z67d6", "g", "g7"), ("0000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wm", cols0);
		DBApp.createBitMapIndex("wm","c");
		DBApp.createBitMapIndex("wm","e");
		DBApp.createBitMapIndex("wm","g");
		DBApp.createBitMapIndex("wm","h");
		DBApp.createBitMapIndex("wm","k");
		DBApp.createBitMapIndex("wm","n");
		String [][] records_wm = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_wm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wm", records_wm[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wm", "c", "c1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wm", "e", "e0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wm", "g", "g5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wm", "h", "h5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wm", "k", "k6"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wm", "n", "n14"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("f8", cols1);
		DBApp.createBitMapIndex("f8","f");
		DBApp.createBitMapIndex("f8","h");
		DBApp.createBitMapIndex("f8","i");
		DBApp.createBitMapIndex("f8","j");
		String [][] records_f8 = new String[28][cols1.length];
		for(int i=0;i<28;i++)
		{
			records_f8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f8", records_f8[i]);
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
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8", "f", "f0"), ("1000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8", "h", "h5"), ("0000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8", "i", "i3"), ("0001000000001000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f8", "j", "j10"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xw48i", cols2);
		DBApp.createBitMapIndex("xw48i","b");
		DBApp.createBitMapIndex("xw48i","c");
		DBApp.createBitMapIndex("xw48i","d");
		DBApp.createBitMapIndex("xw48i","g");
		String [][] records_xw48i = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			records_xw48i[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xw48i[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xw48i", records_xw48i[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xw48i", "b", "b0"), ("101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xw48i", "c", "c0"), ("100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xw48i", "d", "d1"), ("010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xw48i", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lcunq", cols3);
		DBApp.createBitMapIndex("lcunq","c");
		DBApp.createBitMapIndex("lcunq","f");
		DBApp.createBitMapIndex("lcunq","g");
		String [][] records_lcunq = new String[19][cols3.length];
		for(int i=0;i<19;i++)
		{
			records_lcunq[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_lcunq[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("lcunq", records_lcunq[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lcunq", "c", "c0"), ("1001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lcunq", "f", "f0"), ("1000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lcunq", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kl403", cols0);
		DBApp.createBitMapIndex("kl403","b");
		DBApp.createBitMapIndex("kl403","c");
		DBApp.createBitMapIndex("kl403","d");
		DBApp.createBitMapIndex("kl403","e");
		DBApp.createBitMapIndex("kl403","g");
		String [][] records_kl403 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			records_kl403[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kl403[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kl403", records_kl403[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kl403", "b", "b0"), ("101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kl403", "c", "c1"), ("010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kl403", "d", "d2"), ("001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kl403", "e", "e3"), ("000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kl403", "g", "g7"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("p16c", cols1);
		DBApp.createBitMapIndex("p16c","f");
		DBApp.createBitMapIndex("p16c","i");
		DBApp.createBitMapIndex("p16c","j");
		DBApp.createBitMapIndex("p16c","k");
		DBApp.createBitMapIndex("p16c","m");
		DBApp.createBitMapIndex("p16c","n");
		String [][] records_p16c = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_p16c[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p16c[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p16c", records_p16c[i]);
		}
		//first 15 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//last 15 records:
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p16c", "f", "f2"), ("001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p16c", "i", "i4"), ("000010000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p16c", "j", "j5"), ("000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p16c", "k", "k0"), ("100000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p16c", "m", "m7"), ("000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p16c", "n", "n14"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z3g3", cols2);
		DBApp.createBitMapIndex("z3g3","b");
		DBApp.createBitMapIndex("z3g3","c");
		DBApp.createBitMapIndex("z3g3","f");
		DBApp.createBitMapIndex("z3g3","g");
		String [][] records_z3g3 = new String[21][cols2.length];
		for(int i=0;i<21;i++)
		{
			records_z3g3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z3g3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z3g3", records_z3g3[i]);
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
		//record20: [a20, b0, c2, d0, e0, f2, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3g3", "b", "b0"), ("101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3g3", "c", "c0"), ("100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3g3", "f", "f4"), ("000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z3g3", "g", "g7"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vusi", cols3);
		DBApp.createBitMapIndex("vusi","b");
		DBApp.createBitMapIndex("vusi","c");
		DBApp.createBitMapIndex("vusi","g");
		DBApp.createBitMapIndex("vusi","h");
		String [][] records_vusi = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			records_vusi[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vusi[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vusi", records_vusi[i]);
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
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vusi", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vusi", "c", "c0"), ("10010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vusi", "g", "g2"), ("00100000010000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vusi", "h", "h8"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e215u", cols4);
		DBApp.createBitMapIndex("e215u","b");
		DBApp.createBitMapIndex("e215u","e");
		DBApp.createBitMapIndex("e215u","g");
		String [][] records_e215u = new String[29][cols4.length];
		for(int i=0;i<29;i++)
		{
			records_e215u[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_e215u[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("e215u", records_e215u[i]);
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
		//record26: [a26, b0, c2, d2, e1, f2, g5]
		//record27: [a27, b1, c0, d3, e2, f3, g6]
		//record28: [a28, b0, c1, d0, e3, f4, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e215u", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e215u", "e", "e2"), ("00100001000010000100001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e215u", "g", "g7"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d17", cols0);
		DBApp.createBitMapIndex("d17","c");
		DBApp.createBitMapIndex("d17","d");
		DBApp.createBitMapIndex("d17","g");
		String [][] records_d17 = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			records_d17[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d17[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d17", records_d17[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d17", "c", "c0"), ("100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d17", "d", "d0"), ("100010001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("d17", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m9", cols1);
		DBApp.createBitMapIndex("m9","c");
		DBApp.createBitMapIndex("m9","d");
		DBApp.createBitMapIndex("m9","f");
		DBApp.createBitMapIndex("m9","g");
		String [][] records_m9 = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			records_m9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m9", records_m9[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m9", "c", "c1"), ("01001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m9", "d", "d1"), ("01000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m9", "f", "f1"), ("01000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m9", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r1", cols2);
		DBApp.createBitMapIndex("r1","c");
		DBApp.createBitMapIndex("r1","d");
		DBApp.createBitMapIndex("r1","e");
		DBApp.createBitMapIndex("r1","g");
		String [][] records_r1 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_r1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_r1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("r1", records_r1[i]);
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r1", "c", "c1"), ("01001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r1", "d", "d2"), ("00100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r1", "e", "e2"), ("00100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r1", "g", "g7"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sx93y", cols0);
		DBApp.createBitMapIndex("sx93y","b");
		DBApp.createBitMapIndex("sx93y","c");
		DBApp.createBitMapIndex("sx93y","g");
		String [][] records_sx93y = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			records_sx93y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sx93y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sx93y", records_sx93y[i]);
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
		//record16: [a16, b0, c1, d0, e1, f4, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sx93y", "b", "b0"), ("10101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sx93y", "c", "c1"), ("01001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("sx93y", "g", "g7"), ("00000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p66", cols1);
		DBApp.createBitMapIndex("p66","d");
		DBApp.createBitMapIndex("p66","f");
		DBApp.createBitMapIndex("p66","g");
		String [][] records_p66 = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			records_p66[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p66[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p66", records_p66[i]);
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
		//record39: [a39, b1, c0, d3, e4, f3, g4]
		//record40: [a40, b0, c1, d0, e0, f4, g5]
		//record41: [a41, b1, c2, d1, e1, f5, g6]
		//record42: [a42, b0, c0, d2, e2, f0, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p66", "d", "d0"), ("1000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p66", "f", "f4"), ("0000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p66", "g", "g7"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("bez", cols2);
		DBApp.createBitMapIndex("bez","d");
		DBApp.createBitMapIndex("bez","h");
		DBApp.createBitMapIndex("bez","k");
		DBApp.createBitMapIndex("bez","l");
		DBApp.createBitMapIndex("bez","q");
		String [][] records_bez = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			records_bez[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bez[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bez", records_bez[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bez", "d", "d0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bez", "h", "h2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bez", "k", "k9"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bez", "l", "l10"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bez", "q", "q17"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h91", cols3);
		DBApp.createBitMapIndex("h91","b");
		DBApp.createBitMapIndex("h91","c");
		DBApp.createBitMapIndex("h91","d");
		DBApp.createBitMapIndex("h91","e");
		DBApp.createBitMapIndex("h91","g");
		DBApp.createBitMapIndex("h91","i");
		String [][] records_h91 = new String[48][cols3.length];
		for(int i=0;i<48;i++)
		{
			records_h91[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h91[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h91", records_h91[i]);
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
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h91", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h91", "c", "c1"), ("010010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h91", "d", "d0"), ("100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h91", "e", "e3"), ("000100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h91", "g", "g1"), ("010000001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h91", "i", "i9"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vcacz", cols0);
		DBApp.createBitMapIndex("vcacz","g");
		String [][] records_vcacz = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			records_vcacz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vcacz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vcacz", records_vcacz[i]);
		}
		//first 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//last 3 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vcacz", "g", "g7"), ("000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u2w2a", cols1);
		DBApp.createBitMapIndex("u2w2a","b");
		DBApp.createBitMapIndex("u2w2a","c");
		DBApp.createBitMapIndex("u2w2a","g");
		String [][] records_u2w2a = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			records_u2w2a[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u2w2a[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u2w2a", records_u2w2a[i]);
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
		//record33: [a33, b1, c0, d1, e3, f3, g5]
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		//record36: [a36, b0, c0, d0, e1, f0, g1]
		//record37: [a37, b1, c1, d1, e2, f1, g2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2w2a", "b", "b0"), ("10101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u2w2a", "c", "c1"), ("01001001001001001001001001001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u2w2a", "g", "g7"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(314);
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