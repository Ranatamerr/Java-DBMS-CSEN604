package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_06
{

	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("el0", cols0);
		DBApp.createBitMapIndex("el0","d");
		DBApp.createBitMapIndex("el0","f");
		DBApp.createBitMapIndex("el0","g");
		DBApp.createBitMapIndex("el0","h");
		DBApp.createBitMapIndex("el0","k");
		DBApp.createBitMapIndex("el0","l");
		DBApp.createBitMapIndex("el0","m");
		DBApp.createBitMapIndex("el0","o");
		DBApp.createBitMapIndex("el0","s");
		String [][] records_el0 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			records_el0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_el0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("el0", records_el0[i]);
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
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11, s10]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "d", "d1"), ("0100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "f", "f3"), ("0001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "g", "g5"), ("0000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "h", "h6"), ("0000001000000010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "k", "k7"), ("0000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "l", "l4"), ("0000100000000000100000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "m", "m6"), ("0000001000000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("el0", "o", "o10"), ("0000000000100000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("el0", "s", "s19"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("zu55j", cols1);
		DBApp.createBitMapIndex("zu55j","d");
		DBApp.createBitMapIndex("zu55j","g");
		DBApp.createBitMapIndex("zu55j","i");
		DBApp.createBitMapIndex("zu55j","j");
		DBApp.createBitMapIndex("zu55j","m");
		DBApp.createBitMapIndex("zu55j","p");
		String [][] records_zu55j = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			records_zu55j[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zu55j[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zu55j", records_zu55j[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zu55j", "d", "d1"), ("01000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zu55j", "g", "g0"), ("10000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zu55j", "i", "i1"), ("01000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zu55j", "j", "j3"), ("00010000000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zu55j", "m", "m6"), ("00000010000000000001000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zu55j", "p", "p16"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("oi", cols2);
		DBApp.createBitMapIndex("oi","b");
		DBApp.createBitMapIndex("oi","d");
		DBApp.createBitMapIndex("oi","e");
		DBApp.createBitMapIndex("oi","f");
		DBApp.createBitMapIndex("oi","g");
		DBApp.createBitMapIndex("oi","j");
		DBApp.createBitMapIndex("oi","l");
		DBApp.createBitMapIndex("oi","m");
		DBApp.createBitMapIndex("oi","n");
		DBApp.createBitMapIndex("oi","q");
		DBApp.createBitMapIndex("oi","r");
		DBApp.createBitMapIndex("oi","s");
		String [][] records_oi = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			records_oi[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oi[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oi", records_oi[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "e", "e1"), ("0100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "f", "f4"), ("0000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "g", "g3"), ("0001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "j", "j6"), ("0000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "l", "l6"), ("0000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "m", "m7"), ("0000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "n", "n1"), ("0100000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "q", "q11"), ("0000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oi", "r", "r3"), ("0001000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oi", "s", "s19"), ("0000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("tw58", cols0);
		DBApp.createBitMapIndex("tw58","b");
		DBApp.createBitMapIndex("tw58","e");
		DBApp.createBitMapIndex("tw58","f");
		DBApp.createBitMapIndex("tw58","j");
		DBApp.createBitMapIndex("tw58","k");
		DBApp.createBitMapIndex("tw58","l");
		String [][] records_tw58 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_tw58[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tw58[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tw58", records_tw58[i]);
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
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4, j9, k5, l1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw58", "b", "b0"), ("10101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw58", "e", "e1"), ("01000010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw58", "f", "f3"), ("00010000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw58", "j", "j1"), ("01000000000100000000010000000001000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw58", "k", "k8"), ("00000000100000000001000000000010000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tw58", "l", "l12"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oks2n", cols1);
		DBApp.createBitMapIndex("oks2n","f");
		DBApp.createBitMapIndex("oks2n","g");
		String [][] records_oks2n = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_oks2n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_oks2n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("oks2n", records_oks2n[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oks2n", "f", "f4"), ("0000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oks2n", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e5", cols2);
		DBApp.createBitMapIndex("e5","b");
		DBApp.createBitMapIndex("e5","d");
		DBApp.createBitMapIndex("e5","f");
		DBApp.createBitMapIndex("e5","g");
		String [][] records_e5 = new String[29][cols2.length];
		for(int i=0;i<29;i++)
		{
			records_e5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e5", records_e5[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5", "d", "d0"), ("10001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e5", "f", "f4"), ("00001000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e5", "g", "g7"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ss3g", cols3);
		DBApp.createBitMapIndex("ss3g","d");
		DBApp.createBitMapIndex("ss3g","f");
		DBApp.createBitMapIndex("ss3g","h");
		String [][] records_ss3g = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			records_ss3g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ss3g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ss3g", records_ss3g[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ss3g", "d", "d1"), ("0100010001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ss3g", "f", "f4"), ("0000100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ss3g", "h", "h8"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("t3r", cols0);
		DBApp.createBitMapIndex("t3r","b");
		DBApp.createBitMapIndex("t3r","c");
		DBApp.createBitMapIndex("t3r","d");
		DBApp.createBitMapIndex("t3r","e");
		DBApp.createBitMapIndex("t3r","f");
		DBApp.createBitMapIndex("t3r","j");
		DBApp.createBitMapIndex("t3r","m");
		DBApp.createBitMapIndex("t3r","o");
		DBApp.createBitMapIndex("t3r","p");
		DBApp.createBitMapIndex("t3r","q");
		DBApp.createBitMapIndex("t3r","s");
		String [][] records_t3r = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_t3r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t3r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t3r", records_t3r[i]);
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
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7, s5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "b", "b0"), ("10101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "c", "c0"), ("10010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "d", "d0"), ("10001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "e", "e3"), ("00010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "f", "f2"), ("00100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "j", "j2"), ("00100000000010000000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "m", "m7"), ("00000001000000000000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "o", "o3"), ("00010000000000000010000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "p", "p8"), ("00000000100000000000000010000000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t3r", "q", "q13"), ("00000000000001000000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t3r", "s", "s19"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("isd7", cols1);
		DBApp.createBitMapIndex("isd7","b");
		DBApp.createBitMapIndex("isd7","c");
		DBApp.createBitMapIndex("isd7","d");
		DBApp.createBitMapIndex("isd7","e");
		DBApp.createBitMapIndex("isd7","g");
		DBApp.createBitMapIndex("isd7","j");
		DBApp.createBitMapIndex("isd7","k");
		DBApp.createBitMapIndex("isd7","l");
		DBApp.createBitMapIndex("isd7","n");
		DBApp.createBitMapIndex("isd7","o");
		String [][] records_isd7 = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_isd7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_isd7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("isd7", records_isd7[i]);
		}
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "c", "c0"), ("1001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "e", "e3"), ("0001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "g", "g0"), ("1000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "j", "j2"), ("0010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "k", "k3"), ("0001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "l", "l8"), ("0000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("isd7", "n", "n7"), ("0000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("isd7", "o", "o15"), ("0000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("ad", cols0);
		DBApp.createBitMapIndex("ad","b");
		DBApp.createBitMapIndex("ad","d");
		DBApp.createBitMapIndex("ad","e");
		DBApp.createBitMapIndex("ad","h");
		DBApp.createBitMapIndex("ad","i");
		DBApp.createBitMapIndex("ad","j");
		DBApp.createBitMapIndex("ad","n");
		DBApp.createBitMapIndex("ad","t");
		String [][] records_ad = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_ad[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ad[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ad", records_ad[i]);
		}
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "e", "e2"), ("0010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "h", "h5"), ("0000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "i", "i5"), ("0000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "j", "j4"), ("0000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ad", "n", "n0"), ("1000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ad", "t", "t20"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i1r0", cols1);
		DBApp.createBitMapIndex("i1r0","c");
		DBApp.createBitMapIndex("i1r0","d");
		DBApp.createBitMapIndex("i1r0","f");
		DBApp.createBitMapIndex("i1r0","i");
		DBApp.createBitMapIndex("i1r0","j");
		DBApp.createBitMapIndex("i1r0","k");
		DBApp.createBitMapIndex("i1r0","l");
		String [][] records_i1r0 = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			records_i1r0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i1r0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i1r0", records_i1r0[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "d", "d0"), ("100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "f", "f4"), ("000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "i", "i7"), ("000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "j", "j7"), ("000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1r0", "k", "k0"), ("100000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i1r0", "l", "l12"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("hji86", cols0);
		DBApp.createBitMapIndex("hji86","b");
		DBApp.createBitMapIndex("hji86","c");
		DBApp.createBitMapIndex("hji86","d");
		DBApp.createBitMapIndex("hji86","e");
		DBApp.createBitMapIndex("hji86","i");
		DBApp.createBitMapIndex("hji86","j");
		String [][] records_hji86 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_hji86[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hji86[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hji86", records_hji86[i]);
		}
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hji86", "b", "b0"), ("10101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hji86", "c", "c0"), ("10010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hji86", "d", "d0"), ("10001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hji86", "e", "e0"), ("10000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hji86", "i", "i5"), ("00000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hji86", "j", "j10"), ("00000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gf", cols0);
		DBApp.createBitMapIndex("gf","c");
		DBApp.createBitMapIndex("gf","e");
		DBApp.createBitMapIndex("gf","g");
		DBApp.createBitMapIndex("gf","h");
		DBApp.createBitMapIndex("gf","i");
		DBApp.createBitMapIndex("gf","k");
		DBApp.createBitMapIndex("gf","o");
		String [][] records_gf = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			records_gf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gf", records_gf[i]);
		}
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
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "e", "e1"), ("0100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "g", "g5"), ("0000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "h", "h6"), ("0000001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "i", "i0"), ("1000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gf", "k", "k3"), ("0001000000000010000000000100000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gf", "o", "o15"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zjcs", cols0);
		DBApp.createBitMapIndex("zjcs","b");
		DBApp.createBitMapIndex("zjcs","c");
		DBApp.createBitMapIndex("zjcs","e");
		DBApp.createBitMapIndex("zjcs","g");
		String [][] records_zjcs = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			records_zjcs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zjcs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zjcs", records_zjcs[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zjcs", "b", "b0"), ("10101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zjcs", "c", "c0"), ("10010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zjcs", "e", "e3"), ("00010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zjcs", "g", "g7"), ("00000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v1", cols1);
		DBApp.createBitMapIndex("v1","d");
		DBApp.createBitMapIndex("v1","e");
		DBApp.createBitMapIndex("v1","f");
		DBApp.createBitMapIndex("v1","j");
		String [][] records_v1 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_v1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v1", records_v1[i]);
		}
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v1", "d", "d0"), ("10001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v1", "e", "e3"), ("00010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v1", "f", "f0"), ("10000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v1", "j", "j10"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x27qx", cols2);
		DBApp.createBitMapIndex("x27qx","b");
		DBApp.createBitMapIndex("x27qx","c");
		DBApp.createBitMapIndex("x27qx","m");
		DBApp.createBitMapIndex("x27qx","o");
		String [][] records_x27qx = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			records_x27qx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_x27qx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("x27qx", records_x27qx[i]);
		}
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x27qx", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x27qx", "c", "c0"), ("100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x27qx", "m", "m6"), ("000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("x27qx", "o", "o15"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l40", cols3);
		DBApp.createBitMapIndex("l40","b");
		DBApp.createBitMapIndex("l40","e");
		DBApp.createBitMapIndex("l40","g");
		String [][] records_l40 = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			records_l40[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l40[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l40", records_l40[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l40", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l40", "e", "e0"), ("10000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l40", "g", "g7"), ("00000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kjf", cols0);
		DBApp.createBitMapIndex("kjf","c");
		DBApp.createBitMapIndex("kjf","e");
		DBApp.createBitMapIndex("kjf","g");
		String [][] records_kjf = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_kjf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kjf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kjf", records_kjf[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kjf", "c", "c1"), ("01001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kjf", "e", "e3"), ("00010000100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kjf", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w8czj", cols1);
		DBApp.createBitMapIndex("w8czj","b");
		DBApp.createBitMapIndex("w8czj","g");
		String [][] records_w8czj = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			records_w8czj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w8czj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w8czj", records_w8czj[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w8czj", "b", "b0"), ("10"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w8czj", "g", "g7"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u3q", cols2);
		DBApp.createBitMapIndex("u3q","g");
		DBApp.createBitMapIndex("u3q","h");
		DBApp.createBitMapIndex("u3q","j");
		DBApp.createBitMapIndex("u3q","l");
		DBApp.createBitMapIndex("u3q","n");
		String [][] records_u3q = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_u3q[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u3q[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u3q", records_u3q[i]);
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u3q", "g", "g2"), ("0010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u3q", "h", "h3"), ("0001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u3q", "j", "j4"), ("0000100000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u3q", "l", "l8"), ("0000000010000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u3q", "n", "n14"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p6", cols3);
		DBApp.createBitMapIndex("p6","b");
		DBApp.createBitMapIndex("p6","c");
		DBApp.createBitMapIndex("p6","d");
		DBApp.createBitMapIndex("p6","f");
		DBApp.createBitMapIndex("p6","g");
		String [][] records_p6 = new String[17][cols3.length];
		for(int i=0;i<17;i++)
		{
			records_p6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p6", records_p6[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "b", "b0"), ("10101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "c", "c0"), ("10010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "d", "d2"), ("00100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "f", "f1"), ("01000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p6", "g", "g7"), ("00000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("g869", cols4);
		DBApp.createBitMapIndex("g869","b");
		DBApp.createBitMapIndex("g869","c");
		DBApp.createBitMapIndex("g869","d");
		DBApp.createBitMapIndex("g869","e");
		DBApp.createBitMapIndex("g869","f");
		DBApp.createBitMapIndex("g869","h");
		DBApp.createBitMapIndex("g869","i");
		DBApp.createBitMapIndex("g869","j");
		DBApp.createBitMapIndex("g869","k");
		DBApp.createBitMapIndex("g869","l");
		String [][] records_g869 = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			records_g869[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g869[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g869", records_g869[i]);
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
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "b", "b0"), ("1010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "c", "c0"), ("1001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "d", "d0"), ("1000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "e", "e0"), ("1000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "f", "f2"), ("0010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "h", "h2"), ("0010000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "i", "i7"), ("0000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "j", "j6"), ("0000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g869", "k", "k4"), ("0000100000000001000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g869", "l", "l12"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b749", cols0);
		DBApp.createBitMapIndex("b749","b");
		DBApp.createBitMapIndex("b749","e");
		DBApp.createBitMapIndex("b749","g");
		String [][] records_b749 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_b749[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b749[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b749", records_b749[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b749", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b749", "e", "e3"), ("000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b749", "g", "g7"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("au", cols1);
		DBApp.createBitMapIndex("au","i");
		DBApp.createBitMapIndex("au","l");
		DBApp.createBitMapIndex("au","m");
		DBApp.createBitMapIndex("au","n");
		String [][] records_au = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			records_au[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_au[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("au", records_au[i]);
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
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("au", "i", "i6"), ("0000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("au", "l", "l9"), ("0000000001000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("au", "m", "m11"), ("0000000000010000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("au", "n", "n14"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("y8r24", cols2);
		DBApp.createBitMapIndex("y8r24","e");
		DBApp.createBitMapIndex("y8r24","f");
		DBApp.createBitMapIndex("y8r24","g");
		DBApp.createBitMapIndex("y8r24","k");
		DBApp.createBitMapIndex("y8r24","m");
		String [][] records_y8r24 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_y8r24[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y8r24[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y8r24", records_y8r24[i]);
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
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y8r24", "e", "e0"), ("1000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y8r24", "f", "f3"), ("0001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y8r24", "g", "g5"), ("0000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y8r24", "k", "k2"), ("0010000000000100000000001000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y8r24", "m", "m13"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("i1o", cols3);
		DBApp.createBitMapIndex("i1o","b");
		DBApp.createBitMapIndex("i1o","c");
		DBApp.createBitMapIndex("i1o","g");
		DBApp.createBitMapIndex("i1o","h");
		DBApp.createBitMapIndex("i1o","i");
		DBApp.createBitMapIndex("i1o","j");
		DBApp.createBitMapIndex("i1o","n");
		DBApp.createBitMapIndex("i1o","o");
		DBApp.createBitMapIndex("i1o","q");
		String [][] records_i1o = new String[33][cols3.length];
		for(int i=0;i<33;i++)
		{
			records_i1o[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_i1o[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("i1o", records_i1o[i]);
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
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "b", "b0"), ("101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "c", "c1"), ("010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "g", "g5"), ("000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "h", "h1"), ("010000000100000001000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "i", "i0"), ("100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "j", "j0"), ("100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "n", "n2"), ("001000000000000010000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i1o", "o", "o1"), ("010000000000000010000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i1o", "q", "q17"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ev", cols4);
		DBApp.createBitMapIndex("ev","f");
		DBApp.createBitMapIndex("ev","g");
		String [][] records_ev = new String[28][cols4.length];
		for(int i=0;i<28;i++)
		{
			records_ev[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ev[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ev", records_ev[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ev", "f", "f1"), ("0100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ev", "g", "g7"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rt", cols0);
		DBApp.createBitMapIndex("rt","d");
		DBApp.createBitMapIndex("rt","e");
		DBApp.createBitMapIndex("rt","f");
		DBApp.createBitMapIndex("rt","g");
		String [][] records_rt = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_rt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_rt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("rt", records_rt[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rt", "d", "d0"), ("100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rt", "e", "e1"), ("010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rt", "f", "f4"), ("000010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("rt", "g", "g7"), ("000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ub7q", cols1);
		DBApp.createBitMapIndex("ub7q","b");
		DBApp.createBitMapIndex("ub7q","c");
		DBApp.createBitMapIndex("ub7q","e");
		DBApp.createBitMapIndex("ub7q","g");
		DBApp.createBitMapIndex("ub7q","i");
		DBApp.createBitMapIndex("ub7q","j");
		DBApp.createBitMapIndex("ub7q","k");
		DBApp.createBitMapIndex("ub7q","l");
		DBApp.createBitMapIndex("ub7q","m");
		String [][] records_ub7q = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			records_ub7q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ub7q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ub7q", records_ub7q[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "b", "b0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "e", "e1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "g", "g0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "i", "i1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "j", "j2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "k", "k0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ub7q", "l", "l2"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ub7q", "m", "m13"), ("00"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("udwn", cols0);
		DBApp.createBitMapIndex("udwn","c");
		DBApp.createBitMapIndex("udwn","k");
		DBApp.createBitMapIndex("udwn","l");
		DBApp.createBitMapIndex("udwn","m");
		DBApp.createBitMapIndex("udwn","n");
		DBApp.createBitMapIndex("udwn","o");
		DBApp.createBitMapIndex("udwn","p");
		DBApp.createBitMapIndex("udwn","q");
		String [][] records_udwn = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			records_udwn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_udwn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("udwn", records_udwn[i]);
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5, p4, q3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "c", "c1"), ("010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "k", "k6"), ("000000100000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "l", "l1"), ("010000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "m", "m7"), ("000000010000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "n", "n0"), ("100000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "o", "o8"), ("000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("udwn", "p", "p11"), ("000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("udwn", "q", "q17"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("v0ovi", cols1);
		DBApp.createBitMapIndex("v0ovi","b");
		DBApp.createBitMapIndex("v0ovi","d");
		DBApp.createBitMapIndex("v0ovi","e");
		DBApp.createBitMapIndex("v0ovi","g");
		DBApp.createBitMapIndex("v0ovi","h");
		DBApp.createBitMapIndex("v0ovi","i");
		DBApp.createBitMapIndex("v0ovi","j");
		DBApp.createBitMapIndex("v0ovi","k");
		DBApp.createBitMapIndex("v0ovi","l");
		DBApp.createBitMapIndex("v0ovi","m");
		String [][] records_v0ovi = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			records_v0ovi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v0ovi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v0ovi", records_v0ovi[i]);
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
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "b", "b0"), ("101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "d", "d0"), ("100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "e", "e3"), ("000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "g", "g3"), ("000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "h", "h6"), ("000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "i", "i3"), ("000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "j", "j4"), ("000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "k", "k6"), ("000000100000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v0ovi", "l", "l4"), ("000010000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v0ovi", "m", "m13"), ("000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ju8", cols2);
		DBApp.createBitMapIndex("ju8","b");
		DBApp.createBitMapIndex("ju8","c");
		DBApp.createBitMapIndex("ju8","g");
		String [][] records_ju8 = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			records_ju8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ju8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ju8", records_ju8[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ju8", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ju8", "c", "c1"), ("0100100100100100100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ju8", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t41", cols0);
		DBApp.createBitMapIndex("t41","e");
		DBApp.createBitMapIndex("t41","f");
		DBApp.createBitMapIndex("t41","g");
		String [][] records_t41 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_t41[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t41[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t41", records_t41[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t41", "e", "e3"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t41", "f", "f2"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t41", "g", "g7"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("p6wk", cols1);
		DBApp.createBitMapIndex("p6wk","d");
		DBApp.createBitMapIndex("p6wk","e");
		DBApp.createBitMapIndex("p6wk","f");
		DBApp.createBitMapIndex("p6wk","g");
		DBApp.createBitMapIndex("p6wk","h");
		DBApp.createBitMapIndex("p6wk","k");
		DBApp.createBitMapIndex("p6wk","l");
		DBApp.createBitMapIndex("p6wk","m");
		DBApp.createBitMapIndex("p6wk","s");
		String [][] records_p6wk = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			records_p6wk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p6wk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p6wk", records_p6wk[i]);
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
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11, s9]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12, s10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "d", "d2"), ("0010001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "e", "e3"), ("0001000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "f", "f3"), ("0001000001000001000001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "g", "g0"), ("1000000100000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "h", "h2"), ("0010000000100000001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "k", "k9"), ("0000000001000000000010000000000100000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "l", "l10"), ("0000000000100000000000100000000000100000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6wk", "m", "m5"), ("0000010000000000001000000000000100000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p6wk", "s", "s19"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ce9", cols0);
		DBApp.createBitMapIndex("ce9","d");
		DBApp.createBitMapIndex("ce9","e");
		DBApp.createBitMapIndex("ce9","i");
		DBApp.createBitMapIndex("ce9","k");
		DBApp.createBitMapIndex("ce9","p");
		DBApp.createBitMapIndex("ce9","q");
		String [][] records_ce9 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_ce9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ce9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ce9", records_ce9[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ce9", "d", "d2"), ("00100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ce9", "e", "e3"), ("00010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ce9", "i", "i6"), ("00000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ce9", "k", "k5"), ("00000100000000001000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ce9", "p", "p11"), ("00000000000100000000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ce9", "q", "q17"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y31fs", cols1);
		DBApp.createBitMapIndex("y31fs","c");
		DBApp.createBitMapIndex("y31fs","e");
		DBApp.createBitMapIndex("y31fs","g");
		String [][] records_y31fs = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			records_y31fs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y31fs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y31fs", records_y31fs[i]);
		}
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y31fs", "c", "c0"), ("10010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y31fs", "e", "e0"), ("10000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y31fs", "g", "g7"), ("00000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ye8l", cols2);
		DBApp.createBitMapIndex("ye8l","c");
		DBApp.createBitMapIndex("ye8l","d");
		DBApp.createBitMapIndex("ye8l","g");
		DBApp.createBitMapIndex("ye8l","l");
		DBApp.createBitMapIndex("ye8l","o");
		DBApp.createBitMapIndex("ye8l","p");
		DBApp.createBitMapIndex("ye8l","q");
		DBApp.createBitMapIndex("ye8l","r");
		String [][] records_ye8l = new String[33][cols2.length];
		for(int i=0;i<33;i++)
		{
			records_ye8l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ye8l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ye8l", records_ye8l[i]);
		}
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
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "c", "c1"), ("010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "d", "d1"), ("010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "g", "g3"), ("000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "l", "l7"), ("000000010000000000010000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "o", "o2"), ("001000000000000001000000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "p", "p9"), ("000000000100000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ye8l", "q", "q7"), ("000000010000000000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ye8l", "r", "r18"), ("000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("b4xgr", cols3);
		DBApp.createBitMapIndex("b4xgr","b");
		DBApp.createBitMapIndex("b4xgr","d");
		DBApp.createBitMapIndex("b4xgr","e");
		DBApp.createBitMapIndex("b4xgr","f");
		DBApp.createBitMapIndex("b4xgr","g");
		DBApp.createBitMapIndex("b4xgr","i");
		DBApp.createBitMapIndex("b4xgr","j");
		DBApp.createBitMapIndex("b4xgr","l");
		DBApp.createBitMapIndex("b4xgr","m");
		String [][] records_b4xgr = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			records_b4xgr[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b4xgr[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b4xgr", records_b4xgr[i]);
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
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "b", "b0"), ("1010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "d", "d2"), ("0010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "e", "e3"), ("0001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "f", "f1"), ("0100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "g", "g0"), ("1000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "i", "i0"), ("1000000001000000001000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "j", "j1"), ("0100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b4xgr", "l", "l8"), ("0000000010000000000010000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b4xgr", "m", "m13"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("huvk9", cols0);
		DBApp.createBitMapIndex("huvk9","b");
		DBApp.createBitMapIndex("huvk9","j");
		DBApp.createBitMapIndex("huvk9","k");
		DBApp.createBitMapIndex("huvk9","l");
		DBApp.createBitMapIndex("huvk9","m");
		DBApp.createBitMapIndex("huvk9","o");
		DBApp.createBitMapIndex("huvk9","q");
		DBApp.createBitMapIndex("huvk9","t");
		String [][] records_huvk9 = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_huvk9[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_huvk9[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("huvk9", records_huvk9[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15, s15, t15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16, s16, t16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17, s17, t17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0, s18, t18]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "j", "j7"), ("0000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "k", "k5"), ("0000010000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "l", "l10"), ("0000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "m", "m4"), ("0000100000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "o", "o5"), ("0000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("huvk9", "q", "q4"), ("0000100000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("huvk9", "t", "t20"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i4m7", cols1);
		DBApp.createBitMapIndex("i4m7","d");
		DBApp.createBitMapIndex("i4m7","e");
		DBApp.createBitMapIndex("i4m7","f");
		DBApp.createBitMapIndex("i4m7","g");
		String [][] records_i4m7 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_i4m7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i4m7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i4m7", records_i4m7[i]);
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
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4m7", "d", "d2"), ("001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4m7", "e", "e1"), ("010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4m7", "f", "f1"), ("010000010000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i4m7", "g", "g7"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vp2", cols2);
		DBApp.createBitMapIndex("vp2","b");
		DBApp.createBitMapIndex("vp2","d");
		DBApp.createBitMapIndex("vp2","g");
		DBApp.createBitMapIndex("vp2","h");
		String [][] records_vp2 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			records_vp2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vp2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vp2", records_vp2[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vp2", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vp2", "d", "d2"), ("0010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vp2", "g", "g3"), ("0001000000100000010000001000000100000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vp2", "h", "h8"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dca0", cols3);
		DBApp.createBitMapIndex("dca0","b");
		DBApp.createBitMapIndex("dca0","e");
		DBApp.createBitMapIndex("dca0","g");
		String [][] records_dca0 = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			records_dca0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_dca0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("dca0", records_dca0[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dca0", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dca0", "e", "e0"), ("100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dca0", "g", "g7"), ("000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y2po6", cols0);
		DBApp.createBitMapIndex("y2po6","b");
		DBApp.createBitMapIndex("y2po6","g");
		String [][] records_y2po6 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			records_y2po6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y2po6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y2po6", records_y2po6[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y2po6", "b", "b0"), ("1010101"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y2po6", "g", "g7"), ("0000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ydmar", cols1);
		DBApp.createBitMapIndex("ydmar","b");
		DBApp.createBitMapIndex("ydmar","c");
		DBApp.createBitMapIndex("ydmar","d");
		DBApp.createBitMapIndex("ydmar","e");
		DBApp.createBitMapIndex("ydmar","g");
		DBApp.createBitMapIndex("ydmar","h");
		DBApp.createBitMapIndex("ydmar","i");
		DBApp.createBitMapIndex("ydmar","l");
		DBApp.createBitMapIndex("ydmar","m");
		DBApp.createBitMapIndex("ydmar","o");
		DBApp.createBitMapIndex("ydmar","q");
		DBApp.createBitMapIndex("ydmar","r");
		DBApp.createBitMapIndex("ydmar","s");
		String [][] records_ydmar = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_ydmar[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ydmar[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ydmar", records_ydmar[i]);
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
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11, s9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "c", "c1"), ("010010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "d", "d1"), ("010001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "e", "e2"), ("001000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "g", "g0"), ("100000010000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "h", "h6"), ("000000100000001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "i", "i6"), ("000000100000000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "l", "l1"), ("010000000000010000000000010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "m", "m2"), ("001000000000000100000000000010000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "o", "o8"), ("000000001000000000000001000000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "q", "q10"), ("000000000010000000000000000100000000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ydmar", "r", "r0"), ("100000000000000000100000000000000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ydmar", "s", "s19"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("m18", cols2);
		DBApp.createBitMapIndex("m18","d");
		DBApp.createBitMapIndex("m18","e");
		DBApp.createBitMapIndex("m18","g");
		DBApp.createBitMapIndex("m18","h");
		DBApp.createBitMapIndex("m18","k");
		DBApp.createBitMapIndex("m18","l");
		DBApp.createBitMapIndex("m18","m");
		DBApp.createBitMapIndex("m18","o");
		DBApp.createBitMapIndex("m18","p");
		DBApp.createBitMapIndex("m18","q");
		DBApp.createBitMapIndex("m18","r");
		String [][] records_m18 = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			records_m18[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m18[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m18", records_m18[i]);
		}
		//first 13 records:
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
		//last 13 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "d", "d1"), ("0100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "e", "e0"), ("1000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "g", "g1"), ("0100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "h", "h2"), ("0010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "k", "k4"), ("0000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "l", "l10"), ("0000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "m", "m3"), ("0001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "o", "o3"), ("0001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "p", "p8"), ("0000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m18", "q", "q1"), ("0100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m18", "r", "r18"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o5h", cols3);
		DBApp.createBitMapIndex("o5h","d");
		DBApp.createBitMapIndex("o5h","g");
		DBApp.createBitMapIndex("o5h","k");
		DBApp.createBitMapIndex("o5h","l");
		DBApp.createBitMapIndex("o5h","m");
		DBApp.createBitMapIndex("o5h","n");
		String [][] records_o5h = new String[40][cols3.length];
		for(int i=0;i<40;i++)
		{
			records_o5h[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_o5h[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("o5h", records_o5h[i]);
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
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o5h", "d", "d1"), ("0100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o5h", "g", "g2"), ("0010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o5h", "k", "k9"), ("0000000001000000000010000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o5h", "l", "l4"), ("0000100000000000100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o5h", "m", "m9"), ("0000000001000000000000100000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o5h", "n", "n14"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tzx", cols0);
		DBApp.createBitMapIndex("tzx","d");
		DBApp.createBitMapIndex("tzx","e");
		DBApp.createBitMapIndex("tzx","g");
		String [][] records_tzx = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_tzx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tzx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tzx", records_tzx[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tzx", "d", "d2"), ("001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tzx", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tzx", "g", "g7"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l30k", cols1);
		DBApp.createBitMapIndex("l30k","b");
		DBApp.createBitMapIndex("l30k","c");
		DBApp.createBitMapIndex("l30k","e");
		DBApp.createBitMapIndex("l30k","g");
		DBApp.createBitMapIndex("l30k","i");
		String [][] records_l30k = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_l30k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l30k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l30k", records_l30k[i]);
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
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l30k", "b", "b0"), ("101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l30k", "c", "c0"), ("100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l30k", "e", "e1"), ("010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l30k", "g", "g0"), ("100000010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l30k", "i", "i9"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ugti", cols2);
		DBApp.createBitMapIndex("ugti","c");
		DBApp.createBitMapIndex("ugti","d");
		DBApp.createBitMapIndex("ugti","e");
		DBApp.createBitMapIndex("ugti","g");
		DBApp.createBitMapIndex("ugti","k");
		String [][] records_ugti = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			records_ugti[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ugti[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ugti", records_ugti[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ugti", "c", "c1"), ("0100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ugti", "d", "d1"), ("0100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ugti", "e", "e3"), ("0001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ugti", "g", "g0"), ("1000000100000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ugti", "k", "k11"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zf095", cols3);
		DBApp.createBitMapIndex("zf095","g");
		String [][] records_zf095 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			records_zf095[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_zf095[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("zf095", records_zf095[i]);
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
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zf095", "g", "g7"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("wcx45", cols4);
		DBApp.createBitMapIndex("wcx45","d");
		DBApp.createBitMapIndex("wcx45","e");
		DBApp.createBitMapIndex("wcx45","f");
		DBApp.createBitMapIndex("wcx45","g");
		DBApp.createBitMapIndex("wcx45","h");
		DBApp.createBitMapIndex("wcx45","i");
		DBApp.createBitMapIndex("wcx45","n");
		DBApp.createBitMapIndex("wcx45","o");
		DBApp.createBitMapIndex("wcx45","p");
		DBApp.createBitMapIndex("wcx45","q");
		DBApp.createBitMapIndex("wcx45","r");
		String [][] records_wcx45 = new String[1][cols4.length];
		for(int i=0;i<1;i++)
		{
			records_wcx45[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_wcx45[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("wcx45", records_wcx45[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "d", "d0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "e", "e2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "f", "f4"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "g", "g5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "h", "h1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "i", "i4"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "n", "n8"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "o", "o13"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "p", "p1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wcx45", "q", "q4"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wcx45", "r", "r18"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("b50", cols0);
		DBApp.createBitMapIndex("b50","f");
		DBApp.createBitMapIndex("b50","h");
		DBApp.createBitMapIndex("b50","i");
		DBApp.createBitMapIndex("b50","k");
		DBApp.createBitMapIndex("b50","p");
		String [][] records_b50 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_b50[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b50[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b50", records_b50[i]);
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
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b50", "f", "f1"), ("0100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b50", "h", "h5"), ("0000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b50", "i", "i4"), ("0000100000000100000000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b50", "k", "k2"), ("0010000000000100000000001000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b50", "p", "p16"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j61", cols1);
		DBApp.createBitMapIndex("j61","c");
		DBApp.createBitMapIndex("j61","d");
		DBApp.createBitMapIndex("j61","e");
		DBApp.createBitMapIndex("j61","f");
		DBApp.createBitMapIndex("j61","g");
		String [][] records_j61 = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			records_j61[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j61[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j61", records_j61[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j61", "c", "c0"), ("10010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j61", "d", "d1"), ("01000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j61", "e", "e2"), ("00100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j61", "f", "f2"), ("00100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j61", "g", "g7"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("li1z", cols2);
		DBApp.createBitMapIndex("li1z","b");
		DBApp.createBitMapIndex("li1z","c");
		DBApp.createBitMapIndex("li1z","e");
		DBApp.createBitMapIndex("li1z","f");
		DBApp.createBitMapIndex("li1z","h");
		DBApp.createBitMapIndex("li1z","j");
		DBApp.createBitMapIndex("li1z","k");
		DBApp.createBitMapIndex("li1z","l");
		DBApp.createBitMapIndex("li1z","n");
		String [][] records_li1z = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			records_li1z[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_li1z[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("li1z", records_li1z[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "c", "c1"), ("010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "e", "e1"), ("010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "f", "f2"), ("001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "h", "h6"), ("000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "j", "j0"), ("100000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "k", "k1"), ("010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("li1z", "l", "l10"), ("000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("li1z", "n", "n14"), ("000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h65", cols0);
		DBApp.createBitMapIndex("h65","g");
		DBApp.createBitMapIndex("h65","h");
		DBApp.createBitMapIndex("h65","k");
		DBApp.createBitMapIndex("h65","l");
		DBApp.createBitMapIndex("h65","n");
		DBApp.createBitMapIndex("h65","o");
		DBApp.createBitMapIndex("h65","p");
		String [][] records_h65 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			records_h65[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h65[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h65", records_h65[i]);
		}
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "g", "g0"), ("100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "h", "h0"), ("100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "k", "k2"), ("001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "l", "l5"), ("000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "n", "n12"), ("000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h65", "o", "o4"), ("000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h65", "p", "p16"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("w53c6", cols1);
		DBApp.createBitMapIndex("w53c6","c");
		DBApp.createBitMapIndex("w53c6","d");
		DBApp.createBitMapIndex("w53c6","g");
		DBApp.createBitMapIndex("w53c6","k");
		DBApp.createBitMapIndex("w53c6","l");
		DBApp.createBitMapIndex("w53c6","m");
		DBApp.createBitMapIndex("w53c6","q");
		DBApp.createBitMapIndex("w53c6","r");
		String [][] records_w53c6 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_w53c6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w53c6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w53c6", records_w53c6[i]);
		}
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
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "c", "c1"), ("0100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "d", "d1"), ("0100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "g", "g1"), ("0100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "k", "k9"), ("0000000001000000000010000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "l", "l3"), ("0001000000000001000000000001000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "m", "m3"), ("0001000000000000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w53c6", "q", "q6"), ("0000001000000000000000010000000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w53c6", "r", "r18"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("c0ta", cols2);
		DBApp.createBitMapIndex("c0ta","c");
		DBApp.createBitMapIndex("c0ta","d");
		DBApp.createBitMapIndex("c0ta","e");
		DBApp.createBitMapIndex("c0ta","f");
		DBApp.createBitMapIndex("c0ta","g");
		DBApp.createBitMapIndex("c0ta","i");
		String [][] records_c0ta = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_c0ta[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c0ta[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c0ta", records_c0ta[i]);
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
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c0ta", "c", "c1"), ("0100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c0ta", "d", "d1"), ("0100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c0ta", "e", "e2"), ("0010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c0ta", "f", "f0"), ("1000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c0ta", "g", "g1"), ("0100000010000001000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c0ta", "i", "i9"), ("0000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kgg", cols3);
		DBApp.createBitMapIndex("kgg","c");
		DBApp.createBitMapIndex("kgg","d");
		DBApp.createBitMapIndex("kgg","e");
		DBApp.createBitMapIndex("kgg","g");
		String [][] records_kgg = new String[48][cols3.length];
		for(int i=0;i<48;i++)
		{
			records_kgg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kgg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kgg", records_kgg[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kgg", "c", "c0"), ("100100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kgg", "d", "d0"), ("100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kgg", "e", "e0"), ("100001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kgg", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dztv", cols4);
		DBApp.createBitMapIndex("dztv","c");
		DBApp.createBitMapIndex("dztv","e");
		DBApp.createBitMapIndex("dztv","g");
		String [][] records_dztv = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			records_dztv[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_dztv[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("dztv", records_dztv[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dztv", "c", "c0"), ("100100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dztv", "e", "e1"), ("010000100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dztv", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("af0", cols0);
		DBApp.createBitMapIndex("af0","b");
		DBApp.createBitMapIndex("af0","e");
		DBApp.createBitMapIndex("af0","f");
		DBApp.createBitMapIndex("af0","h");
		DBApp.createBitMapIndex("af0","l");
		DBApp.createBitMapIndex("af0","n");
		DBApp.createBitMapIndex("af0","o");
		String [][] records_af0 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			records_af0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_af0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("af0", records_af0[i]);
		}
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "b", "b0"), ("10101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "e", "e3"), ("00010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "f", "f2"), ("00100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "h", "h1"), ("01000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "l", "l2"), ("00100000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af0", "n", "n11"), ("00000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("af0", "o", "o15"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l98h", cols1);
		DBApp.createBitMapIndex("l98h","b");
		DBApp.createBitMapIndex("l98h","c");
		DBApp.createBitMapIndex("l98h","f");
		DBApp.createBitMapIndex("l98h","g");
		String [][] records_l98h = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_l98h[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l98h[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l98h", records_l98h[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l98h", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l98h", "c", "c0"), ("1001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l98h", "f", "f3"), ("0001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l98h", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("nl", cols2);
		DBApp.createBitMapIndex("nl","b");
		DBApp.createBitMapIndex("nl","g");
		DBApp.createBitMapIndex("nl","h");
		DBApp.createBitMapIndex("nl","i");
		DBApp.createBitMapIndex("nl","j");
		DBApp.createBitMapIndex("nl","l");
		String [][] records_nl = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			records_nl[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nl[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nl", records_nl[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nl", "b", "b0"), ("101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nl", "g", "g1"), ("010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nl", "h", "h2"), ("001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nl", "i", "i2"), ("001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nl", "j", "j4"), ("000010000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nl", "l", "l12"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l0858", cols0);
		DBApp.createBitMapIndex("l0858","b");
		DBApp.createBitMapIndex("l0858","c");
		DBApp.createBitMapIndex("l0858","f");
		DBApp.createBitMapIndex("l0858","g");
		DBApp.createBitMapIndex("l0858","h");
		DBApp.createBitMapIndex("l0858","i");
		DBApp.createBitMapIndex("l0858","k");
		DBApp.createBitMapIndex("l0858","m");
		String [][] records_l0858 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_l0858[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l0858[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l0858", records_l0858[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "b", "b0"), ("101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "f", "f0"), ("100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "g", "g2"), ("001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "h", "h3"), ("000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "i", "i4"), ("000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l0858", "k", "k6"), ("000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l0858", "m", "m13"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u40", cols0);
		DBApp.createBitMapIndex("u40","b");
		DBApp.createBitMapIndex("u40","c");
		DBApp.createBitMapIndex("u40","d");
		DBApp.createBitMapIndex("u40","i");
		DBApp.createBitMapIndex("u40","k");
		DBApp.createBitMapIndex("u40","m");
		DBApp.createBitMapIndex("u40","n");
		String [][] records_u40 = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_u40[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u40[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u40", records_u40[i]);
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
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "b", "b0"), ("10101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "c", "c1"), ("01001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "d", "d0"), ("10001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "i", "i7"), ("00000001000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "k", "k7"), ("00000001000000000010000000000100000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u40", "m", "m5"), ("00000100000000000010000000000001000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u40", "n", "n14"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("xps", cols0);
		DBApp.createBitMapIndex("xps","g");
		DBApp.createBitMapIndex("xps","h");
		DBApp.createBitMapIndex("xps","k");
		DBApp.createBitMapIndex("xps","l");
		DBApp.createBitMapIndex("xps","n");
		DBApp.createBitMapIndex("xps","q");
		String [][] records_xps = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_xps[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xps[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xps", records_xps[i]);
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xps", "g", "g0"), ("1000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xps", "h", "h1"), ("0100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xps", "k", "k0"), ("1000000000010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xps", "l", "l9"), ("0000000001000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xps", "n", "n8"), ("0000000010000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xps", "q", "q17"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("bzxg", cols1);
		DBApp.createBitMapIndex("bzxg","b");
		DBApp.createBitMapIndex("bzxg","d");
		DBApp.createBitMapIndex("bzxg","h");
		DBApp.createBitMapIndex("bzxg","k");
		DBApp.createBitMapIndex("bzxg","m");
		DBApp.createBitMapIndex("bzxg","n");
		DBApp.createBitMapIndex("bzxg","q");
		DBApp.createBitMapIndex("bzxg","r");
		DBApp.createBitMapIndex("bzxg","s");
		String [][] records_bzxg = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			records_bzxg[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bzxg[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bzxg", records_bzxg[i]);
		}
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "b", "b0"), ("1010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "d", "d1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "h", "h4"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "k", "k3"), ("0001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "m", "m4"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "n", "n6"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "q", "q11"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bzxg", "r", "r3"), ("0001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bzxg", "s", "s19"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("n0u", cols2);
		DBApp.createBitMapIndex("n0u","c");
		DBApp.createBitMapIndex("n0u","e");
		DBApp.createBitMapIndex("n0u","j");
		DBApp.createBitMapIndex("n0u","k");
		String [][] records_n0u = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			records_n0u[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n0u[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n0u", records_n0u[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n0u", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n0u", "e", "e0"), ("100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n0u", "j", "j3"), ("000100000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n0u", "k", "k11"), ("000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f8181", cols3);
		DBApp.createBitMapIndex("f8181","c");
		DBApp.createBitMapIndex("f8181","d");
		DBApp.createBitMapIndex("f8181","f");
		DBApp.createBitMapIndex("f8181","g");
		String [][] records_f8181 = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			records_f8181[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f8181[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f8181", records_f8181[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8181", "c", "c1"), ("01001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8181", "d", "d2"), ("00100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f8181", "f", "f0"), ("10000010000010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f8181", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ww3tt", cols4);
		DBApp.createBitMapIndex("ww3tt","c");
		DBApp.createBitMapIndex("ww3tt","e");
		DBApp.createBitMapIndex("ww3tt","f");
		DBApp.createBitMapIndex("ww3tt","m");
		String [][] records_ww3tt = new String[14][cols4.length];
		for(int i=0;i<14;i++)
		{
			records_ww3tt[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_ww3tt[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("ww3tt", records_ww3tt[i]);
		}
		//first 14 records:
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
		//last 14 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ww3tt", "c", "c0"), ("10010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ww3tt", "e", "e3"), ("00010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ww3tt", "f", "f2"), ("00100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ww3tt", "m", "m13"), ("00000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vo1", cols0);
		DBApp.createBitMapIndex("vo1","c");
		DBApp.createBitMapIndex("vo1","e");
		DBApp.createBitMapIndex("vo1","f");
		DBApp.createBitMapIndex("vo1","g");
		String [][] records_vo1 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			records_vo1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vo1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vo1", records_vo1[i]);
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
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		//record35: [a35, b1, c2, d3, e0, f5, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vo1", "c", "c0"), ("100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vo1", "e", "e1"), ("010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vo1", "f", "f2"), ("001000001000001000001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vo1", "g", "g7"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("n6", cols1);
		DBApp.createBitMapIndex("n6","c");
		DBApp.createBitMapIndex("n6","d");
		DBApp.createBitMapIndex("n6","j");
		DBApp.createBitMapIndex("n6","l");
		DBApp.createBitMapIndex("n6","s");
		String [][] records_n6 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			records_n6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_n6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("n6", records_n6[i]);
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
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n6", "c", "c0"), ("100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n6", "d", "d0"), ("100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n6", "j", "j5"), ("000001000000000100000000010000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n6", "l", "l3"), ("000100000000000100000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n6", "s", "s19"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("lyk1", cols2);
		DBApp.createBitMapIndex("lyk1","b");
		DBApp.createBitMapIndex("lyk1","c");
		DBApp.createBitMapIndex("lyk1","d");
		DBApp.createBitMapIndex("lyk1","j");
		String [][] records_lyk1 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			records_lyk1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_lyk1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("lyk1", records_lyk1[i]);
		}
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lyk1", "b", "b0"), ("10101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lyk1", "c", "c0"), ("10010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lyk1", "d", "d2"), ("00100010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lyk1", "j", "j10"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("c70", cols3);
		DBApp.createBitMapIndex("c70","b");
		DBApp.createBitMapIndex("c70","d");
		DBApp.createBitMapIndex("c70","f");
		DBApp.createBitMapIndex("c70","h");
		DBApp.createBitMapIndex("c70","i");
		String [][] records_c70 = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			records_c70[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c70[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c70", records_c70[i]);
		}
		//first 13 records:
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
		//last 13 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c70", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c70", "d", "d1"), ("0100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c70", "f", "f2"), ("0010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c70", "h", "h6"), ("0000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c70", "i", "i9"), ("0000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s8f2u", cols0);
		DBApp.createBitMapIndex("s8f2u","b");
		DBApp.createBitMapIndex("s8f2u","c");
		DBApp.createBitMapIndex("s8f2u","e");
		DBApp.createBitMapIndex("s8f2u","g");
		DBApp.createBitMapIndex("s8f2u","h");
		DBApp.createBitMapIndex("s8f2u","i");
		String [][] records_s8f2u = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_s8f2u[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s8f2u[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s8f2u", records_s8f2u[i]);
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
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s8f2u", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s8f2u", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s8f2u", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s8f2u", "g", "g1"), ("010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s8f2u", "h", "h0"), ("100000001000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s8f2u", "i", "i9"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("t4r", cols1);
		DBApp.createBitMapIndex("t4r","b");
		DBApp.createBitMapIndex("t4r","c");
		DBApp.createBitMapIndex("t4r","d");
		DBApp.createBitMapIndex("t4r","e");
		DBApp.createBitMapIndex("t4r","j");
		DBApp.createBitMapIndex("t4r","k");
		String [][] records_t4r = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			records_t4r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t4r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t4r", records_t4r[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t4r", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t4r", "c", "c0"), ("1001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t4r", "d", "d2"), ("0010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t4r", "e", "e3"), ("0001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t4r", "j", "j6"), ("0000001000000000100000000010000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t4r", "k", "k11"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("oy", cols0);
		DBApp.createBitMapIndex("oy","c");
		DBApp.createBitMapIndex("oy","d");
		DBApp.createBitMapIndex("oy","e");
		DBApp.createBitMapIndex("oy","f");
		DBApp.createBitMapIndex("oy","g");
		DBApp.createBitMapIndex("oy","h");
		DBApp.createBitMapIndex("oy","i");
		DBApp.createBitMapIndex("oy","j");
		DBApp.createBitMapIndex("oy","k");
		DBApp.createBitMapIndex("oy","l");
		DBApp.createBitMapIndex("oy","m");
		DBApp.createBitMapIndex("oy","p");
		String [][] records_oy = new String[44][cols0.length];
		for(int i=0;i<44;i++)
		{
			records_oy[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oy[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oy", records_oy[i]);
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
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "c", "c0"), ("10010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "d", "d0"), ("10001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "e", "e0"), ("10000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "f", "f1"), ("01000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "g", "g4"), ("00001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "h", "h6"), ("00000010000000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "i", "i0"), ("10000000010000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "j", "j6"), ("00000010000000001000000000100000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "k", "k0"), ("10000000000100000000001000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "l", "l9"), ("00000000010000000000010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oy", "m", "m8"), ("00000000100000000000010000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oy", "p", "p16"), ("00000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("e7g", cols1);
		DBApp.createBitMapIndex("e7g","b");
		DBApp.createBitMapIndex("e7g","d");
		DBApp.createBitMapIndex("e7g","g");
		DBApp.createBitMapIndex("e7g","l");
		DBApp.createBitMapIndex("e7g","m");
		DBApp.createBitMapIndex("e7g","o");
		String [][] records_e7g = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			records_e7g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e7g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e7g", records_e7g[i]);
		}
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e7g", "b", "b0"), ("101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e7g", "d", "d0"), ("100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e7g", "g", "g0"), ("100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e7g", "l", "l5"), ("000001000000000001000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e7g", "m", "m0"), ("100000000000010000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e7g", "o", "o15"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bt", cols2);
		DBApp.createBitMapIndex("bt","b");
		DBApp.createBitMapIndex("bt","d");
		DBApp.createBitMapIndex("bt","e");
		DBApp.createBitMapIndex("bt","f");
		DBApp.createBitMapIndex("bt","g");
		String [][] records_bt = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			records_bt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bt", records_bt[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bt", "b", "b0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bt", "d", "d1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bt", "e", "e3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bt", "f", "f4"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bt", "g", "g7"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m1n", cols0);
		DBApp.createBitMapIndex("m1n","c");
		DBApp.createBitMapIndex("m1n","d");
		DBApp.createBitMapIndex("m1n","g");
		String [][] records_m1n = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_m1n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m1n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m1n", records_m1n[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m1n", "c", "c0"), ("100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m1n", "d", "d0"), ("100010001000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m1n", "g", "g7"), ("000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j7rji", cols1);
		DBApp.createBitMapIndex("j7rji","g");
		String [][] records_j7rji = new String[30][cols1.length];
		for(int i=0;i<30;i++)
		{
			records_j7rji[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j7rji[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j7rji", records_j7rji[i]);
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
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j7rji", "g", "g7"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("mb4", cols2);
		DBApp.createBitMapIndex("mb4","c");
		DBApp.createBitMapIndex("mb4","d");
		DBApp.createBitMapIndex("mb4","e");
		DBApp.createBitMapIndex("mb4","f");
		DBApp.createBitMapIndex("mb4","g");
		DBApp.createBitMapIndex("mb4","j");
		DBApp.createBitMapIndex("mb4","l");
		DBApp.createBitMapIndex("mb4","m");
		DBApp.createBitMapIndex("mb4","o");
		DBApp.createBitMapIndex("mb4","p");
		String [][] records_mb4 = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_mb4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mb4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mb4", records_mb4[i]);
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
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6, p5]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "c", "c0"), ("100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "d", "d0"), ("100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "e", "e1"), ("010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "f", "f4"), ("000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "g", "g1"), ("010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "j", "j4"), ("000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "l", "l1"), ("010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "m", "m11"), ("000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mb4", "o", "o10"), ("000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mb4", "p", "p16"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r9", cols3);
		DBApp.createBitMapIndex("r9","b");
		DBApp.createBitMapIndex("r9","c");
		DBApp.createBitMapIndex("r9","d");
		DBApp.createBitMapIndex("r9","f");
		DBApp.createBitMapIndex("r9","g");
		String [][] records_r9 = new String[26][cols3.length];
		for(int i=0;i<26;i++)
		{
			records_r9[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r9[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r9", records_r9[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r9", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r9", "c", "c1"), ("01001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r9", "d", "d1"), ("01000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r9", "f", "f1"), ("01000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r9", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s61", cols4);
		DBApp.createBitMapIndex("s61","b");
		DBApp.createBitMapIndex("s61","f");
		DBApp.createBitMapIndex("s61","g");
		String [][] records_s61 = new String[26][cols4.length];
		for(int i=0;i<26;i++)
		{
			records_s61[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_s61[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("s61", records_s61[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s61", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s61", "f", "f1"), ("01000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s61", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oa1v2", cols0);
		DBApp.createBitMapIndex("oa1v2","g");
		String [][] records_oa1v2 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_oa1v2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oa1v2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oa1v2", records_oa1v2[i]);
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
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oa1v2", "g", "g7"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ik", cols1);
		DBApp.createBitMapIndex("ik","c");
		DBApp.createBitMapIndex("ik","d");
		DBApp.createBitMapIndex("ik","g");
		String [][] records_ik = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_ik[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ik[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ik", records_ik[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ik", "c", "c0"), ("100100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ik", "d", "d0"), ("100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ik", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mzhs4", cols2);
		DBApp.createBitMapIndex("mzhs4","b");
		DBApp.createBitMapIndex("mzhs4","d");
		DBApp.createBitMapIndex("mzhs4","e");
		DBApp.createBitMapIndex("mzhs4","f");
		DBApp.createBitMapIndex("mzhs4","h");
		DBApp.createBitMapIndex("mzhs4","i");
		DBApp.createBitMapIndex("mzhs4","j");
		DBApp.createBitMapIndex("mzhs4","m");
		String [][] records_mzhs4 = new String[8][cols2.length];
		for(int i=0;i<8;i++)
		{
			records_mzhs4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mzhs4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mzhs4", records_mzhs4[i]);
		}
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "b", "b0"), ("10101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "d", "d0"), ("10001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "e", "e2"), ("00100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "f", "f0"), ("10000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "h", "h3"), ("00010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "i", "i5"), ("00000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzhs4", "j", "j7"), ("00000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mzhs4", "m", "m13"), ("00000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h1y", cols3);
		DBApp.createBitMapIndex("h1y","b");
		DBApp.createBitMapIndex("h1y","d");
		DBApp.createBitMapIndex("h1y","e");
		DBApp.createBitMapIndex("h1y","f");
		DBApp.createBitMapIndex("h1y","g");
		String [][] records_h1y = new String[14][cols3.length];
		for(int i=0;i<14;i++)
		{
			records_h1y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h1y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h1y", records_h1y[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1y", "b", "b0"), ("10101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1y", "d", "d2"), ("00100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1y", "e", "e3"), ("00010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1y", "f", "f0"), ("10000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h1y", "g", "g7"), ("00000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p4", cols4);
		DBApp.createBitMapIndex("p4","c");
		DBApp.createBitMapIndex("p4","g");
		DBApp.createBitMapIndex("p4","h");
		DBApp.createBitMapIndex("p4","k");
		DBApp.createBitMapIndex("p4","l");
		DBApp.createBitMapIndex("p4","n");
		DBApp.createBitMapIndex("p4","o");
		String [][] records_p4 = new String[36][cols4.length];
		for(int i=0;i<36;i++)
		{
			records_p4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_p4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("p4", records_p4[i]);
		}
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "c", "c0"), ("100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "g", "g4"), ("000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "h", "h6"), ("000000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "k", "k1"), ("010000000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "l", "l1"), ("010000000000010000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p4", "n", "n12"), ("000000000000100000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p4", "o", "o15"), ("000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_33() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nxw3n", cols0);
		DBApp.createBitMapIndex("nxw3n","b");
		DBApp.createBitMapIndex("nxw3n","c");
		DBApp.createBitMapIndex("nxw3n","d");
		DBApp.createBitMapIndex("nxw3n","e");
		DBApp.createBitMapIndex("nxw3n","g");
		String [][] records_nxw3n = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_nxw3n[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nxw3n[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nxw3n", records_nxw3n[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nxw3n", "b", "b0"), ("1010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nxw3n", "c", "c0"), ("1001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nxw3n", "d", "d1"), ("0100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nxw3n", "e", "e2"), ("0010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nxw3n", "g", "g7"), ("0000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h5nz3", cols1);
		DBApp.createBitMapIndex("h5nz3","b");
		DBApp.createBitMapIndex("h5nz3","d");
		DBApp.createBitMapIndex("h5nz3","f");
		DBApp.createBitMapIndex("h5nz3","g");
		DBApp.createBitMapIndex("h5nz3","h");
		DBApp.createBitMapIndex("h5nz3","i");
		String [][] records_h5nz3 = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			records_h5nz3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h5nz3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h5nz3", records_h5nz3[i]);
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
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h5nz3", "b", "b0"), ("10101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h5nz3", "d", "d2"), ("00100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h5nz3", "f", "f4"), ("00001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h5nz3", "g", "g0"), ("10000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h5nz3", "h", "h4"), ("00001000000010000000100000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h5nz3", "i", "i9"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l47t", cols2);
		DBApp.createBitMapIndex("l47t","d");
		DBApp.createBitMapIndex("l47t","f");
		DBApp.createBitMapIndex("l47t","g");
		String [][] records_l47t = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_l47t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l47t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l47t", records_l47t[i]);
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
		//record45: [a45, b1, c0, d1, e0, f3, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l47t", "d", "d0"), ("1000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l47t", "f", "f1"), ("0100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l47t", "g", "g7"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_34() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("kv21", cols0);
		DBApp.createBitMapIndex("kv21","b");
		DBApp.createBitMapIndex("kv21","e");
		DBApp.createBitMapIndex("kv21","i");
		DBApp.createBitMapIndex("kv21","j");
		DBApp.createBitMapIndex("kv21","k");
		DBApp.createBitMapIndex("kv21","l");
		DBApp.createBitMapIndex("kv21","n");
		DBApp.createBitMapIndex("kv21","o");
		DBApp.createBitMapIndex("kv21","p");
		DBApp.createBitMapIndex("kv21","q");
		DBApp.createBitMapIndex("kv21","r");
		DBApp.createBitMapIndex("kv21","s");
		String [][] records_kv21 = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_kv21[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kv21[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kv21", records_kv21[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "b", "b0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "e", "e1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "i", "i3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "j", "j4"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "k", "k1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "l", "l10"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "n", "n12"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "o", "o5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "p", "p7"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "q", "q11"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv21", "r", "r13"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kv21", "s", "s19"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g3dau", cols0);
		DBApp.createBitMapIndex("g3dau","b");
		DBApp.createBitMapIndex("g3dau","h");
		String [][] records_g3dau = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			records_g3dau[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g3dau[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g3dau", records_g3dau[i]);
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
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g3dau", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g3dau", "h", "h8"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e6g3", cols1);
		DBApp.createBitMapIndex("e6g3","b");
		DBApp.createBitMapIndex("e6g3","e");
		DBApp.createBitMapIndex("e6g3","g");
		String [][] records_e6g3 = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_e6g3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e6g3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e6g3", records_e6g3[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e6g3", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e6g3", "e", "e2"), ("001000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e6g3", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("oz67p", cols2);
		DBApp.createBitMapIndex("oz67p","b");
		DBApp.createBitMapIndex("oz67p","d");
		DBApp.createBitMapIndex("oz67p","e");
		DBApp.createBitMapIndex("oz67p","f");
		DBApp.createBitMapIndex("oz67p","h");
		DBApp.createBitMapIndex("oz67p","k");
		DBApp.createBitMapIndex("oz67p","l");
		DBApp.createBitMapIndex("oz67p","o");
		DBApp.createBitMapIndex("oz67p","r");
		DBApp.createBitMapIndex("oz67p","s");
		String [][] records_oz67p = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_oz67p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_oz67p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("oz67p", records_oz67p[i]);
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9, s7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "d", "d1"), ("0100010001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "e", "e1"), ("0100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "f", "f3"), ("0001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "h", "h6"), ("0000001000000010000000100000001000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "k", "k8"), ("0000000010000000000100000000001000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "l", "l8"), ("0000000010000000000010000000000010000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "o", "o8"), ("0000000010000000000000010000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oz67p", "r", "r3"), ("0001000000000000000001000000000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oz67p", "s", "s19"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kb5q", cols3);
		DBApp.createBitMapIndex("kb5q","b");
		DBApp.createBitMapIndex("kb5q","d");
		DBApp.createBitMapIndex("kb5q","e");
		DBApp.createBitMapIndex("kb5q","f");
		DBApp.createBitMapIndex("kb5q","g");
		String [][] records_kb5q = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			records_kb5q[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kb5q[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kb5q", records_kb5q[i]);
		}
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kb5q", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kb5q", "d", "d2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kb5q", "e", "e1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kb5q", "f", "f4"), ("000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kb5q", "g", "g7"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w8", cols0);
		DBApp.createBitMapIndex("w8","d");
		DBApp.createBitMapIndex("w8","g");
		String [][] records_w8 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			records_w8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w8", records_w8[i]);
		}
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w8", "d", "d0"), ("100010001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w8", "g", "g7"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r7212", cols1);
		DBApp.createBitMapIndex("r7212","b");
		DBApp.createBitMapIndex("r7212","e");
		DBApp.createBitMapIndex("r7212","f");
		DBApp.createBitMapIndex("r7212","g");
		String [][] records_r7212 = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_r7212[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r7212[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r7212", records_r7212[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7212", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7212", "e", "e0"), ("100001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7212", "f", "f0"), ("100000100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r7212", "g", "g7"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("toz", cols2);
		DBApp.createBitMapIndex("toz","c");
		DBApp.createBitMapIndex("toz","g");
		DBApp.createBitMapIndex("toz","h");
		DBApp.createBitMapIndex("toz","i");
		DBApp.createBitMapIndex("toz","j");
		DBApp.createBitMapIndex("toz","k");
		DBApp.createBitMapIndex("toz","l");
		String [][] records_toz = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			records_toz[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_toz[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("toz", records_toz[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "g", "g4"), ("0000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "h", "h2"), ("0010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "i", "i7"), ("0000000100000000100000000100000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "j", "j1"), ("0100000000010000000001000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("toz", "k", "k9"), ("0000000001000000000010000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("toz", "l", "l12"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f31t", cols0);
		DBApp.createBitMapIndex("f31t","b");
		DBApp.createBitMapIndex("f31t","d");
		DBApp.createBitMapIndex("f31t","g");
		DBApp.createBitMapIndex("f31t","j");
		DBApp.createBitMapIndex("f31t","k");
		String [][] records_f31t = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_f31t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f31t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f31t", records_f31t[i]);
		}
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f31t", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f31t", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f31t", "g", "g3"), ("0001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f31t", "j", "j3"), ("0001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f31t", "k", "k11"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oj", cols1);
		DBApp.createBitMapIndex("oj","c");
		DBApp.createBitMapIndex("oj","e");
		DBApp.createBitMapIndex("oj","f");
		DBApp.createBitMapIndex("oj","g");
		String [][] records_oj = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			records_oj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_oj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("oj", records_oj[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oj", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oj", "e", "e1"), ("010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oj", "f", "f3"), ("000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oj", "g", "g7"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("tb", cols0);
		DBApp.createBitMapIndex("tb","e");
		DBApp.createBitMapIndex("tb","f");
		DBApp.createBitMapIndex("tb","g");
		DBApp.createBitMapIndex("tb","h");
		DBApp.createBitMapIndex("tb","m");
		DBApp.createBitMapIndex("tb","q");
		String [][] records_tb = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_tb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tb", records_tb[i]);
		}
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tb", "e", "e2"), ("0010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tb", "f", "f2"), ("0010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tb", "g", "g1"), ("0100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tb", "h", "h0"), ("1000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tb", "m", "m3"), ("0001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tb", "q", "q17"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b0b3z", cols1);
		DBApp.createBitMapIndex("b0b3z","b");
		DBApp.createBitMapIndex("b0b3z","c");
		DBApp.createBitMapIndex("b0b3z","d");
		DBApp.createBitMapIndex("b0b3z","g");
		String [][] records_b0b3z = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			records_b0b3z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b0b3z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b0b3z", records_b0b3z[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b0b3z", "b", "b0"), ("1010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b0b3z", "c", "c1"), ("0100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b0b3z", "d", "d0"), ("1000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b0b3z", "g", "g7"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("m29", cols2);
		DBApp.createBitMapIndex("m29","e");
		DBApp.createBitMapIndex("m29","f");
		DBApp.createBitMapIndex("m29","g");
		DBApp.createBitMapIndex("m29","h");
		DBApp.createBitMapIndex("m29","j");
		DBApp.createBitMapIndex("m29","k");
		String [][] records_m29 = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			records_m29[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m29[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m29", records_m29[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m29", "e", "e2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m29", "f", "f2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m29", "g", "g5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m29", "h", "h3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m29", "j", "j7"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m29", "k", "k11"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dq", cols3);
		DBApp.createBitMapIndex("dq","c");
		DBApp.createBitMapIndex("dq","e");
		DBApp.createBitMapIndex("dq","f");
		DBApp.createBitMapIndex("dq","g");
		String [][] records_dq = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_dq[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_dq[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("dq", records_dq[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dq", "c", "c1"), ("0100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dq", "e", "e3"), ("0001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dq", "f", "f3"), ("0001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dq", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tic", cols0);
		DBApp.createBitMapIndex("tic","c");
		DBApp.createBitMapIndex("tic","d");
		DBApp.createBitMapIndex("tic","e");
		DBApp.createBitMapIndex("tic","g");
		String [][] records_tic = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			records_tic[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tic[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tic", records_tic[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tic", "c", "c1"), ("01001001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tic", "d", "d2"), ("00100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tic", "e", "e1"), ("01000010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tic", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v3g7", cols0);
		DBApp.createBitMapIndex("v3g7","b");
		DBApp.createBitMapIndex("v3g7","d");
		DBApp.createBitMapIndex("v3g7","f");
		DBApp.createBitMapIndex("v3g7","g");
		DBApp.createBitMapIndex("v3g7","i");
		DBApp.createBitMapIndex("v3g7","j");
		String [][] records_v3g7 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			records_v3g7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v3g7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v3g7", records_v3g7[i]);
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
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v3g7", "b", "b0"), ("10101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v3g7", "d", "d1"), ("01000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v3g7", "f", "f0"), ("10000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v3g7", "g", "g3"), ("00010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v3g7", "i", "i1"), ("01000000001000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v3g7", "j", "j10"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c3d3w", cols1);
		DBApp.createBitMapIndex("c3d3w","b");
		DBApp.createBitMapIndex("c3d3w","c");
		DBApp.createBitMapIndex("c3d3w","d");
		DBApp.createBitMapIndex("c3d3w","f");
		DBApp.createBitMapIndex("c3d3w","g");
		String [][] records_c3d3w = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_c3d3w[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c3d3w[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c3d3w", records_c3d3w[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c3d3w", "b", "b0"), ("101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c3d3w", "c", "c1"), ("010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c3d3w", "d", "d2"), ("001000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c3d3w", "f", "f3"), ("000100000100000100000100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c3d3w", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("z14", cols2);
		DBApp.createBitMapIndex("z14","b");
		DBApp.createBitMapIndex("z14","d");
		DBApp.createBitMapIndex("z14","e");
		DBApp.createBitMapIndex("z14","f");
		DBApp.createBitMapIndex("z14","g");
		DBApp.createBitMapIndex("z14","h");
		DBApp.createBitMapIndex("z14","j");
		DBApp.createBitMapIndex("z14","k");
		DBApp.createBitMapIndex("z14","l");
		DBApp.createBitMapIndex("z14","m");
		DBApp.createBitMapIndex("z14","n");
		DBApp.createBitMapIndex("z14","q");
		DBApp.createBitMapIndex("z14","t");
		String [][] records_z14 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_z14[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z14[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z14", records_z14[i]);
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
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5, s4, t3]
		//record24: [a24, b0, c0, d0, e4, f0, g3, h0, i6, j4, k2, l0, m11, n10, o9, p8, q7, r6, s5, t4]
		//record25: [a25, b1, c1, d1, e0, f1, g4, h1, i7, j5, k3, l1, m12, n11, o10, p9, q8, r7, s6, t5]
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8, s7, t6]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8, t7]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9, t8]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11, s10, t9]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12, s11, t10]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13, s12, t11]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14, s13, t12]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15, s14, t13]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16, s15, t14]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17, s16, t15]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0, s17, t16]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18, t17]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "b", "b0"), ("10101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "d", "d2"), ("00100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "e", "e1"), ("01000010000100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "f", "f4"), ("00001000001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "g", "g2"), ("00100000010000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "h", "h0"), ("10000000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "j", "j4"), ("00001000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "k", "k9"), ("00000000010000000000100000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "l", "l7"), ("00000001000000000001000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "m", "m7"), ("00000001000000000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "n", "n11"), ("00000000000100000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z14", "q", "q7"), ("00000001000000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z14", "t", "t20"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x19l0", cols3);
		DBApp.createBitMapIndex("x19l0","b");
		DBApp.createBitMapIndex("x19l0","c");
		DBApp.createBitMapIndex("x19l0","d");
		DBApp.createBitMapIndex("x19l0","f");
		DBApp.createBitMapIndex("x19l0","g");
		String [][] records_x19l0 = new String[23][cols3.length];
		for(int i=0;i<23;i++)
		{
			records_x19l0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x19l0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x19l0", records_x19l0[i]);
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
		//record22: [a22, b0, c1, d2, e2, f4, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x19l0", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x19l0", "c", "c1"), ("01001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x19l0", "d", "d1"), ("01000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("x19l0", "f", "f0"), ("10000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("x19l0", "g", "g7"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ht49", cols0);
		DBApp.createBitMapIndex("ht49","c");
		DBApp.createBitMapIndex("ht49","e");
		DBApp.createBitMapIndex("ht49","g");
		String [][] records_ht49 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			records_ht49[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ht49[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ht49", records_ht49[i]);
		}
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ht49", "c", "c0"), ("100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ht49", "e", "e3"), ("000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ht49", "g", "g7"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("sa3", cols1);
		DBApp.createBitMapIndex("sa3","b");
		DBApp.createBitMapIndex("sa3","d");
		DBApp.createBitMapIndex("sa3","h");
		DBApp.createBitMapIndex("sa3","i");
		DBApp.createBitMapIndex("sa3","j");
		String [][] records_sa3 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			records_sa3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_sa3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("sa3", records_sa3[i]);
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
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sa3", "b", "b0"), ("1010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sa3", "d", "d1"), ("0100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sa3", "h", "h3"), ("0001000000010000000100000001000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("sa3", "i", "i1"), ("0100000000100000000100000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("sa3", "j", "j10"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xb1", cols2);
		DBApp.createBitMapIndex("xb1","c");
		DBApp.createBitMapIndex("xb1","f");
		DBApp.createBitMapIndex("xb1","g");
		String [][] records_xb1 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			records_xb1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_xb1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("xb1", records_xb1[i]);
		}
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xb1", "c", "c1"), ("01001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xb1", "f", "f2"), ("00100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xb1", "g", "g7"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("gc", cols3);
		DBApp.createBitMapIndex("gc","h");
		DBApp.createBitMapIndex("gc","k");
		DBApp.createBitMapIndex("gc","m");
		String [][] records_gc = new String[30][cols3.length];
		for(int i=0;i<30;i++)
		{
			records_gc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_gc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("gc", records_gc[i]);
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gc", "h", "h6"), ("000000100000001000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gc", "k", "k8"), ("000000001000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gc", "m", "m13"), ("000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wi", cols0);
		DBApp.createBitMapIndex("wi","c");
		DBApp.createBitMapIndex("wi","d");
		DBApp.createBitMapIndex("wi","f");
		DBApp.createBitMapIndex("wi","g");
		DBApp.createBitMapIndex("wi","h");
		DBApp.createBitMapIndex("wi","k");
		String [][] records_wi = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			records_wi[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_wi[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("wi", records_wi[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wi", "c", "c0"), ("1001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wi", "d", "d2"), ("0010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wi", "f", "f1"), ("0100000100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wi", "g", "g1"), ("0100000010000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wi", "h", "h6"), ("0000001000000010000000100000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wi", "k", "k11"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("ulxb", cols0);
		DBApp.createBitMapIndex("ulxb","f");
		DBApp.createBitMapIndex("ulxb","g");
		DBApp.createBitMapIndex("ulxb","i");
		DBApp.createBitMapIndex("ulxb","j");
		DBApp.createBitMapIndex("ulxb","k");
		DBApp.createBitMapIndex("ulxb","l");
		DBApp.createBitMapIndex("ulxb","m");
		DBApp.createBitMapIndex("ulxb","o");
		DBApp.createBitMapIndex("ulxb","q");
		DBApp.createBitMapIndex("ulxb","s");
		String [][] records_ulxb = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_ulxb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ulxb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ulxb", records_ulxb[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "f", "f2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "g", "g0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "i", "i6"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "j", "j2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "k", "k8"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "l", "l2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "m", "m2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "o", "o2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ulxb", "q", "q9"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ulxb", "s", "s19"), ("0"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("lx8", cols1);
		DBApp.createBitMapIndex("lx8","e");
		DBApp.createBitMapIndex("lx8","f");
		DBApp.createBitMapIndex("lx8","g");
		DBApp.createBitMapIndex("lx8","j");
		DBApp.createBitMapIndex("lx8","l");
		DBApp.createBitMapIndex("lx8","m");
		DBApp.createBitMapIndex("lx8","n");
		DBApp.createBitMapIndex("lx8","o");
		DBApp.createBitMapIndex("lx8","p");
		DBApp.createBitMapIndex("lx8","r");
		String [][] records_lx8 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_lx8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lx8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lx8", records_lx8[i]);
		}
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
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2, p0, q15, r14]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3, p1, q16, r15]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "e", "e0"), ("100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "f", "f2"), ("001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "g", "g4"), ("000010000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "j", "j4"), ("000010000000001000000000100000000010000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "l", "l0"), ("100000000000100000000000100000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "m", "m2"), ("001000000000000100000000000010000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "n", "n9"), ("000000000100000000000001000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "o", "o1"), ("010000000000000010000000000000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("lx8", "p", "p12"), ("000000000000100000000000000010000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("lx8", "r", "r18"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("qc5s3", cols2);
		DBApp.createBitMapIndex("qc5s3","b");
		DBApp.createBitMapIndex("qc5s3","d");
		DBApp.createBitMapIndex("qc5s3","f");
		DBApp.createBitMapIndex("qc5s3","g");
		DBApp.createBitMapIndex("qc5s3","h");
		DBApp.createBitMapIndex("qc5s3","j");
		DBApp.createBitMapIndex("qc5s3","l");
		DBApp.createBitMapIndex("qc5s3","m");
		DBApp.createBitMapIndex("qc5s3","o");
		DBApp.createBitMapIndex("qc5s3","r");
		String [][] records_qc5s3 = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			records_qc5s3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_qc5s3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("qc5s3", records_qc5s3[i]);
		}
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
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1, l10, m8, n6, o4, p2, q0, r16]
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7, o5, p3, q1, r17]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8, o6, p4, q2, r0]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10, o8, p6, q4, r2]
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13, p11, q9, r7]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12, q10, r8]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13, q11, r9]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7, n4, o1, p14, q12, r10]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15, q13, r11]
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3, j8, k4, l0, m9, n6, o3, p0, q14, r12]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "b", "b0"), ("1010101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "d", "d1"), ("0100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "f", "f2"), ("0010000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "g", "g1"), ("0100000010000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "h", "h6"), ("0000001000000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "j", "j4"), ("0000100000000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "l", "l6"), ("0000001000000000001000000000001000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "m", "m7"), ("0000000100000000000010000000000001000000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qc5s3", "o", "o9"), ("0000000001000000000000001000000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qc5s3", "r", "r18"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mw99", cols0);
		DBApp.createBitMapIndex("mw99","d");
		DBApp.createBitMapIndex("mw99","f");
		DBApp.createBitMapIndex("mw99","g");
		String [][] records_mw99 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_mw99[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mw99[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mw99", records_mw99[i]);
		}
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mw99", "d", "d2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mw99", "f", "f0"), ("100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mw99", "g", "g7"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("fkd", cols0);
		DBApp.createBitMapIndex("fkd","d");
		DBApp.createBitMapIndex("fkd","e");
		DBApp.createBitMapIndex("fkd","h");
		DBApp.createBitMapIndex("fkd","i");
		DBApp.createBitMapIndex("fkd","j");
		DBApp.createBitMapIndex("fkd","k");
		DBApp.createBitMapIndex("fkd","m");
		DBApp.createBitMapIndex("fkd","n");
		DBApp.createBitMapIndex("fkd","o");
		DBApp.createBitMapIndex("fkd","q");
		DBApp.createBitMapIndex("fkd","r");
		String [][] records_fkd = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_fkd[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fkd[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fkd", records_fkd[i]);
		}
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
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "d", "d0"), ("10001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "e", "e2"), ("00100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "h", "h1"), ("01000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "i", "i6"), ("00000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "j", "j8"), ("00000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "k", "k9"), ("00000000010000000000100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "m", "m7"), ("00000001000000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "n", "n8"), ("00000000100000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "o", "o8"), ("00000000100000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fkd", "q", "q15"), ("00000000000000010000000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fkd", "r", "r18"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("gk", cols1);
		DBApp.createBitMapIndex("gk","b");
		DBApp.createBitMapIndex("gk","d");
		DBApp.createBitMapIndex("gk","f");
		DBApp.createBitMapIndex("gk","g");
		DBApp.createBitMapIndex("gk","i");
		DBApp.createBitMapIndex("gk","l");
		String [][] records_gk = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_gk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gk", records_gk[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gk", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gk", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gk", "f", "f1"), ("0100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gk", "g", "g1"), ("0100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gk", "i", "i5"), ("0000010000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gk", "l", "l12"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b8", cols2);
		DBApp.createBitMapIndex("b8","d");
		DBApp.createBitMapIndex("b8","e");
		DBApp.createBitMapIndex("b8","g");
		String [][] records_b8 = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			records_b8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b8", records_b8[i]);
		}
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//record2: [a2, b0, c2, d2, e2, f2, g2]
		//record3: [a3, b1, c0, d3, e3, f3, g3]
		//record4: [a4, b0, c1, d0, e4, f4, g4]
		//record5: [a5, b1, c2, d1, e0, f5, g5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8", "d", "d1"), ("010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8", "e", "e1"), ("010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b8", "g", "g7"), ("000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fann", cols3);
		DBApp.createBitMapIndex("fann","e");
		DBApp.createBitMapIndex("fann","f");
		DBApp.createBitMapIndex("fann","g");
		DBApp.createBitMapIndex("fann","l");
		String [][] records_fann = new String[40][cols3.length];
		for(int i=0;i<40;i++)
		{
			records_fann[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_fann[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("fann", records_fann[i]);
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
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fann", "e", "e1"), ("0100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fann", "f", "f1"), ("0100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fann", "g", "g4"), ("0000100000010000001000000100000010000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fann", "l", "l12"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k1", cols4);
		DBApp.createBitMapIndex("k1","b");
		DBApp.createBitMapIndex("k1","d");
		DBApp.createBitMapIndex("k1","e");
		DBApp.createBitMapIndex("k1","g");
		DBApp.createBitMapIndex("k1","m");
		DBApp.createBitMapIndex("k1","n");
		String [][] records_k1 = new String[13][cols4.length];
		for(int i=0;i<13;i++)
		{
			records_k1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_k1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("k1", records_k1[i]);
		}
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "d", "d1"), ("0100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "e", "e3"), ("0001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "g", "g1"), ("0100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "m", "m11"), ("0000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k1", "n", "n14"), ("0000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qw49", cols0);
		DBApp.createBitMapIndex("qw49","b");
		DBApp.createBitMapIndex("qw49","f");
		DBApp.createBitMapIndex("qw49","i");
		DBApp.createBitMapIndex("qw49","k");
		DBApp.createBitMapIndex("qw49","l");
		DBApp.createBitMapIndex("qw49","m");
		DBApp.createBitMapIndex("qw49","n");
		String [][] records_qw49 = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			records_qw49[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qw49[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qw49", records_qw49[i]);
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "f", "f3"), ("00010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "i", "i7"), ("00000001000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "k", "k3"), ("00010000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "l", "l5"), ("00000100000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qw49", "m", "m4"), ("00001000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qw49", "n", "n14"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l68", cols1);
		DBApp.createBitMapIndex("l68","c");
		DBApp.createBitMapIndex("l68","e");
		DBApp.createBitMapIndex("l68","f");
		DBApp.createBitMapIndex("l68","g");
		String [][] records_l68 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			records_l68[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l68[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l68", records_l68[i]);
		}
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l68", "c", "c1"), ("01001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l68", "e", "e1"), ("01000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("l68", "f", "f1"), ("01000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("l68", "g", "g7"), ("00000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k2", cols2);
		DBApp.createBitMapIndex("k2","c");
		DBApp.createBitMapIndex("k2","e");
		DBApp.createBitMapIndex("k2","f");
		DBApp.createBitMapIndex("k2","g");
		String [][] records_k2 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_k2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k2", records_k2[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2", "c", "c0"), ("1001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2", "e", "e1"), ("0100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k2", "f", "f1"), ("0100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k2", "g", "g7"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o0p", cols0);
		DBApp.createBitMapIndex("o0p","b");
		DBApp.createBitMapIndex("o0p","f");
		DBApp.createBitMapIndex("o0p","g");
		String [][] records_o0p = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_o0p[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0p[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0p", records_o0p[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0p", "b", "b0"), ("101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o0p", "f", "f2"), ("001000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o0p", "g", "g7"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d9rj4", cols1);
		DBApp.createBitMapIndex("d9rj4","b");
		DBApp.createBitMapIndex("d9rj4","e");
		DBApp.createBitMapIndex("d9rj4","g");
		String [][] records_d9rj4 = new String[12][cols1.length];
		for(int i=0;i<12;i++)
		{
			records_d9rj4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_d9rj4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("d9rj4", records_d9rj4[i]);
		}
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9rj4", "b", "b0"), ("101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("d9rj4", "e", "e3"), ("000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("d9rj4", "g", "g7"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k1", cols2);
		DBApp.createBitMapIndex("k1","c");
		DBApp.createBitMapIndex("k1","e");
		DBApp.createBitMapIndex("k1","g");
		String [][] records_k1 = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			records_k1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k1", records_k1[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "c", "c1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k1", "e", "e1"), ("01"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k1", "g", "g7"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("vun", cols3);
		DBApp.createBitMapIndex("vun","c");
		DBApp.createBitMapIndex("vun","d");
		DBApp.createBitMapIndex("vun","h");
		String [][] records_vun = new String[7][cols3.length];
		for(int i=0;i<7;i++)
		{
			records_vun[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_vun[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("vun", records_vun[i]);
		}
		//first 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6]
		//last 7 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vun", "c", "c1"), ("0100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vun", "d", "d2"), ("0010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vun", "h", "h8"), ("0000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ywm", cols0);
		DBApp.createBitMapIndex("ywm","c");
		DBApp.createBitMapIndex("ywm","e");
		DBApp.createBitMapIndex("ywm","g");
		DBApp.createBitMapIndex("ywm","h");
		DBApp.createBitMapIndex("ywm","j");
		DBApp.createBitMapIndex("ywm","k");
		DBApp.createBitMapIndex("ywm","n");
		DBApp.createBitMapIndex("ywm","o");
		DBApp.createBitMapIndex("ywm","p");
		DBApp.createBitMapIndex("ywm","r");
		String [][] records_ywm = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_ywm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ywm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ywm", records_ywm[i]);
		}
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
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8]
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9, o9, p9, q9, r9]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "c", "c1"), ("0100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "e", "e2"), ("0010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "g", "g5"), ("0000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "h", "h3"), ("0001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "j", "j3"), ("0001000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "k", "k1"), ("0100000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "n", "n12"), ("0000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "o", "o6"), ("0000001000000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ywm", "p", "p10"), ("0000000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ywm", "r", "r18"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tw38", cols0);
		DBApp.createBitMapIndex("tw38","f");
		DBApp.createBitMapIndex("tw38","g");
		String [][] records_tw38 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_tw38[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tw38[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tw38", records_tw38[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tw38", "f", "f0"), ("1000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tw38", "g", "g7"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("i8", cols1);
		DBApp.createBitMapIndex("i8","b");
		DBApp.createBitMapIndex("i8","f");
		DBApp.createBitMapIndex("i8","g");
		DBApp.createBitMapIndex("i8","i");
		DBApp.createBitMapIndex("i8","k");
		DBApp.createBitMapIndex("i8","l");
		DBApp.createBitMapIndex("i8","m");
		DBApp.createBitMapIndex("i8","n");
		DBApp.createBitMapIndex("i8","o");
		DBApp.createBitMapIndex("i8","q");
		DBApp.createBitMapIndex("i8","t");
		String [][] records_i8 = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_i8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i8", records_i8[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "f", "f1"), ("010000010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "g", "g5"), ("000001000000100000010000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "i", "i6"), ("000000100000000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "k", "k2"), ("001000000000010000000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "l", "l5"), ("000001000000000001000000000001000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "m", "m5"), ("000001000000000000100000000000010000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "n", "n9"), ("000000000100000000000001000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "o", "o13"), ("000000000000010000000000000010000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i8", "q", "q3"), ("000100000000000000001000000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i8", "t", "t20"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gx2", cols2);
		DBApp.createBitMapIndex("gx2","b");
		DBApp.createBitMapIndex("gx2","c");
		DBApp.createBitMapIndex("gx2","e");
		DBApp.createBitMapIndex("gx2","f");
		DBApp.createBitMapIndex("gx2","g");
		DBApp.createBitMapIndex("gx2","h");
		String [][] records_gx2 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_gx2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gx2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gx2", records_gx2[i]);
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
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gx2", "b", "b0"), ("1010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gx2", "c", "c0"), ("1001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gx2", "e", "e3"), ("0001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gx2", "f", "f0"), ("1000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gx2", "g", "g4"), ("0000100000010000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gx2", "h", "h8"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("jl", cols3);
		DBApp.createBitMapIndex("jl","b");
		DBApp.createBitMapIndex("jl","e");
		DBApp.createBitMapIndex("jl","h");
		DBApp.createBitMapIndex("jl","j");
		String [][] records_jl = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			records_jl[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jl[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jl", records_jl[i]);
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
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jl", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jl", "e", "e0"), ("100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jl", "h", "h5"), ("000001000000010000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jl", "j", "j10"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jo1", cols4);
		DBApp.createBitMapIndex("jo1","f");
		DBApp.createBitMapIndex("jo1","g");
		String [][] records_jo1 = new String[50][cols4.length];
		for(int i=0;i<50;i++)
		{
			records_jo1[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_jo1[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("jo1", records_jo1[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jo1", "f", "f0"), ("10000010000010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jo1", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("f1", cols0);
		DBApp.createBitMapIndex("f1","b");
		DBApp.createBitMapIndex("f1","c");
		DBApp.createBitMapIndex("f1","d");
		DBApp.createBitMapIndex("f1","e");
		DBApp.createBitMapIndex("f1","f");
		DBApp.createBitMapIndex("f1","i");
		DBApp.createBitMapIndex("f1","j");
		DBApp.createBitMapIndex("f1","k");
		DBApp.createBitMapIndex("f1","l");
		DBApp.createBitMapIndex("f1","m");
		DBApp.createBitMapIndex("f1","o");
		DBApp.createBitMapIndex("f1","p");
		String [][] records_f1 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_f1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_f1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("f1", records_f1[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "c", "c1"), ("010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "d", "d0"), ("100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "e", "e1"), ("010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "f", "f3"), ("000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "i", "i7"), ("000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "j", "j4"), ("000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "k", "k1"), ("010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "l", "l2"), ("001000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "m", "m6"), ("000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f1", "o", "o8"), ("000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f1", "p", "p16"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("xavi", cols1);
		DBApp.createBitMapIndex("xavi","b");
		DBApp.createBitMapIndex("xavi","c");
		DBApp.createBitMapIndex("xavi","e");
		DBApp.createBitMapIndex("xavi","h");
		DBApp.createBitMapIndex("xavi","j");
		DBApp.createBitMapIndex("xavi","m");
		DBApp.createBitMapIndex("xavi","n");
		DBApp.createBitMapIndex("xavi","o");
		String [][] records_xavi = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			records_xavi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xavi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xavi", records_xavi[i]);
		}
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
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "b", "b0"), ("1010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "c", "c0"), ("1001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "e", "e3"), ("0001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "h", "h5"), ("0000010000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "j", "j0"), ("1000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "m", "m3"), ("0001000000000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xavi", "n", "n4"), ("0000100000000000001000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xavi", "o", "o15"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m32", cols0);
		DBApp.createBitMapIndex("m32","b");
		DBApp.createBitMapIndex("m32","c");
		DBApp.createBitMapIndex("m32","f");
		DBApp.createBitMapIndex("m32","g");
		String [][] records_m32 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_m32[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m32[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m32", records_m32[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m32", "b", "b0"), ("101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m32", "c", "c1"), ("010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m32", "f", "f2"), ("001000001000001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m32", "g", "g7"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("op928", cols1);
		DBApp.createBitMapIndex("op928","b");
		DBApp.createBitMapIndex("op928","c");
		DBApp.createBitMapIndex("op928","d");
		DBApp.createBitMapIndex("op928","e");
		DBApp.createBitMapIndex("op928","i");
		DBApp.createBitMapIndex("op928","j");
		DBApp.createBitMapIndex("op928","k");
		DBApp.createBitMapIndex("op928","l");
		String [][] records_op928 = new String[18][cols1.length];
		for(int i=0;i<18;i++)
		{
			records_op928[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_op928[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("op928", records_op928[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "c", "c0"), ("100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "d", "d2"), ("001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "e", "e2"), ("001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "i", "i4"), ("000010000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "j", "j6"), ("000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("op928", "k", "k6"), ("000000100000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("op928", "l", "l12"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("hjyb", cols2);
		DBApp.createBitMapIndex("hjyb","e");
		DBApp.createBitMapIndex("hjyb","g");
		DBApp.createBitMapIndex("hjyb","i");
		DBApp.createBitMapIndex("hjyb","j");
		DBApp.createBitMapIndex("hjyb","k");
		DBApp.createBitMapIndex("hjyb","n");
		DBApp.createBitMapIndex("hjyb","o");
		String [][] records_hjyb = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			records_hjyb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hjyb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hjyb", records_hjyb[i]);
		}
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "e", "e2"), ("00100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "g", "g0"), ("10000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "i", "i7"), ("00000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "j", "j0"), ("10000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "k", "k7"), ("00000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hjyb", "n", "n0"), ("10000000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hjyb", "o", "o15"), ("00000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("r7e", cols0);
		DBApp.createBitMapIndex("r7e","b");
		DBApp.createBitMapIndex("r7e","c");
		DBApp.createBitMapIndex("r7e","d");
		DBApp.createBitMapIndex("r7e","e");
		DBApp.createBitMapIndex("r7e","f");
		DBApp.createBitMapIndex("r7e","g");
		DBApp.createBitMapIndex("r7e","j");
		DBApp.createBitMapIndex("r7e","k");
		String [][] records_r7e = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_r7e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_r7e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("r7e", records_r7e[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "c", "c1"), ("0100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "d", "d2"), ("0010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "e", "e2"), ("0010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "f", "f0"), ("1000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "g", "g0"), ("1000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r7e", "j", "j1"), ("0100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r7e", "k", "k11"), ("0000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("gwn0e", cols0);
		DBApp.createBitMapIndex("gwn0e","d");
		DBApp.createBitMapIndex("gwn0e","e");
		DBApp.createBitMapIndex("gwn0e","f");
		DBApp.createBitMapIndex("gwn0e","h");
		DBApp.createBitMapIndex("gwn0e","i");
		DBApp.createBitMapIndex("gwn0e","k");
		DBApp.createBitMapIndex("gwn0e","l");
		DBApp.createBitMapIndex("gwn0e","n");
		DBApp.createBitMapIndex("gwn0e","p");
		DBApp.createBitMapIndex("gwn0e","q");
		DBApp.createBitMapIndex("gwn0e","r");
		DBApp.createBitMapIndex("gwn0e","s");
		DBApp.createBitMapIndex("gwn0e","t");
		String [][] records_gwn0e = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			records_gwn0e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gwn0e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gwn0e", records_gwn0e[i]);
		}
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0, t0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "d", "d0"), ("1"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "e", "e3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "f", "f3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "h", "h5"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "i", "i1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "k", "k2"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "l", "l1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "n", "n4"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "p", "p6"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "q", "q1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "r", "r12"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("gwn0e", "s", "s15"), ("0"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("gwn0e", "t", "t20"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yut", cols0);
		DBApp.createBitMapIndex("yut","c");
		DBApp.createBitMapIndex("yut","d");
		DBApp.createBitMapIndex("yut","f");
		DBApp.createBitMapIndex("yut","g");
		String [][] records_yut = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			records_yut[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yut[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yut", records_yut[i]);
		}
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yut", "c", "c0"), ("10010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yut", "d", "d0"), ("10001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yut", "f", "f2"), ("00100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yut", "g", "g7"), ("00000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_55() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("kv", cols0);
		DBApp.createBitMapIndex("kv","b");
		DBApp.createBitMapIndex("kv","e");
		DBApp.createBitMapIndex("kv","g");
		DBApp.createBitMapIndex("kv","h");
		String [][] records_kv = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_kv[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kv[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kv", records_kv[i]);
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv", "e", "e0"), ("1000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("kv", "g", "g5"), ("0000010000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("kv", "h", "h8"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("drb", cols1);
		DBApp.createBitMapIndex("drb","b");
		DBApp.createBitMapIndex("drb","d");
		DBApp.createBitMapIndex("drb","e");
		DBApp.createBitMapIndex("drb","f");
		DBApp.createBitMapIndex("drb","g");
		String [][] records_drb = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			records_drb[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_drb[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("drb", records_drb[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("drb", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("drb", "d", "d0"), ("1000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("drb", "e", "e3"), ("0001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("drb", "f", "f4"), ("0000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("drb", "g", "g7"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("dl", cols2);
		DBApp.createBitMapIndex("dl","b");
		DBApp.createBitMapIndex("dl","c");
		DBApp.createBitMapIndex("dl","d");
		DBApp.createBitMapIndex("dl","e");
		DBApp.createBitMapIndex("dl","f");
		DBApp.createBitMapIndex("dl","g");
		DBApp.createBitMapIndex("dl","i");
		DBApp.createBitMapIndex("dl","j");
		DBApp.createBitMapIndex("dl","k");
		DBApp.createBitMapIndex("dl","n");
		DBApp.createBitMapIndex("dl","o");
		DBApp.createBitMapIndex("dl","p");
		DBApp.createBitMapIndex("dl","q");
		DBApp.createBitMapIndex("dl","r");
		String [][] records_dl = new String[31][cols2.length];
		for(int i=0;i<31;i++)
		{
			records_dl[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_dl[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("dl", records_dl[i]);
		}
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
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13, q12, r11]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14, q13, r12]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "b", "b0"), ("1010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "c", "c1"), ("0100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "d", "d1"), ("0100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "e", "e2"), ("0010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "f", "f0"), ("1000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "g", "g5"), ("0000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "i", "i7"), ("0000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "j", "j1"), ("0100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "k", "k3"), ("0001000000000010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "n", "n11"), ("0000000000010000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "o", "o2"), ("0010000000000000010000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "p", "p2"), ("0010000000000000001000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dl", "q", "q10"), ("0000000000100000000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dl", "r", "r18"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h8y8", cols3);
		DBApp.createBitMapIndex("h8y8","c");
		DBApp.createBitMapIndex("h8y8","d");
		DBApp.createBitMapIndex("h8y8","f");
		DBApp.createBitMapIndex("h8y8","n");
		String [][] records_h8y8 = new String[24][cols3.length];
		for(int i=0;i<24;i++)
		{
			records_h8y8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h8y8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h8y8", records_h8y8[i]);
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
		//record9: [a9, b1, c0, d1, e4, f3, g2, h1, i0, j9, k9, l9, m9, n9]
		//record10: [a10, b0, c1, d2, e0, f4, g3, h2, i1, j0, k10, l10, m10, n10]
		//record11: [a11, b1, c2, d3, e1, f5, g4, h3, i2, j1, k0, l11, m11, n11]
		//record12: [a12, b0, c0, d0, e2, f0, g5, h4, i3, j2, k1, l0, m12, n12]
		//record13: [a13, b1, c1, d1, e3, f1, g6, h5, i4, j3, k2, l1, m0, n13]
		//record14: [a14, b0, c2, d2, e4, f2, g0, h6, i5, j4, k3, l2, m1, n0]
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5]
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h8y8", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h8y8", "d", "d0"), ("100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h8y8", "f", "f0"), ("100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h8y8", "n", "n14"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w3fs3", cols4);
		DBApp.createBitMapIndex("w3fs3","b");
		DBApp.createBitMapIndex("w3fs3","d");
		DBApp.createBitMapIndex("w3fs3","e");
		DBApp.createBitMapIndex("w3fs3","g");
		String [][] records_w3fs3 = new String[40][cols4.length];
		for(int i=0;i<40;i++)
		{
			records_w3fs3[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_w3fs3[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("w3fs3", records_w3fs3[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w3fs3", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w3fs3", "d", "d1"), ("0100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w3fs3", "e", "e2"), ("0010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w3fs3", "g", "g7"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_56() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("e19", cols0);
		DBApp.createBitMapIndex("e19","c");
		DBApp.createBitMapIndex("e19","d");
		DBApp.createBitMapIndex("e19","e");
		DBApp.createBitMapIndex("e19","h");
		DBApp.createBitMapIndex("e19","j");
		DBApp.createBitMapIndex("e19","l");
		DBApp.createBitMapIndex("e19","o");
		DBApp.createBitMapIndex("e19","p");
		String [][] records_e19 = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_e19[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e19[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e19", records_e19[i]);
		}
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "d", "d2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "e", "e3"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "h", "h2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "j", "j7"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "l", "l9"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e19", "o", "o10"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e19", "p", "p16"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("dhtx", cols1);
		DBApp.createBitMapIndex("dhtx","c");
		DBApp.createBitMapIndex("dhtx","e");
		DBApp.createBitMapIndex("dhtx","f");
		DBApp.createBitMapIndex("dhtx","g");
		String [][] records_dhtx = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			records_dhtx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dhtx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dhtx", records_dhtx[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dhtx", "c", "c0"), ("1001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dhtx", "e", "e0"), ("1000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dhtx", "f", "f2"), ("0010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dhtx", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("cr4", cols2);
		DBApp.createBitMapIndex("cr4","b");
		DBApp.createBitMapIndex("cr4","e");
		DBApp.createBitMapIndex("cr4","f");
		DBApp.createBitMapIndex("cr4","g");
		DBApp.createBitMapIndex("cr4","k");
		DBApp.createBitMapIndex("cr4","m");
		DBApp.createBitMapIndex("cr4","t");
		String [][] records_cr4 = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			records_cr4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cr4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cr4", records_cr4[i]);
		}
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "b", "b0"), ("101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "e", "e2"), ("001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "f", "f1"), ("010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "g", "g5"), ("000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "k", "k8"), ("000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("cr4", "m", "m7"), ("000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("cr4", "t", "t20"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("v78e", cols3);
		DBApp.createBitMapIndex("v78e","b");
		DBApp.createBitMapIndex("v78e","d");
		DBApp.createBitMapIndex("v78e","e");
		DBApp.createBitMapIndex("v78e","f");
		DBApp.createBitMapIndex("v78e","n");
		DBApp.createBitMapIndex("v78e","o");
		DBApp.createBitMapIndex("v78e","q");
		String [][] records_v78e = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			records_v78e[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v78e[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v78e", records_v78e[i]);
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
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "b", "b0"), ("101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "d", "d2"), ("001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "e", "e0"), ("100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "f", "f1"), ("010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "n", "n6"), ("000000100000000000001000000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v78e", "o", "o8"), ("000000001000000000000001000000000000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v78e", "q", "q17"), ("000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPostIndexing_57() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b9y61", cols0);
		DBApp.createBitMapIndex("b9y61","b");
		DBApp.createBitMapIndex("b9y61","c");
		DBApp.createBitMapIndex("b9y61","e");
		DBApp.createBitMapIndex("b9y61","g");
		DBApp.createBitMapIndex("b9y61","i");
		DBApp.createBitMapIndex("b9y61","j");
		DBApp.createBitMapIndex("b9y61","l");
		String [][] records_b9y61 = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			records_b9y61[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b9y61[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b9y61", records_b9y61[i]);
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "b", "b0"), ("1010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "c", "c1"), ("0100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "e", "e3"), ("0001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "g", "g3"), ("0001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "i", "i4"), ("0000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b9y61", "j", "j7"), ("0000000100000000010000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b9y61", "l", "l12"), ("0000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i4", cols1);
		DBApp.createBitMapIndex("i4","b");
		DBApp.createBitMapIndex("i4","c");
		DBApp.createBitMapIndex("i4","e");
		DBApp.createBitMapIndex("i4","h");
		DBApp.createBitMapIndex("i4","i");
		DBApp.createBitMapIndex("i4","j");
		DBApp.createBitMapIndex("i4","n");
		DBApp.createBitMapIndex("i4","o");
		String [][] records_i4 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_i4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i4", records_i4[i]);
		}
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
		//record32: [a32, b0, c2, d0, e2, f2, g4, h0, i5, j2, k10, l8, m6, n4, o2]
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0, l9, m7, n5, o3]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "c", "c0"), ("10010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "e", "e2"), ("00100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "h", "h4"), ("00001000000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "i", "i7"), ("00000001000000001000000001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "j", "j7"), ("00000001000000000100000000010000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i4", "n", "n5"), ("00000100000000000001000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i4", "o", "o15"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1895);
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